package engines

import (
	"apercu-cli/helper"
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"strings"
)

// Severity thresholds based on the prod row count of the relation whose access
// method degraded. Promote to config later.
const (
	bigTableRows = 1_000_000
	midTableRows = 10_000
)

// scanMethod ranks relation access methods from worst to best. A drop in rank
// between the pre- and post-migration plan is a candidate regression.
type scanMethod int

const (
	scanNone scanMethod = iota
	scanSeq
	scanBitmap
	scanIndex
	scanIndexOnly
)

func (m scanMethod) String() string {
	switch m {
	case scanSeq:
		return "Seq Scan"
	case scanBitmap:
		return "Bitmap Scan"
	case scanIndex:
		return "Index Scan"
	case scanIndexOnly:
		return "Index Only Scan"
	}
	return "unknown"
}

// scanMethodFromNodeType maps an EXPLAIN node type to a scanMethod
func scanMethodFromNodeType(nodeType string) scanMethod {
	switch nodeType {
	case "Seq Scan":
		return scanSeq
	case "Bitmap Heap Scan":
		return scanBitmap
	case "Index Scan":
		return scanIndex
	case "Index Only Scan":
		return scanIndexOnly
	}
	return scanNone
}

type planDetails struct {
	// scans holds the best (highest-ranked) access method seen per relation.
	scans map[helper.FullTableName]scanMethod
	// sortKeys is the set of normalized sort-key signatures present in the plan.
	sortKeys  map[string]bool
	sortCount int
}

// walkPlan visits a plan node and all of its descendants depth-first.
func walkPlan(p *metricshelper.Plan, visit func(*metricshelper.Plan)) {
	visit(p)
	for i := range p.Plans {
		walkPlan(&p.Plans[i], visit)
	}
}

func collectPlanDetails(res *metricshelper.ExplainResult) planDetails {
	details := planDetails{
		scans:    make(map[helper.FullTableName]scanMethod),
		sortKeys: make(map[string]bool),
	}

	walkPlan(&res.Plan, func(p *metricshelper.Plan) {
		if p.RelationName != "" {
			if m := scanMethodFromNodeType(p.NodeType); m != scanNone {
				key := helper.FullTableName{Schema: p.Schema, Table: p.RelationName}
				// Keep the best access method observed for the relation.
				if m > details.scans[key] {
					details.scans[key] = m
				}
			}
		}

		if p.NodeType == "Sort" && len(p.SortKey) > 0 {
			details.sortCount++
			details.sortKeys[strings.Join(p.SortKey, ", ")] = true
		}
	})

	return details
}

// severityForRows grades a scan degradation by the prod size of the relation.
// Relations missing from prod stats default to Medium (conservative).
func severityForRows(rowCount int64, known bool) warning.Level {
	if !known {
		return warning.WarningLevelMedium
	}
	switch {
	case rowCount >= bigTableRows:
		return warning.WarningLevelHigh
	case rowCount >= midTableRows:
		return warning.WarningLevelMedium
	default:
		return warning.WarningLevelLow
	}
}

// analyzePlanRegression compares pre- and post-migration plans and returns the
// detected scan degradation and lost ordering regressions.
func analyzePlanRegression(pre, post *metricshelper.ExplainResult, prodStats metricshelper.DatabaseMetrics) []PlanRegression {
	preFacts := collectPlanDetails(pre)
	postFacts := collectPlanDetails(post)

	regressions := make([]PlanRegression, 0)

	// scan-method degradation per relation present in both plans.
	for rel, preMethod := range preFacts.scans {
		postMethod, ok := postFacts.scans[rel]
		if !ok || postMethod >= preMethod {
			continue
		}

		switch {
		case postMethod == scanSeq && preMethod >= scanIndex:
			// Lost index usage entirely: grade by table size.
			tm, known := prodStats.TablesMetrics[rel]
			regressions = append(regressions, PlanRegression{
				Kind:     PlanScanRegressions,
				Relation: rel.String(),
				Before:   preMethod.String(),
				After:    postMethod.String(),
				Severity: severityForRows(tm.RowCount, known),
			})
		case preMethod == scanIndexOnly && postMethod == scanIndex:
			// Covering capability lost; minor.
			regressions = append(regressions, PlanRegression{
				Kind:     PlanScanRegressions,
				Relation: rel.String(),
				Before:   preMethod.String(),
				After:    postMethod.String(),
				Severity: warning.WarningLevelLow,
			})
		}
	}

	// ordering produced by a new Sort node that was not in the pre plan.
	for key := range postFacts.sortKeys {
		if !preFacts.sortKeys[key] {
			regressions = append(regressions, PlanRegression{
				Kind:     PlanOrderingRegressions,
				Relation: "",
				Before:   "no sort",
				After:    key,
				Severity: warning.WarningLevelMedium,
			})
		}
	}

	return regressions
}

// PlanRegression is a single finding from comparing two query plans.
type PlanRegression struct {
	Kind     PlanRegressionsKind
	Relation string
	Before   string
	After    string
	Severity warning.Level
}

type PlanRegressionsKind string

const (
	PlanScanRegressions     PlanRegressionsKind = "scan_degradation"
	PlanOrderingRegressions PlanRegressionsKind = "lost_ordering"
)

func (r PlanRegression) toWarning() warning.Warning {
	switch r.Kind {
	case PlanScanRegressions:
		return warning.NewExplainPlanScanRegressionWarning(r.Severity, r.Relation, r.Before, r.After)
	case PlanOrderingRegressions:
		return warning.NewExplainPlanOrderingRegressionWarning(r.Severity, r.After)
	}
	return nil
}
