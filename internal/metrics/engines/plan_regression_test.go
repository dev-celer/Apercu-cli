package engines

import (
	"apercu-cli/helper"
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scanNode builds a single-node plan that scans a relation with the given node type.
func scanNode(nodeType, schema, relation string) *metricshelper.ExplainResult {
	return &metricshelper.ExplainResult{
		Plan: metricshelper.Plan{
			NodeType:     nodeType,
			Schema:       schema,
			RelationName: relation,
		},
	}
}

// sortOver wraps a child relation scan in a Sort node over the given keys.
func sortOver(keys []string, child metricshelper.Plan) *metricshelper.ExplainResult {
	return &metricshelper.ExplainResult{
		Plan: metricshelper.Plan{
			NodeType: "Sort",
			SortKey:  keys,
			Plans:    []metricshelper.Plan{child},
		},
	}
}

func statsWith(schema, table string, rows int64) metricshelper.DatabaseMetrics {
	return metricshelper.DatabaseMetrics{
		TablesMetrics: map[helper.FullTableName]metricshelper.TableMetrics{
			{Schema: schema, Table: table}: {RowCount: rows},
		},
	}
}

func TestAnalyzePlanRegression_ScanDegradationSeverityByRows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rows     int64
		known    bool
		expected warning.Level
	}{
		{"big table is high", bigTableRows, true, warning.WarningLevelHigh},
		{"mid table is medium", midTableRows, true, warning.WarningLevelMedium},
		{"small table is low", 100, true, warning.WarningLevelLow},
		{"unknown table is medium", 0, false, warning.WarningLevelMedium},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pre := scanNode("Index Scan", "public", "users")
			post := scanNode("Seq Scan", "public", "users")

			var stats metricshelper.DatabaseMetrics
			if tt.known {
				stats = statsWith("public", "users", tt.rows)
			}

			regressions := analyzePlanRegression(pre, post, stats)

			require.Len(t, regressions, 1)
			assert.Equal(t, PlanScanRegressions, regressions[0].Kind)
			assert.Equal(t, "Index Scan", regressions[0].Before)
			assert.Equal(t, "Seq Scan", regressions[0].After)
			assert.Equal(t, tt.expected, regressions[0].Severity)
		})
	}
}

func TestAnalyzePlanRegression_IndexOnlyToIndexIsLow(t *testing.T) {
	t.Parallel()

	pre := scanNode("Index Only Scan", "public", "users")
	post := scanNode("Index Scan", "public", "users")

	regressions := analyzePlanRegression(pre, post, statsWith("public", "users", bigTableRows))

	require.Len(t, regressions, 1)
	assert.Equal(t, warning.WarningLevelLow, regressions[0].Severity)
}

func TestAnalyzePlanRegression_ImprovementNotFlagged(t *testing.T) {
	t.Parallel()

	pre := scanNode("Seq Scan", "public", "users")
	post := scanNode("Index Scan", "public", "users")

	regressions := analyzePlanRegression(pre, post, statsWith("public", "users", bigTableRows))

	assert.Empty(t, regressions)
}

func TestAnalyzePlanRegression_IdenticalPlanNoRegression(t *testing.T) {
	t.Parallel()

	pre := scanNode("Index Scan", "public", "users")
	post := scanNode("Index Scan", "public", "users")

	regressions := analyzePlanRegression(pre, post, statsWith("public", "users", bigTableRows))

	assert.Empty(t, regressions)
}

func TestAnalyzePlanRegression_NewSortIsLostOrdering(t *testing.T) {
	t.Parallel()

	// Pre: ordering came from an index scan, no Sort node.
	pre := scanNode("Index Scan", "public", "users")
	// Post: a Sort node appears over the same relation scan.
	post := sortOver([]string{"users.created_at"}, metricshelper.Plan{
		NodeType:     "Seq Scan",
		Schema:       "public",
		RelationName: "users",
	})

	regressions := analyzePlanRegression(pre, post, statsWith("public", "users", 100))

	// Expect both a scan degradation (index->seq) and a lost-ordering finding.
	var kinds []PlanRegressionsKind
	for _, r := range regressions {
		kinds = append(kinds, r.Kind)
	}
	assert.Contains(t, kinds, PlanScanRegressions)
	assert.Contains(t, kinds, PlanOrderingRegressions)

	for _, r := range regressions {
		if r.Kind == PlanOrderingRegressions {
			assert.Equal(t, warning.WarningLevelMedium, r.Severity)
		}
	}
}

func TestAnalyzePlanRegression_RelationOnlyInPostIgnored(t *testing.T) {
	t.Parallel()

	// A relation that only appears post-migration cannot be a degradation
	// (nothing to compare against).
	pre := scanNode("Index Scan", "public", "users")
	post := scanNode("Seq Scan", "public", "orders")

	regressions := analyzePlanRegression(pre, post, statsWith("public", "orders", bigTableRows))

	assert.Empty(t, regressions)
}
