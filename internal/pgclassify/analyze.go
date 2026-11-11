// Package pgclassify turns the statement IR into the locks and findings a migration is graded on.
package pgclassify

import (
	"slices"

	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/validate"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse"
	"apercu-cli/internal/pgparse/ir"
)

// Analyze is the migration pipeline's entry point. It parses every event, classifies the statements
func Analyze(catalog *pgcatalog.Catalog, events []pgcontract.QueryEvent) pgcontract.MigrationAnalysis {
	classifier := NewClassifier(catalog)
	intake := parseEvents(events)

	analysis := pgcontract.MigrationAnalysis{Versions: catalog.VersionRange()}
	for _, source := range intake {
		statement := classifier.NextInCycle(source.statement, source.cycle)
		statement.Cycle = source.cycle
		levelGrading(catalog, statement.Findings)
		analysis.Versions = validate.MigrationVersions(analysis.Versions, source.statement, statement.Errors)
		analysis.Statements = append(analysis.Statements, statement)
	}
	analysis.Envelopes = envelopes(analysis.Statements)
	pruneCoveredUnboundedQueueRisk(analysis.Statements, analysis.Envelopes)
	return analysis
}

// pruneCoveredUnboundedQueueRisk drops findings for unbounded statement lock_timeout on statements that where covered.
func pruneCoveredUnboundedQueueRisk(statements []pgcontract.StatementAnalysis, envelopes []pgcontract.LockEnvelope) {
	covered := map[int]map[pgcontract.Relation]bool{}
	for _, envelope := range envelopes {
		if !envelope.Covered {
			continue
		}
		if covered[envelope.OpenedBy] == nil {
			covered[envelope.OpenedBy] = map[pgcontract.Relation]bool{}
		}
		covered[envelope.OpenedBy][envelope.Relation] = true
	}

	for index, held := range covered {
		findings := statements[index].Findings[:0]
		for _, finding := range statements[index].Findings {
			if finding.Code == validate.QueueRiskCode {
				finding.Targets = slices.DeleteFunc(finding.Targets, func(target pgcontract.Target) bool { return held[target.Relation] })
				if len(finding.Targets) == 0 {
					continue
				}
			}
			findings = append(findings, finding)
		}
		statements[index].Findings = findings
	}
}

// source is one parsed statement with metadata information.
type source struct {
	statement ir.Statement
	cycle     int
}

// parseEvents parse every event into its statements, each one tagged with the cycle it ran under.
func parseEvents(events []pgcontract.QueryEvent) []source {
	var intake []source
	for _, event := range events {
		for _, parsed := range pgparse.Parse(event.SQL) {
			intake = append(intake, source{statement: parsed, cycle: event.Cycle})
		}
	}
	return intake
}

// levelGrading set the finding level for a relation.
func levelGrading(catalog *pgcatalog.Catalog, findings []pgcontract.Finding) {
	if catalog == nil {
		return
	}
	for i, finding := range findings {
		// Handle per-finding decided level
		if finding.Level.Decided() || len(finding.Targets) == 0 {
			continue
		}
		// TODO Handle grading based on prod stats
		created := true
		for _, target := range finding.Targets {
			if !catalog.CreatedByMigration(target.Relation.Name) {
				created = false
				break
			}
		}
		// Handle grading for relation created by the migration
		if created {
			findings[i].Level = pgcontract.LevelLow
		}
	}
}
