package pg_classify

import (
	"time"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// Analyze is the migration pipeline's entry point. It parses every event, classifies the statements
func Analyze(catalog *pg_catalog.Catalog, events []pg_contract.QueryEvent) pg_contract.MigrationAnalysis {
	classifier := NewClassifier(catalog)

	analysis := pg_contract.MigrationAnalysis{Versions: catalog.VersionRange()}
	for index, event := range events {
		for _, parsed := range pg_parse.Parse(event.SQL) {
			statement := classifier.Next(parsed)
			statement.Cycle = event.Cycle
			statement.Event = index
			statement.Duration = event.Duration
			levelGrading(catalog, statement.Findings)
			analysis.Versions = migrationVersions(analysis.Versions, parsed, statement.Errors)
			analysis.Statements = append(analysis.Statements, statement)
		}
	}
	analysis.Envelopes = envelopes(analysis.Statements)
	return analysis
}

// levelGrading set the finding level for a relation.
func levelGrading(catalog *pg_catalog.Catalog, findings []pg_contract.Finding) {
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
			findings[i].Level = pg_contract.LevelLow
		}
	}
}

// envelopes output all LockEnvelope for this migration.
// It extends a lock taken inside a transaction until the end of the transaction. and pinpoint which statement took the lock.
func envelopes(statements []pg_contract.StatementAnalysis) []pg_contract.LockEnvelope {
	var out []pg_contract.LockEnvelope
	for start := 0; start < len(statements); {
		end := start + 1
		for end < len(statements) && sameTransaction(statements[end-1], statements[end]) {
			end++
		}
		if end-start > 1 {
			out = append(out, groupEnvelopes(statements[start:end], start)...)
		}
		start = end
	}
	return out
}

// sameTransaction reports whether two statements are in the same transaction explicit or implicit.
func sameTransaction(previous, next pg_contract.StatementAnalysis) bool {
	if previous.InTransaction {
		// Handle explicit transaction block, BEGIN ... COMMIT.
		return previous.TxnGroup == next.TxnGroup
	}
	// Handle the implicit transaction: one simple Query message or one extended protocol pipeline.
	return previous.Cycle == next.Cycle
}

// groupEnvelopes opens an envelope for every lock a statement takes in its transaction group. The
// window runs to the end of the group, since that is where the locks are released.
func groupEnvelopes(group []pg_contract.StatementAnalysis, offset int) []pg_contract.LockEnvelope {
	held := make([]time.Duration, len(group)+1)
	for i := len(group) - 1; i >= 0; i-- {
		held[i] = held[i+1]
		if i == len(group)-1 || group[i].Event != group[i+1].Event {
			held[i] += group[i].Duration
		}
	}

	running := map[pg_contract.Relation]pg_contract.Lock{}
	var out []pg_contract.LockEnvelope
	for i, statement := range group {
		for _, locked := range statement.LockedRelations() {
			already := running[locked.Relation]
			running[locked.Relation] = pg_contract.MaxLock(already, locked.Lock)
			out = append(out, pg_contract.LockEnvelope{
				Relation:       locked.Relation,
				Lock:           locked.Lock,
				TxnGroup:       statement.TxnGroup,
				OpenedBy:       offset + i,
				BlockingWindow: held[i],
				Statements:     len(group) - i,
				Covered:        locked.Lock <= already,
			})
		}
	}
	return out
}
