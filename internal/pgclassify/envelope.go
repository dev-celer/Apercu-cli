package pgclassify

import (
	"apercu-cli/internal/pgcontract"
)

// envelopes output all LockEnvelope for this migration.
// It extends a lock taken inside a transaction until the end of the transaction. and pinpoint which statement took the lock.
func envelopes(statements []pgcontract.StatementAnalysis) []pgcontract.LockEnvelope {
	var out []pgcontract.LockEnvelope
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
func sameTransaction(previous, next pgcontract.StatementAnalysis) bool {
	if previous.InTransaction {
		// Handle explicit transaction block, BEGIN ... COMMIT.
		return previous.TxnGroup == next.TxnGroup
	}
	if endsTransaction[previous.Command] {
		// A COMMIT or ROLLBACK part-way through a message ends its implicit transaction, the statements after it start another one.
		return false
	}
	// Handle the implicit transaction: one simple Query message or one extended protocol pipeline.
	return previous.Cycle == next.Cycle
}

// endsTransaction are the commands that close the transaction they run in, as applyTransaction treats them.
var endsTransaction = map[pgcontract.Command]bool{
	"COMMIT":              true,
	"ROLLBACK":            true,
	"PREPARE TRANSACTION": true,
}

// groupEnvelopes opens an envelope for every lock a statement takes in its transaction group. The
// envelope runs to the end of the group, since that is where the locks are released.
// The group holds every mode it took on a relation, not the strongest one, so coverage goes through the conflict matrix.
func groupEnvelopes(group []pgcontract.StatementAnalysis, offset int) []pgcontract.LockEnvelope {
	running := map[pgcontract.Relation]pgcontract.LockSet{}
	var out []pgcontract.LockEnvelope
	for i, statement := range group {
		for _, locked := range statement.LockedRelations() {
			already := running[locked.Relation]
			running[locked.Relation] = already.With(locked.Lock)
			out = append(out, pgcontract.LockEnvelope{
				Relation:         locked.Relation,
				Lock:             locked.Lock,
				TxnGroup:         statement.TxnGroup,
				OpenedBy:         offset + i,
				StatementsWindow: len(group) - i,
				Covered:          already.Covers(locked.Lock),
			})
		}
	}
	return out
}
