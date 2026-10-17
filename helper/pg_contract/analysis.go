package pg_contract

import (
	"apercu-cli/helper"
	"apercu-cli/helper/warning_interface"
	"fmt"
	"sort"
)

// TxnGroup numbers the transaction a statement belongs to.
type TxnGroup int

// StatementAnalysis is the parser's record for one statement.
type StatementAnalysis struct {
	RawSQL   string   `json:"raw_sql" yaml:"raw_sql"`
	TxnGroup TxnGroup `json:"txn_group" yaml:"txn_group"`
	// InTransaction mark the statement in an explicit transaction
	InTransaction bool `json:"in_transaction" yaml:"in_transaction"`
	// Cycle is the protocol cycle the statement ran under.
	Cycle       int                         `json:"cycle" yaml:"cycle"`
	Command     Command                     `json:"command" yaml:"command"`
	Subcommands []string                    `json:"subcommands,omitempty" yaml:"subcommands,omitempty"`
	Findings    []Finding                   `json:"findings,omitempty" yaml:"findings,omitempty"`
	Warnings    []warning_interface.Warning `json:"warnings,omitempty" yaml:"warnings,omitempty"`
	Errors      []Error                     `json:"errors,omitempty" yaml:"errors,omitempty"`
}

// MaxLock is the strongest lock the statement takes on any relation. A statement
// holds every lock it takes until it commits, so this is what the user waits on.
func (s StatementAnalysis) MaxLock() Lock {
	strongest := LockNone
	for _, f := range s.Findings {
		strongest = MaxLock(strongest, f.MaxLock())
	}
	return strongest
}

// MaxOpKind is the most severe operation the statement performs on any relation.
func (s StatementAnalysis) MaxOpKind() OpKind {
	worst := OpKindNone
	for _, f := range s.Findings {
		worst = MaxOpKind(worst, f.MaxOpKind())
	}
	return worst
}

// RelationLock is the strongest lock a statement takes on one relation.
type RelationLock struct {
	Relation Relation `json:"relation" yaml:"relation"`
	Lock     Lock     `json:"lock" yaml:"lock"`
}

// LockedRelations lists every relation the statement locks and the strongest lock it takes on it.
func (s StatementAnalysis) LockedRelations() []RelationLock {
	strongest := map[Relation]Lock{}
	for _, f := range s.Findings {
		for _, t := range f.Targets {
			strongest[t.Relation] = MaxLock(strongest[t.Relation], t.Lock)
		}
	}

	out := make([]RelationLock, 0, len(strongest))
	for relation, lock := range strongest {
		if lock == LockNone {
			continue
		}
		out = append(out, RelationLock{Relation: relation, Lock: lock})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Relation.Name.String() < out[j].Relation.Name.String() })
	return out
}

// LockOn is the strongest lock the statement takes on one relation, LockNone if
// it never touches it.
func (s StatementAnalysis) LockOn(relation helper.FullRelationName) Lock {
	strongest := LockNone
	for _, f := range s.Findings {
		for _, t := range f.Targets {
			if t.Relation.Name == relation {
				strongest = MaxLock(strongest, t.Lock)
			}
		}
	}
	return strongest
}

// HasErrors reports whether the statement produced any error.
func (s StatementAnalysis) HasErrors() bool {
	return len(s.Errors) > 0
}

// LockEnvelope describe a lock taken during a transaction, until this transaction is closed.
type LockEnvelope struct {
	Relation Relation `json:"relation" yaml:"relation"`
	Lock     Lock     `json:"lock" yaml:"lock"`
	TxnGroup TxnGroup `json:"txn_group" yaml:"txn_group"`
	// OpenedBy is the position, in the migration's statement list, of the statement that took this lock.
	OpenedBy int `json:"opened_by" yaml:"opened_by"`
	// StatementsWindow is how many statements the lock remains held over, from the statement that took it until the
	// transaction is closed, the raising one included.
	StatementsWindow int `json:"statements_window" yaml:"statements_window"`
	// Covered says the transaction already held this lock, or a stronger one for this relation.
	Covered bool `json:"covered" yaml:"covered"`
}

func (e LockEnvelope) String() string {
	covered := ""
	if e.Covered {
		covered = ", covered"
	}
	return fmt.Sprintf("%s %s over %d statement(s)%s", e.Relation, e.Lock.Short(), e.StatementsWindow, covered)
}

// EffectiveEnvelopes keeps the envelopes that raised what a transaction was blocking, dropping the ones a lock already held covers.
func EffectiveEnvelopes(envelopes []LockEnvelope) []LockEnvelope {
	var out []LockEnvelope
	for _, envelope := range envelopes {
		if !envelope.Covered {
			out = append(out, envelope)
		}
	}
	return out
}

// MigrationAnalysis is the parser's record for a whole migration.
type MigrationAnalysis struct {
	Statements []StatementAnalysis `json:"statements" yaml:"statements"`
	Envelopes  []LockEnvelope      `json:"envelopes,omitempty" yaml:"envelopes,omitempty"`
	// Versions is the server version the migration can run on.
	// Fixed if the version is known from prod else, narrowed from features used.
	Versions VersionRange `json:"versions" yaml:"versions"`
}

// Errors is every error the migration produced, in statement order. They are reported and never
// block approval, so the roll-up that presents them needs them flattened.
func (m MigrationAnalysis) Errors() []Error {
	var errors []Error
	for _, statement := range m.Statements {
		errors = append(errors, statement.Errors...)
	}
	return errors
}

// EnvelopesOn return all lock envelope taken against this relation.
func (m MigrationAnalysis) EnvelopesOn(relation helper.FullRelationName) []LockEnvelope {
	var out []LockEnvelope
	for _, envelope := range m.Envelopes {
		if envelope.Relation.Name == relation {
			out = append(out, envelope)
		}
	}
	return out
}
