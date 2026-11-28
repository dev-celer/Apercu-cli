package shadow

import (
	"strings"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// A serial or identity column owns the sequence it draws from, which always lands in its table's schema: it follows
// the column through a rename, goes with the column, the identity or the table, and moves with the table. A partition
// owns none, its parent's serves it.

// addSequence records the sequence a serial or identity column definition builds.
func (m *Checks) addSequence(s Scope, table pgcatalog.RelationInfo, def *ir.ColumnDef) {
	switch {
	case def == nil || def.Name == "":
	case def.Generated.IsIdentity():
		m.ownSequence(s, table, def.Name, def.SequenceName, true)
	case isSerial(def.Type):
		m.ownSequence(s, table, def.Name, "", false)
	}
}

// ownSequence records a sequence a column owns, under the name an identity's SEQUENCE NAME gives it or the one the
// server's ChooseRelationName picks: <table>_<column>_seq, numbered past every relation of the schema.
func (m *Checks) ownSequence(s Scope, table pgcatalog.RelationInfo, column, name string, identity bool) {
	if name == "" {
		name = chooseName(table.Name.Table, column, "seq", func(name string) bool {
			return s.State.Holds(helper.FullRelationName{Schema: table.Name.Schema, Table: name})
		})
	} else if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
		name = name[dot+1:]
	}
	sequence := m.recorder.Declare(helper.FullRelationName{Schema: table.Name.Schema, Table: name}, pgcontract.RelationKindSequence.Relkind())
	m.recorder.SetSequenceOwner(sequence, pgcatalog.SequenceOwner{Table: table.Key(), Column: column, Identity: identity})
}

// likeIdentities gives a table built LIKE another INCLUDING IDENTITY a sequence of its own for each identity column.
func (m *Checks) likeIdentities(s Scope, table, source pgcatalog.RelationInfo) {
	if !source.Exists() {
		return
	}
	for _, sequence := range s.State.SequencesOwnedBy(source.Key()) {
		if owner, ok := s.State.SequenceOwner(sequence); ok && owner.Identity {
			m.ownSequence(s, table, owner.Column, "", true)
		}
	}
}

// dropSequences drops the sequences a column of the table owns, its identity's alone when identityOnly.
func (m *Checks) dropSequences(s Scope, table pgcatalog.RelationInfo, column string, identityOnly bool) {
	for _, sequence := range s.State.SequencesOwnedBy(table.Key()) {
		owner, ok := s.State.SequenceOwner(sequence)
		if ok && owner.Column == column && (owner.Identity || !identityOnly) {
			m.recorder.Drop(sequence)
		}
	}
}

// ownedBy records the owner a CREATE SEQUENCE or ALTER SEQUENCE … OWNED BY gives the sequence: a column of a table
// of its schema, or none.
func (m *Checks) ownedBy(s Scope) {
	if len(s.Statement.Relations) == 0 {
		return
	}
	value, found := optionValue(s.Statement.Options, "owned_by")
	if !found {
		return
	}
	sequence := s.Resolve(s.Statement.Relations[0])
	if !sequence.Exists() {
		return
	}
	if strings.EqualFold(value, "none") {
		m.recorder.SetSequenceOwner(sequence.Key(), pgcatalog.SequenceOwner{})
		return
	}
	dot := strings.LastIndexByte(value, '.')
	if dot < 0 {
		return
	}
	name := helper.FullRelationName{Table: value[:dot]}
	if schema, table, ok := strings.Cut(name.Table, "."); ok {
		name = helper.FullRelationName{Schema: schema, Table: table}
	}
	table := s.Resolve(ir.RelationRef{Name: name})
	if !table.Exists() {
		return
	}
	m.recorder.SetSequenceOwner(sequence.Key(), pgcatalog.SequenceOwner{Table: table.Key(), Column: value[dot+1:]})
}

// optionValue is the value of an unqualified option, the last one when the list names it twice, and whether it does.
func optionValue(options []ir.Option, name string) (string, bool) {
	value, found := "", false
	for _, option := range options {
		if option.Namespace == "" && strings.EqualFold(option.Name, name) {
			value, found = option.Value, true
		}
	}
	return value, found
}
