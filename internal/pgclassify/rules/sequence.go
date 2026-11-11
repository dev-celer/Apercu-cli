package rules

import (
	"fmt"

	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/rules/engine"
)

// ruleAlterSequence is R-OB-ALTERSEQ.
func ruleAlterSequence(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-OB-ALTERSEQ", "the sequence's parameters are rewritten, which blocks every concurrent nextval() until the statement commits")
	if owner, column, ok := sequenceOwner(s, s.Relation()); ok {
		e.Message += fmt.Sprintf("; the sequence feeds %s.%s, so every insert into that table waits with it", owner, column)
	}
	return e
}

// sequenceOwner is the table and column a sequence draws values for.
func sequenceOwner(s engine.Scope, info pgcatalog.RelationInfo) (string, string, bool) {
	if !info.Exists() {
		return "", "", false
	}
	relID, num, owned := s.Catalog.SequenceOwner(info.Relation.OID)
	if !owned {
		return "", "", false
	}
	table, ok := s.Catalog.ByOID(relID)
	if !ok {
		return "", "", false
	}
	for _, column := range s.Catalog.Columns(relID) {
		if column.Num == num {
			return table.RelationName().String(), column.Name, true
		}
	}
	return table.RelationName().String(), "", true
}

// ruleCreateSequence is R-OB-CREATESEQ.
func ruleCreateSequence(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-OB-CREATESEQ", "the sequence is created on its own and nothing that exists is locked")
	return e.OnNone()
}

// ruleDropSequence is R-OB-DROPSEQ.
func ruleDropSequence(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-OB-DROPSEQ", "the sequence is removed from the catalog; a concurrent nextval() waits and then fails")
	return e.OnEvery(s)
}
