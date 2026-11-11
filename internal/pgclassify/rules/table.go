package rules

import (
	"fmt"

	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// classifyCreateTable is R-TB-CREATE.
func classifyCreateTable(s engine.Scope) ([]pgcontract.Finding, []pgcontract.Error) {
	create := engine.NewEffect(s, "R-TB-CREATE", "the new table is created empty, so nothing that exists is held for long").OnNone()

	var clauses []engine.Effect
	for _, sub := range s.Statement.Subcommands {
		switch sub.Kind {
		case ir.SubAddColumn, ir.SubAddConstraint:
			// A REFERENCES holds the other table while the key's triggers are added to it.
			for _, ref := range sub.Relations {
				create.Touches = append(create.Touches, s.AddTrigger(s.Resolve(ref)))
			}
		case ir.SubLike:
			clauses = append(clauses, ruleCreateLike(s, sub))
		case ir.SubAddInherit:
			clauses = append(clauses, ruleCreateInherits(s, sub))
		case ir.SubAttachPartition:
			clauses = append(clauses, ruleCreatePartitionOf(s, sub))
		}
	}
	if len(create.Touches) > 0 {
		create.Message += "; each table it references is held at SHARE ROW EXCLUSIVE while the foreign key's triggers are added to it"
	}

	var findings []pgcontract.Finding
	var errors []pgcontract.Error
	for _, e := range append([]engine.Effect{create}, clauses...) {
		finding, clauseErrors := e.Report(s)
		errors = append(errors, clauseErrors...)
		if finding != nil {
			findings = append(findings, *finding)
		}
	}
	return findings, errors
}

// ruleCreateLike is R-TB-CREATE-LIKE.
func ruleCreateLike(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-TB-CREATE-LIKE", fmt.Sprintf("the column definitions are copied from %s, which is only read", sub.Name)).OnNone()
	for _, ref := range sub.Relations {
		e.Touches = append(e.Touches, engine.Touch{
			Info: s.Resolve(ref),
			Lock: pgcontract.LockAccessShare,
			Op:   pgcontract.OpKindNone,
			Role: pgcontract.TargetRoleDirect,
		})
	}
	return e
}

// ruleCreateInherits is R-TB-CREATE-INHERITS.
func ruleCreateInherits(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-TB-CREATE-INHERITS", fmt.Sprintf("the new table starts inheriting from %s, which is held at SHARE UPDATE EXCLUSIVE while the edge is recorded", sub.Name)).OnNone()
	for _, ref := range sub.Relations {
		e.Touches = append(e.Touches, engine.Touch{
			Info: s.Resolve(ref),
			Lock: pgcontract.LockShareUpdateExclusive,
			Op:   pgcontract.OpKindMetadata,
			Role: pgcontract.TargetRoleDirect,
		})
	}
	return e
}

// ruleCreatePartitionOf is R-TB-CREATE-PARTOF.
func ruleCreatePartitionOf(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-TB-CREATE-PARTOF", fmt.Sprintf("%s is held at ACCESS EXCLUSIVE while the new partition is recorded", sub.Name)).OnNone()

	for _, ref := range sub.Relations {
		parent := s.Resolve(ref)
		e.Touches = append(e.Touches, engine.Touch{
			Info: parent,
			Lock: pgcontract.LockAccessExclusive,
			Op:   pgcontract.OpKindMetadata,
			Role: pgcontract.TargetRoleDirect,
		})
		// The new table is empty: only the default partition has rows to re-prove.
		e.Touches = append(e.Touches, s.PartitionAdded(parent)...)
		if _, ok := s.DefaultPartitionOf(parent); ok {
			e.Message += "; the default partition is scanned to prove none of its rows belongs in the new one"
		}
	}
	return e
}

// ruleCreateTableAs is R-TB-CTAS, which covers SELECT … INTO too.
func ruleCreateTableAs(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-TB-CTAS", "every source relation is read in full and the rows are written into the new table; the sources are only held at ACCESS SHARE, but the statement lasts as long as the query does")
	return onSources(e, s)
}

// onSources drops the relation a CREATE TABLE AS makes and keeps the ones its query reads.
func onSources(e engine.Effect, s engine.Scope) engine.Effect {
	e = e.OnNone()
	for _, info := range s.Sources() {
		e.On = append(e.On, engine.RelationTarget{Info: info, Role: pgcontract.TargetRoleDirect})
	}
	return e
}

// ruleDropTable is R-TB-DROP.
func ruleDropTable(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-TB-DROP", "the table is removed from the catalog and its files are unlinked at commit")
	e = e.OnEvery(s)

	for _, info := range s.Relations {
		e.Touches = append(e.Touches, s.ReferencingTables(info)...)
		// the tables on the other side of the table's own foreign keys are held too.
		e.Touches = append(e.Touches, referencedTables(s, info)...)
		for _, parent := range partitionParents(s, info) {
			e.Touches = append(e.Touches, parent)
			e.Touches = append(e.Touches, s.DefaultPartition(parent.Info, pgcontract.OpKindMetadata, engine.Nowhere)...)
		}
		if s.Statement.Flags.Cascade {
			e.Touches = append(e.Touches, viewDependents(s, info)...)
		}
	}
	switch {
	case s.Statement.Flags.Cascade:
		e.Message += "; CASCADE drops every dependent view, foreign key and object with it, so the blast radius is whatever the catalog says depends on it"
	case len(e.Touches) > 0:
		e.Message += "; every table holding a foreign key into it is locked with it, because that key's trigger has to go too"
	}
	return e
}

// referencedTables is the tables this one points at with its own foreign keys, each losing the
// key's triggers. A partition's cloned key keeps its triggers on the parent's side, so it reaches nothing.
func referencedTables(s engine.Scope, info pgcatalog.RelationInfo) []engine.Touch {
	if !info.Exists() {
		return nil
	}
	var targets []engine.Touch
	for _, key := range s.Catalog.ForeignKeysFrom(info.Relation.OID) {
		if key.ParentID != 0 {
			continue
		}
		if referenced, ok := s.RelationByOID(key.ForeignRelID); ok {
			targets = append(targets, s.RemoveTrigger(referenced))
		}
	}
	return targets
}

// partitionParents is the partitioned parent a partition hangs off. Dropping a classic child
// leaves its parent alone.
func partitionParents(s engine.Scope, info pgcatalog.RelationInfo) []engine.Touch {
	if !info.Exists() {
		return nil
	}
	var targets []engine.Touch
	for _, link := range s.Catalog.Parents(info.Key()) {
		if !link.ParentIsPartitioned {
			continue
		}
		parent, ok := s.Catalog.RelationByKey(link.Parent)
		if !ok {
			continue
		}
		targets = append(targets, engine.Touch{
			Info: parent,
			Lock: pgcontract.LockAccessExclusive,
			Op:   pgcontract.OpKindMetadata,
			Role: pgcontract.TargetRoleImplicit,
		})
	}
	return targets
}
