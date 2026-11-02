package pg_classify

import (
	"fmt"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// classifyCreateTable is R-TB-CREATE.
func classifyCreateTable(s scope) ([]pg_contract.Finding, []pg_contract.Error) {
	create := newEffect(s, "R-TB-CREATE", "the new table is created empty, so nothing that exists is held for long").onNone()

	var clauses []effect
	for _, sub := range s.statement.Subcommands {
		switch sub.Kind {
		case pg_parse.SubAddColumn, pg_parse.SubAddConstraint:
			// A REFERENCES holds the other table while the key's triggers are added to it.
			for _, ref := range sub.Relations {
				create.touches = append(create.touches, s.addTrigger(s.resolve(ref)))
			}
		case pg_parse.SubLike:
			clauses = append(clauses, ruleCreateLike(s, sub))
		case pg_parse.SubAddInherit:
			clauses = append(clauses, ruleCreateInherits(s, sub))
		case pg_parse.SubAttachPartition:
			clauses = append(clauses, ruleCreatePartitionOf(s, sub))
		}
	}
	if len(create.touches) > 0 {
		create.message += "; each table it references is held at SHARE ROW EXCLUSIVE while the foreign key's triggers are added to it"
	}

	var findings []pg_contract.Finding
	var errors []pg_contract.Error
	for _, e := range append([]effect{create}, clauses...) {
		finding, clauseErrors := e.report(s)
		errors = append(errors, clauseErrors...)
		if finding != nil {
			findings = append(findings, *finding)
		}
	}
	return findings, errors
}

// ruleCreateLike is R-TB-CREATE-LIKE.
func ruleCreateLike(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-TB-CREATE-LIKE", fmt.Sprintf("the column definitions are copied from %s, which is only read", sub.Name)).onNone()
	for _, ref := range sub.Relations {
		e.touches = append(e.touches, touch{
			info: s.resolve(ref),
			lock: pg_contract.LockAccessShare,
			op:   pg_contract.OpKindNone,
			role: pg_contract.TargetRoleDirect,
		})
	}
	return e
}

// ruleCreateInherits is R-TB-CREATE-INHERITS.
func ruleCreateInherits(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-TB-CREATE-INHERITS", fmt.Sprintf("the new table starts inheriting from %s, which is held at SHARE UPDATE EXCLUSIVE while the edge is recorded", sub.Name)).onNone()
	for _, ref := range sub.Relations {
		e.touches = append(e.touches, touch{
			info: s.resolve(ref),
			lock: pg_contract.LockShareUpdateExclusive,
			op:   pg_contract.OpKindMetadata,
			role: pg_contract.TargetRoleDirect,
		})
	}
	return e
}

// ruleCreatePartitionOf is R-TB-CREATE-PARTOF.
func ruleCreatePartitionOf(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-TB-CREATE-PARTOF", fmt.Sprintf("%s is held at ACCESS EXCLUSIVE while the new partition is recorded", sub.Name)).onNone()

	for _, ref := range sub.Relations {
		parent := s.resolve(ref)
		e.touches = append(e.touches, touch{
			info: parent,
			lock: pg_contract.LockAccessExclusive,
			op:   pg_contract.OpKindMetadata,
			role: pg_contract.TargetRoleDirect,
		})
		// The new table is empty: only the default partition has rows to re-prove.
		e.touches = append(e.touches, s.partitionAdded(parent)...)
		if _, ok := s.defaultPartitionOf(parent); ok {
			e.message += "; the default partition is scanned to prove none of its rows belongs in the new one"
		}
	}
	return e
}

// ruleCreateTableAs is R-TB-CTAS, which covers SELECT … INTO too.
func ruleCreateTableAs(s scope) effect {
	e := newEffect(s, "R-TB-CTAS", "every source relation is read in full and the rows are written into the new table; the sources are only held at ACCESS SHARE, but the statement lasts as long as the query does")
	return e.onSources(s)
}

// onSources drops the relation a CREATE TABLE AS makes and keeps the ones its query reads.
func (e effect) onSources(s scope) effect {
	e = e.onNone()
	for _, info := range s.sources() {
		e.on = append(e.on, relationTarget{info: info, role: pg_contract.TargetRoleDirect})
	}
	return e
}

// sources is every relation the CREATE TABLE / CREATE VIEW statement names after the one it creates.
func (s scope) sources() []pg_catalog.RelationInfo {
	if len(s.relations) < 2 {
		return nil
	}
	return s.relations[1:]
}

// ruleDropTable is R-TB-DROP.
func ruleDropTable(s scope) effect {
	e := newEffect(s, "R-TB-DROP", "the table is removed from the catalog and its files are unlinked at commit")
	e = e.onEvery(s)

	for _, info := range s.relations {
		e.touches = append(e.touches, s.referencingTables(info)...)
		// the tables on the other side of the table's own foreign keys are held too.
		e.touches = append(e.touches, s.referencedTables(info)...)
		for _, parent := range s.partitionParents(info) {
			e.touches = append(e.touches, parent)
			e.touches = append(e.touches, s.defaultPartition(parent.info, pg_contract.OpKindMetadata, nowhere)...)
		}
		if s.statement.Flags.Cascade {
			e.touches = append(e.touches, s.viewDependents(info)...)
		}
	}
	switch {
	case s.statement.Flags.Cascade:
		e.message += "; CASCADE drops every dependent view, foreign key and object with it, so the blast radius is whatever the catalog says depends on it"
	case len(e.touches) > 0:
		e.message += "; every table holding a foreign key into it is locked with it, because that key's trigger has to go too"
	}
	return e
}

// referencingTables is the tables that hold a foreign key to this relation, each losing the
// key's constraint rows on every one of its partitions. A clone is reached through its original.
func (s scope) referencingTables(info pg_catalog.RelationInfo) []touch {
	if !info.Exists() {
		return nil
	}
	var targets []touch
	for _, key := range s.catalog.ForeignKeysTo(info.Relation.OID) {
		if key.ParentID != 0 {
			continue
		}
		if referencing, ok := s.relationByOID(key.RelID); ok {
			targets = append(targets, s.removeTrigger(referencing))
		}
	}
	return targets
}

// referencedTables is the tables this one points at with its own foreign keys, each losing the
// key's triggers. A partition's cloned key keeps its triggers on the parent's side, so it reaches nothing.
func (s scope) referencedTables(info pg_catalog.RelationInfo) []touch {
	if !info.Exists() {
		return nil
	}
	var targets []touch
	for _, key := range s.catalog.ForeignKeysFrom(info.Relation.OID) {
		if key.ParentID != 0 {
			continue
		}
		if referenced, ok := s.relationByOID(key.ForeignRelID); ok {
			targets = append(targets, s.removeTrigger(referenced))
		}
	}
	return targets
}

// partitionParents is the partitioned parent a partition hangs off. Dropping a classic child
// leaves its parent alone.
func (s scope) partitionParents(info pg_catalog.RelationInfo) []touch {
	if !info.Exists() {
		return nil
	}
	var targets []touch
	for _, edge := range s.catalog.Parents(info.Relation.OID) {
		if !edge.ParentIsPartitioned {
			continue
		}
		rel, ok := s.catalog.ByOID(edge.Parent)
		if !ok {
			continue
		}
		targets = append(targets, touch{
			info: s.infoOf(rel),
			lock: pg_contract.LockAccessExclusive,
			op:   pg_contract.OpKindMetadata,
			role: pg_contract.TargetRoleImplicit,
		})
	}
	return targets
}

// viewDependents is the views and materialized views a CASCADE takes with it.
func (s scope) viewDependents(info pg_catalog.RelationInfo) []touch {
	if !info.Exists() {
		return nil
	}
	var targets []touch
	for _, dep := range s.catalog.ViewDependents(info.Relation.OID) {
		rel, ok := s.catalog.ByOID(dep.DependentRelID)
		if !ok {
			continue
		}
		targets = append(targets, touch{
			info: s.infoOf(rel),
			lock: pg_contract.LockAccessExclusive,
			op:   pg_contract.OpKindMetadata,
			role: pg_contract.TargetRoleImplicit,
		})
	}
	return targets
}

// ruleCreateView is R-OB-CREATEVIEW and R-OB-REPLACEVIEW.
func ruleCreateView(s scope) effect {
	if s.statement.Flags.OrReplace && s.relation().Exists() {
		e := newEffect(s, "R-OB-REPLACEVIEW", "the view definition is replaced, which blocks every query already reading through it until the statement commits")
		for _, info := range s.sources() {
			e.touches = append(e.touches, touch{
				info: info,
				lock: pg_contract.LockAccessShare,
				op:   pg_contract.OpKindNone,
				role: pg_contract.TargetRoleDirect,
			})
		}
		return e
	}

	e := newEffect(s, "R-OB-CREATEVIEW", "the view is only a stored query; the relations it reads are opened to resolve their columns and nothing else")
	return e.onSources(s)
}

// ruleDropView is R-OB-DROPVIEW, which covers a materialized view the same way.
func ruleDropView(s scope) effect {
	e := newEffect(s, "R-OB-DROPVIEW", "the view is removed from the catalog; every query still reading through it waits and then fails")
	e = e.onEvery(s)
	if s.statement.Flags.Cascade {
		for _, info := range s.relations {
			e.touches = append(e.touches, s.viewDependents(info)...)
		}
		e.message += "; CASCADE takes every view built on top of it as well"
	}
	return e
}

// ruleCreateMatView is R-OB-CREATEMV.
func ruleCreateMatView(s scope) effect {
	e := newEffect(s, "R-OB-CREATEMV", "the query is run once and its result is written to disk; the sources are only held at ACCESS SHARE, but the statement lasts as long as the query does")
	if !s.statement.Flags.WithData {
		e.op = pg_contract.OpKindNone
		e.message = "WITH NO DATA records the query and reads nothing; the matview is unscannable until a REFRESH fills it"
	}
	return e.onSources(s)
}

// ruleRefreshMatView is R-OB-REFRESHMV and R-OB-REFRESHMV-CONC.
func ruleRefreshMatView(s scope) effect {
	var e effect
	if s.statement.Flags.Concurrently {
		e = newEffect(s, "R-OB-REFRESHMV-CONC",
			"the new contents are computed into a temporary table and merged row by row, so reads keep working while writes wait; it needs a non-partial, non-expression UNIQUE index and an already-populated matview, and it is slower than the plain form")
	} else {
		e = newEffect(s, "R-OB-REFRESHMV", "the query is re-run and the matview's file is replaced, so nothing can read it until the statement commits")
	}
	e.touches = append(e.touches, s.matViewSources(s.relation())...)

	if s.statement.Flags.Concurrently && !s.statement.Flags.WithData {
		return e.reject("REFRESH MATERIALIZED VIEW CONCURRENTLY … WITH NO DATA is not a valid combination", pg_contract.AnyVersion)
	}
	return e
}

// matViewSources is the relations the stored query reads when it runs again.
func (s scope) matViewSources(info pg_catalog.RelationInfo) []touch {
	if !info.Exists() {
		return nil
	}
	var targets []touch
	for _, dep := range s.catalog.ViewSources(info.Relation.OID) {
		rel, ok := s.catalog.ByOID(dep.ReferencedRelID)
		if !ok {
			continue
		}
		targets = append(targets, touch{
			info: s.infoOf(rel),
			lock: pg_contract.LockAccessShare,
			op:   pg_contract.OpKindNone,
			role: pg_contract.TargetRoleResolved,
		})
	}
	return targets
}

// ruleAlterSequence is R-OB-ALTERSEQ.
func ruleAlterSequence(s scope) effect {
	e := newEffect(s, "R-OB-ALTERSEQ", "the sequence's parameters are rewritten, which blocks every concurrent nextval() until the statement commits")
	if owner, column, ok := s.sequenceOwner(s.relation()); ok {
		e.message += fmt.Sprintf("; the sequence feeds %s.%s, so every insert into that table waits with it", owner, column)
	}
	return e
}

// sequenceOwner is the table and column a sequence draws values for.
func (s scope) sequenceOwner(info pg_catalog.RelationInfo) (string, string, bool) {
	if !info.Exists() {
		return "", "", false
	}
	relID, num, owned := s.catalog.SequenceOwner(info.Relation.OID)
	if !owned {
		return "", "", false
	}
	table, ok := s.catalog.ByOID(relID)
	if !ok {
		return "", "", false
	}
	for _, column := range s.catalog.Columns(relID) {
		if column.Num == num {
			return table.RelationName().String(), column.Name, true
		}
	}
	return table.RelationName().String(), "", true
}

// ruleCreateSequence is R-OB-CREATESEQ.
func ruleCreateSequence(s scope) effect {
	e := newEffect(s, "R-OB-CREATESEQ", "the sequence is created on its own and nothing that exists is locked")
	return e.onNone()
}

// ruleDropSequence is R-OB-DROPSEQ.
func ruleDropSequence(s scope) effect {
	e := newEffect(s, "R-OB-DROPSEQ", "the sequence is removed from the catalog; a concurrent nextval() waits and then fails")
	return e.onEvery(s)
}
