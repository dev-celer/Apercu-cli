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
	create.lock = pg_contract.LockNone
	create.op = pg_contract.OpKindNone
	create.recursion = parentOnly

	var clauses []effect
	for _, sub := range s.statement.Subcommands {
		switch sub.Kind {
		case pg_parse.SubAddColumn, pg_parse.SubAddConstraint:
			// A REFERENCES holds the other table while the key's triggers are added to it.
			for _, ref := range sub.Relations {
				create.extra = append(create.extra, pg_contract.Target{
					Relation: contract(s.resolve(ref)),
					Lock:     pg_contract.LockShareRowExclusive,
					OpKind:   pg_contract.OpKindMetadata,
					Role:     pg_contract.TargetRoleImplicit,
				})
			}
		case pg_parse.SubLike:
			clauses = append(clauses, ruleCreateLike(s, sub))
		case pg_parse.SubAddInherit:
			clauses = append(clauses, ruleCreateInherits(s, sub))
		case pg_parse.SubAttachPartition:
			clauses = append(clauses, ruleCreatePartitionOf(s, sub))
		}
	}
	if len(create.extra) > 0 {
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
	e.recursion = parentOnly
	for _, ref := range sub.Relations {
		e.extra = append(e.extra, pg_contract.Target{
			Relation: contract(s.resolve(ref)),
			Lock:     pg_contract.LockAccessShare,
			OpKind:   pg_contract.OpKindNone,
			Role:     pg_contract.TargetRoleDirect,
		})
	}
	return e
}

// ruleCreateInherits is R-TB-CREATE-INHERITS.
func ruleCreateInherits(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-TB-CREATE-INHERITS", fmt.Sprintf("the new table starts inheriting from %s, which is held at SHARE UPDATE EXCLUSIVE while the edge is recorded", sub.Name)).onNone()
	e.recursion = parentOnly
	for _, ref := range sub.Relations {
		e.extra = append(e.extra, pg_contract.Target{
			Relation: contract(s.resolve(ref)),
			Lock:     pg_contract.LockShareUpdateExclusive,
			OpKind:   pg_contract.OpKindMetadata,
			Role:     pg_contract.TargetRoleDirect,
		})
	}
	return e
}

// ruleCreatePartitionOf is R-TB-CREATE-PARTOF.
func ruleCreatePartitionOf(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-TB-CREATE-PARTOF",
		fmt.Sprintf("%s is held at ACCESS EXCLUSIVE while the new partition is recorded — a stronger lock than the SHARE UPDATE EXCLUSIVE of CREATE TABLE followed by ATTACH PARTITION, which does the same thing", sub.Name)).onNone()
	e.recursion = parentOnly

	for _, ref := range sub.Relations {
		parent := s.resolve(ref)
		e.extra = append(e.extra, pg_contract.Target{
			Relation: contract(parent),
			Lock:     pg_contract.LockAccessExclusive,
			OpKind:   pg_contract.OpKindMetadata,
			Role:     pg_contract.TargetRoleDirect,
		})
		e.extra = append(e.extra, s.defaultPartitionTarget(parent, pg_contract.LockAccessExclusive, pg_contract.OpKindScan)...)
	}
	if len(e.extra) > 1 {
		e.message += "; the default partition is scanned to prove none of its rows belongs in the new one"
	}
	return e
}

// defaultPartitionTarget is the default partition of a parent, which every partition change re-proves.
func (s scope) defaultPartitionTarget(parent pg_catalog.RelationInfo, lock pg_contract.Lock, op pg_contract.OpKind) []pg_contract.Target {
	if !parent.Exists() {
		return nil
	}
	oid, ok := s.catalog.DefaultPartition(parent.Relation.OID)
	if !ok {
		return nil
	}
	rel, ok := s.catalog.ByOID(oid)
	if !ok {
		return nil
	}
	return []pg_contract.Target{{
		Relation: rel.Contract(),
		Lock:     lock,
		OpKind:   op,
		Role:     pg_contract.TargetRoleImplicit,
	}}
}

// ruleCreateTableAs is R-TB-CTAS, which covers SELECT … INTO too.
func ruleCreateTableAs(s scope) effect {
	e := newEffect(s, "R-TB-CTAS", "every source relation is read in full and the rows are written into the new table; the sources are only held at ACCESS SHARE, but the statement lasts as long as the query does")
	e.lock = pg_contract.LockAccessShare
	e.op = pg_contract.OpKindScan
	e.recursion = parentOnly
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
		e.extra = append(e.extra, s.referencingTables(info)...)
		// the tables on the other side of the table's own foreign keys are held too.
		e.extra = append(e.extra, s.referencedTables(info)...)
		for _, parent := range s.parentTargets(info) {
			e.extra = append(e.extra, parent)
			e.extra = append(e.extra, s.defaultPartitionTarget(s.byName(parent.Relation), pg_contract.LockAccessExclusive, pg_contract.OpKindMetadata)...)
		}
		if s.statement.Flags.Cascade {
			e.extra = append(e.extra, s.viewDependents(info)...)
		}
	}
	switch {
	case s.statement.Flags.Cascade:
		e.message += "; CASCADE drops every dependent view, foreign key and object with it, so the blast radius is whatever the catalog says depends on it"
	case len(e.extra) > 0:
		e.message += "; every table holding a foreign key into it is locked with it, because that key's trigger has to go too"
	}
	return e
}

// referencingTables is the tables that hold a foreign key to this relation.
func (s scope) referencingTables(info pg_catalog.RelationInfo) []pg_contract.Target {
	if !info.Exists() {
		return nil
	}
	var targets []pg_contract.Target
	for _, key := range s.catalog.ForeignKeysTo(info.Relation.OID) {
		rel, ok := s.catalog.ByOID(key.RelID)
		if !ok {
			continue
		}
		targets = append(targets, pg_contract.Target{
			Relation: rel.Contract(),
			Lock:     pg_contract.LockAccessExclusive,
			OpKind:   pg_contract.OpKindMetadata,
			Role:     pg_contract.TargetRoleImplicit,
		})
	}
	return targets
}

// referencedTables is the tables this one points at with a foreign key.
func (s scope) referencedTables(info pg_catalog.RelationInfo) []pg_contract.Target {
	if !info.Exists() {
		return nil
	}
	var targets []pg_contract.Target
	for _, key := range s.catalog.ForeignKeysFrom(info.Relation.OID) {
		rel, ok := s.catalog.ByOID(key.ForeignRelID)
		if !ok {
			continue
		}
		targets = append(targets, pg_contract.Target{
			Relation: rel.Contract(),
			Lock:     pg_contract.LockAccessExclusive,
			OpKind:   pg_contract.OpKindMetadata,
			Role:     pg_contract.TargetRoleImplicit,
		})
	}
	return targets
}

// parentTargets is the parents a relation is attached to.
func (s scope) parentTargets(info pg_catalog.RelationInfo) []pg_contract.Target {
	if !info.Exists() {
		return nil
	}
	var targets []pg_contract.Target
	for _, edge := range s.catalog.Parents(info.Relation.OID) {
		rel, ok := s.catalog.ByOID(edge.Parent)
		if !ok {
			continue
		}
		targets = append(targets, pg_contract.Target{
			Relation: rel.Contract(),
			Lock:     pg_contract.LockAccessExclusive,
			OpKind:   pg_contract.OpKindMetadata,
			Role:     pg_contract.TargetRoleImplicit,
		})
	}
	return targets
}

// viewDependents is the views and materialized views a CASCADE takes with it.
func (s scope) viewDependents(info pg_catalog.RelationInfo) []pg_contract.Target {
	if !info.Exists() {
		return nil
	}
	var targets []pg_contract.Target
	for _, dep := range s.catalog.ViewDependents(info.Relation.OID) {
		rel, ok := s.catalog.ByOID(dep.DependentRelID)
		if !ok {
			continue
		}
		targets = append(targets, pg_contract.Target{
			Relation: rel.Contract(),
			Lock:     pg_contract.LockAccessExclusive,
			OpKind:   pg_contract.OpKindMetadata,
			Role:     pg_contract.TargetRoleImplicit,
		})
	}
	return targets
}

// ruleCreateView is R-OB-CREATEVIEW and R-OB-REPLACEVIEW.
func ruleCreateView(s scope) effect {
	if s.statement.Flags.OrReplace && s.relation().Exists() {
		e := newEffect(s, "R-OB-REPLACEVIEW", "the view definition is replaced, which blocks every query already reading through it until the statement commits")
		e.recursion = parentOnly
		for _, info := range s.sources() {
			e.extra = append(e.extra, pg_contract.Target{
				Relation: contract(info),
				Lock:     pg_contract.LockAccessShare,
				OpKind:   pg_contract.OpKindNone,
				Role:     pg_contract.TargetRoleDirect,
			})
		}
		return e
	}

	e := newEffect(s, "R-OB-CREATEVIEW", "the view is only a stored query; the relations it reads are opened to resolve their columns and nothing else")
	e.lock = pg_contract.LockAccessShare
	e.op = pg_contract.OpKindNone
	e.recursion = parentOnly
	return e.onSources(s)
}

// ruleDropView is R-OB-DROPVIEW, which covers a materialized view the same way.
func ruleDropView(s scope) effect {
	e := newEffect(s, "R-OB-DROPVIEW", "the view is removed from the catalog; every query still reading through it waits and then fails")
	e = e.onEvery(s)
	if s.statement.Flags.Cascade {
		for _, info := range s.relations {
			e.extra = append(e.extra, s.viewDependents(info)...)
		}
		e.message += "; CASCADE takes every view built on top of it as well"
	}
	return e
}

// ruleCreateMatView is R-OB-CREATEMV.
func ruleCreateMatView(s scope) effect {
	e := newEffect(s, "R-OB-CREATEMV", "the query is run once and its result is written to disk; the sources are only held at ACCESS SHARE, but the statement lasts as long as the query does")
	e.lock = pg_contract.LockAccessShare
	e.op = pg_contract.OpKindScan
	e.recursion = parentOnly
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
		e.lock = pg_contract.LockExclusive
		e.op = pg_contract.OpKindDML
	} else {
		e = newEffect(s, "R-OB-REFRESHMV", "the query is re-run and the matview's file is replaced, so nothing can read it until the statement commits")
		e.op = pg_contract.OpKindRewrite
	}
	e.recursion = parentOnly
	e.extra = append(e.extra, s.matViewSources(s.relation())...)

	if s.statement.Flags.Concurrently && !s.statement.Flags.WithData {
		return e.reject("REFRESH MATERIALIZED VIEW CONCURRENTLY … WITH NO DATA is not a valid combination", pg_contract.AnyVersion)
	}
	return e
}

// matViewSources is the relations the stored query reads when it runs again.
func (s scope) matViewSources(info pg_catalog.RelationInfo) []pg_contract.Target {
	if !info.Exists() {
		return nil
	}
	var targets []pg_contract.Target
	for _, dep := range s.catalog.ViewSources(info.Relation.OID) {
		rel, ok := s.catalog.ByOID(dep.ReferencedRelID)
		if !ok {
			continue
		}
		targets = append(targets, pg_contract.Target{
			Relation: rel.Contract(),
			Lock:     pg_contract.LockAccessShare,
			OpKind:   pg_contract.OpKindNone,
			Role:     pg_contract.TargetRoleResolved,
		})
	}
	return targets
}

// ruleAlterSequence is R-OB-ALTERSEQ.
func ruleAlterSequence(s scope) effect {
	e := newEffect(s, "R-OB-ALTERSEQ", "the sequence's parameters are rewritten, which blocks every concurrent nextval() until the statement commits")
	e.lock = pg_contract.LockShareRowExclusive
	e.recursion = parentOnly
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
	e.lock = pg_contract.LockNone
	e.op = pg_contract.OpKindNone
	e.recursion = parentOnly
	return e.onNone()
}

// ruleDropSequence is R-OB-DROPSEQ.
func ruleDropSequence(s scope) effect {
	e := newEffect(s, "R-OB-DROPSEQ", "the sequence is removed from the catalog; a concurrent nextval() waits and then fails")
	e.recursion = parentOnly
	return e.onEvery(s)
}
