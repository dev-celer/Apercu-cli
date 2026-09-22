package pg_classify

import (
	"fmt"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// ruleCreateIndex is R-IX-CREATE and R-IX-CREATE-CONC.
func ruleCreateIndex(s scope) effect {
	if s.statement.Flags.Concurrently {
		e := newEffect(s, "R-IX-CREATE-CONC",
			"the index is built in two passes under SHARE UPDATE EXCLUSIVE, so reads and writes keep running; it waits for every transaction open when each pass starts, takes roughly twice as long as a plain build, and leaves an INVALID index behind if it fails")
		e.lock = pg_contract.LockShareUpdateExclusive
		e.op = pg_contract.OpKindConcurrent
		e.recursion = parentOnly
		if s.partitioned() {
			return e.reject("CREATE INDEX CONCURRENTLY is not supported on a partitioned table on any of PostgreSQL 15-18; build the index on each partition, then ON ONLY the parent and ALTER INDEX … ATTACH PARTITION", pg_contract.AnyVersion)
		}
		return e
	}

	e := newEffect(s, "R-IX-CREATE", "every row is read to build the index, and writes wait for the whole build; CREATE INDEX CONCURRENTLY does the same work without blocking them")
	e.lock = pg_contract.LockShare
	e.op = pg_contract.OpKindScan
	e.recursion = partitionsOnly

	if s.only() {
		e.op = pg_contract.OpKindMetadata
		e.message = "ON ONLY records an invalid parent index and builds nothing; it becomes valid once every partition has a matching index attached to it"
	}
	return e
}

// ruleDropIndex is R-IX-DROP and R-IX-DROP-CONC.
func ruleDropIndex(s scope) effect {
	e := newEffect(s, "R-IX-DROP", "the index and the table behind it are both held at ACCESS EXCLUSIVE while the catalog entry goes")
	tableLock := pg_contract.LockAccessExclusive

	if s.statement.Flags.Concurrently {
		e = newEffect(s, "R-IX-DROP-CONC",
			"the index is retired in stages, the table staying at SHARE UPDATE EXCLUSIVE throughout while the index itself ends at ACCESS EXCLUSIVE; the statement waits for every transaction that could still be using the index")
		tableLock = pg_contract.LockShareUpdateExclusive
	}
	e.recursion = parentOnly
	e = e.onNone()

	for _, index := range s.relations {
		e.on = append(e.on, relationTarget{info: index, role: pg_contract.TargetRoleDirect})
		if table, ok := s.tableOfIndex(index); ok {
			e.extra = append(e.extra, pg_contract.Target{
				Relation: table,
				Lock:     tableLock,
				OpKind:   pg_contract.OpKindMetadata,
				Role:     pg_contract.TargetRoleResolved,
			})
		}
	}
	return e
}

// tableOfIndex return, for an index, the table it belongs to.
func (s scope) tableOfIndex(index pg_catalog.RelationInfo) (pg_contract.Relation, bool) {
	if !index.Exists() {
		return pg_contract.Relation{}, false
	}
	table, ok := s.catalog.TableOfIndex(index.Relation.OID)
	if !ok {
		return pg_contract.Relation{}, false
	}
	return table.Contract(), true
}

// indexClauseRules is the ALTER INDEX half of the clause registry.
var indexClauseRules = map[pg_parse.SubKind]func(scope, pg_parse.Subcommand) effect{
	pg_parse.SubSetRelOptions:   ruleIndexOptions,
	pg_parse.SubResetRelOptions: ruleIndexOptions,
	pg_parse.SubSetStatistics:   ruleIndexStatistics,
	pg_parse.SubSetTablespace:   ruleIndexTablespace,
	pg_parse.SubAttachPartition: ruleIndexAttach,
}

// classifyAlterIndex is ALTER INDEX, it classifies every subcommands.
func classifyAlterIndex(s scope) ([]pg_contract.Finding, []pg_contract.Error) {
	return classifyClauses(s, indexClauseRules)
}

// ruleIndexOptions is R-IX-SETOPT, which reuse the table option classification.
func ruleIndexOptions(s scope, sub pg_parse.Subcommand) effect {
	e := ruleRelOptions(s, sub)
	e.code = "R-IX-SETOPT"
	e.recursion = parentOnly
	return e
}

// ruleIndexStatistics is R-IX-SETSTATS.
func ruleIndexStatistics(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-IX-SETSTATS", fmt.Sprintf("the statistics target of expression column %s is recorded; nothing is sampled until the next ANALYZE", sub.Name))
	e.lock = pg_contract.LockShareUpdateExclusive
	e.recursion = parentOnly
	return e
}

// ruleIndexTablespace is R-IX-TABLESPACE.
func ruleIndexTablespace(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-IX-TABLESPACE", fmt.Sprintf("the index file is copied into tablespace %q, which blocks every query that could use it", sub.Value))
	e.op = pg_contract.OpKindRewrite
	e.recursion = parentOnly
	return e
}

// ruleIndexAttach is R-IX-ATTACH.
func ruleIndexAttach(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-IX-ATTACH", "the partition's index is recorded under the parent index; no row is read, and the parent index becomes valid once every partition has one")
	e.lock = pg_contract.LockShareUpdateExclusive
	e.recursion = parentOnly

	if table, ok := s.tableOfIndex(s.relation()); ok {
		e.extra = append(e.extra, pg_contract.Target{
			Relation: table, Lock: pg_contract.LockAccessShare,
			OpKind: pg_contract.OpKindMetadata, Role: pg_contract.TargetRoleResolved,
		})
	}
	if len(sub.Relations) > 0 {
		attached := s.resolve(sub.Relations[0])
		e.extra = append(e.extra, pg_contract.Target{
			Relation: contract(attached),
			Lock:     pg_contract.LockAccessExclusive,
			OpKind:   pg_contract.OpKindMetadata,
			Role:     pg_contract.TargetRoleDirect,
		})
		if table, ok := s.tableOfIndex(attached); ok {
			e.extra = append(e.extra, pg_contract.Target{
				Relation: table, Lock: pg_contract.LockAccessShare,
				OpKind: pg_contract.OpKindMetadata, Role: pg_contract.TargetRoleResolved,
			})
		}
	}
	return e
}

// ruleRenameIndex is R-IX-RENAME.
func ruleRenameIndex(s scope) effect {
	newName := ""
	if len(s.statement.Subcommands) > 0 {
		newName = s.statement.Subcommands[0].NewName
	}
	e := newEffect(s, "R-IX-RENAME", fmt.Sprintf("the index is renamed to %q in the catalog, under a lock that blocks neither reads nor writes", newName))
	e.lock = pg_contract.LockShareUpdateExclusive
	e.recursion = parentOnly
	return e
}

// reindexShape is what a REINDEX does to the table it reads and to each index it rebuilds.
type reindexShape struct {
	table     effect
	indexLock pg_contract.Lock
	indexOp   pg_contract.OpKind
}

// ruleReindex is R-IX-REINDEX and R-IX-REINDEX-CONC.
func ruleReindex(s scope) effect {
	shape := newReindexShape(s, s.statement.Flags.Concurrently)
	e, named := shape.table, s.relation()

	if pg_contract.RelationKindFromRelkind(named.Relation.Kind).IsIndex() {
		// REINDEX INDEX rebuilds the one index and opens the table behind it.
		tableLock := e.lock
		e.lock, e.op = shape.indexLock, shape.indexOp
		if table, ok := s.tableOfIndex(named); ok {
			e.extra = append(e.extra, pg_contract.Target{
				Relation: table,
				Lock:     tableLock,
				OpKind:   pg_contract.OpKindMetadata,
				Role:     pg_contract.TargetRoleResolved,
			})
		}
		return e
	}

	// REINDEX TABLE reads the table, and its partitions, to rebuild every index hanging off them.
	e.recursion = partitionsOnly
	e.extra = append(e.extra, s.indexesOf(named, shape.indexLock, shape.indexOp)...)
	for _, child := range s.descendantsOf(named) {
		e.extra = append(e.extra, s.indexesOf(s.byName(child.relation), shape.indexLock, shape.indexOp)...)
	}
	return e
}

// newReindexShape is the shared shape of both REINDEX rule ids.
func newReindexShape(s scope, concurrent bool) reindexShape {
	if concurrent {
		e := newEffect(s, "R-IX-REINDEX-CONC",
			"each index is rebuilt beside the old one under SHARE UPDATE EXCLUSIVE, so reads and writes keep running; it waits for every transaction open when a pass starts and leaves an INVALID index behind if it fails")
		e.lock = pg_contract.LockShareUpdateExclusive
		e.op = pg_contract.OpKindConcurrent
		e.recursion = parentOnly
		return reindexShape{table: e, indexLock: pg_contract.LockShareUpdateExclusive, indexOp: pg_contract.OpKindConcurrent}
	}
	e := newEffect(s, "R-IX-REINDEX",
		"every index is rebuilt from a full read of the table; the table itself is only held at SHARE, but no query can be planned without its indexes and those are held at ACCESS EXCLUSIVE, so grade the whole statement as if the table were too")
	e.lock = pg_contract.LockShare
	e.op = pg_contract.OpKindScan
	e.recursion = parentOnly
	return reindexShape{table: e, indexLock: pg_contract.LockAccessExclusive, indexOp: pg_contract.OpKindRewrite}
}

// byName is a relation a lookup already answered with, back in the shape the rules pass around.
func (s scope) byName(relation pg_contract.Relation) pg_catalog.RelationInfo {
	return s.catalog.Resolve(relation.Name, s.context.SearchPath)
}

// indexesOf is the indexes hanging off a table.
func (s scope) indexesOf(table pg_catalog.RelationInfo, lock pg_contract.Lock, op pg_contract.OpKind) []pg_contract.Target {
	if !table.Exists() {
		return nil
	}
	var targets []pg_contract.Target
	for _, index := range s.catalog.IndexesOf(table.Relation.OID) {
		rel, ok := s.catalog.ByOID(index.IndexRelID)
		if !ok {
			continue
		}
		targets = append(targets, pg_contract.Target{
			Relation: rel.Contract(),
			Lock:     lock,
			OpKind:   op,
			Role:     pg_contract.TargetRoleResolved,
		})
	}
	return targets
}

// ruleReindexWide is R-IX-REINDEX-WIDE: the statement names a schema, a database or the whole catalog.
func ruleReindexWide(s scope) effect {
	scopeName := ""
	if len(s.statement.Subcommands) > 0 {
		scopeName = s.statement.Subcommands[0].Name
	}
	// REINDEX DATABASE and REINDEX SYSTEM name no schema, so every relation is in range.
	schema := ""
	if s.statement.Command == "REINDEX SCHEMA" {
		schema = scopeName
	}

	shape := newReindexShape(s, s.statement.Flags.Concurrently)
	e := shape.table
	e.code = "R-IX-REINDEX-WIDE"
	e.message += "; every table the statement covers is reindexed in turn, one transaction each, and the statement cannot run inside a transaction block"
	tableLock := e.lock
	e = e.onNone()

	for _, table := range s.catalog.RelationsInSchema(schema, []pg_contract.RelationKind{
		pg_contract.RelationKindTable, pg_contract.RelationKindMaterializedView,
	}) {
		info := pg_catalog.RelationInfo{Name: table.RelationName(), Relation: table, Origin: pg_catalog.OriginExisting}
		e.extra = append(e.extra, pg_contract.Target{
			Relation: table.Contract(),
			Lock:     tableLock,
			OpKind:   e.op,
			Role:     pg_contract.TargetRoleExpanded,
		})
		e.extra = append(e.extra, s.indexesOf(info, shape.indexLock, shape.indexOp)...)
	}
	return e
}
