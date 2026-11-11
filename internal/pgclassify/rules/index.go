package rules

import (
	"fmt"

	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// ruleCreateIndex is R-IX-CREATE and R-IX-CREATE-CONC.
func ruleCreateIndex(s engine.Scope) engine.Effect {
	if s.Statement.Flags.Concurrently {
		e := engine.NewEffect(s, "R-IX-CREATE-CONC",
			"the index is built in two passes under SHARE UPDATE EXCLUSIVE, so reads and writes keep running; it waits for every transaction open when each pass starts, takes roughly twice as long as a plain build, and leaves an INVALID index behind if it fails")
		if s.Partitioned() {
			return e.Reject("CREATE INDEX CONCURRENTLY is not supported on a partitioned table on any of PostgreSQL 15-18; build the index on each partition, then ON ONLY the parent and ALTER INDEX … ATTACH PARTITION", pgcontract.AnyVersion)
		}
		return e
	}

	e := engine.NewEffect(s, "R-IX-CREATE", "every row is read to build the index, and writes wait for the whole build; CREATE INDEX CONCURRENTLY does the same work without blocking them")

	// ON ONLY a partitioned table records an invalid parent index; on any other table there is
	// nothing to stop at, and the index is built all the same.
	if s.Only() && s.Partitioned() {
		e.Op = pgcontract.OpKindMetadata
		e.Message = "ON ONLY records an invalid parent index and builds nothing; it becomes valid once every partition has a matching index attached to it"
	}
	return e
}

// ruleDropIndex is R-IX-DROP and R-IX-DROP-CONC.
func ruleDropIndex(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-IX-DROP", "the index and the table behind it are both held at ACCESS EXCLUSIVE while the catalog entry goes")
	tableLock := pgcontract.LockAccessExclusive

	if s.Statement.Flags.Concurrently {
		e = engine.NewEffect(s, "R-IX-DROP-CONC",
			"the index is retired in stages, the table staying at SHARE UPDATE EXCLUSIVE throughout while the index itself ends at ACCESS EXCLUSIVE; the statement waits for every transaction that could still be using the index")
		tableLock = pgcontract.LockShareUpdateExclusive
	}
	e = e.OnNone()
	e.TableLock = tableLock

	for _, index := range s.Relations {
		e.On = append(e.On, engine.RelationTarget{Info: index, Role: pgcontract.TargetRoleDirect})
	}
	return e
}

// ruleIndexOptions is R-IX-SETOPT, which reuse the table option classification.
func ruleIndexOptions(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := ruleRelOptions(s, sub)
	e.Code = "R-IX-SETOPT"
	return e
}

// ruleIndexStatistics is R-IX-SETSTATS.
func ruleIndexStatistics(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-IX-SETSTATS", fmt.Sprintf("the statistics target of expression column %s is recorded; nothing is sampled until the next ANALYZE", sub.Name))
	return e
}

// ruleIndexTablespace is R-IX-TABLESPACE.
func ruleIndexTablespace(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-IX-TABLESPACE", fmt.Sprintf("the index file is copied into tablespace %q, which blocks every query that could use it", sub.Value))
	return e
}

// ruleIndexAttach is R-IX-ATTACH.
func ruleIndexAttach(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-IX-ATTACH", "the partition's index is recorded under the parent index; no row is read, and the parent index becomes valid once every partition has one")
	// Both indexes are opened through their tables, which are only read.
	e.TableLock = pgcontract.LockAccessShare

	if len(sub.Relations) > 0 {
		e.Touches = append(e.Touches, engine.Touch{
			Info:      s.Resolve(sub.Relations[0]),
			Lock:      pgcontract.LockAccessExclusive,
			Op:        pgcontract.OpKindMetadata,
			Role:      pgcontract.TargetRoleDirect,
			TableLock: pgcontract.LockAccessShare,
		})
	}
	return e
}

// ruleRenameIndex is R-IX-RENAME.
func ruleRenameIndex(s engine.Scope) engine.Effect {
	newName := ""
	if len(s.Statement.Subcommands) > 0 {
		newName = s.Statement.Subcommands[0].NewName
	}
	e := engine.NewEffect(s, "R-IX-RENAME", fmt.Sprintf("the index is renamed to %q in the catalog, under a lock that blocks neither reads nor writes", newName))
	return e
}

// reindexShape is what a REINDEX does to the table it reads and to each index it rebuilds.
type reindexShape struct {
	table     engine.Effect
	indexLock pgcontract.Lock
	indexOp   pgcontract.OpKind
}

// ruleReindex is R-IX-REINDEX and R-IX-REINDEX-CONC.
func ruleReindex(s engine.Scope) engine.Effect {
	shape := newReindexShape(s, s.Statement.Flags.Concurrently)
	e, named := shape.table, s.Relation()

	if pgcontract.RelationKindFromRelkind(named.Relation.Kind).IsIndex() {
		// REINDEX INDEX rebuilds the index, every index of its tree when it is partitioned, and opens the table behind each one.
		e.TableLock = e.Lock
		e.Lock, e.Op = shape.indexLock, shape.indexOp
		e.Below = engine.ToPartitions
		return e
	}

	// REINDEX TABLE reads the table, and its partitions, to rebuild every index hanging off them.
	e.Below = engine.ToPartitions
	e.Touches = append(e.Touches, indexesOf(s, named, shape.indexLock, shape.indexOp)...)
	for _, child := range s.DescendantsOf(named) {
		e.Touches = append(e.Touches, indexesOf(s, s.ByName(child.Relation), shape.indexLock, shape.indexOp)...)
	}
	return e
}

// newReindexShape is the shared shape of both REINDEX rule ids.
func newReindexShape(s engine.Scope, concurrent bool) reindexShape {
	if concurrent {
		e := engine.NewEffect(s, "R-IX-REINDEX-CONC",
			"each index is rebuilt beside the old one under SHARE UPDATE EXCLUSIVE, so reads and writes keep running; it waits for every transaction open when a pass starts and leaves an INVALID index behind if it fails")
		return reindexShape{table: e, indexLock: pgcontract.LockShareUpdateExclusive, indexOp: pgcontract.OpKindConcurrent}
	}
	e := engine.NewEffect(s, "R-IX-REINDEX",
		"every index is rebuilt from a full read of the table; the table itself is only held at SHARE, but no query can be planned without its indexes and those are held at ACCESS EXCLUSIVE, so grade the whole statement as if the table were too")
	return reindexShape{table: e, indexLock: pgcontract.LockAccessExclusive, indexOp: pgcontract.OpKindRewrite}
}

// indexesOf is the indexes hanging off a table.
func indexesOf(s engine.Scope, table pgcatalog.RelationInfo, lock pgcontract.Lock, op pgcontract.OpKind) []engine.Touch {
	if !table.Exists() {
		return nil
	}
	var targets []engine.Touch
	for _, index := range s.Catalog.IndexesOf(table.Relation.OID) {
		rel, ok := s.Catalog.ByOID(index.IndexRelID)
		if !ok {
			continue
		}
		targets = append(targets, engine.Touch{
			Info: s.InfoOf(rel),
			Lock: lock,
			Op:   op,
			Role: pgcontract.TargetRoleResolved,
		})
	}
	return targets
}

// ruleReindexWide is R-IX-REINDEX-WIDE: the statement names a schema, a database or the whole catalog.
func ruleReindexWide(s engine.Scope) engine.Effect {
	scopeName := ""
	if len(s.Statement.Subcommands) > 0 {
		scopeName = s.Statement.Subcommands[0].Name
	}
	// REINDEX DATABASE and REINDEX SYSTEM name no schema, so every relation is in range.
	schema := ""
	if s.Statement.Command == "REINDEX SCHEMA" {
		schema = scopeName
	}

	shape := newReindexShape(s, s.Statement.Flags.Concurrently)
	e := shape.table
	e.Code = "R-IX-REINDEX-WIDE"
	e.Message += "; every table the statement covers is reindexed in turn, one transaction each, and the statement cannot run inside a transaction block"
	tableLock := e.Lock
	e = e.OnNone()

	for _, table := range s.Catalog.RelationsInSchema(schema, []pgcontract.RelationKind{
		pgcontract.RelationKindTable, pgcontract.RelationKindMaterializedView,
	}) {
		info := pgcatalog.RelationInfo{Name: table.RelationName(), Relation: table, Origin: pgcatalog.OriginExisting}
		e.Touches = append(e.Touches, engine.Touch{
			Info: s.InfoOf(table),
			Lock: tableLock,
			Op:   e.Op,
			Role: pgcontract.TargetRoleExpanded,
		})
		e.Touches = append(e.Touches, indexesOf(s, info, shape.indexLock, shape.indexOp)...)
	}
	return e
}
