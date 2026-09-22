package pg_classify

import (
	"fmt"
	"strings"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// everyTableKind is the relkinds that global statement target.
var everyTableKind = []pg_contract.RelationKind{
	pg_contract.RelationKindTable,
	pg_contract.RelationKindPartitionedTable,
	pg_contract.RelationKindMaterializedView,
}

// ruleVacuum is R-MT-VACUUM and R-MT-VACUUMFULL.
func ruleVacuum(s scope) effect {
	if enabled(s.statement.Options, "full") {
		e := newEffect(s, "R-MT-VACUUMFULL",
			"the whole table is rewritten into a new file under ACCESS EXCLUSIVE: nothing reads or writes it for the duration, and it needs room for a second copy before the first one is released")
		e.op = pg_contract.OpKindRewrite
		return s.maintenanceTargets(e)
	}

	e := newEffect(s, "R-MT-VACUUM", "dead tuples are reclaimed in place while reads and writes keep running")
	e.lock = pg_contract.LockShareUpdateExclusive
	e.op = pg_contract.OpKindScan
	if !disabled(s.statement.Options, "truncate") {
		e.message += "; the trailing empty pages are truncated at the end, which takes a brief ACCESS EXCLUSIVE lock — TRUNCATE false avoids it"
	}
	return s.maintenanceTargets(s.vacuumRecursion(e))
}

// ruleAnalyze is R-MT-ANALYZE.
func ruleAnalyze(s scope) effect {
	e := newEffect(s, "R-MT-ANALYZE", "a sample of the rows is read to refresh the planner's statistics; reads and writes keep running")
	e.lock = pg_contract.LockShareUpdateExclusive
	e.op = pg_contract.OpKindScan
	e = s.maintenanceTargets(s.vacuumRecursion(e))

	// the descendants are locked on ACCESS SHARE, even with ONLY.
	for _, on := range e.on {
		for _, child := range s.descendantsOf(on.info) {
			e.extra = append(e.extra, pg_contract.Target{
				Relation: child.relation,
				Lock:     pg_contract.LockAccessShare,
				OpKind:   pg_contract.OpKindScan,
				Role:     pg_contract.TargetRoleExpanded,
			})
		}
	}
	return e
}

// VACUUM and ANALYZE reach the partitions on every version plus the classic inheritance children from PG18.
func (s scope) vacuumRecursion(e effect) effect {
	e.recursion = partitionsOnly
	if s.atLeast18() {
		e.recursion = recurses
	}
	return e
}

// maintenanceTargets is the relation list of a maintenance statement,
// it may name none in which case it reaches every table in the database.
func (s scope) maintenanceTargets(e effect) effect {
	if len(s.relations) > 0 {
		return e.onEvery(s)
	}

	e.message += "; the statement names no relation, so it works through every table in the database in turn"
	base := e.onNone()
	for _, table := range s.catalog.RelationsInSchema("", everyTableKind) {
		base.extra = append(base.extra, pg_contract.Target{
			Relation: table.Contract(),
			Lock:     e.lock,
			OpKind:   opOn(e.op, pg_contract.RelationKindFromRelkind(table.Kind).IsPartitioned()),
			Role:     pg_contract.TargetRoleExpanded,
		})
	}
	return base
}

// ruleClusterCommand is R-MT-CLUSTER.
func ruleClusterCommand(s scope) effect {
	e := newEffect(s, "R-MT-CLUSTER",
		"the table is rewritten in index order into a new file under ACCESS EXCLUSIVE, and every index on it is rebuilt; nothing reads or writes it for the duration")
	e.op = pg_contract.OpKindRewrite
	e.recursion = partitionsOnly

	if len(s.statement.Subcommands) > 0 && s.statement.Subcommands[0].Name != "" {
		e.extra = append(e.extra, s.indexTarget(s.statement.Subcommands[0].Name, pg_contract.LockAccessExclusive, pg_contract.OpKindRewrite))
	}
	return s.maintenanceTargets(e)
}

// ruleTruncate is R-MT-TRUNCATE.
func ruleTruncate(s scope) effect {
	e := newEffect(s, "R-MT-TRUNCATE", "every row goes at once by replacing the file; it is transactional and can be rolled back, but nothing reads or writes the table until it commits")
	e = e.onEvery(s)

	named := map[pg_contract.Relation]bool{}
	for _, info := range s.relations {
		named[contract(info)] = true
		for _, child := range s.descendantsOf(info) {
			named[child.relation] = true
		}
	}

	var unTruncated []string
	for _, info := range s.relations {
		if s.statement.Flags.RestartIdentity {
			e.extra = append(e.extra, s.ownedSequenceTargets(info)...)
		}
		for _, target := range s.referencingTables(info) {
			if named[target.Relation] {
				continue
			}
			e.extra = append(e.extra, target)
			unTruncated = append(unTruncated, target.Relation.Name.String())
		}
	}

	switch {
	case len(unTruncated) == 0:
	case s.statement.Flags.Cascade:
		e.message += fmt.Sprintf("; CASCADE empties %s as well, because a foreign key points from there into this table", strings.Join(unTruncated, ", "))
	default:
		return e.reject(fmt.Sprintf("TRUNCATE is refused while %s references the table and is not being truncated with it; add CASCADE or name it too", strings.Join(unTruncated, ", ")), pg_contract.AnyVersion)
	}
	if s.statement.Flags.RestartIdentity {
		e.message += "; RESTART IDENTITY resets the sequences the table's identity and serial columns own"
	}
	return e
}

// ownedSequenceTargets return all the sequences linked to a relation
func (s scope) ownedSequenceTargets(info pg_catalog.RelationInfo) []pg_contract.Target {
	if !info.Exists() {
		return nil
	}
	var targets []pg_contract.Target
	for _, sequence := range s.catalog.SequencesOwnedBy(info.Relation.OID) {
		rel, ok := s.catalog.ByOID(sequence.SeqRelID)
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

// ruleLock is R-MT-LOCK.
func ruleLock(s scope) effect {
	mode := pg_contract.LockAccessExclusive
	if len(s.statement.Subcommands) > 0 {
		if parsed, err := pg_contract.ParseLock(s.statement.Subcommands[0].Value); err == nil && parsed.IsValid() {
			mode = parsed
		}
	}

	e := newEffect(s, "R-MT-LOCK", fmt.Sprintf("the lock is taken explicitly at %s and held until the transaction ends, so it sets the floor for every statement that follows it in the same block", mode))
	e.lock = mode
	if s.statement.Flags.Nowait {
		e.message += "; NOWAIT makes the statement fail rather than queue if the lock is not free"
	}
	return e.onEvery(s)
}

// ruleCopyFrom is R-MT-COPYFROM.
func ruleCopyFrom(s scope) effect {
	e := newEffect(s, "R-MT-COPYFROM", "rows are appended to the table under the same lock an INSERT takes; concurrent readers and writers are unaffected")
	e.lock = pg_contract.LockRowExclusive
	e.op = pg_contract.OpKindDML
	e.recursion = partitionsOnly
	return e
}

// ruleCopyTo is R-MT-COPYTO.
func ruleCopyTo(s scope) effect {
	e := newEffect(s, "R-MT-COPYTO", "the rows are read out and nothing is written; the statement lasts as long as the read does")
	e.lock = pg_contract.LockAccessShare
	e.op = pg_contract.OpKindNone
	e.recursion = parentOnly
	return e.onEvery(s)
}

// enabled reads a boolean statement option that defaults to off.
func enabled(options []pg_parse.Option, name string) bool {
	for _, option := range options {
		if !strings.EqualFold(option.Name, name) {
			continue
		}
		return option.Value == "" || strings.EqualFold(option.Value, "true") || strings.EqualFold(option.Value, "on")
	}
	return false
}

// disabled reads a boolean statement option that defaults to on.
func disabled(options []pg_parse.Option, name string) bool {
	for _, option := range options {
		if !strings.EqualFold(option.Name, name) {
			continue
		}
		return strings.EqualFold(option.Value, "false") || strings.EqualFold(option.Value, "off")
	}
	return false
}
