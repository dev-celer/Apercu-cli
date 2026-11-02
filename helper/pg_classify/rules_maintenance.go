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
		return s.maintenanceTargets(e)
	}

	e := newEffect(s, "R-MT-VACUUM", "dead tuples are reclaimed in place while reads and writes keep running")
	if !disabled(s.statement.Options, "truncate") {
		e.message += "; the trailing empty pages are truncated at the end, which takes a brief ACCESS EXCLUSIVE lock — TRUNCATE false avoids it"
	}
	return s.maintenanceTargets(e)
}

// ruleAnalyze is R-MT-ANALYZE.
func ruleAnalyze(s scope) effect {
	e := newEffect(s, "R-MT-ANALYZE", "a sample of the rows is read to refresh the planner's statistics; reads and writes keep running")
	e = s.maintenanceTargets(e)

	// the descendants are locked on ACCESS SHARE, even with ONLY.
	for _, on := range e.on {
		e.touches = append(e.touches, touch{
			info:  on.info,
			lock:  pg_contract.LockAccessShare,
			op:    pg_contract.OpKindScan,
			role:  on.role,
			below: everywhere,
		})
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
		base.touches = append(base.touches, touch{
			info: s.infoOf(table),
			lock: e.lock,
			op:   opOn(e.op, pg_contract.RelationKindFromRelkind(table.Kind).IsPartitioned()),
			role: pg_contract.TargetRoleExpanded,
		})
	}
	return base
}

// ruleClusterCommand is R-MT-CLUSTER.
func ruleClusterCommand(s scope) effect {
	e := newEffect(s, "R-MT-CLUSTER",
		"the table is rewritten in index order into a new file under ACCESS EXCLUSIVE, and every index on it is rebuilt; nothing reads or writes it for the duration")

	if len(s.statement.Subcommands) > 0 && s.statement.Subcommands[0].Name != "" {
		e.touches = append(e.touches, s.indexTarget(s.statement.Subcommands[0].Name, pg_contract.LockAccessExclusive, pg_contract.OpKindRewrite))
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
			e.touches = append(e.touches, s.ownedSequenceTargets(info)...)
		}
		for _, referencing := range s.referencingTables(info) {
			if named[contract(referencing.info)] {
				continue
			}
			e.touches = append(e.touches, referencing)
			unTruncated = append(unTruncated, referencing.info.Name.String())
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
func (s scope) ownedSequenceTargets(info pg_catalog.RelationInfo) []touch {
	if !info.Exists() {
		return nil
	}
	var targets []touch
	for _, sequence := range s.catalog.SequencesOwnedBy(info.Relation.OID) {
		rel, ok := s.catalog.ByOID(sequence.SeqRelID)
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

// ruleLock is R-MT-LOCK.
func ruleLock(s scope) effect {
	mode := pg_contract.LockAccessExclusive
	if len(s.statement.Subcommands) > 0 {
		if parsed, err := pg_contract.ParseLock(s.statement.Subcommands[0].Value); err == nil && parsed.IsValid() {
			mode = parsed
		}
	}

	e := newEffect(s, "R-MT-LOCK", fmt.Sprintf("the lock is taken explicitly at %s and held until the transaction ends, so a statement after it in the same block never waits for a lock it covers", mode))
	e.lock = mode
	if s.statement.Flags.Nowait {
		e.message += "; NOWAIT makes the statement fail rather than queue if the lock is not free"
	}
	return e.onEvery(s)
}

// ruleCopyFrom is R-MT-COPYFROM.
func ruleCopyFrom(s scope) effect {
	e := newEffect(s, "R-MT-COPYFROM", "rows are appended to the table under the same lock an INSERT takes; concurrent readers and writers are unaffected")
	return e
}

// ruleCopyTo is R-MT-COPYTO.
func ruleCopyTo(s scope) effect {
	e := newEffect(s, "R-MT-COPYTO", "the rows are read out and nothing is written; the statement lasts as long as the read does")
	return e.onEvery(s)
}

// enabled reads a boolean statement option that defaults to off.
func enabled(options []pg_parse.Option, name string) bool {
	for _, option := range options {
		if !strings.EqualFold(option.Name, name) {
			continue
		}
		return option.Value == "" || option.Value == "1" || strings.EqualFold(option.Value, "true") || strings.EqualFold(option.Value, "on")
	}
	return false
}

// disabled reads a boolean statement option that defaults to on.
func disabled(options []pg_parse.Option, name string) bool {
	for _, option := range options {
		if !strings.EqualFold(option.Name, name) {
			continue
		}
		return option.Value == "0" || strings.EqualFold(option.Value, "false") || strings.EqualFold(option.Value, "off")
	}
	return false
}
