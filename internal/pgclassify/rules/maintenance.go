package rules

import (
	"fmt"
	"strings"

	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgclassify/stmt"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// everyTableKind is the relkinds that global statement target.
var everyTableKind = []pgcontract.RelationKind{
	pgcontract.RelationKindTable,
	pgcontract.RelationKindPartitionedTable,
	pgcontract.RelationKindMaterializedView,
}

// ruleVacuum is R-MT-VACUUM and R-MT-VACUUMFULL.
func ruleVacuum(s engine.Scope) engine.Effect {
	if enabled(s.Statement.Options, "full") {
		e := engine.NewEffect(s, "R-MT-VACUUMFULL",
			"the whole table is rewritten into a new file under ACCESS EXCLUSIVE: nothing reads or writes it for the duration, and it needs room for a second copy before the first one is released")
		return maintenanceTargets(s, e)
	}

	e := engine.NewEffect(s, "R-MT-VACUUM", "dead tuples are reclaimed in place while reads and writes keep running")
	if !disabled(s.Statement.Options, "truncate") {
		e.Message += "; the trailing empty pages are truncated at the end, which takes a brief ACCESS EXCLUSIVE lock — TRUNCATE false avoids it"
	}
	return maintenanceTargets(s, e)
}

// ruleAnalyze is R-MT-ANALYZE.
func ruleAnalyze(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-MT-ANALYZE", "a sample of the rows is read to refresh the planner's statistics; reads and writes keep running")
	e = maintenanceTargets(s, e)

	// the descendants are locked on ACCESS SHARE, even with ONLY.
	for _, on := range e.On {
		e.Touches = append(e.Touches, engine.Touch{
			Info:  on.Info,
			Lock:  pgcontract.LockAccessShare,
			Op:    pgcontract.OpKindScan,
			Role:  on.Role,
			Below: engine.Everywhere,
		})
	}
	return e
}

// maintenanceTargets is the relation list of a maintenance statement,
// it may name none in which case it reaches every table in the database.
func maintenanceTargets(s engine.Scope, e engine.Effect) engine.Effect {
	if len(s.Relations) > 0 {
		return e.OnEvery(s)
	}

	e.Message += "; the statement names no relation, so it works through every table in the database in turn"
	base := e.OnNone()
	for _, table := range s.Catalog.RelationsInSchema("", everyTableKind) {
		base.Touches = append(base.Touches, engine.Touch{
			Info: s.InfoOf(table),
			Lock: e.Lock,
			Op:   engine.OpOn(e.Op, pgcontract.RelationKindFromRelkind(table.Kind).IsPartitioned()),
			Role: pgcontract.TargetRoleExpanded,
		})
	}
	return base
}

// ruleClusterCommand is R-MT-CLUSTER.
func ruleClusterCommand(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-MT-CLUSTER",
		"the table is rewritten in index order into a new file under ACCESS EXCLUSIVE, and every index on it is rebuilt; nothing reads or writes it for the duration")

	if len(s.Statement.Subcommands) > 0 && s.Statement.Subcommands[0].Name != "" {
		e.Touches = append(e.Touches, s.IndexTarget(s.Statement.Subcommands[0].Name, pgcontract.LockAccessExclusive, pgcontract.OpKindRewrite))
	}
	return maintenanceTargets(s, e)
}

// ruleTruncate is R-MT-TRUNCATE.
func ruleTruncate(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-MT-TRUNCATE", "every row goes at once by replacing the file; it is transactional and can be rolled back, but nothing reads or writes the table until it commits")
	e = e.OnEvery(s)

	named := map[pgcontract.Relation]bool{}
	for _, info := range s.Relations {
		named[stmt.Contract(info)] = true
		for _, child := range s.DescendantsOf(info) {
			named[child.Relation] = true
		}
	}

	var unTruncated []string
	for _, info := range s.Relations {
		if s.Statement.Flags.RestartIdentity {
			e.Touches = append(e.Touches, ownedSequenceTargets(s, info)...)
		}
		for _, referencing := range s.ReferencingTables(info) {
			if named[stmt.Contract(referencing.Info)] {
				continue
			}
			e.Touches = append(e.Touches, referencing)
			unTruncated = append(unTruncated, referencing.Info.Name.String())
		}
	}

	switch {
	case len(unTruncated) == 0:
	case s.Statement.Flags.Cascade:
		e.Message += fmt.Sprintf("; CASCADE empties %s as well, because a foreign key points from there into this table", strings.Join(unTruncated, ", "))
	default:
		return e.Reject(fmt.Sprintf("TRUNCATE is refused while %s references the table and is not being truncated with it; add CASCADE or name it too", strings.Join(unTruncated, ", ")), pgcontract.AnyVersion)
	}
	if s.Statement.Flags.RestartIdentity {
		e.Message += "; RESTART IDENTITY resets the sequences the table's identity and serial columns own"
	}
	return e
}

// ownedSequenceTargets return all the sequences linked to a relation
func ownedSequenceTargets(s engine.Scope, info pgcatalog.RelationInfo) []engine.Touch {
	if !info.Exists() {
		return nil
	}
	var targets []engine.Touch
	for _, sequence := range s.Catalog.SequencesOwnedBy(info.Relation.OID) {
		rel, ok := s.Catalog.ByOID(sequence.SeqRelID)
		if !ok {
			continue
		}
		targets = append(targets, engine.Touch{
			Info: s.InfoOf(rel),
			Lock: pgcontract.LockAccessExclusive,
			Op:   pgcontract.OpKindMetadata,
			Role: pgcontract.TargetRoleImplicit,
		})
	}
	return targets
}

// ruleLock is R-MT-LOCK.
func ruleLock(s engine.Scope) engine.Effect {
	mode := pgcontract.LockAccessExclusive
	if len(s.Statement.Subcommands) > 0 {
		if parsed, err := pgcontract.ParseLock(s.Statement.Subcommands[0].Value); err == nil && parsed.IsValid() {
			mode = parsed
		}
	}

	e := engine.NewEffect(s, "R-MT-LOCK", fmt.Sprintf("the lock is taken explicitly at %s and held until the transaction ends, so a statement after it in the same block never waits for a lock it covers", mode))
	e.Lock = mode
	if s.Statement.Flags.Nowait {
		e.Message += "; NOWAIT makes the statement fail rather than queue if the lock is not free"
	}
	return e.OnEvery(s)
}

// ruleCopyFrom is R-MT-COPYFROM.
func ruleCopyFrom(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-MT-COPYFROM", "rows are appended to the table under the same lock an INSERT takes; concurrent readers and writers are unaffected")
	return e
}

// ruleCopyTo is R-MT-COPYTO.
func ruleCopyTo(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-MT-COPYTO", "the rows are read out and nothing is written; the statement lasts as long as the read does")
	return e.OnEvery(s)
}

// enabled reads a boolean statement option that defaults to off.
func enabled(options []ir.Option, name string) bool {
	for _, option := range options {
		if !strings.EqualFold(option.Name, name) {
			continue
		}
		return option.Value == "" || option.Value == "1" || strings.EqualFold(option.Value, "true") || strings.EqualFold(option.Value, "on")
	}
	return false
}

// disabled reads a boolean statement option that defaults to on.
func disabled(options []ir.Option, name string) bool {
	for _, option := range options {
		if !strings.EqualFold(option.Name, name) {
			continue
		}
		return option.Value == "0" || strings.EqualFold(option.Value, "false") || strings.EqualFold(option.Value, "off")
	}
	return false
}
