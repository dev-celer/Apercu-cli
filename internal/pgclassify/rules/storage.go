package rules

import (
	"fmt"
	"strings"

	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// sueRelOptions are the storage parameters PostgreSQL will change while taking only SHARE UPDATE EXCLUSIVE.
// Everything else takes ACCESS EXCLUSIVE.
var sueRelOptions = map[string]bool{
	"fillfactor":                           true,
	"toast_tuple_target":                   true,
	"parallel_workers":                     true,
	"deduplicate_items":                    true,
	"vacuum_truncate":                      true,
	"vacuum_index_cleanup":                 true,
	"vacuum_cleanup_index_scale_factor":    true,
	"vacuum_max_eager_freeze_failure_rate": true,
	"log_autovacuum_min_duration":          true,
	"n_distinct":                           true,
	"n_distinct_inherited":                 true,
}

// relOptionLock is the lock one storage parameter needs.
func relOptionLock(option ir.Option) pgcontract.Lock {
	name := strings.ToLower(option.Name)
	if sueRelOptions[name] || strings.HasPrefix(name, "autovacuum_") {
		return pgcontract.LockShareUpdateExclusive
	}
	return pgcontract.LockAccessExclusive
}

// ruleRelOptions is R-AT-RELOPT.
func ruleRelOptions(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-RELOPT", "")
	names := make([]string, 0, len(sub.Options))
	for _, option := range sub.Options {
		e.Lock = pgcontract.MaxLock(e.Lock, relOptionLock(option))
		names = append(names, option.Name)
	}
	if e.Lock == pgcontract.LockNone {
		e.Lock = pgcontract.LockAccessExclusive
	}

	if len(names) == 0 {
		e.Message = "no storage parameter is named, so the statement takes the strongest lock anyway"
	} else {
		e.Message = fmt.Sprintf("storage parameters %s are recorded on the relation", strings.Join(names, ", "))
	}
	return e
}

// ruleSetTablespace is R-AT-TABLESPACE.
func ruleSetTablespace(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-TABLESPACE", fmt.Sprintf("the heap is copied file by file into tablespace %q; the indexes are left where they are", sub.Value))
	return e
}

// ruleSetAccessMethod is R-AT-ACCESSMETHOD.
func ruleSetAccessMethod(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-ACCESSMETHOD", "")

	current := s.Relation().Relation.AccessMethod
	if current != "" && strings.EqualFold(current, sub.Value) {
		e.Message = fmt.Sprintf("the table already uses %q, so the statement changes nothing", current)
		return e
	}
	e.Op = pgcontract.OpKindRewrite
	e.Message = fmt.Sprintf("every tuple is rewritten into the %s format", sub.Value)
	return e
}

// ruleSetLogged is R-AT-LOGGED.
func ruleSetLogged(s engine.Scope, sub ir.Subcommand) engine.Effect {
	target := "LOGGED"
	if sub.Kind == ir.SubSetUnlogged {
		target = "UNLOGGED"
	}
	e := engine.NewEffect(s, "R-AT-LOGGED", fmt.Sprintf("the table becomes %s, which rewrites the heap and follows through to the sequences its identity and serial columns own", target))

	if !s.Partitioned() {
		return e
	}
	if s.AtLeast18() {
		return e.Reject("PostgreSQL 18 refuses SET LOGGED / SET UNLOGGED on a partitioned table", s.VersionsWhen(pgcontract.AtMost(pgcontract.Version17)))
	}
	e.Op = pgcontract.OpKindMetadata
	e.Message = "a partitioned table has no storage, so PostgreSQL 15-17 accept the statement and silently do nothing"
	return e
}

// ruleCluster is R-AT-CLUSTERON.
func ruleCluster(s engine.Scope, sub ir.Subcommand) engine.Effect {
	marked := "no index"
	if sub.Kind == ir.SubClusterOn && sub.Name != "" {
		marked = sub.Name
	}
	e := engine.NewEffect(s, "R-AT-CLUSTERON", fmt.Sprintf("%s is recorded as the index a later CLUSTER would use; no row moves now", marked))
	if marked != "no index" {
		e.Touches = append(e.Touches, s.IndexTarget(marked, pgcontract.LockShareUpdateExclusive, pgcontract.OpKindMetadata))
	}
	return e
}

// ruleWithoutOids is R-AT-WITHOUTOIDS.
func ruleWithoutOids(s engine.Scope, _ ir.Subcommand) engine.Effect {
	return engine.NewEffect(s, "R-AT-WITHOUTOIDS", "system OID columns no longer exist, so the statement is accepted and changes nothing")
}

// moveAll is what one "ALL IN TABLESPACE" object type moves.
type moveAll struct {
	code  pgcontract.Code
	noun  string
	kinds []pgcontract.RelationKind
}

var moveAllForms = map[pgcontract.Command]moveAll{
	"ALTER TABLE ALL IN TABLESPACE": {"R-AT-002", "table",
		[]pgcontract.RelationKind{pgcontract.RelationKindTable, pgcontract.RelationKindPartitionedTable}},
	"ALTER INDEX ALL IN TABLESPACE": {"R-IX-ALLTABLESPACE", "index",
		[]pgcontract.RelationKind{pgcontract.RelationKindIndex, pgcontract.RelationKindPartitionedIndex}},
	"ALTER MATERIALIZED VIEW ALL IN TABLESPACE": {"R-MV-ALLTABLESPACE", "materialized view",
		[]pgcontract.RelationKind{pgcontract.RelationKindMaterializedView}},
}

// classifyMoveAll classify "ALL IN TABLESPACE".
func classifyMoveAll(s engine.Scope) ([]pgcontract.Finding, []pgcontract.Error) {
	sub := ir.Subcommand{}
	if len(s.Statement.Subcommands) > 0 {
		sub = s.Statement.Subcommands[0]
	}
	owner := ""
	for _, option := range sub.Options {
		if option.Name == "owned_by" {
			owner = option.Value
		}
	}

	form := moveAllForms[s.Statement.Command]
	relations, exact := s.Catalog.RelationsInTablespace(sub.Name, owner, form.kinds)
	message := fmt.Sprintf("every %s in tablespace %q is copied to %q under ACCESS EXCLUSIVE; nothing else in %q moves", form.noun, sub.Name, sub.Value, sub.Name)
	if !exact {
		message += fmt.Sprintf(" — the snapshot has no tablespace named %q, so this is every %s that is not in the database's default one", sub.Name, form.noun)
	}

	var targets []pgcontract.Target
	for _, rel := range relations {
		kind := pgcontract.RelationKindFromRelkind(rel.Kind)
		targets = append(targets, pgcontract.Target{
			Relation: rel.Contract(),
			Lock:     pgcontract.LockAccessExclusive,
			// A partitioned parent has no file to copy, and its partitions are not carried along:
			// they move only when they are in the named tablespace themselves.
			OpKind: engine.OpOn(pgcontract.OpKindRewrite, kind.IsPartitioned()),
			Role:   pgcontract.TargetRoleExpanded,
		})
	}
	return []pgcontract.Finding{{
		Code:     form.code,
		Severity: pgcontract.SeverityInfo,
		Message:  message,
		Targets:  targets,
	}}, nil
}
