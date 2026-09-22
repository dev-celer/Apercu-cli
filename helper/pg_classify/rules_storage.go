package pg_classify

import (
	"fmt"
	"strings"

	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// ruleTrigger is R-AT-TRIG and R-AT-TRIG-REP.
func ruleTrigger(s scope, sub pg_parse.Subcommand) effect {
	code := pg_contract.Code("R-AT-TRIG")
	if sub.Value == "REPLICA" || sub.Value == "ALWAYS" {
		code = "R-AT-TRIG-REP"
	}
	e := newEffect(s, code, "the trigger's firing mode changes in the catalog; writes on the table wait, reads do not")
	e.lock = pg_contract.LockShareRowExclusive
	return e
}

// ruleRule is R-AT-RULE.
func ruleRule(s scope, sub pg_parse.Subcommand) effect {
	return newEffect(s, "R-AT-RULE", fmt.Sprintf("rule %q changes its firing mode in the catalog", sub.Name))
}

// ruleRowSecurity is R-AT-RLS.
func ruleRowSecurity(s scope, _ pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-RLS", "row level security is switched on or off for the table; the policies themselves are untouched")
	e.recursion = parentOnly
	return e
}

// ruleForceRowSecurity is R-AT-RLS-FORCE.
func ruleForceRowSecurity(s scope, _ pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-RLS-FORCE", "row level security starts or stops applying to the table's owner as well")
	e.recursion = parentOnly
	return e
}

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
func relOptionLock(option pg_parse.Option) pg_contract.Lock {
	name := strings.ToLower(option.Name)
	if sueRelOptions[name] || strings.HasPrefix(name, "autovacuum_") {
		return pg_contract.LockShareUpdateExclusive
	}
	return pg_contract.LockAccessExclusive
}

// ruleRelOptions is R-AT-RELOPT.
func ruleRelOptions(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-RELOPT", "")
	e.lock = pg_contract.LockNone
	names := make([]string, 0, len(sub.Options))
	for _, option := range sub.Options {
		e.lock = pg_contract.MaxLock(e.lock, relOptionLock(option))
		names = append(names, option.Name)
	}
	if e.lock == pg_contract.LockNone {
		e.lock = pg_contract.LockAccessExclusive
	}

	if len(names) == 0 {
		e.message = "no storage parameter is named, so the statement takes the strongest lock anyway"
	} else {
		e.message = fmt.Sprintf("storage parameters %s are recorded on the relation", strings.Join(names, ", "))
	}
	if s.partitioned() {
		return e.reject("storage parameters cannot be specified for a partitioned table, which has no storage", pg_contract.AnyVersion)
	}
	return e
}

// ruleSetTablespace is R-AT-TABLESPACE.
func ruleSetTablespace(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-TABLESPACE", fmt.Sprintf("the heap is copied file by file into tablespace %q; the indexes are left where they are", sub.Value))
	e.op = pg_contract.OpKindRewrite
	e.recursion = parentOnly
	return e
}

// ruleSetAccessMethod is R-AT-ACCESSMETHOD.
func ruleSetAccessMethod(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-ACCESSMETHOD", "")
	e.recursion = parentOnly

	current := s.relation().Relation.AccessMethod
	if current != "" && strings.EqualFold(current, sub.Value) {
		e.message = fmt.Sprintf("the table already uses %q, so the statement changes nothing", current)
		return e
	}
	e.op = pg_contract.OpKindRewrite
	e.message = fmt.Sprintf("every tuple is rewritten into the %s format", sub.Value)
	return e
}

// ruleSetLogged is R-AT-LOGGED.
func ruleSetLogged(s scope, sub pg_parse.Subcommand) effect {
	target := "LOGGED"
	if sub.Kind == pg_parse.SubSetUnlogged {
		target = "UNLOGGED"
	}
	e := newEffect(s, "R-AT-LOGGED", fmt.Sprintf("the table becomes %s, which rewrites the heap and follows through to the sequences its identity and serial columns own", target))
	e.op = pg_contract.OpKindRewrite
	e.recursion = parentOnly

	if !s.partitioned() {
		return e
	}
	if s.atLeast18() {
		return e.reject("PostgreSQL 18 refuses SET LOGGED / SET UNLOGGED on a partitioned table", s.versionsWhen(pg_contract.AtMost(pg_contract.Version17)))
	}
	e.op = pg_contract.OpKindMetadata
	e.message = "a partitioned table has no storage, so PostgreSQL 15-17 accept the statement and silently do nothing"
	return e
}

// ruleCluster is R-AT-CLUSTERON.
func ruleCluster(s scope, sub pg_parse.Subcommand) effect {
	marked := "no index"
	if sub.Kind == pg_parse.SubClusterOn && sub.Name != "" {
		marked = sub.Name
	}
	e := newEffect(s, "R-AT-CLUSTERON", fmt.Sprintf("%s is recorded as the index a later CLUSTER would use; no row moves now", marked))
	e.lock = pg_contract.LockShareUpdateExclusive
	if marked != "no index" {
		e.extra = append(e.extra, s.indexTarget(marked, pg_contract.LockShareUpdateExclusive, pg_contract.OpKindMetadata))
	}
	if s.partitioned() {
		return e.reject("a partitioned table cannot be marked for clustering", pg_contract.AnyVersion)
	}
	return e
}

// ruleWithoutOids is R-AT-WITHOUTOIDS.
func ruleWithoutOids(s scope, _ pg_parse.Subcommand) effect {
	return newEffect(s, "R-AT-WITHOUTOIDS", "system OID columns no longer exist, so the statement is accepted and changes nothing")
}

// ruleOwner is R-AT-OWNER.
func ruleOwner(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-OWNER", fmt.Sprintf("the table and its indexes change owner to %s", sub.Value))
	e.recursion = parentOnly
	if !s.relation().Exists() {
		return e
	}
	for _, index := range s.catalog.IndexesOf(s.relation().Relation.OID) {
		rel, ok := s.catalog.ByOID(index.IndexRelID)
		if !ok {
			continue
		}
		e.extra = append(e.extra, pg_contract.Target{
			Relation: rel.Contract(),
			Lock:     pg_contract.LockAccessExclusive,
			OpKind:   pg_contract.OpKindMetadata,
			Role:     pg_contract.TargetRoleImplicit,
		})
	}
	return e
}

// ruleReplicaIdentity is R-AT-REPLIDENT.
func ruleReplicaIdentity(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-REPLIDENT", fmt.Sprintf("the replica identity becomes %s", sub.Value))
	e.recursion = parentOnly
	if sub.Value == "FULL" {
		e.message += "; every later UPDATE and DELETE will log the whole old row, which is a replication cost and not a lock cost"
	}
	if sub.Name != "" {
		e.message += fmt.Sprintf(", carried by index %q", sub.Name)
		e.extra = append(e.extra, s.indexTarget(sub.Name, pg_contract.LockShare, pg_contract.OpKindMetadata))
	}
	return e
}

// ruleRenameRelation is R-AT-RENAME.
func ruleRenameRelation(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-RENAME", fmt.Sprintf("the relation is renamed to %q in the catalog", sub.NewName))
	e.recursion = parentOnly
	if pg_contract.RelationKindFromRelkind(s.relation().Relation.Kind).IsIndex() {
		e.message += "; the target is an index, and ALTER INDEX … RENAME does the same under SHARE UPDATE EXCLUSIVE"
	}
	return e
}

// ruleSetSchema is R-AT-SETSCHEMA.
func ruleSetSchema(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-SETSCHEMA", fmt.Sprintf("the relation moves to schema %q in the catalog", sub.Value))
	e.recursion = parentOnly
	return e
}

// ruleInherit is R-AT-INHERIT.
func ruleInherit(s scope, sub pg_parse.Subcommand) effect {
	verb, parentLock := "starts inheriting from", pg_contract.LockShareUpdateExclusive
	if sub.Kind == pg_parse.SubDropInherit {
		verb, parentLock = "stops inheriting from", pg_contract.LockAccessShare
	}

	e := newEffect(s, "R-AT-INHERIT", "")
	parent := "an unnamed parent"
	if len(sub.Relations) > 0 {
		resolved := contract(s.resolve(sub.Relations[0]))
		parent = resolved.Name.String()
		e.extra = append(e.extra, pg_contract.Target{
			Relation: resolved,
			Lock:     parentLock,
			OpKind:   pg_contract.OpKindMetadata,
			Role:     pg_contract.TargetRoleDirect,
		})
	}
	e.message = fmt.Sprintf("the table %s %s; only the catalog edge changes", verb, parent)
	return e
}

// ruleOf is R-AT-OF.
func ruleOf(s scope, sub pg_parse.Subcommand) effect {
	if sub.Kind == pg_parse.SubDropOf {
		return newEffect(s, "R-AT-OF", "the table stops being a typed table; its columns are unchanged")
	}
	return newEffect(s, "R-AT-OF", fmt.Sprintf("the table becomes a typed table over %s; its columns already match", sub.Value))
}
