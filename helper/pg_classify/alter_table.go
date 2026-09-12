package pg_classify

import (
	"fmt"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

type recursion uint8

const (
	// recurses reaches the partitions and the inheritance children.
	recurses recursion = iota
	// parentOnly stops at the parent table.
	parentOnly
	// rejected is a clause the server refuses on a partitioned parent.
	rejected
)

// effect is what one subcommand does.
type effect struct {
	code      pg_contract.Code
	message   string
	lock      pg_contract.Lock
	op        pg_contract.OpKind
	recursion recursion
	// partitionLock overrides the lock the partitions take. LockNone leaves them on the table's.
	partitionLock pg_contract.Lock
	// childrenUnderOnly marks a clause that stops at the named table under ONLY but still opens the children.
	// They are locked exactly as hard as they would have been without the ONLY.
	childrenUnderOnly bool
	// extra are the relations the clause names or implies beyond the table itself.
	extra []pg_contract.Target
	// errors are what the server would refuse.
	errors []pg_contract.Error
	// silent drops the clause from the findings: it errored and never runs.
	silent bool
}

// newEffect is the shape most of ALTER TABLE has, which each rule then contradicts where it must.
func newEffect(code pg_contract.Code, message string) effect {
	return effect{
		code:      code,
		message:   message,
		lock:      pg_contract.LockAccessExclusive,
		op:        pg_contract.OpKindMetadata,
		recursion: recurses,
	}
}

// reject is the REJECTED branch: the clause does not run, and the statement it sits in fails with it.
func (e effect) reject(reason string, versions pg_contract.VersionRange) effect {
	e.recursion = rejected
	e.silent = true
	e.errors = append(e.errors, pg_contract.Error{Code: e.code, Message: reason, Versions: versions})
	return e
}

var subcommandRules = map[pg_parse.SubKind]func(scope, pg_parse.Subcommand) effect{
	// columns.
	pg_parse.SubAddColumn:             ruleAddColumn,
	pg_parse.SubDropColumn:            ruleDropColumn,
	pg_parse.SubAlterColumnType:       ruleAlterColumnType,
	pg_parse.SubSetDefault:            ruleSetDefault,
	pg_parse.SubDropDefault:           ruleDropDefault,
	pg_parse.SubSetNotNull:            ruleSetNotNull,
	pg_parse.SubDropNotNull:           ruleDropNotNull,
	pg_parse.SubSetExpression:         ruleSetExpression,
	pg_parse.SubDropExpression:        ruleDropExpression,
	pg_parse.SubAddIdentity:           ruleAddIdentity,
	pg_parse.SubSetIdentity:           ruleSetIdentity,
	pg_parse.SubDropIdentity:          ruleDropIdentity,
	pg_parse.SubSetStatistics:         ruleSetStatistics,
	pg_parse.SubSetAttributeOptions:   ruleAttributeOptions,
	pg_parse.SubResetAttributeOptions: ruleAttributeOptions,
	pg_parse.SubSetStorage:            ruleSetStorage,
	pg_parse.SubSetCompression:        ruleSetCompression,
	pg_parse.SubRenameColumn:          ruleRenameColumn,

	// constraints.
	pg_parse.SubAddConstraint:      ruleAddConstraint,
	pg_parse.SubValidateConstraint: ruleValidateConstraint,
	pg_parse.SubDropConstraint:     ruleDropConstraint,
	pg_parse.SubAlterConstraint:    ruleAlterConstraint,
	pg_parse.SubRenameConstraint:   ruleRenameConstraint,

	// triggers, rules, row-level security.
	pg_parse.SubEnableTrigger:      ruleTrigger,
	pg_parse.SubDisableTrigger:     ruleTrigger,
	pg_parse.SubEnableRule:         ruleRule,
	pg_parse.SubDisableRule:        ruleRule,
	pg_parse.SubEnableRowSecurity:  ruleRowSecurity,
	pg_parse.SubDisableRowSecurity: ruleRowSecurity,
	pg_parse.SubForceRowSecurity:   ruleForceRowSecurity,
	pg_parse.SubNoForceRowSecurity: ruleForceRowSecurity,

	// storage, layout, ownership.
	pg_parse.SubSetRelOptions:   ruleRelOptions,
	pg_parse.SubResetRelOptions: ruleRelOptions,
	pg_parse.SubSetTablespace:   ruleSetTablespace,
	pg_parse.SubSetAccessMethod: ruleSetAccessMethod,
	pg_parse.SubSetLogged:       ruleSetLogged,
	pg_parse.SubSetUnlogged:     ruleSetLogged,
	pg_parse.SubClusterOn:       ruleCluster,
	pg_parse.SubDropCluster:     ruleCluster,
	pg_parse.SubDropOids:        ruleWithoutOids,
	pg_parse.SubChangeOwner:     ruleOwner,
	pg_parse.SubReplicaIdentity: ruleReplicaIdentity,
	pg_parse.SubRenameRelation:  ruleRenameRelation,
	pg_parse.SubSetSchema:       ruleSetSchema,

	// partitions and inheritance.
	pg_parse.SubAttachPartition: ruleAttachPartition,
	pg_parse.SubDetachPartition: ruleDetachPartition,
	pg_parse.SubAddInherit:      ruleInherit,
	pg_parse.SubDropInherit:     ruleInherit,
	pg_parse.SubAddOf:           ruleOf,
	pg_parse.SubDropOf:          ruleOf,
}

// classifyAlterTable decompose the statement, classify each clause on its own, and
// let the statement hold the strongest lock any of them asked for.
func classifyAlterTable(s scope) ([]pg_contract.Finding, []pg_contract.Error) {
	var findings []pg_contract.Finding
	var errors []pg_contract.Error

	for _, sub := range s.statement.Subcommands {
		rule := subcommandRules[sub.Kind]
		if rule == nil {
			continue
		}
		result := rule(s, sub)
		if reason, versions, refused := s.onlyRefusal(sub); refused {
			result = result.reject(reason, versions)
		}
		errors = append(errors, result.errors...)
		if result.silent {
			continue
		}
		findings = append(findings, pg_contract.Finding{
			Code:     result.code,
			Severity: pg_contract.SeverityInfo,
			Message:  result.message,
			Targets:  dedupeTargets(result.targets(s)),
		})
	}
	return findings, errors
}

// onlyRefusals is what ALTER TABLE ONLY cannot do to a table that has children.
var onlyRefusals = map[pg_parse.SubKind]string{
	pg_parse.SubAddColumn:       "the column has to be added to the child tables too",
	pg_parse.SubAlterColumnType: "the type of an inherited column has to be changed in the child tables too",
	pg_parse.SubRenameColumn:    "an inherited column has to be renamed in the child tables too",
	pg_parse.SubAddConstraint:   "the constraint has to be added to the child tables too, NOT VALID included",
}

// onlyRefusal is the REJECTED branch
func (s scope) onlyRefusal(sub pg_parse.Subcommand) (string, pg_contract.VersionRange, bool) {
	if !s.only() || len(s.descendants()) == 0 {
		return "", pg_contract.AnyVersion, false
	}
	if reason, refused := onlyRefusals[sub.Kind]; refused {
		return reason, pg_contract.AnyVersion, true
	}
	if !s.partitioned() {
		// A classic inheritance parent accepts everything else on its own: dropping a column or a
		// constraint from it simply stops the children inheriting it.
		return "", pg_contract.AnyVersion, false
	}

	switch sub.Kind {
	case pg_parse.SubDropColumn:
		return "a column cannot be dropped from only the partitioned table while partitions exist", pg_contract.AnyVersion, true
	case pg_parse.SubDropConstraint, pg_parse.SubDropNotNull:
		// The same 42P16 both spellings raise, lifted in 18.
		if s.atLeast18() {
			return "", pg_contract.AnyVersion, false
		}
		return "PostgreSQL 15-17 cannot remove a constraint from only the partitioned table while partitions exist",
			s.versionsWhen(pg_contract.AtLeast(pg_contract.Version18)), true
	}
	return "", pg_contract.AnyVersion, false
}

// targets is the named table, whatever the clause dragged in, and the descendants a recursing clause reaches.
func (e effect) targets(s scope) []pg_contract.Target {
	targets := []pg_contract.Target{{
		Relation: contract(s.relation()),
		Lock:     e.lock,
		OpKind:   opOn(e.op, s.partitioned()),
		Role:     pg_contract.TargetRoleDirect,
	}}
	targets = append(targets, e.extra...)

	if e.recursion != recurses || (s.only() && !e.childrenUnderOnly) {
		return targets
	}
	for _, child := range s.descendants() {
		lock := e.lock
		if e.partitionLock != pg_contract.LockNone && child.partition {
			lock = e.partitionLock
		}
		targets = append(targets, pg_contract.Target{
			Relation: child.relation,
			Lock:     lock,
			OpKind:   opOn(e.op, child.relation.Kind.IsPartitioned()),
			Role:     pg_contract.TargetRoleExpanded,
		})
	}
	return targets
}

// opOn downgrades work that is measured in rows to metadata on a partitioned parent, which holds
// no rows: the scan or the rewrite happens in the leaves, and they are targets of their own.
func opOn(op pg_contract.OpKind, partitioned bool) pg_contract.OpKind {
	if partitioned && op.ScalesWithTableSize() {
		return pg_contract.OpKindMetadata
	}
	return op
}

// descendant is one relation a recursing clause reaches.
type descendant struct {
	relation pg_contract.Relation
	// partition separates declarative partitioning from classic INHERITS.
	partition bool
}

// descendants is the inherited children an alter table locks.
func (s scope) descendants() []descendant {
	if !s.relation().Exists() {
		return nil
	}
	partitions := map[pg_catalog.OID]bool{}
	for _, oid := range s.catalog.PartitionDescendants(s.relation().Relation.OID) {
		partitions[oid] = true
	}
	var out []descendant
	for _, oid := range s.catalog.Descendants(s.relation().Relation.OID) {
		rel, ok := s.catalog.ByOID(oid)
		if !ok {
			continue
		}
		out = append(out, descendant{relation: rel.Contract(), partition: partitions[oid]})
	}
	return out
}

// dedupeTargets collapse relation, carrying the strongest lock and the most severe operation.
func dedupeTargets(targets []pg_contract.Target) []pg_contract.Target {
	out := make([]pg_contract.Target, 0, len(targets))
	at := map[pg_contract.Relation]int{}
	for _, target := range targets {
		index, seen := at[target.Relation]
		if !seen {
			at[target.Relation] = len(out)
			out = append(out, target)
			continue
		}
		out[index].Lock = pg_contract.MaxLock(out[index].Lock, target.Lock)
		out[index].OpKind = pg_contract.MaxOpKind(out[index].OpKind, target.OpKind)
	}
	return out
}

// moveAll is what one "ALL IN TABLESPACE" object type moves.
type moveAll struct {
	code  pg_contract.Code
	noun  string
	kinds []pg_contract.RelationKind
}

var moveAllForms = map[pg_contract.Command]moveAll{
	"ALTER TABLE ALL IN TABLESPACE": {"R-AT-002", "table",
		[]pg_contract.RelationKind{pg_contract.RelationKindTable, pg_contract.RelationKindPartitionedTable}},
	"ALTER INDEX ALL IN TABLESPACE": {"R-IX-ALLTABLESPACE", "index",
		[]pg_contract.RelationKind{pg_contract.RelationKindIndex, pg_contract.RelationKindPartitionedIndex}},
	"ALTER MATERIALIZED VIEW ALL IN TABLESPACE": {"R-MV-ALLTABLESPACE", "materialized view",
		[]pg_contract.RelationKind{pg_contract.RelationKindMaterializedView}},
}

// classifyMoveAll classify "ALL IN TABLESPACE".
func classifyMoveAll(s scope) ([]pg_contract.Finding, []pg_contract.Error) {
	sub := pg_parse.Subcommand{}
	if len(s.statement.Subcommands) > 0 {
		sub = s.statement.Subcommands[0]
	}
	owner := ""
	for _, option := range sub.Options {
		if option.Name == "owned_by" {
			owner = option.Value
		}
	}

	form := moveAllForms[s.statement.Command]
	relations, exact := s.catalog.RelationsInTablespace(sub.Name, owner, form.kinds)
	message := fmt.Sprintf("every %s in tablespace %q is copied to %q under ACCESS EXCLUSIVE; nothing else in %q moves", form.noun, sub.Name, sub.Value, sub.Name)
	if !exact {
		message += fmt.Sprintf(" — the snapshot has no tablespace named %q, so this is every %s that is not in the database's default one", sub.Name, form.noun)
	}

	var targets []pg_contract.Target
	for _, rel := range relations {
		kind := pg_contract.RelationKindFromRelkind(rel.Kind)
		targets = append(targets, pg_contract.Target{
			Relation: rel.Contract(),
			Lock:     pg_contract.LockAccessExclusive,
			// A partitioned parent has no file to copy, and its partitions are not carried along:
			// they move only when they are in the named tablespace themselves.
			OpKind: opOn(pg_contract.OpKindRewrite, kind.IsPartitioned()),
			Role:   pg_contract.TargetRoleExpanded,
		})
	}
	return []pg_contract.Finding{{
		Code:     form.code,
		Severity: pg_contract.SeverityInfo,
		Message:  message,
		Targets:  targets,
	}}, nil
}
