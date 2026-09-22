package pg_classify

import (
	"fmt"

	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

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
	return classifyClauses(s, subcommandRules)
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
