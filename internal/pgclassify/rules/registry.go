// Package rules holds one rule per statement or clause the classifier knows, and the registry
// mapping each command and subcommand to its rule.
package rules

import (
	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// Commands is the registry. A command with no entry produces no finding.
var Commands = map[pgcontract.Command]func(engine.Scope) ([]pgcontract.Finding, []pgcontract.Error){
	// ALTER TABLE.
	"ALTER TABLE":                               classifyAlterTable,
	"ALTER TABLE RENAME":                        classifyAlterTable,
	"ALTER TABLE SET SCHEMA":                    classifyAlterTable,
	"ALTER TABLE ALL IN TABLESPACE":             classifyMoveAll,
	"ALTER INDEX ALL IN TABLESPACE":             classifyMoveAll,
	"ALTER MATERIALIZED VIEW ALL IN TABLESPACE": classifyMoveAll,

	// index DDL.
	"CREATE INDEX":       engine.One(ruleCreateIndex),
	"DROP INDEX":         engine.One(ruleDropIndex),
	"ALTER INDEX":        classifyAlterIndex,
	"ALTER INDEX RENAME": engine.One(ruleRenameIndex),
	"REINDEX INDEX":      engine.One(ruleReindex),
	"REINDEX TABLE":      engine.One(ruleReindex),
	"REINDEX SCHEMA":     engine.One(ruleReindexWide),
	"REINDEX DATABASE":   engine.One(ruleReindexWide),
	"REINDEX SYSTEM":     engine.One(ruleReindexWide),

	// table creation and destruction.
	"CREATE TABLE":    classifyCreateTable,
	"CREATE TABLE AS": engine.One(ruleCreateTableAs),
	"DROP TABLE":      engine.One(ruleDropTable),

	// views, matviews, sequences.
	"CREATE VIEW":               engine.One(ruleCreateView),
	"DROP VIEW":                 engine.One(ruleDropView),
	"DROP MATERIALIZED VIEW":    engine.One(ruleDropView),
	"CREATE MATERIALIZED VIEW":  engine.One(ruleCreateMatView),
	"REFRESH MATERIALIZED VIEW": engine.One(ruleRefreshMatView),
	"CREATE SEQUENCE":           engine.One(ruleCreateSequence),
	"ALTER SEQUENCE":            engine.One(ruleAlterSequence),
	"DROP SEQUENCE":             engine.One(ruleDropSequence),

	// the objects that hang off a table.
	"CREATE TRIGGER":       engine.One(ruleCreateTrigger),
	"DROP TRIGGER":         engine.One(ruleDropTrigger),
	"ALTER TRIGGER RENAME": engine.One(ruleAlterTrigger),
	"CREATE POLICY":        engine.One(rulePolicy),
	"ALTER POLICY":         engine.One(rulePolicy),
	"ALTER POLICY RENAME":  engine.One(rulePolicy),
	"DROP POLICY":          engine.One(rulePolicy),
	"CREATE RULE":          engine.One(ruleObjectRule),
	"ALTER RULE RENAME":    engine.One(ruleObjectRule),
	"DROP RULE":            engine.One(ruleObjectRule),
	"CREATE STATISTICS":    engine.One(ruleStatistics),
	"ALTER STATISTICS":     engine.One(ruleStatistics),
	"DROP STATISTICS":      engine.One(ruleStatistics),
	"GRANT":                engine.One(ruleGrant),
	"REVOKE":               engine.One(ruleGrant),

	"ALTER DEFAULT PRIVILEGES": engine.One(ruleDefaultPrivileges),

	// types and domains.
	"ALTER TYPE ADD VALUE":    engine.One(ruleAddEnumValue),
	"ALTER TYPE RENAME VALUE": engine.One(ruleRenameEnumValue),
	"ALTER TYPE":              classifyAlterType,
	"ALTER DOMAIN":            classifyAlterDomain,
	"DROP TYPE":               engine.One(ruleDropType),
	"DROP DOMAIN":             engine.One(ruleDropType),

	// publications, subscriptions, extensions, schemas.
	"CREATE PUBLICATION":  engine.One(rulePublication),
	"ALTER PUBLICATION":   engine.One(rulePublication),
	"CREATE SUBSCRIPTION": engine.One(ruleSubscription),
	"ALTER SUBSCRIPTION":  engine.One(ruleSubscription),
	"CREATE EXTENSION":    engine.One(ruleExtension),
	"ALTER EXTENSION":     engine.One(ruleExtension),
	"CREATE SCHEMA":       engine.One(ruleSchema),
	"DROP SCHEMA":         engine.One(ruleSchema),

	// maintenance.
	"VACUUM":    engine.One(ruleVacuum),
	"ANALYZE":   engine.One(ruleAnalyze),
	"CLUSTER":   engine.One(ruleClusterCommand),
	"TRUNCATE":  engine.One(ruleTruncate),
	"LOCK":      engine.One(ruleLock),
	"COPY FROM": engine.One(ruleCopyFrom),
	"COPY TO":   engine.One(ruleCopyTo),

	// data backfills.
	"INSERT":     engine.One(ruleDML),
	"UPDATE":     engine.One(ruleDML),
	"DELETE":     engine.One(ruleDML),
	"MERGE":      engine.One(ruleDML),
	"SELECT FOR": engine.One(ruleSelectFor),
}

// commentedObjects are the objects COMMENT ON names a relation for.
var commentedObjects = []pgcontract.Command{
	"TABLE", "INDEX", "VIEW", "MATERIALIZED VIEW", "SEQUENCE", "FOREIGN TABLE",
	"COLUMN", "CONSTRAINT", "TRIGGER", "POLICY", "RULE",
}

func init() {
	for _, object := range commentedObjects {
		Commands["COMMENT ON "+object] = engine.One(ruleComment)
	}
}

var subcommandRules = map[ir.SubKind]func(engine.Scope, ir.Subcommand) engine.Effect{
	// columns.
	ir.SubAddColumn:             ruleAddColumn,
	ir.SubDropColumn:            ruleDropColumn,
	ir.SubAlterColumnType:       ruleAlterColumnType,
	ir.SubSetDefault:            ruleSetDefault,
	ir.SubDropDefault:           ruleDropDefault,
	ir.SubSetNotNull:            ruleSetNotNull,
	ir.SubDropNotNull:           ruleDropNotNull,
	ir.SubSetExpression:         ruleSetExpression,
	ir.SubDropExpression:        ruleDropExpression,
	ir.SubAddIdentity:           ruleAddIdentity,
	ir.SubSetIdentity:           ruleSetIdentity,
	ir.SubDropIdentity:          ruleDropIdentity,
	ir.SubSetStatistics:         ruleSetStatistics,
	ir.SubSetAttributeOptions:   ruleAttributeOptions,
	ir.SubResetAttributeOptions: ruleAttributeOptions,
	ir.SubSetStorage:            ruleSetStorage,
	ir.SubSetCompression:        ruleSetCompression,
	ir.SubRenameColumn:          ruleRenameColumn,

	// constraints.
	ir.SubAddConstraint:      ruleAddConstraint,
	ir.SubValidateConstraint: ruleValidateConstraint,
	ir.SubDropConstraint:     ruleDropConstraint,
	ir.SubAlterConstraint:    ruleAlterConstraint,
	ir.SubRenameConstraint:   ruleRenameConstraint,

	// triggers, rules, row-level security.
	ir.SubEnableTrigger:      ruleTrigger,
	ir.SubDisableTrigger:     ruleTrigger,
	ir.SubEnableRule:         ruleRule,
	ir.SubDisableRule:        ruleRule,
	ir.SubEnableRowSecurity:  ruleRowSecurity,
	ir.SubDisableRowSecurity: ruleRowSecurity,
	ir.SubForceRowSecurity:   ruleForceRowSecurity,
	ir.SubNoForceRowSecurity: ruleForceRowSecurity,

	// storage, layout, ownership.
	ir.SubSetRelOptions:   ruleRelOptions,
	ir.SubResetRelOptions: ruleRelOptions,
	ir.SubSetTablespace:   ruleSetTablespace,
	ir.SubSetAccessMethod: ruleSetAccessMethod,
	ir.SubSetLogged:       ruleSetLogged,
	ir.SubSetUnlogged:     ruleSetLogged,
	ir.SubClusterOn:       ruleCluster,
	ir.SubDropCluster:     ruleCluster,
	ir.SubDropOids:        ruleWithoutOids,
	ir.SubChangeOwner:     ruleOwner,
	ir.SubReplicaIdentity: ruleReplicaIdentity,
	ir.SubRenameRelation:  ruleRenameRelation,
	ir.SubSetSchema:       ruleSetSchema,

	// partitions and inheritance.
	ir.SubAttachPartition: ruleAttachPartition,
	ir.SubDetachPartition: ruleDetachPartition,
	ir.SubAddInherit:      ruleInherit,
	ir.SubDropInherit:     ruleInherit,
	ir.SubAddOf:           ruleOf,
	ir.SubDropOf:          ruleOf,
}

// classifyAlterTable decompose the statement, classify each clause on its own, and
// let the statement hold the strongest lock any of them asked for.
func classifyAlterTable(s engine.Scope) ([]pgcontract.Finding, []pgcontract.Error) {
	return engine.ClassifyClauses(s, subcommandRules)
}

// indexClauseRules is the ALTER INDEX half of the clause registry.
var indexClauseRules = map[ir.SubKind]func(engine.Scope, ir.Subcommand) engine.Effect{
	ir.SubSetRelOptions:   ruleIndexOptions,
	ir.SubResetRelOptions: ruleIndexOptions,
	ir.SubSetStatistics:   ruleIndexStatistics,
	ir.SubSetTablespace:   ruleIndexTablespace,
	ir.SubAttachPartition: ruleIndexAttach,
}

// classifyAlterIndex is ALTER INDEX, it classifies every subcommands.
func classifyAlterIndex(s engine.Scope) ([]pgcontract.Finding, []pgcontract.Error) {
	return engine.ClassifyClauses(s, indexClauseRules)
}

// typeClauseRules is the ALTER TYPE half of the clause registry.
var typeClauseRules = map[ir.SubKind]func(engine.Scope, ir.Subcommand) engine.Effect{
	ir.SubAddColumn:       ruleAddAttribute,
	ir.SubDropColumn:      ruleDropAttribute,
	ir.SubAlterColumnType: ruleAlterAttribute,
}

// classifyAlterType is ALTER TYPE … ATTRIBUTE.
func classifyAlterType(s engine.Scope) ([]pgcontract.Finding, []pgcontract.Error) {
	return engine.ClassifyClauses(s, typeClauseRules)
}

// domainClauseRules is the ALTER DOMAIN half of the clause registry.
var domainClauseRules = map[ir.SubKind]func(engine.Scope, ir.Subcommand) engine.Effect{
	ir.SubAddConstraint: ruleDomainAddConstraint,
}

// classifyAlterDomain is ALTER DOMAIN.
func classifyAlterDomain(s engine.Scope) ([]pgcontract.Finding, []pgcontract.Error) {
	return engine.ClassifyClauses(s, domainClauseRules)
}
