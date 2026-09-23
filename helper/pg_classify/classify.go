package pg_classify

import (
	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// Classifier walks a migration in order and produces one record per statement.
type Classifier struct {
	catalog *pg_catalog.Catalog
	session *Session
}

func NewClassifier(catalog *pg_catalog.Catalog) *Classifier {
	return &Classifier{catalog: catalog, session: NewSession(catalog)}
}

// Analyze classifies a whole migration.
func (c *Classifier) Analyze(statements []pg_parse.Statement) []pg_contract.StatementAnalysis {
	out := make([]pg_contract.StatementAnalysis, 0, len(statements))
	for _, statement := range statements {
		out = append(out, c.Next(statement))
	}
	return out
}

// Next classifies one statement and advances the session past it.
func (c *Classifier) Next(statement pg_parse.Statement) pg_contract.StatementAnalysis {
	context := c.session.Next(statement)

	analysis := pg_contract.StatementAnalysis{
		RawSQL:   statement.RawSQL,
		TxnGroup: pg_contract.TxnGroup(context.Group),
		Command:  statement.Command,
	}
	for _, sub := range statement.Subcommands {
		analysis.Subcommands = append(analysis.Subcommands, sub.Kind.String())
	}
	if !statement.Parsed() {
		c.session.Declare(statement, context)
		return analysis
	}

	if classify := commandRules[statement.Command]; classify != nil {
		findings, errors := classify(c.scope(statement, context))
		analysis.Findings = findings
		analysis.Errors = errors
	}
	c.session.Declare(statement, context)

	collapseStatementLocks(analysis.Findings)
	return analysis
}

// commandRules is the registry. A command with no entry produces no finding.
var commandRules = map[pg_contract.Command]func(scope) ([]pg_contract.Finding, []pg_contract.Error){
	// ALTER TABLE.
	"ALTER TABLE":                               classifyAlterTable,
	"ALTER TABLE RENAME":                        classifyAlterTable,
	"ALTER TABLE SET SCHEMA":                    classifyAlterTable,
	"ALTER TABLE ALL IN TABLESPACE":             classifyMoveAll,
	"ALTER INDEX ALL IN TABLESPACE":             classifyMoveAll,
	"ALTER MATERIALIZED VIEW ALL IN TABLESPACE": classifyMoveAll,

	// index DDL.
	"CREATE INDEX":       one(ruleCreateIndex),
	"DROP INDEX":         one(ruleDropIndex),
	"ALTER INDEX":        classifyAlterIndex,
	"ALTER INDEX RENAME": one(ruleRenameIndex),
	"REINDEX INDEX":      one(ruleReindex),
	"REINDEX TABLE":      one(ruleReindex),
	"REINDEX SCHEMA":     one(ruleReindexWide),
	"REINDEX DATABASE":   one(ruleReindexWide),
	"REINDEX SYSTEM":     one(ruleReindexWide),

	// table creation and destruction.
	"CREATE TABLE":    classifyCreateTable,
	"CREATE TABLE AS": one(ruleCreateTableAs),
	"DROP TABLE":      one(ruleDropTable),

	// views, matviews, sequences.
	"CREATE VIEW":               one(ruleCreateView),
	"DROP VIEW":                 one(ruleDropView),
	"DROP MATERIALIZED VIEW":    one(ruleDropView),
	"CREATE MATERIALIZED VIEW":  one(ruleCreateMatView),
	"REFRESH MATERIALIZED VIEW": one(ruleRefreshMatView),
	"CREATE SEQUENCE":           one(ruleCreateSequence),
	"ALTER SEQUENCE":            one(ruleAlterSequence),
	"DROP SEQUENCE":             one(ruleDropSequence),

	// the objects that hang off a table.
	"CREATE TRIGGER":       one(ruleCreateTrigger),
	"DROP TRIGGER":         one(ruleDropTrigger),
	"ALTER TRIGGER RENAME": one(ruleAlterTrigger),
	"CREATE POLICY":        one(rulePolicy),
	"ALTER POLICY":         one(rulePolicy),
	"ALTER POLICY RENAME":  one(rulePolicy),
	"DROP POLICY":          one(rulePolicy),
	"CREATE RULE":          one(ruleObjectRule),
	"ALTER RULE RENAME":    one(ruleObjectRule),
	"DROP RULE":            one(ruleObjectRule),
	"CREATE STATISTICS":    one(ruleStatistics),
	"ALTER STATISTICS":     one(ruleStatistics),
	"DROP STATISTICS":      one(ruleStatistics),
	"GRANT":                one(ruleGrant),
	"REVOKE":               one(ruleGrant),

	"ALTER DEFAULT PRIVILEGES": one(ruleDefaultPrivileges),

	// types and domains.
	"ALTER TYPE ADD VALUE":    one(ruleAddEnumValue),
	"ALTER TYPE RENAME VALUE": one(ruleRenameEnumValue),
	"ALTER TYPE":              classifyAlterType,
	"ALTER DOMAIN":            classifyAlterDomain,
	"DROP TYPE":               one(ruleDropType),
	"DROP DOMAIN":             one(ruleDropType),

	// publications, subscriptions, extensions, schemas.
	"CREATE PUBLICATION":  one(rulePublication),
	"ALTER PUBLICATION":   one(rulePublication),
	"CREATE SUBSCRIPTION": one(ruleSubscription),
	"ALTER SUBSCRIPTION":  one(ruleSubscription),
	"CREATE EXTENSION":    one(ruleExtension),
	"CREATE SCHEMA":       one(ruleSchema),
	"DROP SCHEMA":         one(ruleSchema),

	// maintenance.
	"VACUUM":    one(ruleVacuum),
	"ANALYZE":   one(ruleAnalyze),
	"CLUSTER":   one(ruleClusterCommand),
	"TRUNCATE":  one(ruleTruncate),
	"LOCK":      one(ruleLock),
	"COPY FROM": one(ruleCopyFrom),
	"COPY TO":   one(ruleCopyTo),

	// data backfills.
	"INSERT":     one(ruleDML),
	"UPDATE":     one(ruleDML),
	"DELETE":     one(ruleDML),
	"MERGE":      one(ruleDML),
	"SELECT FOR": one(ruleSelectFor),
}

// commentedObjects are the objects COMMENT ON names a relation for.
var commentedObjects = []pg_contract.Command{
	"TABLE", "INDEX", "VIEW", "MATERIALIZED VIEW", "SEQUENCE", "FOREIGN TABLE",
	"COLUMN", "CONSTRAINT", "TRIGGER", "POLICY", "RULE",
}

func init() {
	for _, object := range commentedObjects {
		commandRules["COMMENT ON "+object] = one(ruleComment)
	}
}

type scope struct {
	catalog   *pg_catalog.Catalog
	context   Context
	statement pg_parse.Statement
	// version is production's. It is VersionUnknown when production was unreachable.
	version pg_contract.Version
	// relations is statement.Relations resolved through the search path.
	relations []pg_catalog.RelationInfo
}

func (c *Classifier) scope(statement pg_parse.Statement, context Context) scope {
	s := scope{
		catalog:   c.catalog,
		context:   context,
		statement: statement,
		version:   c.catalog.Version(),
	}
	for _, named := range statement.Relations {
		s.relations = append(s.relations, c.catalog.Resolve(named.Name, context.SearchPath))
	}
	return s
}

// relation is the first relation the statement names.
func (s scope) relation() pg_catalog.RelationInfo {
	if len(s.relations) == 0 {
		return pg_catalog.RelationInfo{}
	}
	return s.relations[0]
}

// only is the ONLY keyword, which stops the statement from reaching partitions and children.
func (s scope) only() bool {
	return len(s.statement.Relations) > 0 && s.statement.Relations[0].Only
}

// partitioned reports whether the named relation is a partitioned parent.
func (s scope) partitioned() bool {
	return partitionedInfo(s.relation())
}

// partitionedInfo is the same question for any resolved relation.
func partitionedInfo(info pg_catalog.RelationInfo) bool {
	return pg_contract.RelationKindFromRelkind(info.Relation.Kind).IsPartitioned()
}

// resolve turns a name a subcommand wrote into a relation, through the search path in force.
func (s scope) resolve(ref pg_parse.RelationRef) pg_catalog.RelationInfo {
	return s.catalog.Resolve(ref.Name, s.context.SearchPath)
}

// descendant is one relation a recursing clause reaches.
type descendant struct {
	relation pg_contract.Relation
	// partition separates declarative partitioning from classic INHERITS.
	partition bool
}

// descendants is the inherited children an alter table locks.
func (s scope) descendants() []descendant {
	return s.descendantsOf(s.relation())
}

// descendantsOf is the same for any relation a rule reached on its own.
func (s scope) descendantsOf(info pg_catalog.RelationInfo) []descendant {
	if !info.Exists() {
		return nil
	}
	partitions := map[pg_catalog.OID]bool{}
	for _, oid := range s.catalog.PartitionDescendants(info.Relation.OID) {
		partitions[oid] = true
	}
	var out []descendant
	for _, oid := range s.catalog.Descendants(info.Relation.OID) {
		rel, ok := s.catalog.ByOID(oid)
		if !ok {
			continue
		}
		out = append(out, descendant{relation: rel.Contract(), partition: partitions[oid]})
	}
	return out
}

// contract is a resolved relation in the vocabulary a finding speaks.
func contract(info pg_catalog.RelationInfo) pg_contract.Relation {
	return pg_contract.Relation{
		Name: info.Name,
		Kind: pg_contract.RelationKindFromRelkind(info.Relation.Kind),
	}
}

func (s scope) atLeast18() bool {
	return s.version == pg_contract.VersionUnknown || s.version >= pg_contract.Version18
}

func (s scope) versionsWhen(conditional pg_contract.VersionRange) pg_contract.VersionRange {
	if s.version != pg_contract.VersionUnknown {
		return pg_contract.AnyVersion
	}
	return conditional
}

// collapseStatementLocks keep the strongest lock for every relation a statement lock.
func collapseStatementLocks(findings []pg_contract.Finding) {
	strongest := map[pg_contract.Relation]pg_contract.Lock{}
	for _, finding := range findings {
		for _, target := range finding.Targets {
			strongest[target.Relation] = pg_contract.MaxLock(strongest[target.Relation], target.Lock)
		}
	}
	for _, finding := range findings {
		for i, target := range finding.Targets {
			finding.Targets[i].Lock = strongest[target.Relation]
		}
	}
}
