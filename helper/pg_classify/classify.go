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
		return analysis
	}

	if classify := commandRules[statement.Command]; classify != nil {
		findings, errors := classify(c.scope(statement, context))
		analysis.Findings = findings
		analysis.Errors = errors
	}

	collapseStatementLocks(analysis.Findings)
	return analysis
}

// commandRules is the registry. A command with no entry produces no finding.
var commandRules = map[pg_contract.Command]func(scope) ([]pg_contract.Finding, []pg_contract.Error){
	"ALTER TABLE":                               classifyAlterTable,
	"ALTER TABLE RENAME":                        classifyAlterTable,
	"ALTER TABLE SET SCHEMA":                    classifyAlterTable,
	"ALTER TABLE ALL IN TABLESPACE":             classifyMoveAll,
	"ALTER INDEX ALL IN TABLESPACE":             classifyMoveAll,
	"ALTER MATERIALIZED VIEW ALL IN TABLESPACE": classifyMoveAll,
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
	return pg_contract.RelationKindFromRelkind(s.relation().Relation.Kind).IsPartitioned()
}

// resolve turns a name a subcommand wrote into a relation, through the search path in force.
func (s scope) resolve(ref pg_parse.RelationRef) pg_catalog.RelationInfo {
	return s.catalog.Resolve(ref.Name, s.context.SearchPath)
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
