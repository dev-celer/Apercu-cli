package pgclassify

import (
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/rules"
	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgclassify/session"
	"apercu-cli/internal/pgclassify/shadow"
	"apercu-cli/internal/pgclassify/stmt"
	"apercu-cli/internal/pgclassify/validate"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// Classifier walks a migration in order and produces one record per statement.
type Classifier struct {
	catalog   *pgcatalog.Catalog
	session   *session.Session
	relations *shadow.Relations
	checks    *shadow.Checks
}

func NewClassifier(catalog *pgcatalog.Catalog) *Classifier {
	return &Classifier{catalog: catalog, session: session.NewSession(catalog), relations: shadow.NewRelations(catalog), checks: shadow.NewChecks()}
}

// Analyze classifies a whole migration.
func (c *Classifier) Analyze(statements []ir.Statement) []pgcontract.StatementAnalysis {
	out := make([]pgcontract.StatementAnalysis, 0, len(statements))
	for _, statement := range statements {
		out = append(out, c.Next(statement))
	}
	return out
}

// Next classifies one statement that had its protocol cycle to itself, and advances the session past it.
func (c *Classifier) Next(statement ir.Statement) pgcontract.StatementAnalysis {
	return c.classify(statement, c.session.Next(statement))
}

// NextInCycle classifies one statement of the given protocol cycle and advances the session past it.
func (c *Classifier) NextInCycle(statement ir.Statement, cycle int) pgcontract.StatementAnalysis {
	return c.classify(statement, c.session.NextInCycle(statement, cycle))
}

// classify produces the record of one statement running under the context the session handed out.
func (c *Classifier) classify(statement ir.Statement, context session.Context) pgcontract.StatementAnalysis {
	analysis := pgcontract.StatementAnalysis{
		RawSQL:        statement.RawSQL,
		TxnGroup:      pgcontract.TxnGroup(context.Group),
		InTransaction: context.InTransaction,
		Command:       statement.Command,
	}
	for _, sub := range statement.Subcommands {
		analysis.Subcommands = append(analysis.Subcommands, sub.Kind.String())
	}
	if !statement.Parsed() {
		c.relations.Declare(statement, context)
		c.checks.Blind = true
		return analysis
	}

	ruled := c.scope(statement, context)
	if classify := rules.Commands[statement.Command]; classify != nil {
		findings, errors := classify(ruled)
		analysis.Findings = findings
		analysis.Errors = errors
	}
	c.relations.Declare(statement, context)

	// Server refusal checks
	refusals := validate.VersionGate(c.catalog.Version(), statement)
	analysis.Errors = append(analysis.Errors, refusals...)
	if len(refusals) > 0 {
		analysis.Findings = nil
	}

	ran := !analysis.HasErrors()
	if ran {
		c.relations.Link(statement, context)
		c.checks.Record(ruled.Scope)
	}

	collapseStatementLocks(analysis.Findings)
	analysis.Findings = append(analysis.Findings, validate.UnboundedQueue(ruled, analysis.Findings)...)
	analysis.Findings = append(analysis.Findings, validate.UnprovenNullableKey(ruled, analysis.Findings)...)
	analysis.Findings = append(analysis.Findings, validate.MaintenanceStatements(ruled, analysis.Findings)...)
	analysis.Findings = append(analysis.Findings, validate.IndexRenamedAsTable(ruled, analysis.Findings)...)
	analysis.Findings = append(analysis.Findings, validate.PartitionCreatedInPlace(ruled, analysis.Findings)...)
	analysis.Findings = append(analysis.Findings, validate.ExtensionScript(ruled, analysis.Findings)...)

	// Drops and renames come last: everything above reads the relations under the names the statement used.
	if ran {
		c.checks.Forget(c.catalog, c.relations.Retire(statement, context))
	}
	return analysis
}

// collapseStatementLocks keep the strongest lock for every relation a statement lock.
func collapseStatementLocks(findings []pgcontract.Finding) {
	strongest := map[pgcontract.Relation]pgcontract.Lock{}
	for _, finding := range findings {
		for _, target := range finding.Targets {
			strongest[target.Relation] = pgcontract.MaxLock(strongest[target.Relation], target.Lock)
		}
	}
	for _, finding := range findings {
		for i, target := range finding.Targets {
			finding.Targets[i].Lock = strongest[target.Relation]
		}
	}
}

func (c *Classifier) scope(statement ir.Statement, context session.Context) engine.Scope {
	return engine.Scope{Scope: shadow.Scope{View: stmt.New(c.catalog, context, statement), Checks: c.checks}}
}
