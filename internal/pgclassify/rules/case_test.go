package rules_test

import (
	"testing"

	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
)

// ruleCase is one rule id read off one statement: what it is called, and what it does to the
// table the statement names.
type ruleCase struct {
	name string
	sql  string
	code pgcontract.Code
	lock pgcontract.Lock
	op   pgcontract.OpKind
	// table defaults to the orders table, which most of the cases below act on.
	table string
	// noTargets is a rule that names an object the target vocabulary cannot carry: a type, an
	// extension, a schema. It reports what the statement does and locks nothing.
	noTargets bool
	errors    []string
}

func (c ruleCase) run(t *testing.T, catalog *pgcatalog.Catalog) {
	t.Helper()

	analysis := classifytest.Single(t, catalog, c.sql)

	messages := make([]string, 0, len(analysis.Errors))
	for _, err := range analysis.Errors {
		messages = append(messages, err.Message)
	}
	assert.Equal(t, c.errors, nilWhenEmpty(messages), "errors of %q", c.sql)
	if c.code == "" {
		assert.Empty(t, analysis.Findings, "%q should classify to nothing", c.sql)
		return
	}

	finding := classifytest.FindingOf(t, analysis, c.code)
	if c.noTargets {
		assert.Emptyf(t, finding.Targets, "%s should name no relation for %q", c.code, c.sql)
		assert.NotEmptyf(t, finding.Message, "%s says nothing about %q", c.code, c.sql)
		return
	}
	relation := c.table
	if relation == "" {
		relation = "apercu_snapshot_test.orders"
	}
	target := classifytest.TargetOf(t, finding, relation)
	assert.Equalf(t, c.lock.Short(), target.Lock.Short(), "lock on %s for %q", relation, c.sql)
	assert.Equalf(t, c.op.String(), target.OpKind.String(), "op on %s for %q", relation, c.sql)
	assert.NotEmptyf(t, finding.Message, "%s says nothing about %q", c.code, c.sql)
}

func nilWhenEmpty(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return values
}
