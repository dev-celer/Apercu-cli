package pg_classify

import (
	"fmt"
	"path/filepath"
	"testing"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testCatalogOn get the catalog corresponding to the passed version, for Unit-testing.
func testCatalogOn(t *testing.T, version pg_contract.Version) *pg_catalog.Catalog {
	t.Helper()

	name := fmt.Sprintf("snapshot_pg%d_preview.json.gz", version)
	pre, err := pg_catalog.LoadJSON(filepath.Join("..", "pg_catalog", "testdata", name))
	require.NoError(t, err)
	require.Equal(t, version, pre.Header.Version, "%s was captured on another server", name)

	prod := &pg_catalog.Snapshot{
		Source: pg_catalog.SourceProd,
		PIT:    pg_catalog.PITPre,
		Header: pg_catalog.Header{Version: version, ServerVersionNum: int(version) * 10000},
	}
	catalog, err := pg_catalog.NewCatalog(pg_catalog.CatalogOptions{Pre: pre, Prod: prod})
	require.NoError(t, err)
	return catalog
}

// analyze classifies a whole script through one classifier, session and shadow catalog.
func analyze(t *testing.T, catalog *pg_catalog.Catalog, script string) []pg_contract.StatementAnalysis {
	t.Helper()

	statements := pg_parse.Parse(script)
	require.NotEmpty(t, statements)
	return NewClassifier(catalog).Analyze(statements)
}

// single classifies one statement.
func single(t *testing.T, catalog *pg_catalog.Catalog, sql string) pg_contract.StatementAnalysis {
	t.Helper()

	analyses := analyze(t, catalog, sql)
	require.Len(t, analyses, 1)
	return analyses[0]
}

// findingOf is the one finding carrying a rule id.
func findingOf(t *testing.T, analysis pg_contract.StatementAnalysis, code pg_contract.Code) pg_contract.Finding {
	t.Helper()

	for _, finding := range analysis.Findings {
		if finding.Code == code {
			return finding
		}
	}
	require.Failf(t, "missing finding", "%q produced no %s, only %v", analysis.RawSQL, code, codesOf(analysis))
	return pg_contract.Finding{}
}

func codesOf(analysis pg_contract.StatementAnalysis) []pg_contract.Code {
	codes := make([]pg_contract.Code, 0, len(analysis.Findings))
	for _, finding := range analysis.Findings {
		codes = append(codes, finding.Code)
	}
	return codes
}

// targetOf is the entry a finding holds for one relation.
func targetOf(t *testing.T, finding pg_contract.Finding, relation string) pg_contract.Target {
	t.Helper()

	for _, target := range finding.Targets {
		if target.Relation.Name.String() == relation {
			return target
		}
	}
	require.Failf(t, "missing target", "%s names no %s, only %v", finding.Code, relation, finding.Targets)
	return pg_contract.Target{}
}

// ruleCase is one rule id read off one statement: what it is called, and what it does to the
// table the statement names.
type ruleCase struct {
	name string
	sql  string
	code pg_contract.Code
	lock pg_contract.Lock
	op   pg_contract.OpKind
	// table defaults to the orders table, which most of the cases below act on.
	table  string
	errors []string
}

func (c ruleCase) run(t *testing.T, catalog *pg_catalog.Catalog) {
	t.Helper()

	analysis := single(t, catalog, c.sql)

	messages := make([]string, 0, len(analysis.Errors))
	for _, err := range analysis.Errors {
		messages = append(messages, err.Message)
	}
	assert.Equal(t, c.errors, nilWhenEmpty(messages), "errors of %q", c.sql)
	if c.code == "" {
		assert.Empty(t, analysis.Findings, "%q should classify to nothing", c.sql)
		return
	}

	finding := findingOf(t, analysis, c.code)
	relation := c.table
	if relation == "" {
		relation = "apercu_snapshot_test.orders"
	}
	target := targetOf(t, finding, relation)
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

func tablespaceCatalog(t *testing.T) *pg_catalog.Catalog {
	t.Helper()

	pre := &pg_catalog.Snapshot{
		Source: pg_catalog.SourcePreview,
		PIT:    pg_catalog.PITPre,
		Header: pg_catalog.Header{
			Version: pg_contract.Version17, User: "postgres", SearchPath: "public",
			DefaultTablespace: 16385,
		},
		Tablespaces: []pg_catalog.Tablespace{
			{OID: 1663, Name: "pg_default"},
			{OID: 1664, Name: "pg_global"},
			{OID: 16385, Name: "slow"},
		},
		Relations: []pg_catalog.Relation{
			{OID: 1, Namespace: "public", Name: "here", Kind: "r", AccessMethod: "heap", Owner: "postgres", Tablespace: 0},
			{OID: 2, Namespace: "public", Name: "there", Kind: "r", AccessMethod: "heap", Owner: "postgres", Tablespace: 1663},
		},
	}
	catalog, err := pg_catalog.NewCatalog(pg_catalog.CatalogOptions{Pre: pre})
	require.NoError(t, err)
	return catalog
}
