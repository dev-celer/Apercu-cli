package pg_classify

import (
	"testing"

	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errorOf is the one error carrying a rule id.
func errorOf(t *testing.T, analysis pg_contract.StatementAnalysis, code pg_contract.Code) pg_contract.Error {
	t.Helper()

	for _, err := range analysis.Errors {
		if err.Code == code {
			return err
		}
	}
	require.FailNowf(t, "no error", "%s is not among %v", code, analysis.Errors)
	return pg_contract.Error{}
}

func TestVersionGating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rule      string
		sql       string
		construct string
		floor     pg_contract.Version
	}{
		{"G-01", "ALTER TABLE orders ALTER COLUMN status SET EXPRESSION AS ('x')", "SET EXPRESSION AS", pg_contract.Version17},
		{"G-01", "ALTER TABLE orders SET ACCESS METHOD DEFAULT", "SET ACCESS METHOD DEFAULT", pg_contract.Version17},
		{"G-02", "ALTER TABLE orders ALTER COLUMN status SET STORAGE DEFAULT", "SET STORAGE DEFAULT", pg_contract.Version16},
		{"G-03", "ALTER TABLE orders ADD CONSTRAINT c CHECK (total > 0) NOT ENFORCED", "NOT ENFORCED", pg_contract.Version18},
		{"G-03", "VACUUM ONLY orders", "VACUUM ONLY", pg_contract.Version18},
		{"G-04", "ALTER TABLE orders ADD COLUMN g int GENERATED ALWAYS AS (1)", "GENERATED WITHOUT STORAGE KEYWORD", pg_contract.Version18},
	}
	for _, test := range tests {
		t.Run(test.rule+" "+test.construct, func(t *testing.T) {
			t.Parallel()

			below := single(t, testCatalogOn(t, test.floor-1), test.sql)
			gate := errorOf(t, below, "V-05")
			assert.Contains(t, gate.Message, test.construct)
			assert.Contains(t, gate.Message, "requires PostgreSQL "+test.floor.String())
			assert.Empty(t, below.Findings, "a statement the server refuses takes no lock")
			assert.Equal(t, pg_contract.LockNone, below.MaxLock())

			accepted := single(t, testCatalogOn(t, test.floor), test.sql)
			assert.Empty(t, accepted.Errors, "%s accepts it", test.floor)
		})
	}
}

func TestVersionGatingDisabledWhenNoKnownVersion(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	require.Equal(t, pg_contract.VersionUnknown, catalog.Version(), "the fixture has no prod snapshot")

	analysis := Analyze(catalog, apart(event("ALTER TABLE orders ADD CONSTRAINT c CHECK (total > 0) NOT ENFORCED", 1)))
	require.Len(t, analysis.Statements, 1)
	assert.Empty(t, analysis.Statements[0].Errors)
	assert.NotEmpty(t, analysis.Statements[0].Findings, "the statement is read as the newest server reads it")
	assert.Equal(t, pg_contract.Exactly(pg_contract.Version18), analysis.Versions)
}

func TestVersionGatingIgnoreNotVersionDependent(t *testing.T) {
	t.Parallel()

	unfloored := pg_parse.Statement{Features: []pg_parse.Feature{{Name: "MADE UP", Since: pg_contract.VersionUnknown}}}
	errors := versionGate(pg_contract.Version15, unfloored)
	assert.Empty(t, errors)

	floored := pg_parse.Statement{Features: []pg_parse.Feature{{Name: "MADE UP", Since: pg_contract.Version18}}}
	errors = versionGate(pg_contract.Version15, floored)
	require.Len(t, errors, 1)
	assert.Equal(t, pg_contract.Code("V-05"), errors[0].Code)
}

func TestMigrationVersion(t *testing.T) {
	t.Parallel()

	plain := Analyze(testCatalog(t), apart(event("ALTER TABLE orders ADD COLUMN z int", 1)))
	assert.Equal(t, pg_contract.Between(pg_contract.MinSupportedVersion, pg_contract.MaxSupportedVersion), plain.Versions,
		"nothing version-bound, so every supported server runs it")

	mixed := Analyze(testCatalog(t), apart(
		event("ALTER TABLE orders ALTER COLUMN status SET STORAGE DEFAULT", 1),
		event("VACUUM ONLY orders", 1),
	))
	assert.Equal(t, pg_contract.Exactly(pg_contract.Version18), mixed.Versions, "the highest floor wins")
	assert.False(t, mixed.Versions.IsEmpty())
}

func TestMigrationVersionImpossible(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("VACUUM ONLY orders", 1),
		event("ALTER TABLE events SET UNLOGGED", 1),
	))

	require.Len(t, analysis.Statements, 2)
	capped := errorOf(t, analysis.Statements[1], "R-AT-LOGGED")
	require.Equal(t, pg_contract.AtMost(pg_contract.Version17), capped.Versions, "G-05 only errors from 18")
	assert.True(t, analysis.Versions.IsEmpty(), "18 for the VACUUM ONLY, 17 at most for the SET UNLOGGED: %s", analysis.Versions)
}
