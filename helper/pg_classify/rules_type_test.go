package pg_classify

import (
	"testing"

	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
)

func TestTypesAndDomains(t *testing.T) {
	t.Parallel()

	cases := []ruleCase{
		// R-TY-ADDVALUE / R-TY-RENAMEVALUE. An enum is not a relation, so there is nothing to lock.
		{name: "adding an enum value locks the type and no table", sql: "ALTER TYPE mood ADD VALUE 'meh'", code: "R-TY-ADDVALUE", noTargets: true},
		{name: "renaming one is the same", sql: "ALTER TYPE mood RENAME VALUE 'ok' TO 'fine'", code: "R-TY-RENAMEVALUE", noTargets: true},

		// R-TY-ATTR. address is the type of the typed table places.
		{
			name:   "an attribute clause is refused while a typed table uses the type",
			sql:    "ALTER TYPE address ADD ATTRIBUTE zip text",
			errors: []string{"a table is declared OF this type, so an attribute clause needs CASCADE to carry the change into it"},
		},
		{name: "CASCADE carries it into the typed table", sql: "ALTER TYPE address ADD ATTRIBUTE zip text CASCADE", code: "R-TY-ATTR", lock: ael, op: meta, table: "apercu_snapshot_test.places"},
		{name: "dropping an attribute is catalog work too", sql: "ALTER TYPE address DROP ATTRIBUTE city CASCADE", code: "R-TY-ATTR", lock: ael, op: meta, table: "apercu_snapshot_test.places"},
		{name: "changing an attribute's type rebuilds the typed table", sql: "ALTER TYPE address ALTER ATTRIBUTE street TYPE varchar(80) CASCADE", code: "R-TY-ATTR", lock: ael, op: rw, table: "apercu_snapshot_test.places"},

		// R-TY-DOMAIN-ADD. positive_int is the type of profiles.score.
		{name: "a domain constraint reads every table holding a column of it", sql: "ALTER DOMAIN positive_int ADD CONSTRAINT d CHECK (VALUE > 1)", code: "R-TY-DOMAIN-ADD", lock: sh, op: scan, table: "apercu_snapshot_test.profiles"},
		{name: "NOT VALID reads nothing and opens nothing", sql: "ALTER DOMAIN positive_int ADD CONSTRAINT d CHECK (VALUE > 1) NOT VALID", code: "R-TY-DOMAIN-ADD", noTargets: true},
		{name: "a domain nothing declares reaches no table", sql: "ALTER DOMAIN loose_text ADD CONSTRAINT d CHECK (length(VALUE) < 10)", code: "R-TY-DOMAIN-ADD", lock: sh, op: scan, table: "apercu_snapshot_test.profiles"},

		// R-TY-DROP.
		{
			name: "a type in use cannot be dropped", sql: "DROP TYPE mood",
			errors: []string{"type mood cannot be dropped while columns are declared with it; CASCADE would drop those columns"},
		},
		{name: "CASCADE drops the columns with it", sql: "DROP TYPE mood CASCADE", code: "R-TY-DROP", lock: ael, op: meta, table: "apercu_snapshot_test.profiles"},
		{name: "a domain nothing uses goes quietly", sql: "DROP DOMAIN nothing_uses_this", code: "R-TY-DROP", noTargets: true},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

// TestAttributeClausesSayWhichOneTheyAre pins the three R-TY-ATTR clauses apart. They share a
// rule id and a lock, so the message is the only thing that tells a reader which one ran.
func TestAttributeClausesSayWhichOneTheyAre(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sql  string
		says string
	}{
		{"ALTER TYPE address ADD ATTRIBUTE zip text CASCADE", `attribute "zip" is added`},
		{"ALTER TYPE address DROP ATTRIBUTE city CASCADE", `attribute "city" is dropped`},
		{"ALTER TYPE address ALTER ATTRIBUTE street TYPE varchar(80) CASCADE", `attribute "street" changes type`},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.says, func(t *testing.T) {
			finding := findingOf(t, single(t, catalog, testCase.sql), "R-TY-ATTR")
			assert.Contains(t, finding.Message, testCase.says)
			assert.Contains(t, finding.Message, "declared OF this type are locked")
		})
	}
}

func TestDroppingATypeNamesWhatWouldLoseAColumn(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	refused := single(t, catalog, "DROP TYPE mood")
	assert.Empty(t, refused.Findings, "a statement the server refuses reports no lock")

	cascade := findingOf(t, single(t, catalog, "DROP TYPE mood CASCADE"), "R-TY-DROP")
	target := targetOf(t, cascade, "apercu_snapshot_test.profiles")
	assert.Equal(t, pg_contract.TargetRoleImplicit, target.Role, "the statement never names the table")
	assert.Contains(t, cascade.Message, "every column declared with it")
}
