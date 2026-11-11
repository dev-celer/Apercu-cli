package rules_test

import (
	"testing"

	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
)

func TestTableCreationAndDestruction(t *testing.T) {
	t.Parallel()

	cases := []ruleCase{
		// R-TB-CREATE and the three clauses that reach an existing relation.
		{name: "a plain create locks nothing that exists", sql: "CREATE TABLE tb_plain (id bigint)", code: "R-TB-CREATE", noTargets: true},
		{name: "a foreign key holds the other side", sql: "CREATE TABLE tb_fk (id bigint REFERENCES users (id))", code: "R-TB-CREATE", lock: sre, op: meta, table: "apercu_snapshot_test.users"},
		{name: "LIKE only reads the model", sql: "CREATE TABLE tb_like (LIKE orders)", code: "R-TB-CREATE-LIKE", lock: as, op: noop},
		{name: "INHERITS holds the parent at the attach lock", sql: "CREATE TABLE tb_inh () INHERITS (legacy_parent)", code: "R-TB-CREATE-INHERITS", lock: sue, op: meta, table: "apercu_snapshot_test.legacy_parent"},
		{
			name: "PARTITION OF holds the parent harder than ATTACH would",
			sql:  "CREATE TABLE tb_part PARTITION OF events FOR VALUES FROM ('2027-01-01') TO ('2028-01-01')",
			code: "R-TB-CREATE-PARTOF", lock: ael, op: meta, table: "apercu_snapshot_test.events",
		},
		{name: "CREATE TABLE AS reads its sources in full", sql: "CREATE TABLE tb_ctas AS SELECT id FROM orders", code: "R-TB-CTAS", lock: as, op: scan},

		// R-TB-DROP.
		{name: "dropping a table is catalog work under the strongest lock", sql: "DROP TABLE profiles", code: "R-TB-DROP", lock: ael, op: meta, table: "apercu_snapshot_test.profiles"},
		{name: "dropping a partition holds the parent too", sql: "DROP TABLE events_2025", code: "R-TB-DROP", lock: ael, op: meta, table: "apercu_snapshot_test.events"},
	}

	catalog := classifytest.Catalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

func TestDropTableCarriesEverythingPointingAtIt(t *testing.T) {
	t.Parallel()

	finding := classifytest.FindingOf(t, classifytest.Single(t, classifytest.Catalog(t), "DROP TABLE users"), "R-TB-DROP")
	for _, referencing := range []string{"apercu_snapshot_test.orders", "apercu_snapshot_test.profiles"} {
		target := classifytest.TargetOf(t, finding, referencing)
		assert.Equal(t, pgcontract.LockAccessExclusive, target.Lock, "%s holds a foreign key into users", referencing)
		assert.Equal(t, pgcontract.TargetRoleImplicit, target.Role)
	}
	assert.NotContains(t, classifytest.CodesOf(classifytest.Single(t, classifytest.Catalog(t), "DROP TABLE users")), pgcontract.Code("R-OB-DROPVIEW"))

	// CASCADE is what takes the views with it, so it is what names them.
	plain := classifytest.FindingOf(t, classifytest.Single(t, classifytest.Catalog(t), "DROP TABLE users"), "R-TB-DROP")
	for _, target := range plain.Targets {
		assert.NotEqual(t, "apercu_snapshot_test.active_users", target.Relation.Name.String())
	}
	cascade := classifytest.FindingOf(t, classifytest.Single(t, classifytest.Catalog(t), "DROP TABLE users CASCADE"), "R-TB-DROP")
	assert.Equal(t, pgcontract.LockAccessExclusive, classifytest.TargetOf(t, cascade, "apercu_snapshot_test.active_users").Lock)
}

func TestDropPartitionReProvesTheDefaultOne(t *testing.T) {
	t.Parallel()

	finding := classifytest.FindingOf(t, classifytest.Single(t, classifytest.Catalog(t), "DROP TABLE events_2025"), "R-TB-DROP")
	target := classifytest.TargetOf(t, finding, "apercu_snapshot_test.events_default")
	assert.Equal(t, pgcontract.LockAccessExclusive, target.Lock, "the default partition's bound widens again")
	assert.Equal(t, pgcontract.TargetRoleImplicit, target.Role)
}
