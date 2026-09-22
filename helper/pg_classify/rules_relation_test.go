package pg_classify

import (
	"testing"

	"apercu-cli/helper/pg_contract"

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

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

func TestDropTableCarriesEverythingPointingAtIt(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	finding := findingOf(t, single(t, catalog, "DROP TABLE users"), "R-TB-DROP")
	for _, referencing := range []string{"apercu_snapshot_test.orders", "apercu_snapshot_test.profiles"} {
		target := targetOf(t, finding, referencing)
		assert.Equal(t, pg_contract.LockAccessExclusive, target.Lock, "%s holds a foreign key into users", referencing)
		assert.Equal(t, pg_contract.TargetRoleImplicit, target.Role)
	}
	assert.NotContains(t, codesOf(single(t, catalog, "DROP TABLE users")), pg_contract.Code("R-OB-DROPVIEW"))

	// CASCADE is what takes the views with it, so it is what names them.
	plain := findingOf(t, single(t, catalog, "DROP TABLE users"), "R-TB-DROP")
	for _, target := range plain.Targets {
		assert.NotEqual(t, "apercu_snapshot_test.active_users", target.Relation.Name.String())
	}
	cascade := findingOf(t, single(t, catalog, "DROP TABLE users CASCADE"), "R-TB-DROP")
	assert.Equal(t, pg_contract.LockAccessExclusive, targetOf(t, cascade, "apercu_snapshot_test.active_users").Lock)
}

func TestDropPartitionReProvesTheDefaultOne(t *testing.T) {
	t.Parallel()

	finding := findingOf(t, single(t, testCatalog(t), "DROP TABLE events_2025"), "R-TB-DROP")
	target := targetOf(t, finding, "apercu_snapshot_test.events_default")
	assert.Equal(t, pg_contract.LockAccessExclusive, target.Lock, "the default partition's bound widens again")
	assert.Equal(t, pg_contract.TargetRoleImplicit, target.Role)
}

func TestViewsMatviewsAndSequences(t *testing.T) {
	t.Parallel()

	cases := []ruleCase{
		// R-OB-CREATEVIEW / R-OB-REPLACEVIEW / R-OB-DROPVIEW.
		{name: "a view is a stored query, so its sources are only opened", sql: "CREATE VIEW ob_view AS SELECT id FROM orders", code: "R-OB-CREATEVIEW", lock: as, op: noop},
		{name: "OR REPLACE on a view that does not exist is still a create", sql: "CREATE OR REPLACE VIEW ob_absent AS SELECT id FROM orders", code: "R-OB-CREATEVIEW", lock: as, op: noop},
		{
			name: "replacing one that does exist blocks every reader of it",
			sql:  "CREATE OR REPLACE VIEW active_users AS SELECT id, email FROM users WHERE id > 0",
			code: "R-OB-REPLACEVIEW", lock: ael, op: meta, table: "apercu_snapshot_test.active_users",
		},
		{name: "dropping a view is catalog only", sql: "DROP VIEW active_users", code: "R-OB-DROPVIEW", lock: ael, op: meta, table: "apercu_snapshot_test.active_users"},
		{name: "and a materialized view goes the same way", sql: "DROP MATERIALIZED VIEW order_totals", code: "R-OB-DROPVIEW", lock: ael, op: meta, table: "apercu_snapshot_test.order_totals"},

		// R-OB-CREATEMV / R-OB-REFRESHMV.
		{name: "a matview runs its query once", sql: "CREATE MATERIALIZED VIEW ob_mv AS SELECT id FROM orders", code: "R-OB-CREATEMV", lock: as, op: scan},
		{name: "WITH NO DATA runs nothing", sql: "CREATE MATERIALIZED VIEW ob_mv_empty AS SELECT id FROM orders WITH NO DATA", code: "R-OB-CREATEMV", lock: as, op: noop},
		{name: "a refresh replaces the file under the strongest lock", sql: "REFRESH MATERIALIZED VIEW order_totals", code: "R-OB-REFRESHMV", lock: ael, op: rw, table: "apercu_snapshot_test.order_totals"},
		{
			name: "a concurrent refresh merges row by row and lets readers through",
			sql:  "REFRESH MATERIALIZED VIEW CONCURRENTLY order_totals",
			code: "R-OB-REFRESHMV-CONC", lock: pg_contract.LockExclusive, op: pg_contract.OpKindDML, table: "apercu_snapshot_test.order_totals",
		},
		{
			name:   "and CONCURRENTLY with no data is not a combination that exists",
			sql:    "REFRESH MATERIALIZED VIEW CONCURRENTLY order_totals WITH NO DATA",
			errors: []string{"REFRESH MATERIALIZED VIEW CONCURRENTLY … WITH NO DATA is not a valid combination"},
		},

		// R-OB-ALTERSEQ / R-OB-CREATESEQ / R-OB-DROPSEQ.
		{name: "altering a sequence blocks every nextval on it", sql: "ALTER SEQUENCE standalone_seq RESTART WITH 5", code: "R-OB-ALTERSEQ", lock: sre, op: meta, table: "apercu_snapshot_test.standalone_seq"},
		{name: "creating one locks nothing", sql: "CREATE SEQUENCE ob_seq", code: "R-OB-CREATESEQ", noTargets: true},
		{name: "dropping one is catalog only", sql: "DROP SEQUENCE standalone_seq", code: "R-OB-DROPSEQ", lock: ael, op: meta, table: "apercu_snapshot_test.standalone_seq"},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

func TestRefreshReadsTheSourcesTheMatviewStores(t *testing.T) {
	t.Parallel()

	finding := findingOf(t, single(t, testCatalog(t), "REFRESH MATERIALIZED VIEW order_totals"), "R-OB-REFRESHMV")
	source := targetOf(t, finding, "apercu_snapshot_test.orders")
	assert.Equal(t, pg_contract.LockAccessShare, source.Lock, "the stored query runs again over its sources")
	assert.Equal(t, pg_contract.TargetRoleResolved, source.Role, "the statement never names them")
}

func TestAlterSequenceNamesTheColumnItFeeds(t *testing.T) {
	t.Parallel()

	finding := findingOf(t, single(t, testCatalog(t), "ALTER SEQUENCE users_id_seq RESTART WITH 5"), "R-OB-ALTERSEQ")
	assert.Contains(t, finding.Message, "apercu_snapshot_test.users.id")
}
