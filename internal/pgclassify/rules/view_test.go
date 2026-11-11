package rules_test

import (
	"testing"

	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
)

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
			code: "R-OB-REFRESHMV-CONC", lock: pgcontract.LockExclusive, op: pgcontract.OpKindDML, table: "apercu_snapshot_test.order_totals",
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

	catalog := classifytest.Catalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

func TestRefreshReadsTheSourcesTheMatviewStores(t *testing.T) {
	t.Parallel()

	finding := classifytest.FindingOf(t, classifytest.Single(t, classifytest.Catalog(t), "REFRESH MATERIALIZED VIEW order_totals"), "R-OB-REFRESHMV")
	source := classifytest.TargetOf(t, finding, "apercu_snapshot_test.orders")
	assert.Equal(t, pgcontract.LockAccessShare, source.Lock, "the stored query runs again over its sources")
	assert.Equal(t, pgcontract.TargetRoleResolved, source.Role, "the statement never names them")
}
