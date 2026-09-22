package pg_classify

import (
	"testing"

	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
)

func TestObjectsHangingOffATable(t *testing.T) {
	t.Parallel()

	cases := []ruleCase{
		// R-OB-CREATETRIG / R-OB-DROPTRIG / R-OB-ALTERTRIG.
		{name: "creating a trigger waits for writers, not readers", sql: "CREATE TRIGGER ob_trig BEFORE UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION bump()", code: "R-OB-CREATETRIG", lock: sre, op: meta},
		{name: "dropping one takes a stronger lock than creating it", sql: "DROP TRIGGER orders_bump ON orders", code: "R-OB-DROPTRIG", lock: ael, op: meta},
		{name: "renaming one reaches every inheritor", sql: "ALTER TRIGGER orders_bump ON orders RENAME TO ob_trig", code: "R-OB-ALTERTRIG", lock: ael, op: meta},

		// R-OB-POLICY / R-OB-RULE.
		{name: "a policy invalidates every plan over the table", sql: "CREATE POLICY ob_policy ON orders USING (true)", code: "R-OB-POLICY", lock: ael, op: meta},
		{name: "altering one is the same clause", sql: "ALTER POLICY ob_policy ON orders USING (false)", code: "R-OB-POLICY", lock: ael, op: meta},
		{name: "dropping one too", sql: "DROP POLICY ob_policy ON orders", code: "R-OB-POLICY", lock: ael, op: meta},
		{name: "a rewrite rule is the same shape", sql: "CREATE RULE ob_rule AS ON DELETE TO orders DO INSTEAD NOTHING", code: "R-OB-RULE", lock: ael, op: meta},
		{name: "and dropping it", sql: "DROP RULE ob_rule ON orders", code: "R-OB-RULE", lock: ael, op: meta},

		// R-OB-STATS.
		{name: "creating extended statistics needs no more than a vacuum's lock", sql: "CREATE STATISTICS ob_stats ON id, status FROM orders", code: "R-OB-STATS", lock: sue, op: meta},
		{name: "dropping one holds the table behind it", sql: "DROP STATISTICS labels_stats", code: "R-OB-STATS", lock: sue, op: meta, table: "apercu_snapshot_test.labels"},
		{name: "altering one never opens the table at all", sql: "ALTER STATISTICS labels_stats SET STATISTICS 100", code: "R-OB-STATS", noTargets: true},
		{name: "an unknown statistics object cannot be traced back to its table", sql: "DROP STATISTICS nope", code: "R-OB-STATS", noTargets: true},

		// R-OB-COMMENT.
		{name: "a table comment is a catalog row", sql: "COMMENT ON TABLE orders IS 'the orders'", code: "R-OB-COMMENT", lock: sue, op: meta},
		{name: "a column comment names its table", sql: "COMMENT ON COLUMN orders.status IS 'the status'", code: "R-OB-COMMENT", lock: sue, op: meta},

		// R-OB-DEFACL.
		{name: "default privileges touch nothing that exists", sql: "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO app", code: "R-OB-DEFACL", noTargets: true},

		// R-OB-PUB
		{name: "a publication membership is recorded under a weak lock", sql: "ALTER PUBLICATION p ADD TABLE orders", code: "R-OB-PUB", lock: sue, op: meta},
		{name: "creating one for a table is the same", sql: "CREATE PUBLICATION p2 FOR TABLE orders", code: "R-OB-PUB", lock: sue, op: meta},
		{name: "a subscription locks nothing here", sql: "CREATE SUBSCRIPTION sb CONNECTION 'host=x' PUBLICATION pb", code: "R-OB-SUB", noTargets: true},

		{name: "an extension script cannot be read", sql: "CREATE EXTENSION IF NOT EXISTS pgcrypto", code: "R-OB-EXTENSION", noTargets: true},
		{name: "a schema is a namespace", sql: "CREATE SCHEMA ob_schema", code: "R-OB-SCHEMA", noTargets: true},
		{name: "and dropping one with CASCADE says so", sql: "DROP SCHEMA ob_schema CASCADE", code: "R-OB-SCHEMA", noTargets: true},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

func TestTriggerReachesThePartitionsAndNotTheChildren(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	partitioned := findingOf(t, single(t, catalog, "CREATE TRIGGER ob_trig BEFORE UPDATE ON events FOR EACH ROW EXECUTE FUNCTION bump()"), "R-OB-CREATETRIG")
	assert.Equal(t, pg_contract.LockShareRowExclusive, targetOf(t, partitioned, "apercu_snapshot_test.events_2025").Lock,
		"the trigger is cloned onto every partition")

	inherited := findingOf(t, single(t, catalog, "CREATE TRIGGER ob_trig BEFORE UPDATE ON legacy_parent FOR EACH ROW EXECUTE FUNCTION bump()"), "R-OB-CREATETRIG")
	assert.Len(t, inherited.Targets, 1, "an inheritance child inherits no trigger: %v", inherited.Targets)
}

func TestGrantFollowsTheServerVersion(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		version pg_contract.Version
		lock    pg_contract.Lock
	}{
		{pg_contract.Version17, pg_contract.LockNone},
		{pg_contract.Version18, pg_contract.LockAccessShare},
	} {
		t.Run(testCase.version.String(), func(t *testing.T) {
			finding := findingOf(t, single(t, testCatalogOn(t, testCase.version), "GRANT SELECT ON orders TO app"), "R-OB-GRANT")
			assert.Equal(t, testCase.lock, targetOf(t, finding, "apercu_snapshot_test.orders").Lock)
		})
	}
}
