package pg_classify

import (
	"testing"

	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
)

const (
	as   = pg_contract.LockAccessShare
	none = pg_contract.LockNone
	conc = pg_contract.OpKindConcurrent
	noop = pg_contract.OpKindNone
)

func TestIndexDDL(t *testing.T) {
	t.Parallel()

	cases := []ruleCase{
		// R-IX-CREATE / R-IX-CREATE-CONC.
		{name: "a plain build reads every row and blocks writers", sql: "CREATE INDEX i ON orders (status)", code: "R-IX-CREATE", lock: sh, op: scan},
		{name: "a partitioned parent holds no rows of its own", sql: "CREATE INDEX i ON events (at)", code: "R-IX-CREATE", lock: sh, op: meta, table: "apercu_snapshot_test.events"},
		{name: "ON ONLY records an invalid parent index and builds nothing", sql: "CREATE INDEX i ON ONLY events (at)", code: "R-IX-CREATE", lock: sh, op: meta, table: "apercu_snapshot_test.events"},
		{name: "a concurrent build trades the lock for two passes", sql: "CREATE INDEX CONCURRENTLY i ON orders (status)", code: "R-IX-CREATE-CONC", lock: sue, op: conc},
		{
			name: "a concurrent build is refused on a partitioned table", sql: "CREATE INDEX CONCURRENTLY i ON events (at)",
			errors: []string{"CREATE INDEX CONCURRENTLY is not supported on a partitioned table on any of PostgreSQL 15-18; build the index on each partition, then ON ONLY the parent and ALTER INDEX … ATTACH PARTITION"},
		},

		// R-IX-DROP / R-IX-DROP-CONC.
		{name: "dropping an index holds the table too", sql: "DROP INDEX orders_open_idx", code: "R-IX-DROP", lock: ael, op: meta, table: "apercu_snapshot_test.orders_open_idx"},
		{name: "dropping it concurrently leaves the table readable", sql: "DROP INDEX CONCURRENTLY orders_open_idx", code: "R-IX-DROP-CONC", lock: ael, op: meta, table: "apercu_snapshot_test.orders_open_idx"},

		// R-IX-RENAME / R-IX-SETOPT / R-IX-SETSTATS / R-IX-TABLESPACE.
		{name: "renaming an index is catalog only", sql: "ALTER INDEX orders_open_idx RENAME TO i", code: "R-IX-RENAME", lock: sue, op: meta, table: "apercu_snapshot_test.orders_open_idx"},
		{name: "a vacuum-tunable storage parameter keeps the weak lock", sql: "ALTER INDEX orders_open_idx SET (fillfactor = 70)", code: "R-IX-SETOPT", lock: sue, op: meta, table: "apercu_snapshot_test.orders_open_idx"},
		{name: "an unknown one takes the strong one", sql: "ALTER INDEX orders_open_idx SET (nope = 1)", code: "R-IX-SETOPT", lock: ael, op: meta, table: "apercu_snapshot_test.orders_open_idx"},
		{name: "an expression column's statistics target is catalog only", sql: "ALTER INDEX labels_name_idx ALTER COLUMN 1 SET STATISTICS 100", code: "R-IX-SETSTATS", lock: sue, op: meta, table: "apercu_snapshot_test.labels_name_idx"},
		{name: "moving an index copies its file", sql: "ALTER INDEX orders_open_idx SET TABLESPACE fast", code: "R-IX-TABLESPACE", lock: ael, op: rw, table: "apercu_snapshot_test.orders_open_idx"},

		// R-IX-REINDEX and friends.
		{name: "reindexing a table reads it at SHARE", sql: "REINDEX TABLE orders", code: "R-IX-REINDEX", lock: sh, op: scan},
		{name: "reindexing one index rebuilds that one", sql: "REINDEX INDEX orders_pkey", code: "R-IX-REINDEX", lock: ael, op: rw, table: "apercu_snapshot_test.orders_pkey"},
		{name: "doing it concurrently keeps writers running", sql: "REINDEX TABLE CONCURRENTLY orders", code: "R-IX-REINDEX-CONC", lock: sue, op: conc},
		{name: "a whole schema is every table in it", sql: "REINDEX SCHEMA apercu_snapshot_test", code: "R-IX-REINDEX-WIDE", lock: sh, op: scan},

		// R-IX-ATTACH.
		{
			name: "attaching a partition's index to the parent index blocks nothing",
			sql:  "ALTER INDEX events_at_idx ATTACH PARTITION events_2025_at_idx",
			code: "R-IX-ATTACH", lock: sue, op: meta, table: "apercu_snapshot_test.events_at_idx",
		},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

// TestAttachIndexOpensBothTables record that the tables behind the two indexes are opened at ACCESS SHARE.
func TestAttachIndexOpensBothTables(t *testing.T) {
	t.Parallel()

	finding := findingOf(t, single(t, testCatalog(t), "ALTER INDEX events_at_idx ATTACH PARTITION events_2025_at_idx"), "R-IX-ATTACH")
	table := targetOf(t, finding, "apercu_snapshot_test.events")
	assert.Equal(t, pg_contract.LockAccessShare, table.Lock)
	assert.Equal(t, pg_contract.TargetRoleResolved, table.Role)
}

func TestCreateIndexReachesThePartitionsAndNotTheChildren(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	partitioned := findingOf(t, single(t, catalog, "CREATE INDEX i ON events (at)"), "R-IX-CREATE")
	leaf := targetOf(t, partitioned, "apercu_snapshot_test.events_2025")
	assert.Equal(t, pg_contract.LockShare, leaf.Lock)
	assert.Equal(t, pg_contract.OpKindScan, leaf.OpKind, "the leaf is where the rows are")

	only := findingOf(t, single(t, catalog, "CREATE INDEX i ON ONLY events (at)"), "R-IX-CREATE")
	assert.Len(t, only.Targets, 1, "ON ONLY reaches no partition: %v", only.Targets)

	// An index is never inherited, so a classic parent's children are left alone.
	inherited := findingOf(t, single(t, catalog, "CREATE INDEX i ON legacy_parent (id)"), "R-IX-CREATE")
	assert.Len(t, inherited.Targets, 1, "an inheritance child gets no index: %v", inherited.Targets)
}

func TestDropIndexResolvesItsTable(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	plain := findingOf(t, single(t, catalog, "DROP INDEX orders_open_idx"), "R-IX-DROP")
	table := targetOf(t, plain, "apercu_snapshot_test.orders")
	assert.Equal(t, pg_contract.LockAccessExclusive, table.Lock)
	assert.Equal(t, pg_contract.TargetRoleResolved, table.Role)

	concurrent := findingOf(t, single(t, catalog, "DROP INDEX CONCURRENTLY orders_open_idx"), "R-IX-DROP-CONC")
	assert.Equal(t, pg_contract.LockShareUpdateExclusive, targetOf(t, concurrent, "apercu_snapshot_test.orders").Lock,
		"the table stays readable and writable while the index is retired")
}

func TestReindexNamesEveryIndexItRebuilds(t *testing.T) {
	t.Parallel()

	finding := findingOf(t, single(t, testCatalog(t), "REINDEX TABLE orders"), "R-IX-REINDEX")
	for _, index := range []string{"apercu_snapshot_test.orders_pkey", "apercu_snapshot_test.orders_open_idx"} {
		target := targetOf(t, finding, index)
		assert.Equal(t, pg_contract.LockAccessExclusive, target.Lock, "%s is what a query cannot be planned without", index)
		assert.Equal(t, pg_contract.OpKindRewrite, target.OpKind)
	}
}
