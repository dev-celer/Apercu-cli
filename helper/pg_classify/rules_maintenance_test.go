package pg_classify

import (
	"testing"

	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
)

func TestMaintenanceCommands(t *testing.T) {
	t.Parallel()

	cases := []ruleCase{
		// R-MT-VACUUM / R-MT-VACUUMFULL.
		{name: "a vacuum reclaims in place beside readers and writers", sql: "VACUUM orders", code: "R-MT-VACUUM", lock: sue, op: scan},
		{name: "VACUUM FULL is a full outage for the table", sql: "VACUUM FULL orders", code: "R-MT-VACUUMFULL", lock: ael, op: rw},
		{name: "and the option spelling is the same statement", sql: "VACUUM (FULL, ANALYZE) orders", code: "R-MT-VACUUMFULL", lock: ael, op: rw},
		{name: "naming no table works through the whole database", sql: "VACUUM", code: "R-MT-VACUUM", lock: sue, op: scan},

		// R-MT-ANALYZE.
		{name: "analyze samples rows without blocking anything", sql: "ANALYZE orders", code: "R-MT-ANALYZE", lock: sue, op: scan},
		{name: "a partitioned parent holds no rows to sample", sql: "ANALYZE events", code: "R-MT-ANALYZE", lock: sue, op: meta, table: "apercu_snapshot_test.events"},

		// R-MT-CLUSTER.
		{name: "clustering rewrites the table in index order", sql: "CLUSTER orders USING orders_pkey", code: "R-MT-CLUSTER", lock: ael, op: rw},

		// R-MT-TRUNCATE.
		{name: "truncating replaces the file", sql: "TRUNCATE profiles", code: "R-MT-TRUNCATE", lock: ael, op: meta, table: "apercu_snapshot_test.profiles"},
		{
			name: "and is refused while something outside the list references it", sql: "TRUNCATE users",
			errors: []string{"TRUNCATE is refused while apercu_snapshot_test.orders, apercu_snapshot_test.profiles references the table and is not being truncated with it; add CASCADE or name it too"},
		},
		{name: "CASCADE empties the referencing tables too", sql: "TRUNCATE users CASCADE", code: "R-MT-TRUNCATE", lock: ael, op: meta, table: "apercu_snapshot_test.orders"},
		{name: "naming them all is the other way out", sql: "TRUNCATE users, orders, profiles", code: "R-MT-TRUNCATE", lock: ael, op: meta, table: "apercu_snapshot_test.users"},
		{name: "RESTART IDENTITY resets the sequences the table owns", sql: "TRUNCATE users, orders, profiles RESTART IDENTITY", code: "R-MT-TRUNCATE", lock: ael, op: meta, table: "apercu_snapshot_test.users_id_seq"},

		// R-MT-LOCK.
		{name: "an explicit lock defaults to the strongest one", sql: "LOCK TABLE orders", code: "R-MT-LOCK", lock: ael, op: meta},
		{name: "a named mode is taken verbatim", sql: "LOCK TABLE orders IN SHARE MODE", code: "R-MT-LOCK", lock: sh, op: meta},

		// R-MT-COPYFROM / R-MT-COPYTO.
		{name: "copying in is an insert", sql: "COPY orders FROM '/tmp/x.csv'", code: "R-MT-COPYFROM", lock: pg_contract.LockRowExclusive, op: pg_contract.OpKindDML},
		{name: "copying out is a read", sql: "COPY orders TO '/tmp/x.csv'", code: "R-MT-COPYTO", lock: as, op: noop},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

func TestAnalyzeOpensTheChildrenWhateverTheVersion(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		version pg_contract.Version
		child   pg_contract.Lock
	}{
		{pg_contract.Version17, pg_contract.LockAccessShare},
		{pg_contract.Version18, pg_contract.LockShareUpdateExclusive},
	} {
		t.Run(testCase.version.String(), func(t *testing.T) {
			finding := findingOf(t, single(t, testCatalogOn(t, testCase.version), "ANALYZE legacy_parent"), "R-MT-ANALYZE")
			assert.Equal(t, testCase.child, targetOf(t, finding, "apercu_snapshot_test.legacy_child").Lock)
		})
	}
}

func TestVacuumReachesThePartitionsOnEveryVersion(t *testing.T) {
	t.Parallel()

	finding := findingOf(t, single(t, testCatalogOn(t, pg_contract.Version15), "VACUUM events"), "R-MT-VACUUM")
	leaf := targetOf(t, finding, "apercu_snapshot_test.events_2025")
	assert.Equal(t, pg_contract.LockShareUpdateExclusive, leaf.Lock)
	assert.Equal(t, pg_contract.OpKindScan, leaf.OpKind, "the partition is where the dead tuples are")

	parent := findingOf(t, single(t, testCatalogOn(t, pg_contract.Version15), "VACUUM legacy_parent"), "R-MT-VACUUM")
	assert.Len(t, parent.Targets, 1, "15 to 17 vacuum only what they name: %v", parent.Targets)
}

func TestClusterNamesTheIndexItOrdersBy(t *testing.T) {
	t.Parallel()

	finding := findingOf(t, single(t, testCatalog(t), "CLUSTER orders USING orders_pkey"), "R-MT-CLUSTER")
	target := targetOf(t, finding, "apercu_snapshot_test.orders_pkey")
	assert.Equal(t, pg_contract.LockAccessExclusive, target.Lock)
	assert.Equal(t, pg_contract.OpKindRewrite, target.OpKind)
}
