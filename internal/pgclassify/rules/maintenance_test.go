package rules_test

import (
	"testing"

	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
)

func TestMaintenanceCommands(t *testing.T) {
	t.Parallel()

	cases := []ruleCase{
		// R-MT-VACUUM / R-MT-VACUUMFULL.
		{name: "a vacuum reclaims in place beside readers and writers", sql: "VACUUM orders", code: "R-MT-VACUUM", lock: sue, op: scan},
		{name: "VACUUM FULL is a full outage for the table", sql: "VACUUM FULL orders", code: "R-MT-VACUUMFULL", lock: ael, op: rw},
		{name: "and the option spelling is the same statement", sql: "VACUUM (FULL, ANALYZE) orders", code: "R-MT-VACUUMFULL", lock: ael, op: rw},
		{name: "a boolean option may be spelled as an integer", sql: "VACUUM (FULL 1) orders", code: "R-MT-VACUUMFULL", lock: ael, op: rw},
		{name: "and zero turns it off", sql: "VACUUM (FULL 0) orders", code: "R-MT-VACUUM", lock: sue, op: scan},
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
			errors: []string{"TRUNCATE is refused while apercu_snapshot_test.orders, apercu_snapshot_test.metrics, apercu_snapshot_test.profiles references the table and is not being truncated with it; add CASCADE or name it too"},
		},
		{name: "CASCADE empties the referencing tables too", sql: "TRUNCATE users CASCADE", code: "R-MT-TRUNCATE", lock: ael, op: meta, table: "apercu_snapshot_test.orders"},
		{name: "naming them all is the other way out", sql: "TRUNCATE users, orders, profiles, metrics", code: "R-MT-TRUNCATE", lock: ael, op: meta, table: "apercu_snapshot_test.users"},
		{name: "RESTART IDENTITY resets the sequences the table owns", sql: "TRUNCATE users, orders, profiles, metrics RESTART IDENTITY", code: "R-MT-TRUNCATE", lock: ael, op: meta, table: "apercu_snapshot_test.users_id_seq"},

		// R-MT-LOCK.
		{name: "an explicit lock defaults to the strongest one", sql: "LOCK TABLE orders", code: "R-MT-LOCK", lock: ael, op: meta},
		{name: "a named mode is taken verbatim", sql: "LOCK TABLE orders IN SHARE MODE", code: "R-MT-LOCK", lock: sh, op: meta},

		// R-MT-COPYFROM / R-MT-COPYTO.
		{name: "copying in is an insert", sql: "COPY orders FROM '/tmp/x.csv'", code: "R-MT-COPYFROM", lock: pgcontract.LockRowExclusive, op: pgcontract.OpKindDML},
		{name: "copying out is a read", sql: "COPY orders TO '/tmp/x.csv'", code: "R-MT-COPYTO", lock: as, op: noop},
	}

	catalog := classifytest.Catalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

func TestAnalyzeOpensTheChildrenWhateverTheVersion(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		version pgcontract.Version
		child   pgcontract.Lock
	}{
		{pgcontract.Version17, pgcontract.LockAccessShare},
		{pgcontract.Version18, pgcontract.LockShareUpdateExclusive},
	} {
		t.Run(testCase.version.String(), func(t *testing.T) {
			finding := classifytest.FindingOf(t, classifytest.Single(t, classifytest.CatalogOn(t, testCase.version), "ANALYZE legacy_parent"), "R-MT-ANALYZE")
			assert.Equal(t, testCase.child, classifytest.TargetOf(t, finding, "apercu_snapshot_test.legacy_child").Lock)
		})
	}
}

func TestVacuumReachesThePartitionsOnEveryVersion(t *testing.T) {
	t.Parallel()

	finding := classifytest.FindingOf(t, classifytest.Single(t, classifytest.CatalogOn(t, pgcontract.Version15), "VACUUM events"), "R-MT-VACUUM")
	leaf := classifytest.TargetOf(t, finding, "apercu_snapshot_test.events_2025")
	assert.Equal(t, pgcontract.LockShareUpdateExclusive, leaf.Lock)
	assert.Equal(t, pgcontract.OpKindScan, leaf.OpKind, "the partition is where the dead tuples are")

	parent := classifytest.FindingOf(t, classifytest.Single(t, classifytest.CatalogOn(t, pgcontract.Version15), "VACUUM legacy_parent"), "R-MT-VACUUM")
	assert.Len(t, parent.Targets, 1, "15 to 17 vacuum only what they name: %v", parent.Targets)
}

func TestClusterNamesTheIndexItOrdersBy(t *testing.T) {
	t.Parallel()

	finding := classifytest.FindingOf(t, classifytest.Single(t, classifytest.Catalog(t), "CLUSTER orders USING orders_pkey"), "R-MT-CLUSTER")
	target := classifytest.TargetOf(t, finding, "apercu_snapshot_test.orders_pkey")
	assert.Equal(t, pgcontract.LockAccessExclusive, target.Lock)
	assert.Equal(t, pgcontract.OpKindRewrite, target.OpKind)
}
