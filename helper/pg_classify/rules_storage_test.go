package pg_classify

import (
	"testing"

	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTriggerAndSecuritySubcommands(t *testing.T) {
	t.Parallel()

	cases := []ruleCase{
		{name: "disabling a trigger waits for writers, not readers", sql: "ALTER TABLE orders DISABLE TRIGGER orders_bump", code: "R-AT-TRIG", lock: sre, op: meta},
		{name: "enabling every trigger is the same clause", sql: "ALTER TABLE orders ENABLE TRIGGER ALL", code: "R-AT-TRIG", lock: sre, op: meta},
		{name: "a replica trigger is its own rule id", sql: "ALTER TABLE orders ENABLE REPLICA TRIGGER orders_bump", code: "R-AT-TRIG-REP", lock: sre, op: meta},
		{name: "an always trigger is the same", sql: "ALTER TABLE orders ENABLE ALWAYS TRIGGER orders_bump", code: "R-AT-TRIG-REP", lock: sre, op: meta},
		{name: "a rule rewrites queries, so every plan is invalidated", sql: "ALTER TABLE orders DISABLE RULE r", code: "R-AT-RULE", lock: ael, op: meta},
		{name: "row level security is switched on under the strongest lock", sql: "ALTER TABLE orders ENABLE ROW LEVEL SECURITY", code: "R-AT-RLS", lock: ael, op: meta},
		{name: "and off the same way", sql: "ALTER TABLE orders DISABLE ROW LEVEL SECURITY", code: "R-AT-RLS", lock: ael, op: meta},
		{name: "forcing it on the owner is its own rule id", sql: "ALTER TABLE orders FORCE ROW LEVEL SECURITY", code: "R-AT-RLS-FORCE", lock: ael, op: meta},
		{name: "and releasing the owner too", sql: "ALTER TABLE orders NO FORCE ROW LEVEL SECURITY", code: "R-AT-RLS-FORCE", lock: ael, op: meta},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

func TestStorageSubcommands(t *testing.T) {
	t.Parallel()

	cases := []ruleCase{
		// R-AT-RELOPT.
		{name: "a vacuum-tunable option needs no more than a vacuum's lock", sql: "ALTER TABLE orders SET (fillfactor = 70)", code: "R-AT-RELOPT", lock: sue, op: meta},
		{name: "an autovacuum option is matched by prefix", sql: "ALTER TABLE orders SET (autovacuum_vacuum_scale_factor = 0.1)", code: "R-AT-RELOPT", lock: sue, op: meta},
		{name: "a mixed list takes the stronger lock", sql: "ALTER TABLE orders SET (fillfactor = 70, user_catalog_table = true)", code: "R-AT-RELOPT", lock: ael, op: meta},
		{name: "an unknown option is assumed to need the stronger lock", sql: "ALTER TABLE orders SET (no_such_option = 1)", code: "R-AT-RELOPT", lock: ael, op: meta},
		{name: "resetting a vacuum-tunable option is the same clause", sql: "ALTER TABLE orders RESET (fillfactor)", code: "R-AT-RELOPT", lock: sue, op: meta},

		// R-AT-TABLESPACE / R-AT-ACCESSMETHOD / R-AT-LOGGED.
		{name: "moving a tablespace copies the heap", sql: "ALTER TABLE orders SET TABLESPACE fast", code: "R-AT-TABLESPACE", lock: ael, op: rw},
		{name: "naming the access method already in use changes nothing", sql: "ALTER TABLE orders SET ACCESS METHOD heap", code: "R-AT-ACCESSMETHOD", lock: ael, op: meta},
		{name: "naming another one rewrites every tuple", sql: "ALTER TABLE orders SET ACCESS METHOD columnar", code: "R-AT-ACCESSMETHOD", lock: ael, op: rw},
		{name: "changing persistence rewrites the heap", sql: "ALTER TABLE orders SET UNLOGGED", code: "R-AT-LOGGED", lock: ael, op: rw},
		{name: "and back again", sql: "ALTER TABLE orders SET LOGGED", code: "R-AT-LOGGED", lock: ael, op: rw},

		// R-AT-CLUSTERON / R-AT-WITHOUTOIDS.
		{name: "marking an index for CLUSTER clusters nothing now", sql: "ALTER TABLE orders CLUSTER ON orders_pkey", code: "R-AT-CLUSTERON", lock: sue, op: meta},
		{name: "unmarking it is the same clause", sql: "ALTER TABLE orders SET WITHOUT CLUSTER", code: "R-AT-CLUSTERON", lock: sue, op: meta},
		{name: "SET WITHOUT OIDS has had nothing to do since PostgreSQL 12", sql: "ALTER TABLE orders SET WITHOUT OIDS", code: "R-AT-WITHOUTOIDS", lock: ael, op: meta},

		// R-AT-OWNER / R-AT-REPLIDENT.
		{name: "changing owner is catalog only", sql: "ALTER TABLE orders OWNER TO app", code: "R-AT-OWNER", lock: ael, op: meta},
		{name: "replica identity is catalog only", sql: "ALTER TABLE orders REPLICA IDENTITY FULL", code: "R-AT-REPLIDENT", lock: ael, op: meta},
		{name: "replica identity by index is too", sql: "ALTER TABLE orders REPLICA IDENTITY USING INDEX orders_pkey", code: "R-AT-REPLIDENT", lock: ael, op: meta},

		// R-AT-INHERIT / R-AT-OF.
		{name: "an inheritance edge is catalog only", sql: "ALTER TABLE legacy_child NO INHERIT legacy_parent", code: "R-AT-INHERIT", lock: ael, op: meta, table: "apercu_snapshot_test.legacy_child"},
		{name: "binding a table to a type is catalog only", sql: "ALTER TABLE places NOT OF", code: "R-AT-OF", lock: ael, op: meta, table: "apercu_snapshot_test.places"},

		// R-AT-RENAME / R-AT-SETSCHEMA.
		{name: "renaming a table is catalog only", sql: "ALTER TABLE orders RENAME TO orders_old", code: "R-AT-RENAME", lock: ael, op: meta},
		{name: "moving a table between schemas is catalog only", sql: "ALTER TABLE orders SET SCHEMA public", code: "R-AT-SETSCHEMA", lock: ael, op: meta},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

func TestOwnerCarriesTheIndexes(t *testing.T) {
	t.Parallel()

	finding := findingOf(t, single(t, testCatalog(t), "ALTER TABLE orders OWNER TO app"), "R-AT-OWNER")
	for _, index := range []string{"apercu_snapshot_test.orders_pkey", "apercu_snapshot_test.orders_open_idx"} {
		target := targetOf(t, finding, index)
		assert.Equal(t, pg_contract.LockAccessExclusive, target.Lock)
		assert.Equal(t, pg_contract.TargetRoleImplicit, target.Role)
	}
}

func TestPartitionSubcommands(t *testing.T) {
	t.Parallel()

	cases := []ruleCase{
		{name: "attaching holds the parent at a lock that keeps it serving", sql: "ALTER TABLE events ATTACH PARTITION events_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')", code: "R-AT-ATTACH", lock: sue, op: meta, table: "apercu_snapshot_test.events"},
		{name: "detaching moves no row", sql: "ALTER TABLE events DETACH PARTITION events_2025", code: "R-AT-DETACH", lock: ael, op: meta, table: "apercu_snapshot_test.events"},
		{name: "finalising an interrupted detach is cheap", sql: "ALTER TABLE events DETACH PARTITION events_2025 FINALIZE", code: "R-AT-DETACH-FIN", lock: sue, op: meta, table: "apercu_snapshot_test.events"},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

// TestAttachPartitionScansBothSides is R-AT-ATTACH.
func TestAttachPartitionScansBothSides(t *testing.T) {
	t.Parallel()

	finding := findingOf(t,
		single(t, testCatalog(t), "ALTER TABLE events ATTACH PARTITION events_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"),
		"R-AT-ATTACH")

	attached := targetOf(t, finding, "apercu_snapshot_test.events_2026")
	assert.Equal(t, pg_contract.LockAccessExclusive, attached.Lock)
	assert.Equal(t, pg_contract.OpKindScan, attached.OpKind)

	fallback := targetOf(t, finding, "apercu_snapshot_test.events_default")
	assert.Equal(t, pg_contract.OpKindScan, fallback.OpKind)
	assert.Equal(t, pg_contract.TargetRoleImplicit, fallback.Role)

	assert.Contains(t, finding.Message, "IS NOT NULL", "the implied partition constraint includes it, and a CHECK that omits it does not skip the scan")
}

// TestDetachConcurrentlyIsRefusedWithADefaultPartition test R-AT-DETACH-CONC error case.
func TestDetachConcurrentlyIsRefusedWithADefaultPartition(t *testing.T) {
	t.Parallel()

	analysis := single(t, testCatalog(t), "ALTER TABLE events DETACH PARTITION events_2025 CONCURRENTLY")
	assert.Empty(t, analysis.Findings)
	require.Len(t, analysis.Errors, 1)
	assert.Contains(t, analysis.Errors[0].Message, "default partition")
}

func TestMoveAllInTablespace(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)

	t.Run("a named tablespace resolves to the oid the relations carry", func(t *testing.T) {
		finding := findingOf(t, single(t, catalog, "ALTER TABLE ALL IN TABLESPACE pg_default SET TABLESPACE fast"), "R-AT-002")

		table := targetOf(t, finding, "apercu_snapshot_test.orders")
		assert.Equal(t, pg_contract.LockAccessExclusive, table.Lock)
		assert.Equal(t, pg_contract.OpKindRewrite, table.OpKind)

		// A partitioned parent has no file to copy, and it does not carry its partitions along.
		// They move only when they are in the named tablespace themselves.
		assert.Equal(t, pg_contract.OpKindMetadata, targetOf(t, finding, "apercu_snapshot_test.events").OpKind)
		assert.Equal(t, pg_contract.OpKindRewrite, targetOf(t, finding, "apercu_snapshot_test.events_2025").OpKind)

		for _, target := range finding.Targets {
			assert.Truef(t, target.Relation.Kind.IsTable(), "%s is a %s and ALTER TABLE moves nothing but tables",
				target.Relation, target.Relation.Kind)
		}
	})

	t.Run("each object type moves its own relkinds", func(t *testing.T) {
		indexes := findingOf(t, single(t, catalog, "ALTER INDEX ALL IN TABLESPACE pg_default SET TABLESPACE fast"), "R-IX-ALLTABLESPACE")
		assert.Equal(t, pg_contract.LockAccessExclusive, targetOf(t, indexes, "apercu_snapshot_test.orders_open_idx").Lock)
		for _, target := range indexes.Targets {
			assert.Truef(t, target.Relation.Kind.IsIndex(), "%s is a %s and ALTER INDEX moves nothing but indexes",
				target.Relation, target.Relation.Kind)
		}

		views := findingOf(t, single(t, catalog, "ALTER MATERIALIZED VIEW ALL IN TABLESPACE pg_default SET TABLESPACE fast"), "R-MV-ALLTABLESPACE")
		assert.Len(t, views.Targets, 1)
		assert.Equal(t, pg_contract.LockAccessExclusive, targetOf(t, views, "apercu_snapshot_test.order_totals").Lock)
	})

	t.Run("OWNED BY narrows it to one role", func(t *testing.T) {
		analysis := single(t, catalog, "ALTER TABLE ALL IN TABLESPACE pg_default OWNED BY nobody SET TABLESPACE fast")
		assert.Empty(t, findingOf(t, analysis, "R-AT-002").Targets)
	})

	t.Run("a tablespace the snapshot never saw can only be narrowed to what is not in the default", func(t *testing.T) {
		finding := findingOf(t, single(t, catalog, "ALTER TABLE ALL IN TABLESPACE slow SET TABLESPACE fast"), "R-AT-002")
		assert.Contains(t, finding.Message, "no tablespace named")
		// Every relation in the fixture sits in the default tablespace, so none of them can be in "slow".
		assert.Empty(t, finding.Targets)
	})
}

func TestMoveAllFollowsTheDatabaseDefault(t *testing.T) {
	t.Parallel()

	catalog := tablespaceCatalog(t)

	t.Run("the database default matches the relations that record none", func(t *testing.T) {
		finding := findingOf(t, single(t, catalog, "ALTER TABLE ALL IN TABLESPACE slow SET TABLESPACE fast"), "R-AT-002")
		assert.Len(t, finding.Targets, 1)
		assert.Equal(t, pg_contract.LockAccessExclusive, targetOf(t, finding, "public.here").Lock)
	})

	t.Run("pg_default matches the relations that record it explicitly", func(t *testing.T) {
		finding := findingOf(t, single(t, catalog, "ALTER TABLE ALL IN TABLESPACE pg_default SET TABLESPACE fast"), "R-AT-002")
		assert.Len(t, finding.Targets, 1)
		assert.Equal(t, pg_contract.LockAccessExclusive, targetOf(t, finding, "public.there").Lock)
	})
}
