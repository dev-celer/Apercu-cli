package engine_test

import (
	"testing"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The fixture's metrics tree: metrics → metrics_2025, metrics_default → metrics_default_{a,b,other},
// with a foreign key to users cloned onto every level and the index tree of metrics_at_idx.

func fixtureName(table string) string { return "apercu_snapshot_test." + table }

func TestAttachReprovesThePartitionedDefaultAndClonesTheForeignKey(t *testing.T) {
	t.Parallel()

	catalog := classifytest.CatalogOn(t, pgcontract.Version17)
	finding := classifytest.FindingOf(t, classifytest.Single(t, catalog, "ALTER TABLE metrics ATTACH PARTITION orders FOR VALUES FROM ('2030-01-01') TO ('2031-01-01')"), "R-AT-ATTACH")

	assert.Equal(t, pgcontract.LockShareUpdateExclusive, classifytest.TargetOf(t, finding, fixtureName("metrics")).Lock)
	for _, leaf := range []string{"metrics_default_a", "metrics_default_b", "metrics_default_other"} {
		target := classifytest.TargetOf(t, finding, fixtureName(leaf))
		assert.Equal(t, pgcontract.LockAccessExclusive, target.Lock, leaf)
		assert.Equal(t, pgcontract.OpKindScan, target.OpKind, "%s is re-proved against the new bound", leaf)
	}
	users := classifytest.TargetOf(t, finding, fixtureName("users"))
	assert.Equal(t, pgcontract.LockShareRowExclusive, users.Lock, "the key is cloned onto the newcomer, with triggers on users")
	assert.Equal(t, pgcontract.TargetRoleImplicit, users.Role)

	for _, target := range finding.Targets {
		assert.NotEqual(t, "metrics_2025", target.Relation.Name.Table, "the other partitions are left alone")
	}
}

func TestAttachUnderAnIntermediateParentReadsTheAncestors(t *testing.T) {
	t.Parallel()

	catalog := classifytest.CatalogOn(t, pgcontract.Version17)
	finding := classifytest.FindingOf(t, classifytest.Single(t, catalog, "ALTER TABLE metrics_default ATTACH PARTITION orders FOR VALUES IN ('z')"), "R-AT-ATTACH")
	assert.Equal(t, pgcontract.LockAccessShare, classifytest.TargetOf(t, finding, fixtureName("metrics")).Lock)
}

func TestDetachDoesNotReachIntoTheDefault(t *testing.T) {
	t.Parallel()

	catalog := classifytest.CatalogOn(t, pgcontract.Version17)
	finding := classifytest.FindingOf(t, classifytest.Single(t, catalog, "ALTER TABLE metrics DETACH PARTITION metrics_2025"), "R-AT-DETACH")
	assert.Equal(t, pgcontract.LockAccessExclusive, classifytest.TargetOf(t, finding, fixtureName("metrics_default")).Lock)
	for _, target := range finding.Targets {
		assert.NotContains(t, []string{"metrics_default_a", "metrics_default_b", "metrics_default_other"}, target.Relation.Name.Table,
			"a detach takes the default partition itself, not its tree")
	}
	// The detached partition's cloned key becomes a key of its own, with triggers on users.
	assert.Equal(t, pgcontract.LockShareRowExclusive, classifytest.TargetOf(t, finding, fixtureName("users")).Lock)

	tree := classifytest.FindingOf(t, classifytest.Single(t, catalog, "ALTER TABLE metrics DETACH PARTITION metrics_default"), "R-AT-DETACH")
	for _, leaf := range []string{"metrics_default_a", "metrics_default_b", "metrics_default_other"} {
		assert.Equal(t, pgcontract.LockAccessExclusive, classifytest.TargetOf(t, tree, fixtureName(leaf)).Lock, "a detached tree goes whole")
	}
}

func TestDropPartitionedIndexReachesEveryIndexAndTable(t *testing.T) {
	t.Parallel()

	catalog := classifytest.CatalogOn(t, pgcontract.Version17)
	finding := classifytest.FindingOf(t, classifytest.Single(t, catalog, "DROP INDEX metrics_at_idx"), "R-IX-DROP")
	for _, table := range []string{"metrics", "metrics_2025", "metrics_default", "metrics_default_a", "metrics_default_b", "metrics_default_other"} {
		assert.Equal(t, pgcontract.LockAccessExclusive, classifytest.TargetOf(t, finding, fixtureName(table)).Lock, table)
	}
	indexes := 0
	for _, target := range finding.Targets {
		if target.Relation.Kind.IsIndex() {
			indexes++
		}
	}
	assert.Equal(t, 6, indexes, "the partitioned index and the five below it")
}

func TestDropPartitionLeavesTheClonedKeysTablesAlone(t *testing.T) {
	t.Parallel()

	catalog := classifytest.CatalogOn(t, pgcontract.Version17)
	finding := classifytest.FindingOf(t, classifytest.Single(t, catalog, "DROP TABLE metrics_default_a"), "R-TB-DROP")
	assert.Equal(t, pgcontract.LockAccessExclusive, classifytest.TargetOf(t, finding, fixtureName("metrics_default")).Lock, "the parent of a partition is locked")
	for _, target := range finding.Targets {
		assert.NotEqual(t, "users", target.Relation.Name.Table, "the cloned key's triggers on users belong to the original")
	}

	// The original takes them away.
	original := classifytest.FindingOf(t, classifytest.Single(t, catalog, "DROP TABLE metrics"), "R-TB-DROP")
	assert.Equal(t, pgcontract.LockAccessExclusive, classifytest.TargetOf(t, original, fixtureName("users")).Lock)

	// A classic child is not a partition: its parent is not touched.
	classic := classifytest.FindingOf(t, classifytest.Single(t, catalog, "DROP TABLE legacy_child"), "R-TB-DROP")
	for _, target := range classic.Targets {
		assert.NotEqual(t, "legacy_parent", target.Relation.Name.Table)
	}
}

func TestDroppingAReferencedTableReachesTheReferencingTree(t *testing.T) {
	t.Parallel()

	catalog := classifytest.CatalogOn(t, pgcontract.Version17)
	finding := classifytest.FindingOf(t, classifytest.Single(t, catalog, "DROP TABLE users CASCADE"), "R-TB-DROP")
	for _, table := range []string{"metrics", "metrics_2025", "metrics_default", "metrics_default_a", "metrics_default_b", "metrics_default_other"} {
		target := classifytest.TargetOf(t, finding, fixtureName(table))
		assert.Equal(t, pgcontract.LockAccessExclusive, target.Lock, table)
		assert.Equal(t, pgcontract.TargetRoleImplicit, target.Role, table)
	}
}

// The shadow catalog: a statement expands through the tree the migration has built so far, links
// between a declared relation and a snapshot one included.
func TestExpandFollowsTheLinksTheMigrationDraws(t *testing.T) {
	t.Parallel()

	catalog := classifytest.CatalogOn(t, pgcontract.Version17)
	analyses := classifytest.Analyze(t, catalog, `
CREATE TABLE np (id bigint, user_id bigint, total numeric(10,2), status text, code text) PARTITION BY RANGE (id);
ALTER TABLE np ATTACH PARTITION orders FOR VALUES FROM (0) TO (1000);
ALTER TABLE np ADD COLUMN z text DEFAULT gen_random_uuid()::text;
ALTER TABLE np DETACH PARTITION orders;
ALTER TABLE np ADD COLUMN y text DEFAULT gen_random_uuid()::text;`)
	require.Len(t, analyses, 5)

	attached := classifytest.TargetOf(t, classifytest.FindingOf(t, analyses[2], "R-AT-ADDCOL"), fixtureName("orders"))
	assert.Equal(t, pgcontract.LockAccessExclusive, attached.Lock)
	assert.Equal(t, pgcontract.OpKindRewrite, attached.OpKind, "the attached table is rewritten through its new parent")
	assert.Equal(t, pgcontract.TargetRoleExpanded, attached.Role)

	for _, target := range classifytest.FindingOf(t, analyses[4], "R-AT-ADDCOL").Targets {
		assert.NotEqual(t, "orders", target.Relation.Name.Table, "a detached table is no longer reached")
	}

	np := catalog.Resolve(helper.FullRelationName{Schema: "apercu_snapshot_test", Table: "np"}, nil)
	require.Equal(t, pgcatalog.OriginCreated, np.Origin)
	assert.Zero(t, np.Relation.OID, "a relation the migration creates carries no OID")
}
