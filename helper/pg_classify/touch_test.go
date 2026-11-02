package pg_classify

import (
	"testing"

	"apercu-cli/helper"
	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The fixture's metrics tree: metrics → metrics_2025, metrics_default → metrics_default_{a,b,other},
// with a foreign key to users cloned onto every level and the index tree of metrics_at_idx.

func fixtureName(table string) string { return "apercu_snapshot_test." + table }

// scopeOf is the scope a rule would see for one statement.
func scopeOf(t *testing.T, catalog *pg_catalog.Catalog, sql string) scope {
	t.Helper()

	statements := pg_parse.Parse(sql)
	require.Len(t, statements, 1)
	classifier := NewClassifier(catalog)
	return classifier.scope(statements[0], classifier.session.Next(statements[0]))
}

func TestExpandWalksEveryLevelAtTheTouchLock(t *testing.T) {
	t.Parallel()

	catalog := testCatalogOn(t, pg_contract.Version17)
	s := scopeOf(t, catalog, "SELECT 1")
	metrics := catalog.Resolve(helper.FullRelationName{Schema: "apercu_snapshot_test", Table: "metrics"}, nil)
	require.True(t, metrics.Exists())

	targets := s.expand([]touch{{
		info: metrics, lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindRewrite,
		role: pg_contract.TargetRoleDirect, below: toPartitions,
	}})
	byName := map[string]pg_contract.Target{}
	for _, target := range targets {
		byName[target.Relation.Name.Table] = target
	}
	require.Len(t, byName, 6, "the parent and its five partitions")
	assert.Equal(t, pg_contract.OpKindMetadata, byName["metrics"].OpKind, "a partitioned level has no storage to rewrite")
	assert.Equal(t, pg_contract.OpKindMetadata, byName["metrics_default"].OpKind)
	for _, leaf := range []string{"metrics_2025", "metrics_default_a", "metrics_default_b", "metrics_default_other"} {
		assert.Equal(t, pg_contract.OpKindRewrite, byName[leaf].OpKind, leaf)
		assert.Equal(t, pg_contract.LockAccessExclusive, byName[leaf].Lock, leaf)
		assert.Equal(t, pg_contract.TargetRoleExpanded, byName[leaf].Role, leaf)
	}
}

func TestExpandReachVariants(t *testing.T) {
	t.Parallel()

	catalog := testCatalogOn(t, pg_contract.Version17)
	s := scopeOf(t, catalog, "SELECT 1")
	metrics := catalog.Resolve(helper.FullRelationName{Schema: "apercu_snapshot_test", Table: "metrics"}, nil)
	leaf := func(targets []pg_contract.Target) pg_contract.Target {
		for _, target := range targets {
			if target.Relation.Name.Table == "metrics_default_a" {
				return target
			}
		}
		require.Fail(t, "metrics_default_a not reached")
		return pg_contract.Target{}
	}

	// locked opens the partitions without doing the work in them.
	lockedOnly := leaf(s.expand([]touch{{info: metrics, lock: pg_contract.LockShare, op: pg_contract.OpKindScan, below: below{partitions: locked}}}))
	assert.Equal(t, pg_contract.OpKindMetadata, lockedOnly.OpKind)

	// partitionLock replaces the touch's lock below it.
	shared := leaf(s.expand([]touch{{info: metrics, lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindScan,
		below: below{partitions: worked, partitionLock: pg_contract.LockShare}}}))
	assert.Equal(t, pg_contract.LockShare, shared.Lock)

	// leafOp decides the work leaf by leaf, the lock stays.
	hooked := leaf(s.expand([]touch{{info: metrics, lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindScan, below: toPartitions,
		leafOp: func(pg_catalog.RelationInfo) pg_contract.OpKind { return pg_contract.OpKindMetadata }}}))
	assert.Equal(t, pg_contract.OpKindMetadata, hooked.OpKind)
	assert.Equal(t, pg_contract.LockAccessExclusive, hooked.Lock)

	// What hangs off an implicit relation stays implicit.
	partner := leaf(s.expand([]touch{{info: metrics, lock: pg_contract.LockShareRowExclusive, role: pg_contract.TargetRoleImplicit, below: toPartitions}}))
	assert.Equal(t, pg_contract.TargetRoleImplicit, partner.Role)

	// untouched stops at the relation itself.
	assert.Len(t, s.expand([]touch{{info: metrics, lock: pg_contract.LockAccessShare, below: nowhere}}), 1)
}

func TestAttachReprovesThePartitionedDefaultAndClonesTheForeignKey(t *testing.T) {
	t.Parallel()

	catalog := testCatalogOn(t, pg_contract.Version17)
	finding := findingOf(t, single(t, catalog, "ALTER TABLE metrics ATTACH PARTITION orders FOR VALUES FROM ('2030-01-01') TO ('2031-01-01')"), "R-AT-ATTACH")

	assert.Equal(t, pg_contract.LockShareUpdateExclusive, targetOf(t, finding, fixtureName("metrics")).Lock)
	for _, leaf := range []string{"metrics_default_a", "metrics_default_b", "metrics_default_other"} {
		target := targetOf(t, finding, fixtureName(leaf))
		assert.Equal(t, pg_contract.LockAccessExclusive, target.Lock, leaf)
		assert.Equal(t, pg_contract.OpKindScan, target.OpKind, "%s is re-proved against the new bound", leaf)
	}
	users := targetOf(t, finding, fixtureName("users"))
	assert.Equal(t, pg_contract.LockShareRowExclusive, users.Lock, "the key is cloned onto the newcomer, with triggers on users")
	assert.Equal(t, pg_contract.TargetRoleImplicit, users.Role)

	for _, target := range finding.Targets {
		assert.NotEqual(t, "metrics_2025", target.Relation.Name.Table, "the other partitions are left alone")
	}
}

func TestAttachUnderAnIntermediateParentReadsTheAncestors(t *testing.T) {
	t.Parallel()

	catalog := testCatalogOn(t, pg_contract.Version17)
	finding := findingOf(t, single(t, catalog, "ALTER TABLE metrics_default ATTACH PARTITION orders FOR VALUES IN ('z')"), "R-AT-ATTACH")
	assert.Equal(t, pg_contract.LockAccessShare, targetOf(t, finding, fixtureName("metrics")).Lock)
}

func TestDetachDoesNotReachIntoTheDefault(t *testing.T) {
	t.Parallel()

	catalog := testCatalogOn(t, pg_contract.Version17)
	finding := findingOf(t, single(t, catalog, "ALTER TABLE metrics DETACH PARTITION metrics_2025"), "R-AT-DETACH")
	assert.Equal(t, pg_contract.LockAccessExclusive, targetOf(t, finding, fixtureName("metrics_default")).Lock)
	for _, target := range finding.Targets {
		assert.NotContains(t, []string{"metrics_default_a", "metrics_default_b", "metrics_default_other"}, target.Relation.Name.Table,
			"a detach takes the default partition itself, not its tree")
	}
	// The detached partition's cloned key becomes a key of its own, with triggers on users.
	assert.Equal(t, pg_contract.LockShareRowExclusive, targetOf(t, finding, fixtureName("users")).Lock)

	tree := findingOf(t, single(t, catalog, "ALTER TABLE metrics DETACH PARTITION metrics_default"), "R-AT-DETACH")
	for _, leaf := range []string{"metrics_default_a", "metrics_default_b", "metrics_default_other"} {
		assert.Equal(t, pg_contract.LockAccessExclusive, targetOf(t, tree, fixtureName(leaf)).Lock, "a detached tree goes whole")
	}
}

func TestDropPartitionedIndexReachesEveryIndexAndTable(t *testing.T) {
	t.Parallel()

	catalog := testCatalogOn(t, pg_contract.Version17)
	finding := findingOf(t, single(t, catalog, "DROP INDEX metrics_at_idx"), "R-IX-DROP")
	for _, table := range []string{"metrics", "metrics_2025", "metrics_default", "metrics_default_a", "metrics_default_b", "metrics_default_other"} {
		assert.Equal(t, pg_contract.LockAccessExclusive, targetOf(t, finding, fixtureName(table)).Lock, table)
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

	catalog := testCatalogOn(t, pg_contract.Version17)
	finding := findingOf(t, single(t, catalog, "DROP TABLE metrics_default_a"), "R-TB-DROP")
	assert.Equal(t, pg_contract.LockAccessExclusive, targetOf(t, finding, fixtureName("metrics_default")).Lock, "the parent of a partition is locked")
	for _, target := range finding.Targets {
		assert.NotEqual(t, "users", target.Relation.Name.Table, "the cloned key's triggers on users belong to the original")
	}

	// The original takes them away.
	original := findingOf(t, single(t, catalog, "DROP TABLE metrics"), "R-TB-DROP")
	assert.Equal(t, pg_contract.LockAccessExclusive, targetOf(t, original, fixtureName("users")).Lock)

	// A classic child is not a partition: its parent is not touched.
	classic := findingOf(t, single(t, catalog, "DROP TABLE legacy_child"), "R-TB-DROP")
	for _, target := range classic.Targets {
		assert.NotEqual(t, "legacy_parent", target.Relation.Name.Table)
	}
}

func TestDroppingAReferencedTableReachesTheReferencingTree(t *testing.T) {
	t.Parallel()

	catalog := testCatalogOn(t, pg_contract.Version17)
	finding := findingOf(t, single(t, catalog, "DROP TABLE users CASCADE"), "R-TB-DROP")
	for _, table := range []string{"metrics", "metrics_2025", "metrics_default", "metrics_default_a", "metrics_default_b", "metrics_default_other"} {
		target := targetOf(t, finding, fixtureName(table))
		assert.Equal(t, pg_contract.LockAccessExclusive, target.Lock, table)
		assert.Equal(t, pg_contract.TargetRoleImplicit, target.Role, table)
	}
}
