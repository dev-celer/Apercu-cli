package engine

import (
	"fmt"
	"path/filepath"
	"testing"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgclassify/session"
	"apercu-cli/internal/pgclassify/shadow"
	"apercu-cli/internal/pgclassify/stmt"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testCatalogOn is one committed snapshot of the given server, with production on the same version.
func testCatalogOn(t *testing.T, version pgcontract.Version) *pgcatalog.Catalog {
	t.Helper()

	name := fmt.Sprintf("snapshot_pg%d_preview.json.gz", version)
	pre, err := snapshot.LoadJSON(filepath.Join("..", "..", "..", "pgcatalog", "snapshot", "testdata", name))
	require.NoError(t, err)
	require.Equal(t, version, pre.Header.Version, "%s was captured on another server", name)

	prod := &snapshot.Snapshot{
		Source: snapshot.SourceProd,
		PIT:    snapshot.PITPre,
		Header: snapshot.Header{Version: version, ServerVersionNum: int(version) * 10000},
	}
	catalog, err := pgcatalog.NewCatalog(pgcatalog.CatalogOptions{Pre: pre, Prod: prod})
	require.NoError(t, err)
	return catalog
}

// scopeOf is the scope a rule would see for one statement, as a fresh classifier builds it.
func scopeOf(t *testing.T, catalog *pgcatalog.Catalog, sql string) Scope {
	t.Helper()

	statements := pgparse.Parse(sql)
	require.Len(t, statements, 1)
	context := session.NewSession(catalog).Next(statements[0])
	return Scope{Scope: shadow.Scope{View: stmt.New(catalog, context, statements[0]), Checks: shadow.NewChecks()}}
}

func TestExpandWalksEveryLevelAtTheTouchLock(t *testing.T) {
	t.Parallel()

	catalog := testCatalogOn(t, pgcontract.Version17)
	s := scopeOf(t, catalog, "SELECT 1")
	metrics := catalog.Resolve(helper.FullRelationName{Schema: "apercu_snapshot_test", Table: "metrics"}, nil)
	require.True(t, metrics.Exists())

	targets := s.Expand([]Touch{{
		Info: metrics, Lock: pgcontract.LockAccessExclusive, Op: pgcontract.OpKindRewrite,
		Role: pgcontract.TargetRoleDirect, Below: ToPartitions,
	}})
	byName := map[string]pgcontract.Target{}
	for _, target := range targets {
		byName[target.Relation.Name.Table] = target
	}
	require.Len(t, byName, 6, "the parent and its five partitions")
	assert.Equal(t, pgcontract.OpKindMetadata, byName["metrics"].OpKind, "a partitioned level has no storage to rewrite")
	assert.Equal(t, pgcontract.OpKindMetadata, byName["metrics_default"].OpKind)
	for _, leaf := range []string{"metrics_2025", "metrics_default_a", "metrics_default_b", "metrics_default_other"} {
		assert.Equal(t, pgcontract.OpKindRewrite, byName[leaf].OpKind, leaf)
		assert.Equal(t, pgcontract.LockAccessExclusive, byName[leaf].Lock, leaf)
		assert.Equal(t, pgcontract.TargetRoleExpanded, byName[leaf].Role, leaf)
	}
}

func TestExpandReachVariants(t *testing.T) {
	t.Parallel()

	catalog := testCatalogOn(t, pgcontract.Version17)
	s := scopeOf(t, catalog, "SELECT 1")
	metrics := catalog.Resolve(helper.FullRelationName{Schema: "apercu_snapshot_test", Table: "metrics"}, nil)
	leaf := func(targets []pgcontract.Target) pgcontract.Target {
		for _, target := range targets {
			if target.Relation.Name.Table == "metrics_default_a" {
				return target
			}
		}
		require.Fail(t, "metrics_default_a not reached")
		return pgcontract.Target{}
	}

	// locked opens the partitions without doing the work in them.
	lockedOnly := leaf(s.Expand([]Touch{{Info: metrics, Lock: pgcontract.LockShare, Op: pgcontract.OpKindScan, Below: Below{Partitions: Locked}}}))
	assert.Equal(t, pgcontract.OpKindMetadata, lockedOnly.OpKind)

	// partitionLock replaces the touch's lock below it.
	shared := leaf(s.Expand([]Touch{{Info: metrics, Lock: pgcontract.LockAccessExclusive, Op: pgcontract.OpKindScan,
		Below: Below{Partitions: Worked, PartitionLock: pgcontract.LockShare}}}))
	assert.Equal(t, pgcontract.LockShare, shared.Lock)

	// leafOp decides the work leaf by leaf, the lock stays.
	hooked := leaf(s.Expand([]Touch{{Info: metrics, Lock: pgcontract.LockAccessExclusive, Op: pgcontract.OpKindScan, Below: ToPartitions,
		LeafOp: func(pgcatalog.RelationInfo) pgcontract.OpKind { return pgcontract.OpKindMetadata }}}))
	assert.Equal(t, pgcontract.OpKindMetadata, hooked.OpKind)
	assert.Equal(t, pgcontract.LockAccessExclusive, hooked.Lock)

	// What hangs off an implicit relation stays implicit.
	partner := leaf(s.Expand([]Touch{{Info: metrics, Lock: pgcontract.LockShareRowExclusive, Role: pgcontract.TargetRoleImplicit, Below: ToPartitions}}))
	assert.Equal(t, pgcontract.TargetRoleImplicit, partner.Role)

	// untouched stops at the relation itself.
	assert.Len(t, s.Expand([]Touch{{Info: metrics, Lock: pgcontract.LockAccessShare, Below: Nowhere}}), 1)
}
