package shadow_test

import (
	"testing"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/classifytest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShadowCatalogRecordsWhatWasCreated validate that the shadow catalog record the relation created and their kind.
func TestShadowCatalogRecordsWhatWasCreated(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sql     string
		table   string
		relkind string
	}{
		{"CREATE TABLE t_plain (a int)", "t_plain", "r"},
		{"CREATE TABLE t_parted (a int) PARTITION BY RANGE (a)", "t_parted", "p"},
		{"CREATE TABLE t_ctas AS SELECT 1 AS a", "t_ctas", "r"},
		{"CREATE VIEW v_new AS SELECT 1", "v_new", "v"},
		{"CREATE MATERIALIZED VIEW m_new AS SELECT 1", "m_new", "m"},
		{"CREATE SEQUENCE s_new", "s_new", "S"},
		{"CREATE INDEX i_new ON users (id)", "i_new", "i"},
		{"CREATE INDEX i_parted ON events (id)", "i_parted", "I"},
	}

	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			t.Parallel()

			catalog := classifytest.Catalog(t)
			classifytest.Run(t, catalog, tc.sql)

			info := catalog.Resolve(helper.FullRelationName{Schema: "apercu_snapshot_test", Table: tc.table}, nil)
			require.Truef(t, info.Exists(), "%s should have been declared", tc.table)
			assert.Equal(t, tc.relkind, info.Relation.Kind)
		})
	}
}

func TestShadowCatalogDoesNotShadowARealRelation(t *testing.T) {
	t.Parallel()

	catalog := classifytest.Catalog(t)
	before := catalog.Resolve(helper.FullRelationName{Schema: "apercu_snapshot_test", Table: "users"}, nil)
	require.Equal(t, pgcatalog.OriginExisting, before.Origin)

	classifytest.Run(t, catalog, "CREATE TEMP TABLE users (a int)")

	after := catalog.Resolve(helper.FullRelationName{Schema: "apercu_snapshot_test", Table: "users"}, nil)
	assert.Equal(t, pgcatalog.OriginExisting, after.Origin)
	assert.Equal(t, before.Relation.OID, after.Relation.OID)
}

func TestShadowCatalogFollowsDropsAndRenames(t *testing.T) {
	t.Parallel()

	name := func(table string) helper.FullRelationName {
		return helper.FullRelationName{Schema: "apercu_snapshot_test", Table: table}
	}

	t.Run("a dropped table created again is the migration's", func(t *testing.T) {
		catalog := classifytest.Catalog(t)
		classifytest.Run(t, catalog, "DROP TABLE users CASCADE; CREATE TABLE users (a int)")
		info := catalog.Resolve(name("users"), nil)
		assert.Equal(t, pgcatalog.OriginCreated, info.Origin)
		assert.True(t, catalog.CreatedByMigration(name("users")))
	})

	t.Run("a dropped partitioned table takes its partitions", func(t *testing.T) {
		catalog := classifytest.Catalog(t)
		events := catalog.Resolve(name("events"), nil)
		require.NotEmpty(t, catalog.Children(events.Key()))
		classifytest.Run(t, catalog, "DROP TABLE events")
		assert.False(t, catalog.Resolve(name("events"), nil).Exists())
		assert.False(t, catalog.Resolve(name("events_2025"), nil).Exists())
		assert.Empty(t, catalog.Children(events.Key()))
	})

	t.Run("a renamed table answers to its new name only", func(t *testing.T) {
		catalog := classifytest.Catalog(t)
		before := catalog.Resolve(name("users"), nil)
		classifytest.Run(t, catalog, "ALTER TABLE users RENAME TO members")
		assert.False(t, catalog.Resolve(name("users"), nil).Exists())
		after := catalog.Resolve(name("members"), nil)
		assert.Equal(t, pgcatalog.OriginExisting, after.Origin)
		assert.Equal(t, before.Relation.OID, after.Relation.OID)
		byKey, ok := catalog.RelationByKey(after.Key())
		require.True(t, ok)
		assert.Equal(t, name("members"), byKey.Name)
	})

	t.Run("a table moved to another schema", func(t *testing.T) {
		catalog := classifytest.Catalog(t)
		classifytest.Run(t, catalog, "ALTER TABLE orders SET SCHEMA archive")
		assert.False(t, catalog.Resolve(name("orders"), nil).Exists())
		assert.True(t, catalog.Resolve(helper.FullRelationName{Schema: "archive", Table: "orders"}, nil).Exists())
		assert.True(t, catalog.Resolve(helper.FullRelationName{Schema: "archive", Table: "orders_open_idx"}, nil).Exists(),
			"its indexes follow it")
	})

	t.Run("a renamed created table keeps its partitions", func(t *testing.T) {
		catalog := classifytest.Catalog(t)
		classifytest.Run(t, catalog, "CREATE TABLE p (a int) PARTITION BY RANGE (a); "+
			"CREATE TABLE p1 PARTITION OF p FOR VALUES FROM (1) TO (2); ALTER TABLE p RENAME TO q")
		assert.False(t, catalog.Resolve(name("p"), nil).Exists())
		q := catalog.Resolve(name("q"), nil)
		require.Equal(t, pgcatalog.OriginCreated, q.Origin)
		children := catalog.Children(q.Key())
		require.Len(t, children, 1)
		assert.Equal(t, catalog.Resolve(name("p1"), nil).Key(), children[0].Child)
	})
}
