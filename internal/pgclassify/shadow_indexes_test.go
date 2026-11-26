package pgclassify_test

import (
	"testing"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify"
	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stateAfter is the state a script leaves, every statement of which has to be accepted.
func stateAfter(t *testing.T, script string) *pgcatalog.State {
	t.Helper()
	classifier := pgclassify.NewClassifier(classifytest.Catalog(t))
	for _, analysis := range classifier.Analyze(pgparse.Parse(script)) {
		require.Empty(t, analysis.Errors, analysis.RawSQL)
	}
	return classifier.State()
}

// childIndexes is the names of the indexes attached to an index.
func childIndexes(t *testing.T, state *pgcatalog.State, index string) []string {
	t.Helper()
	info := state.Resolve(relationName(index), nil)
	require.True(t, info.Exists(), index)
	var out []string
	for _, link := range state.Children(info.Key()) {
		child, ok := state.RelationByKey(link.Child)
		require.True(t, ok)
		out = append(out, child.Name.Table)
	}
	return out
}

// indexOn asserts that an index exists, on its table.
func indexOn(t *testing.T, state *pgcatalog.State, index, table string) {
	t.Helper()
	info := state.Resolve(relationName(index), nil)
	require.True(t, info.Exists(), "%s exists", index)
	owner, ok := state.TableOfIndex(info.Key())
	require.True(t, ok, index)
	assert.Equal(t, table, owner.Name.Table, index)
}

func noRelation(t *testing.T, state *pgcatalog.State, relation string) {
	t.Helper()
	assert.False(t, state.Resolve(relationName(relation), nil).Exists(), "%s does not exist", relation)
}

// TestRulesReadTheMigrationsIndexes checks that the rules naming a table's indexes name them as the migration left
// them: the ones it created, without the ones it dropped.
func TestRulesReadTheMigrationsIndexes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		script string
		code   pgcontract.Code
		// named and absent are indexes of the schema the last statement has to name, or must not.
		named, absent []string
	}{
		{name: "ALTER TYPE rebuilds an index the migration created",
			script: "CREATE INDEX orders_code_idx ON orders (code); ALTER TABLE orders ALTER COLUMN code TYPE varchar(10)",
			code:   "R-AT-TYPE", named: []string{"orders_code_idx"}},
		{name: "ALTER TYPE rebuilds an index over an expression on the column",
			script: "CREATE INDEX orders_lower_idx ON orders (lower(code)); ALTER TABLE orders ALTER COLUMN code TYPE varchar(10)",
			code:   "R-AT-TYPE", named: []string{"orders_lower_idx"}},
		{name: "ALTER TYPE rebuilds an index whose predicate reads the column",
			script: "CREATE INDEX orders_coded_idx ON orders (id) WHERE code IS NOT NULL; ALTER TABLE orders ALTER COLUMN code TYPE varchar(10)",
			code:   "R-AT-TYPE", named: []string{"orders_coded_idx"}},
		{name: "ALTER TYPE rebuilds an INCLUDE column's index",
			script: "CREATE INDEX orders_id_code_idx ON orders (id) INCLUDE (code); ALTER TABLE orders ALTER COLUMN code TYPE varchar(10)",
			code:   "R-AT-TYPE", named: []string{"orders_id_code_idx"}},
		{name: "ALTER TYPE follows the index through a column rename",
			script: "CREATE INDEX orders_code_idx ON orders (code); ALTER TABLE orders RENAME COLUMN code TO ref; " +
				"ALTER TABLE orders ALTER COLUMN ref TYPE varchar(10)",
			code: "R-AT-TYPE", named: []string{"orders_code_idx"}},
		{name: "ALTER TYPE leaves an index on another column",
			script: "CREATE INDEX orders_total_idx ON orders (total); ALTER TABLE orders ALTER COLUMN code TYPE varchar(10)",
			code:   "R-AT-TYPE", absent: []string{"orders_total_idx"}},
		{name: "ALTER TYPE does not rebuild an index the migration dropped",
			script: "DROP INDEX orders_open_idx; ALTER TABLE orders ALTER COLUMN status TYPE varchar(10)",
			code:   "R-AT-TYPE", absent: []string{"orders_open_idx"}},
		{name: "ALTER TYPE on a table the migration created",
			script: "CREATE TABLE t (a int); CREATE INDEX t_a_idx ON t (a); ALTER TABLE t ALTER COLUMN a TYPE bigint",
			code:   "R-AT-TYPE", named: []string{"t_a_idx"}},
		{name: "OWNER TO reaches an index the migration created",
			script: "CREATE INDEX orders_code_idx ON orders (code); ALTER TABLE orders OWNER TO someone",
			code:   "R-AT-OWNER", named: []string{"orders_code_idx", "orders_pkey"}},
		{name: "REINDEX TABLE rebuilds the migration's indexes, not the dropped ones",
			script: "CREATE INDEX orders_code_idx ON orders (code); DROP INDEX orders_open_idx; REINDEX TABLE orders",
			code:   "R-IX-REINDEX", named: []string{"orders_code_idx", "orders_pkey"}, absent: []string{"orders_open_idx"}},
		{name: "REINDEX INDEX rebuilds the partitions' indexes the migration built",
			script: "CREATE INDEX events_id_idx ON events (id); REINDEX INDEX events_id_idx",
			code:   "R-IX-REINDEX", named: []string{"events_2025_id_idx", "events_default_id_idx", "events_2025", "events_default"}},
		{name: "ALTER TYPE rebuilds the index of a UNIQUE the migration added",
			script: "ALTER TABLE orders ADD CONSTRAINT orders_code_key UNIQUE (code); ALTER TABLE orders ALTER COLUMN code TYPE varchar(10)",
			code:   "R-AT-TYPE", named: []string{"orders_code_key"}},
		{name: "ALTER TYPE rebuilds the index of a UNIQUE that INCLUDEs the column",
			script: "ALTER TABLE orders ADD UNIQUE (id) INCLUDE (code); ALTER TABLE orders ALTER COLUMN code TYPE varchar(10)",
			code:   "R-AT-TYPE", named: []string{"orders_id_code_key"}},
		{name: "ALTER TYPE rebuilds the PRIMARY KEY of a table the migration created",
			script: "CREATE TABLE t (a int PRIMARY KEY); ALTER TABLE t ALTER COLUMN a TYPE bigint",
			code:   "R-AT-TYPE", named: []string{"t_pkey"}},
		{name: "REINDEX TABLE no longer rebuilds a dropped PRIMARY KEY's index",
			script: "ALTER TABLE orders DROP CONSTRAINT orders_pkey; REINDEX TABLE orders",
			code:   "R-IX-REINDEX", named: []string{"orders_open_idx"}, absent: []string{"orders_pkey"}},
		{name: "REINDEX INDEX on a PRIMARY KEY's index the migration built",
			script: "CREATE TABLE t (a int PRIMARY KEY); REINDEX INDEX t_pkey",
			code:   "R-IX-REINDEX", named: []string{"t_pkey", "t"}},
		{name: "REINDEX INDEX on a constraint's index under its new name",
			script: "ALTER TABLE orders ADD CONSTRAINT k UNIQUE (code); ALTER TABLE orders RENAME CONSTRAINT k TO k2; REINDEX INDEX k2",
			code:   "R-IX-REINDEX", named: []string{"k2", "orders"}},
		{name: "REINDEX INDEX on a partitioned PRIMARY KEY reaches the partitions' indexes",
			script: "ALTER TABLE events ADD PRIMARY KEY (id, at); REINDEX INDEX events_pkey",
			code:   "R-IX-REINDEX", named: []string{"events_2025_pkey", "events_default_pkey", "events_2025", "events_default"}},
		{name: "REINDEX INDEX leaves a detached partition's index",
			script: "ALTER TABLE events DETACH PARTITION events_2025; REINDEX INDEX events_at_idx",
			code:   "R-IX-REINDEX", named: []string{"events_default_at_idx"}, absent: []string{"events_2025_at_idx", "events_2025"}},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			assertLastTargets(t, testCase.script, testCase.code, testCase.named, testCase.absent)
		})
	}
}

// assertLastTargets checks that the script's last statement raises code naming the named objects of the schema, and
// none of the absent ones.
func assertLastTargets(t *testing.T, script string, code pgcontract.Code, named, absent []string) {
	t.Helper()
	analyses := classifytest.Analyze(t, classifytest.Catalog(t), script)
	last := analyses[len(analyses)-1]
	assert.Empty(t, last.Errors)
	finding := classifytest.FindingOf(t, last, code)
	names := classifytest.TargetNames(&finding)
	for _, name := range named {
		assert.Contains(t, names, "apercu_snapshot_test."+name, finding.Message)
	}
	for _, name := range absent {
		assert.NotContains(t, names, "apercu_snapshot_test."+name, finding.Message)
	}
}

// An index the migration created belongs to its table: it is found from it, follows it to another schema, and goes
// with it, or with a column it depends on.
func TestIndexesOfTheMigration(t *testing.T) {
	t.Parallel()

	name := func(schema, relation string) helper.FullRelationName {
		return helper.FullRelationName{Schema: schema, Table: relation}
	}
	const schema = "apercu_snapshot_test"
	run := func(script string) *pgclassify.Classifier {
		classifier := pgclassify.NewClassifier(classifytest.Catalog(t))
		classifier.Analyze(pgparse.Parse(script))
		return classifier
	}

	t.Run("its table", func(t *testing.T) {
		state := run("CREATE INDEX orders_code_idx ON orders (code)").State()
		index := state.Resolve(name(schema, "orders_code_idx"), nil)
		require.True(t, index.Exists())
		table, ok := state.TableOfIndex(index.Key())
		require.True(t, ok)
		assert.Equal(t, "orders", table.Name.Table)
	})
	t.Run("dropped with its column", func(t *testing.T) {
		state := run("CREATE INDEX orders_lower_idx ON orders (lower(code)); ALTER TABLE orders DROP COLUMN code").State()
		assert.False(t, state.Resolve(name(schema, "orders_lower_idx"), nil).Exists())
	})
	t.Run("dropped with its table", func(t *testing.T) {
		state := run("CREATE TABLE t (a int); CREATE INDEX t_a_idx ON t (a); DROP TABLE t").State()
		assert.False(t, state.Resolve(name(schema, "t_a_idx"), nil).Exists())
	})
	t.Run("moved with its table", func(t *testing.T) {
		state := run("CREATE SCHEMA other; CREATE INDEX orders_code_idx ON orders (code); ALTER TABLE orders SET SCHEMA other").State()
		assert.False(t, state.Resolve(name(schema, "orders_code_idx"), nil).Exists())
		assert.True(t, state.Resolve(name("other", "orders_code_idx"), nil).Exists())
	})
}

// An index built on a partitioned table is an index tree: each partition gets its own index, one it already has that
// matches or one under a name the server generates, and the tree follows the partitions as they come and go.
func TestIndexTreesOfTheMigration(t *testing.T) {
	t.Parallel()

	t.Run("CREATE INDEX builds one on each partition", func(t *testing.T) {
		state := stateAfter(t, "CREATE INDEX events_id_idx ON events (id)")
		indexOn(t, state, "events_2025_id_idx", "events_2025")
		indexOn(t, state, "events_default_id_idx", "events_default")
		assert.ElementsMatch(t, []string{"events_2025_id_idx", "events_default_id_idx"}, childIndexes(t, state, "events_id_idx"))
	})
	t.Run("down every level", func(t *testing.T) {
		state := stateAfter(t, "CREATE INDEX metrics_kind_idx ON metrics (kind)")
		indexOn(t, state, "metrics_default_kind_idx", "metrics_default")
		assert.ElementsMatch(t, []string{"metrics_default_a_kind_idx", "metrics_default_b_kind_idx", "metrics_default_other_kind_idx"},
			childIndexes(t, state, "metrics_default_kind_idx"))
	})
	t.Run("adopting the index a partition already has", func(t *testing.T) {
		state := stateAfter(t, "CREATE INDEX events_2025_mine ON events_2025 (id); CREATE INDEX events_id_idx ON events (id)")
		assert.ElementsMatch(t, []string{"events_2025_mine", "events_default_id_idx"}, childIndexes(t, state, "events_id_idx"))
		noRelation(t, state, "events_2025_id_idx")
	})
	t.Run("not one on other columns", func(t *testing.T) {
		state := stateAfter(t, "CREATE INDEX events_2025_mine ON events_2025 (id, at); CREATE INDEX events_id_idx ON events (id)")
		indexOn(t, state, "events_2025_id_idx", "events_2025")
	})
	t.Run("under a numbered name when the generated one is taken", func(t *testing.T) {
		state := stateAfter(t, "CREATE TABLE events_2025_id_idx (x int); CREATE INDEX events_id_idx ON events (id)")
		indexOn(t, state, "events_2025_id_idx1", "events_2025")
	})
	t.Run("named after the names its expressions lend", func(t *testing.T) {
		state := stateAfter(t, "CREATE INDEX events_text_idx ON events ((id::text), (id + 1))")
		indexOn(t, state, "events_2025_id_expr_idx", "events_2025")
	})
	t.Run("named after an unnamed parent's", func(t *testing.T) {
		state := stateAfter(t, "CREATE INDEX ON events (lower(id::text))")
		indexOn(t, state, "events_lower_idx", "events")
		indexOn(t, state, "events_2025_lower_idx", "events_2025")
	})
	t.Run("a new partition's after the parent index's columns", func(t *testing.T) {
		state := stateAfter(t, "CREATE INDEX events_text_idx ON events (lower(id::text)); "+
			"CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')")
		indexOn(t, state, "events_2026_lower_idx", "events_2026")
	})
	t.Run("not after a column renamed since", func(t *testing.T) {
		state := stateAfter(t, "CREATE INDEX events_id_idx ON events (id); ALTER TABLE events RENAME COLUMN id TO ref; "+
			"CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')")
		indexOn(t, state, "events_2026_id_idx", "events_2026")
	})
	t.Run("ON ONLY builds none", func(t *testing.T) {
		state := stateAfter(t, "CREATE INDEX events_id_idx ON ONLY events (id)")
		noRelation(t, state, "events_2025_id_idx")
		assert.Empty(t, childIndexes(t, state, "events_id_idx"))
	})
	t.Run("PARTITION OF clones the parent's indexes", func(t *testing.T) {
		state := stateAfter(t, "CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')")
		indexOn(t, state, "events_2026_at_idx", "events_2026")
		assert.Contains(t, childIndexes(t, state, "events_at_idx"), "events_2026_at_idx")
	})
	t.Run("ATTACH adopts a matching index", func(t *testing.T) {
		state := stateAfter(t, "CREATE TABLE ev (id bigint, at timestamptz NOT NULL); CREATE INDEX ev_mine ON ev (at); "+
			"ALTER TABLE events ATTACH PARTITION ev FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')")
		assert.Contains(t, childIndexes(t, state, "events_at_idx"), "ev_mine")
		noRelation(t, state, "ev_at_idx")
	})
	t.Run("ATTACH builds a missing one", func(t *testing.T) {
		state := stateAfter(t, "CREATE TABLE ev (id bigint, at timestamptz NOT NULL); "+
			"ALTER TABLE events ATTACH PARTITION ev FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')")
		indexOn(t, state, "ev_at_idx", "ev")
		assert.Contains(t, childIndexes(t, state, "events_at_idx"), "ev_at_idx")
	})
	t.Run("DETACH keeps the partition's index, out of the tree", func(t *testing.T) {
		state := stateAfter(t, "ALTER TABLE events DETACH PARTITION events_2025")
		indexOn(t, state, "events_2025_at_idx", "events_2025")
		assert.Equal(t, []string{"events_default_at_idx"}, childIndexes(t, state, "events_at_idx"))
	})
	t.Run("DROP INDEX drops the tree", func(t *testing.T) {
		state := stateAfter(t, "CREATE INDEX events_id_idx ON events (id); DROP INDEX events_id_idx")
		noRelation(t, state, "events_2025_id_idx")
		noRelation(t, state, "events_default_id_idx")
	})
}

// The index behind a PRIMARY KEY, UNIQUE or EXCLUDE is a relation like any other: it goes by the constraint's name,
// follows it through renames and drops, and is in the partitions' index trees.
func TestConstraintIndexesOfTheMigration(t *testing.T) {
	t.Parallel()

	t.Run("an added constraint builds its index", func(t *testing.T) {
		state := stateAfter(t, "ALTER TABLE orders ADD CONSTRAINT orders_code_key UNIQUE (code)")
		indexOn(t, state, "orders_code_key", "orders")
	})
	t.Run("an unnamed one under the constraint's generated name", func(t *testing.T) {
		state := stateAfter(t, "CREATE TABLE t (a int, b int, UNIQUE (a, b), EXCLUDE USING gist (a WITH =))")
		indexOn(t, state, "t_a_b_key", "t")
		indexOn(t, state, "t_a_excl", "t")
	})
	t.Run("renamed with the constraint", func(t *testing.T) {
		state := stateAfter(t, "ALTER TABLE orders ADD CONSTRAINT k UNIQUE (code); ALTER TABLE orders RENAME CONSTRAINT k TO k2")
		indexOn(t, state, "k2", "orders")
		noRelation(t, state, "k")
	})
	t.Run("a snapshot one renamed with the constraint", func(t *testing.T) {
		state := stateAfter(t, "ALTER TABLE orders RENAME CONSTRAINT orders_pkey TO orders_key")
		indexOn(t, state, "orders_key", "orders")
		noRelation(t, state, "orders_pkey")
	})
	t.Run("the constraint renamed with the index", func(t *testing.T) {
		state := stateAfter(t, "ALTER TABLE orders ADD CONSTRAINT k UNIQUE (code); ALTER INDEX k RENAME TO k2")
		orders := state.Resolve(relationName("orders"), nil)
		_, ok := state.ConstraintByName(orders.Key(), "k2")
		assert.True(t, ok)
	})
	t.Run("dropped with the constraint", func(t *testing.T) {
		state := stateAfter(t, "ALTER TABLE orders ADD CONSTRAINT k UNIQUE (code); ALTER TABLE orders DROP CONSTRAINT k; "+
			"ALTER TABLE orders DROP CONSTRAINT orders_pkey")
		noRelation(t, state, "k")
		noRelation(t, state, "orders_pkey")
	})
	t.Run("USING INDEX takes the index under the constraint's name", func(t *testing.T) {
		state := stateAfter(t, "CREATE UNIQUE INDEX orders_code_uq ON orders (code); "+
			"ALTER TABLE orders ADD CONSTRAINT orders_code_key UNIQUE USING INDEX orders_code_uq")
		indexOn(t, state, "orders_code_key", "orders")
		noRelation(t, state, "orders_code_uq")
	})
	t.Run("one on each partition", func(t *testing.T) {
		state := stateAfter(t, "ALTER TABLE events ADD PRIMARY KEY (id, at)")
		indexOn(t, state, "events_2025_pkey", "events_2025")
		assert.ElementsMatch(t, []string{"events_2025_pkey", "events_default_pkey"}, childIndexes(t, state, "events_pkey"))
	})
	t.Run("down every level", func(t *testing.T) {
		state := stateAfter(t, "ALTER TABLE metrics ADD UNIQUE (id, at)")
		assert.Contains(t, childIndexes(t, state, "metrics_id_at_key"), "metrics_default_id_at_key")
		assert.Contains(t, childIndexes(t, state, "metrics_default_id_at_key"), "metrics_default_a_id_at_key")
	})
	t.Run("PARTITION OF clones it", func(t *testing.T) {
		state := stateAfter(t, "ALTER TABLE events ADD PRIMARY KEY (id, at); "+
			"CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')")
		assert.Contains(t, childIndexes(t, state, "events_pkey"), "events_2026_pkey")
	})
	t.Run("ATTACH adopts the partition's own", func(t *testing.T) {
		state := stateAfter(t, "ALTER TABLE events ADD PRIMARY KEY (id, at); "+
			"CREATE TABLE ev (id bigint NOT NULL, at timestamptz NOT NULL, CONSTRAINT ev_key PRIMARY KEY (id, at)); "+
			"ALTER TABLE events ATTACH PARTITION ev FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')")
		assert.Contains(t, childIndexes(t, state, "events_pkey"), "ev_key")
		noRelation(t, state, "ev_pkey")
	})
	t.Run("LIKE INCLUDING INDEXES copies them, and the plain ones", func(t *testing.T) {
		state := stateAfter(t, "CREATE TABLE t (LIKE orders INCLUDING INDEXES)")
		indexOn(t, state, "t_pkey", "t")
		indexOn(t, state, "t_status_idx", "t")
	})
	t.Run("a plain index's generated name steps around them", func(t *testing.T) {
		state := stateAfter(t, "ALTER TABLE orders ADD CONSTRAINT orders_code_idx UNIQUE (code); CREATE INDEX ON orders (code)")
		indexOn(t, state, "orders_code_idx1", "orders")
	})
}
