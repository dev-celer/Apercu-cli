package pgclassify_test

import (
	"testing"

	"apercu-cli/helper"
	"apercu-cli/internal/pgclassify"
	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRulesReadTheMigrationsColumns checks that the column rules read a column as the migration left it: one it
// created, added, copied from another table or retyped, not only the snapshot's.
func TestRulesReadTheMigrationsColumns(t *testing.T) {
	t.Parallel()

	const (
		meta    = pgcontract.OpKindMetadata
		scan    = pgcontract.OpKindScan
		rewrite = pgcontract.OpKindRewrite
	)
	cases := []struct {
		name    string
		version pgcontract.Version
		script  string
		code    pgcontract.Code
		table   string
		op      pgcontract.OpKind
	}{
		// ALTER COLUMN TYPE compares with the type the column has now.
		{name: "a snapshot column retyped twice",
			script: "ALTER TABLE orders ALTER COLUMN code TYPE varchar(10); ALTER TABLE orders ALTER COLUMN code TYPE varchar(20)",
			code:   "R-AT-TYPE", table: "orders", op: meta},
		{name: "a column the migration added",
			script: "ALTER TABLE orders ADD COLUMN ref varchar(10); ALTER TABLE orders ALTER COLUMN ref TYPE varchar(20)",
			code:   "R-AT-TYPE", table: "orders", op: meta},
		{name: "an added column under its new name",
			script: "ALTER TABLE orders ADD COLUMN ref varchar(10); ALTER TABLE orders RENAME COLUMN ref TO r2; " +
				"ALTER TABLE orders ALTER COLUMN r2 TYPE varchar(20)",
			code: "R-AT-TYPE", table: "orders", op: meta},
		{name: "an added column narrowed",
			script: "ALTER TABLE orders ADD COLUMN ref varchar(20); ALTER TABLE orders ALTER COLUMN ref TYPE varchar(10)",
			code:   "R-AT-TYPE", table: "orders", op: rewrite},
		{name: "a column of a table the migration created",
			script: "CREATE TABLE t (a varchar(10)); ALTER TABLE t ALTER COLUMN a TYPE varchar(20)",
			code:   "R-AT-TYPE", table: "t", op: meta},
		{name: "a column LIKE copied",
			script: "CREATE TABLE t (a varchar(10)); CREATE TABLE u (LIKE t); ALTER TABLE u ALTER COLUMN a TYPE varchar(20)",
			code:   "R-AT-TYPE", table: "u", op: meta},

		// SET and DROP EXPRESSION read how the column is generated.
		{name: "SET EXPRESSION on a created virtual column", version: pgcontract.Version18,
			script: "CREATE TABLE t (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL); ALTER TABLE t ALTER COLUMN b SET EXPRESSION AS (a * 3)",
			code:   "R-AT-SETEXPR", table: "t", op: meta},
		{name: "SET EXPRESSION on a created stored column", version: pgcontract.Version18,
			script: "CREATE TABLE t (a int, b int GENERATED ALWAYS AS (a * 2) STORED); ALTER TABLE t ALTER COLUMN b SET EXPRESSION AS (a * 3)",
			code:   "R-AT-SETEXPR", table: "t", op: rewrite},
		{name: "DROP EXPRESSION on an added virtual column", version: pgcontract.Version18,
			script: "ALTER TABLE orders ADD COLUMN b bigint GENERATED ALWAYS AS (id * 2) VIRTUAL; ALTER TABLE orders ALTER COLUMN b DROP EXPRESSION",
			code:   "R-AT-DROPEXPR", table: "orders", op: rewrite},

		// SET NOT NULL reads the NOT NULL a column was created with. Up to 17 only the column itself says so: from 18
		// on, a NOT NULL constraint does.
		{name: "an inline NOT NULL", version: pgcontract.Version17,
			script: "CREATE TABLE t (a int NOT NULL); ALTER TABLE t ALTER COLUMN a SET NOT NULL",
			code:   "R-AT-SETNOTNULL", table: "t", op: meta},
		{name: "an inline PRIMARY KEY", version: pgcontract.Version17,
			script: "CREATE TABLE t (a int PRIMARY KEY); ALTER TABLE t ALTER COLUMN a SET NOT NULL",
			code:   "R-AT-SETNOTNULL", table: "t", op: meta},
		{name: "a table PRIMARY KEY", version: pgcontract.Version17,
			script: "CREATE TABLE t (a int, b int, PRIMARY KEY (a, b)); ALTER TABLE t ALTER COLUMN b SET NOT NULL",
			code:   "R-AT-SETNOTNULL", table: "t", op: meta},
		{name: "an added NOT NULL column", version: pgcontract.Version17,
			script: "ALTER TABLE orders ADD COLUMN ref int NOT NULL DEFAULT 0; ALTER TABLE orders ALTER COLUMN ref SET NOT NULL",
			code:   "R-AT-SETNOTNULL", table: "orders", op: meta},
		{name: "a NOT NULL LIKE copied", version: pgcontract.Version17,
			script: "CREATE TABLE t (a int NOT NULL); CREATE TABLE u (LIKE t); ALTER TABLE u ALTER COLUMN a SET NOT NULL",
			code:   "R-AT-SETNOTNULL", table: "u", op: meta},
		{name: "a NOT NULL a partition took from its parent", version: pgcontract.Version17,
			script: "CREATE TABLE p (a int NOT NULL, k int) PARTITION BY LIST (k); CREATE TABLE c PARTITION OF p FOR VALUES IN (1); " +
				"ALTER TABLE c ALTER COLUMN a SET NOT NULL",
			code: "R-AT-SETNOTNULL", table: "c", op: meta},
		{name: "an inline NOT NULL on PG 18", version: pgcontract.Version18,
			script: "CREATE TABLE t (a int NOT NULL); ALTER TABLE t ALTER COLUMN a SET NOT NULL",
			code:   "R-AT-SETNOTNULL", table: "t", op: meta},
		{name: "a created column without NOT NULL", version: pgcontract.Version17,
			script: "CREATE TABLE t (a int); ALTER TABLE t ALTER COLUMN a SET NOT NULL",
			code:   "R-AT-SETNOTNULL", table: "t", op: scan},
		{name: "an inline NOT NULL dropped again", version: pgcontract.Version17,
			script: "CREATE TABLE t (a int NOT NULL); ALTER TABLE t ALTER COLUMN a DROP NOT NULL; ALTER TABLE t ALTER COLUMN a SET NOT NULL",
			code:   "R-AT-SETNOTNULL", table: "t", op: scan},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			catalog := classifytest.Catalog(t)
			if testCase.version != 0 {
				catalog = classifytest.CatalogOn(t, testCase.version)
			}
			analyses := classifytest.Analyze(t, catalog, testCase.script)
			last := analyses[len(analyses)-1]
			assert.Empty(t, last.Errors)
			finding := classifytest.FindingOf(t, last, testCase.code)
			target := classifytest.TargetOf(t, finding, "apercu_snapshot_test."+testCase.table)
			assert.Equal(t, testCase.op, target.OpKind, finding.Message)
		})
	}
}

// The migration's columns are resolved like the snapshot's: a created table's, a partition's taken from its parent,
// a retyped snapshot column under its new type.
func TestColumnsOfTheMigration(t *testing.T) {
	t.Parallel()

	classifier := pgclassify.NewClassifier(classifytest.Catalog(t))
	classifier.Analyze(pgparse.Parse("CREATE TABLE p (a varchar(10) NOT NULL, k int) PARTITION BY LIST (k); " +
		"CREATE TABLE c PARTITION OF p FOR VALUES IN (1); " +
		"ALTER TABLE p ADD COLUMN n numeric(12, 2); " +
		"ALTER TABLE orders ALTER COLUMN code TYPE varchar(10)"))
	state := classifier.State()
	name := func(table string) helper.FullRelationName {
		return helper.FullRelationName{Schema: "apercu_snapshot_test", Table: table}
	}

	child := state.Resolve(name("c"), nil)
	require.True(t, child.Exists())
	a, ok := state.Column(child.Key(), "a")
	require.True(t, ok, "a partition has its parent's columns")
	assert.Equal(t, "character varying(10)", a.FormattedType)
	assert.True(t, a.NotNull)
	n, ok := state.Column(child.Key(), "n")
	require.True(t, ok, "a column added to the parent reaches the partition")
	assert.Equal(t, "numeric(12,2)", n.FormattedType)
	_, ok = state.Column(child.Key(), "missing")
	assert.False(t, ok)

	orders := state.Resolve(name("orders"), nil)
	code, ok := state.Column(orders.Key(), "code")
	require.True(t, ok)
	assert.Equal(t, "character varying(10)", code.FormattedType)
	assert.Equal(t, int16(5), code.Num, "a retyped snapshot column keeps its attnum")
}
