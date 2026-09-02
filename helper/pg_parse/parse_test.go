package pg_parse

import (
	"strings"
	"testing"

	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSplit covers what makes the scanner necessary: a proxy event can hold several statements,
// and a semicolon inside a comment or a dollar-quoted body is not a boundary.
func TestSplit(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "several statements in one event",
			sql:  "BEGIN; ALTER TABLE t ADD COLUMN a int; COMMIT;",
			want: []string{"BEGIN", "ALTER TABLE t ADD COLUMN a int", "COMMIT"},
		},
		{
			name: "a semicolon inside a line comment is not a boundary",
			sql:  "ALTER TABLE t -- drop it; really\nADD COLUMN a int;",
			want: []string{"ALTER TABLE t -- drop it; really\nADD COLUMN a int"},
		},
		{
			name: "a semicolon inside a dollar-quoted body is not a boundary",
			sql:  "CREATE FUNCTION f() RETURNS int AS $$ BEGIN; RETURN 1; END $$ LANGUAGE plpgsql; SELECT 1",
			want: []string{"CREATE FUNCTION f() RETURNS int AS $$ BEGIN; RETURN 1; END $$ LANGUAGE plpgsql", "SELECT 1"},
		},
		{
			name: "a semicolon inside a string literal is not a boundary",
			sql:  "INSERT INTO t VALUES ('a;b'); SELECT 1",
			want: []string{"INSERT INTO t VALUES ('a;b')", "SELECT 1"},
		},
		{
			name: "empty statements are dropped",
			sql:  ";;  ;;",
			want: []string{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, Split(tc.sql))
		})
	}
}

// TestParseSplitsEvent checks that Parse works at the level the proxy delivers: one event in,
// one IR per statement out, each keeping the text it came from.
func TestParseSplitsEvent(t *testing.T) {
	t.Parallel()

	statements := Parse("BEGIN; ALTER TABLE t ADD COLUMN a int; COMMIT")
	require.Len(t, statements, 3)

	assert.Equal(t, pg_contract.Command("BEGIN"), statements[0].Command)
	assert.Equal(t, pg_contract.Command("ALTER TABLE"), statements[1].Command)
	assert.Equal(t, "ALTER TABLE t ADD COLUMN a int", statements[1].RawSQL)
	assert.Equal(t, pg_contract.Command("COMMIT"), statements[2].Command)
}

// TestUnparseableFailsSafe is S-FAILSAFE for the parser: a statement nothing understands still
// produces a record, because the migration will run it either way.
func TestUnparseableFailsSafe(t *testing.T) {
	t.Parallel()

	statement := ParseOne("ALTER TABLE t DO SOMETHING WEIRD")
	assert.False(t, statement.Parsed())
	assert.NotEmpty(t, statement.Unparsed)
	assert.Equal(t, "ALTER TABLE t DO SOMETHING WEIRD", statement.RawSQL)
	assert.Empty(t, statement.Relations)
}

// TestUnmodelledStatementKeepsItsName covers the other fail-safe direction: a statement the IR
// does not model parses fine and is named, rather than reported as broken.
func TestUnmodelledStatementKeepsItsName(t *testing.T) {
	t.Parallel()

	statement := ParseOne("CREATE FUNCTION f() RETURNS int AS 'SELECT 1' LANGUAGE sql")
	assert.True(t, statement.Parsed())
	assert.Equal(t, pg_contract.Command("CREATE FUNCTION"), statement.Command)
}

// TestMinVersion is what §8 reads: the oldest server that accepts everything the statement uses.
func TestMinVersion(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want pg_contract.Version
	}{
		{"plain syntax runs everywhere", "ALTER TABLE t ADD COLUMN a int", pg_contract.Version15},
		{"SET STORAGE DEFAULT needs 16", "ALTER TABLE t ALTER COLUMN c SET STORAGE DEFAULT", pg_contract.Version16},
		{"SET EXPRESSION needs 17", "ALTER TABLE t ALTER COLUMN c SET EXPRESSION AS (a)", pg_contract.Version17},
		{"NOT ENFORCED needs 18", "ALTER TABLE t ADD CONSTRAINT c CHECK (x) NOT ENFORCED", pg_contract.Version18},
		{"the strictest construct wins", "ALTER TABLE t ALTER COLUMN c SET STORAGE DEFAULT, ADD CONSTRAINT c2 CHECK (x) NOT ENFORCED", pg_contract.Version18},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ParseOne(tc.sql).MinVersion())
		})
	}
}

// TestOnlyIsPerRelation guards R-AT-001's input: ONLY suppresses expansion for the relation it
// was written on and for no other.
func TestOnlyIsPerRelation(t *testing.T) {
	t.Parallel()

	statement := ParseOne("TRUNCATE TABLE ONLY a, b")
	require.Len(t, statement.Relations, 2)
	assert.True(t, statement.Relations[0].Only)
	assert.False(t, statement.Relations[1].Only)
}

// TestUnqualifiedNamesStayUnqualified: resolving against the search_path is C-01's job, and the
// path in force is a property of where the statement sits in the file, not of the statement.
func TestUnqualifiedNamesStayUnqualified(t *testing.T) {
	t.Parallel()

	statement := ParseOne("ALTER TABLE t ADD COLUMN a int")
	require.Len(t, statement.Relations, 1)
	assert.Empty(t, statement.Relations[0].Name.Schema)
	assert.Equal(t, "t", statement.Relations[0].Name.Table)

	qualified := ParseOne("ALTER TABLE s.t ADD COLUMN a int")
	assert.Equal(t, "s", qualified.Relations[0].Name.Schema)
}

// TestSingleLockStatementDecomposition is R-AT-000: the clauses arrive in the order written, and
// the clauses that can never share a statement say so.
func TestSingleLockStatementDecomposition(t *testing.T) {
	t.Parallel()

	statement := ParseOne("ALTER TABLE t ADD COLUMN a int, ALTER COLUMN b SET STATISTICS 100, DROP COLUMN c")
	require.Len(t, statement.Subcommands, 3)
	assert.Equal(t, SubAddColumn, statement.Subcommands[0].Kind)
	assert.Equal(t, SubSetStatistics, statement.Subcommands[1].Kind)
	assert.Equal(t, SubDropColumn, statement.Subcommands[2].Kind)
	for _, sub := range statement.Subcommands {
		assert.False(t, sub.Kind.Exclusive(), "%s should be combinable", sub.Kind)
	}

	exclusive := ParseOne("ALTER TABLE t RENAME TO u")
	require.Len(t, exclusive.Subcommands, 1)
	assert.True(t, exclusive.Subcommands[0].Kind.Exclusive())
}

// TestCommandsAreDistinct: §5 keys its rules on the command, so two statements that take
// different locks must not arrive under the same name.
func TestCommandsAreDistinct(t *testing.T) {
	t.Parallel()

	cases := map[string]pg_contract.Command{
		"ALTER TABLE t RENAME TO u":     "ALTER TABLE RENAME",
		"ALTER INDEX i RENAME TO j":     "ALTER INDEX RENAME",
		"ALTER TABLE t SET SCHEMA s":    "ALTER TABLE SET SCHEMA",
		"VACUUM t":                      "VACUUM",
		"ANALYZE t":                     "ANALYZE",
		"COPY t FROM STDIN":             "COPY FROM",
		"COPY t TO STDOUT":              "COPY TO",
		"SELECT 1":                      "SELECT",
		"SELECT 1 FOR UPDATE":           "SELECT FOR",
		"SET LOCAL lock_timeout = '1s'": "SET LOCAL",
		"SET lock_timeout = '1s'":       "SET",
		"RESET lock_timeout":            "RESET",
	}

	for sql, want := range cases {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, want, ParseOne(sql).Command)
		})
	}
}

// TestExpressionWalker is what §4.1 stands on: every call an expression can make must be
// reachable, whatever shape it hides in, and a string literal must never look like one.
func TestExpressionWalker(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		sql   string
		calls []string
	}{
		{"a plain call", "ALTER TABLE t ALTER COLUMN c SET DEFAULT now()", []string{"now"}},
		{"a keyword call resolves to its function", "ALTER TABLE t ALTER COLUMN c SET DEFAULT CURRENT_TIMESTAMP", []string{"now"}},
		{"an operator runs a function too", "ALTER TABLE t ALTER COLUMN c SET DEFAULT 1 + 2", []string{"+"}},
		{"a cast runs a function too", "ALTER TABLE t ALTER COLUMN c SET DEFAULT '1'::int", []string{"pg_catalog.int4"}},
		{"nested through a CASE", "ALTER TABLE t ALTER COLUMN c SET DEFAULT CASE WHEN true THEN random() ELSE 0 END", []string{"random"}},
		{"nested through an array", "ALTER TABLE t ALTER COLUMN c SET DEFAULT ARRAY[gen_random_uuid()]", []string{"gen_random_uuid"}},
		{"nested through COALESCE", "ALTER TABLE t ALTER COLUMN c SET DEFAULT coalesce(clock_timestamp(), now())", []string{"coalesce", "clock_timestamp", "now"}},
		{"a schema-qualified call keeps its schema", "ALTER TABLE t ALTER COLUMN c SET DEFAULT ext.gen()", []string{"ext.gen"}},
		{"a literal that spells a function is still a literal", "ALTER TABLE t ALTER COLUMN c SET DEFAULT 'now()'", nil},
		{"an aggregate's FILTER is not an argument but still runs", "ALTER TABLE t ALTER COLUMN c SET DEFAULT count(*) FILTER (WHERE random() > 0)", []string{"count", ">", "random"}},
		{"an aggregate's ORDER BY runs too", "ALTER TABLE t ALTER COLUMN c SET DEFAULT string_agg(a, ',' ORDER BY random())", []string{"string_agg", "random"}},
		{"an ordering operator is a call", "ALTER TABLE t ALTER COLUMN c SET DEFAULT array_agg(a ORDER BY b USING >)", []string{"array_agg", ">"}},
		{"a window's PARTITION BY runs too", "ALTER TABLE t ALTER COLUMN c SET DEFAULT count(*) OVER (PARTITION BY random())", []string{"count", "random"}},
		{"a window's ORDER BY runs too", "ALTER TABLE t ALTER COLUMN c SET DEFAULT count(*) OVER (ORDER BY random())", []string{"count", "random"}},
		{"a window's frame offset runs too", "ALTER TABLE t ALTER COLUMN c SET DEFAULT count(*) OVER (ORDER BY a ROWS BETWEEN clock_timestamp() PRECEDING AND 1 FOLLOWING)", []string{"count", "clock_timestamp"}},
		{"a named window reaches nothing", "ALTER TABLE t ALTER COLUMN c SET DEFAULT count(*) OVER w", []string{"count"}},
		{"what stands next to a subquery still runs", "ALTER TABLE t ALTER COLUMN c SET DEFAULT (random() IN (SELECT id FROM other))", []string{"random"}},
		{"a subquery comparison names an operator", "ALTER TABLE t ALTER COLUMN c SET DEFAULT (random() = ANY (SELECT id FROM other))", []string{"=", "random"}},
		{"the subquery's own body stays out of reach", "ALTER TABLE t ALTER COLUMN c SET DEFAULT (x IN (SELECT random()))", nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			statement := ParseOne(tc.sql)
			require.Len(t, statement.Subcommands, 1)
			require.NotNil(t, statement.Subcommands[0].Expr)

			var got []string
			for _, call := range statement.Subcommands[0].Expr.Calls() {
				got = append(got, strings.Join(call, "."))
			}
			assert.Equal(t, tc.calls, got)
		})
	}
}

// TestExpressionKnowsABareColumnReference is R-AT-TYPE check 1: "USING c" is the no-op form,
// anything else is a rewrite.
func TestExpressionKnowsABareColumnReference(t *testing.T) {
	t.Parallel()

	bare := ParseOne("ALTER TABLE t ALTER COLUMN c TYPE text USING c")
	require.NotNil(t, bare.Subcommands[0].Column)
	assert.True(t, bare.Subcommands[0].Column.Using.IsColumnRef("c"))

	wrapped := ParseOne("ALTER TABLE t ALTER COLUMN c TYPE text USING c::text")
	assert.False(t, wrapped.Subcommands[0].Column.Using.IsColumnRef("c"))

	other := ParseOne("ALTER TABLE t ALTER COLUMN c TYPE text USING d")
	assert.False(t, other.Subcommands[0].Column.Using.IsColumnRef("c"))
}

// TestExpressionSeesSubqueries: a rule that meets one cannot claim to know every relation the
// statement reaches.
func TestExpressionSeesSubqueries(t *testing.T) {
	t.Parallel()

	statement := ParseOne("ALTER TABLE t ADD CONSTRAINT c CHECK (x IN (SELECT id FROM other))")
	require.NotNil(t, statement.Subcommands[0].Constraint)
	assert.True(t, statement.Subcommands[0].Constraint.Expr.HasSubquery())

	// Keeping the tested expression must not make an ordinary comparison look like a subquery.
	plain := ParseOne("ALTER TABLE t ADD CONSTRAINT c CHECK (x IN (1, 2))")
	assert.False(t, plain.Subcommands[0].Constraint.Expr.HasSubquery())
}

// TestExclusionKeepsExpressionElements guards the shape an EXCLUDE constraint usually has. Only
// the bare-column form lands in Columns, so dropping the expression elements would leave the
// constraint looking like it constrains nothing — a false statement, not merely a poorer one.
func TestExclusionKeepsExpressionElements(t *testing.T) {
	t.Parallel()

	columns := constraintOf(t, ParseOne("ALTER TABLE t ADD CONSTRAINT c EXCLUDE USING gist (room WITH =, during WITH &&)"))
	assert.Equal(t, []string{"room", "during"}, columns.Columns)
	assert.Empty(t, columns.KeyExprs)

	exprs := constraintOf(t, ParseOne("ALTER TABLE t ADD CONSTRAINT c EXCLUDE USING gist (tsrange(a, b) WITH &&)"))
	assert.Empty(t, exprs.Columns)
	require.Len(t, exprs.KeyExprs, 1)
	assert.Equal(t, [][]string{{"tsrange"}}, exprs.KeyExprs[0].Calls())

	// The columns an expression element reads stay reachable, which is the whole point of keeping
	// the expression rather than a marker saying one was there.
	var referenced []string
	exprs.KeyExprs[0].Walk(func(e *Expr) bool {
		if e.Kind == ExprColumnRef {
			referenced = append(referenced, e.Literal)
		}
		return true
	})
	assert.Equal(t, []string{"a", "b"}, referenced)

	mixed := constraintOf(t, ParseOne("ALTER TABLE t ADD CONSTRAINT c EXCLUDE USING gist (room WITH =, tsrange(a, b) WITH &&)"))
	assert.Equal(t, []string{"room"}, mixed.Columns)
	assert.Len(t, mixed.KeyExprs, 1)
}

func TestPartitionByMakesTheNewTablePartitioned(t *testing.T) {
	t.Parallel()

	plain := ParseOne("CREATE TABLE t (a int)")
	require.Len(t, plain.Relations, 1)
	assert.Equal(t, pg_contract.RelationKindTable, plain.Relations[0].Kind)

	parted := ParseOne("CREATE TABLE t (a int) PARTITION BY RANGE (a)")
	require.Len(t, parted.Relations, 1)
	assert.Equal(t, pg_contract.RelationKindPartitionedTable, parted.Relations[0].Kind)

	// A sub-partitioned child is both a partition and a parent, and R-AT-001 reads the parent half.
	sub := ParseOne("CREATE TABLE c PARTITION OF p FOR VALUES FROM (1) TO (2) PARTITION BY RANGE (b)")
	require.NotEmpty(t, sub.Relations)
	assert.Equal(t, pg_contract.RelationKindPartitionedTable, sub.Relations[0].Kind)
}
