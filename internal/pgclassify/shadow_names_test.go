package pgclassify_test

import (
	"testing"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify"
	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgparse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlindStatements checks what lets a DROP CONSTRAINT fall back on guessing: only DDL the file does not spell.
func TestBlindStatements(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sql   string
		blind bool
	}{
		{"DO $$ BEGIN END $$", true},
		{"CALL some_procedure()", true},
		{"CREATE EXTENSION hstore", true},
		{"THIS IS NOT SQL", true},
		{"ALTER TABLE rd_plain ADD CHECK (at > 0)", false},
		{"CREATE TABLE t_new (a int PRIMARY KEY)", false},
		{"SELECT 1", false},
	}
	for _, testCase := range cases {
		t.Run(testCase.sql, func(t *testing.T) {
			classifier := pgclassify.NewClassifier(classifytest.NullableKeyCatalog(t))
			classifier.Analyze(pgparse.Parse(testCase.sql))
			assert.Equal(t, testCase.blind, classifier.State().Blind())
		})
	}
}

// A CHECK belongs to its table, not to the name the table goes by: it follows the table through a rename, and a
// table created later under the old name starts with none.
func TestCheckFollowsItsTableThroughARename(t *testing.T) {
	t.Parallel()

	classifier := pgclassify.NewClassifier(classifytest.Catalog(t))
	classifier.Analyze(pgparse.Parse(
		"CREATE TABLE t (a int CHECK (t.a > 0)); ALTER TABLE t RENAME TO u; CREATE TABLE t (b int)"))
	state := classifier.State()

	renamed := state.Resolve(helper.FullRelationName{Schema: "apercu_snapshot_test", Table: "u"}, nil)
	require.True(t, renamed.Exists())
	check, ok := state.ConstraintByName(renamed.Key(), "t_a_check")
	require.True(t, ok)
	require.NotNil(t, check.Expr)
	assert.Equal(t, []string{"a"}, check.Columns)
	assert.Equal(t, "a", check.Expr.Args[0].Literal, "recorded without the table name it was qualified with")

	recreated := state.Resolve(helper.FullRelationName{Schema: "apercu_snapshot_test", Table: "t"}, nil)
	require.True(t, recreated.Exists())
	assert.NotEqual(t, renamed.Key(), recreated.Key())
	assert.Empty(t, state.Constraints(recreated.Key()))
}

// Two snapshot columns swapping names through a third: the snapshot CHECK reads the column under its new name.
func TestSnapshotCheckFollowsAColumnSwap(t *testing.T) {
	t.Parallel()

	classifier := pgclassify.NewClassifier(classifytest.Catalog(t))
	classifier.Analyze(pgparse.Parse("ALTER TABLE orders RENAME total TO tmp; " +
		"ALTER TABLE orders RENAME status TO total; ALTER TABLE orders RENAME tmp TO status"))
	state := classifier.State()

	orders := state.Resolve(helper.FullRelationName{Schema: "apercu_snapshot_test", Table: "orders"}, nil)
	check, ok := state.ConstraintByName(orders.Key(), "orders_total_check")
	require.True(t, ok)
	assert.Equal(t, []string{"status"}, check.Columns)
	notNull, ok := state.ConstraintByName(orders.Key(), "orders_status_not_null")
	require.True(t, ok)
	assert.Equal(t, []string{"total"}, notNull.Columns)
	assert.Equal(t, pgcatalog.NotNullCheck, state.NotNullProof(orders.Key(), "total"))
}
