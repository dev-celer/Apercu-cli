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

// A copy cloned from a parent's constraint keeps its link to it through a rename of the parent's: a DROP of the
// parent's under its new name still takes the copy with it.
func TestClonesFollowTheirParentThroughARename(t *testing.T) {
	t.Parallel()

	const setup = "CREATE TABLE p (id int) PARTITION BY RANGE (id); " +
		"CREATE TABLE c PARTITION OF p FOR VALUES FROM (0) TO (10); " +
		"ALTER TABLE p ADD CONSTRAINT p_key PRIMARY KEY (id); " +
		"CREATE TABLE r (pid int); " +
		"ALTER TABLE r ADD CONSTRAINT r_fk FOREIGN KEY (pid) REFERENCES p (id); "
	cases := []struct {
		// owner holds the parent's constraint, table its copy.
		name, rename, owner, table, parent string
	}{
		{"an index-backed constraint's copy on a partition", "ALTER TABLE p RENAME CONSTRAINT p_key TO p_id", "p", "c", "p_id"},
		{"an index-backed constraint renamed through its index", "ALTER INDEX p_key RENAME TO p_id", "p", "c", "p_id"},
		{"a FOREIGN KEY's row for a referenced partition", "ALTER TABLE r RENAME CONSTRAINT r_fk TO r_ref", "r", "r", "r_ref"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			name := func(table string) helper.FullRelationName {
				return helper.FullRelationName{Schema: "apercu_snapshot_test", Table: table}
			}
			parentOf := func(state *pgcatalog.State) map[string]string {
				copies := map[string]string{}
				owner := state.Resolve(name(testCase.table), nil)
				require.True(t, owner.Exists())
				for _, constraint := range state.Constraints(owner.Key()) {
					if constraint.ParentName != "" {
						copies[constraint.Name] = constraint.ParentName
					}
				}
				return copies
			}

			classifier := pgclassify.NewClassifier(classifytest.Catalog(t))
			classifier.Analyze(pgparse.Parse(setup + testCase.rename))
			copies := parentOf(classifier.State())
			require.Len(t, copies, 1)
			for _, parent := range copies {
				assert.Equal(t, testCase.parent, parent)
			}

			dropped := pgclassify.NewClassifier(classifytest.Catalog(t))
			dropped.Analyze(pgparse.Parse(setup + testCase.rename + "; ALTER TABLE " + testCase.owner + " DROP CONSTRAINT " + testCase.parent))
			assert.Empty(t, parentOf(dropped.State()), "the copy goes with the constraint it was cloned from")
		})
	}
}
