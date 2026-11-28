package pgclassify_test

import (
	"testing"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ownerOf is the table and column a sequence belongs to.
func ownerOf(t *testing.T, state *pgcatalog.State, sequence string) (table, column string, identity, ok bool) {
	t.Helper()
	info := state.Resolve(relationName(sequence), nil)
	require.True(t, info.Exists(), sequence)
	owner, ok := state.SequenceOwner(info.Key())
	if !ok {
		return "", "", false, false
	}
	owning, found := state.RelationByKey(owner.Table)
	require.True(t, found, "the owner of %s", sequence)
	return owning.Name.Table, owner.Column, owner.Identity, true
}

// ownedBy is the names of the sequences a table's columns own.
func ownedBy(t *testing.T, state *pgcatalog.State, table string) []string {
	t.Helper()
	info := state.Resolve(relationName(table), nil)
	require.True(t, info.Exists(), table)
	var out []string
	for _, key := range state.SequencesOwnedBy(info.Key()) {
		sequence, ok := state.RelationByKey(key)
		require.True(t, ok)
		out = append(out, sequence.Name.Table)
	}
	return out
}

// A serial or identity column owns the sequence it draws from, under the name the server gives it: the sequence
// follows the column through a rename, goes with it, its identity or its table, and moves with the table.
func TestOwnedSequencesOfTheMigration(t *testing.T) {
	t.Parallel()

	assertOwner := func(t *testing.T, state *pgcatalog.State, sequence, table, column string, identity bool) {
		t.Helper()
		owningTable, owningColumn, isIdentity, ok := ownerOf(t, state, sequence)
		require.True(t, ok, "%s is owned", sequence)
		assert.Equal(t, table, owningTable, sequence)
		assert.Equal(t, column, owningColumn, sequence)
		assert.Equal(t, identity, isIdentity, sequence)
	}

	t.Run("a created table's serial and identity columns own a sequence each", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, "CREATE TABLE t (id serial, n bigint GENERATED ALWAYS AS IDENTITY, x int)")
		assertOwner(t, state, "t_id_seq", "t", "id", false)
		assertOwner(t, state, "t_n_seq", "t", "n", true)
		assert.ElementsMatch(t, []string{"t_id_seq", "t_n_seq"}, ownedBy(t, state, "t"))
	})

	t.Run("the sequence's name is numbered past a relation holding it", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, "CREATE TABLE t_id_seq (); CREATE TABLE t (id serial)")
		assertOwner(t, state, "t_id_seq1", "t", "id", false)
	})

	t.Run("SEQUENCE NAME names an identity's sequence", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, "CREATE TABLE t (a int GENERATED ALWAYS AS IDENTITY (SEQUENCE NAME t_seq)); "+
			"ALTER TABLE orders ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (SEQUENCE NAME o_seq)")
		assertOwner(t, state, "t_seq", "t", "a", true)
		assertOwner(t, state, "o_seq", "orders", "id", true)
		noRelation(t, state, "t_a_seq")
	})

	t.Run("a column added to a partitioned table owns one sequence, on the parent", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, "ALTER TABLE events ADD COLUMN n bigserial")
		assertOwner(t, state, "events_n_seq", "events", "n", false)
		assert.Empty(t, ownedBy(t, state, "events_2025"))
	})

	t.Run("ADD IDENTITY builds a sequence, DROP IDENTITY drops it", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, "ALTER TABLE orders ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY")
		assertOwner(t, state, "orders_id_seq", "orders", "id", true)

		state = stateAfter(t, "ALTER TABLE orders ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY; "+
			"ALTER TABLE orders ALTER COLUMN id DROP IDENTITY")
		noRelation(t, state, "orders_id_seq")
		assert.Empty(t, ownedBy(t, state, "orders"))
	})

	t.Run("the owner follows its column through a rename", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, "ALTER TABLE users RENAME COLUMN id TO uid; "+
			"CREATE TABLE t (id serial); ALTER TABLE t RENAME COLUMN id TO k")
		assertOwner(t, state, "users_id_seq", "users", "uid", false)
		assertOwner(t, state, "t_id_seq", "t", "k", false)
	})

	t.Run("the sequence goes with its column and its table", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, "CREATE TABLE t (id serial, a int); ALTER TABLE t DROP COLUMN id; DROP TABLE users CASCADE")
		noRelation(t, state, "t_id_seq")
		noRelation(t, state, "users_id_seq")
	})

	t.Run("a dropped sequence is owned by nothing", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, "DROP SEQUENCE users_id_seq CASCADE")
		assert.Empty(t, ownedBy(t, state, "users"))
	})

	t.Run("OWNED BY gives the sequence an owner, OWNED BY NONE takes it away", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, "ALTER SEQUENCE users_id_seq OWNED BY NONE; "+
			"ALTER SEQUENCE standalone_seq OWNED BY orders.total; CREATE SEQUENCE s OWNED BY orders.status")
		_, _, _, ok := ownerOf(t, state, "users_id_seq")
		assert.False(t, ok)
		assert.Empty(t, ownedBy(t, state, "users"))
		assertOwner(t, state, "standalone_seq", "orders", "total", false)
		assertOwner(t, state, "s", "orders", "status", false)
		assert.ElementsMatch(t, []string{"standalone_seq", "s"}, ownedBy(t, state, "orders"))
	})

	t.Run("the sequences move with their table", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, "CREATE SCHEMA other; ALTER TABLE users SET SCHEMA other")
		assert.False(t, state.Resolve(relationName("users_id_seq"), nil).Exists())
		assert.True(t, state.Resolve(helper.FullRelationName{Schema: "other", Table: "users_id_seq"}, nil).Exists())
	})

	t.Run("LIKE INCLUDING IDENTITY gives the new table its own identity sequences", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, "CREATE TABLE t (id serial, n int GENERATED ALWAYS AS IDENTITY); "+
			"CREATE TABLE l (LIKE t INCLUDING IDENTITY); CREATE TABLE l2 (LIKE t)")
		assertOwner(t, state, "l_n_seq", "l", "n", true)
		noRelation(t, state, "l_id_seq")
		noRelation(t, state, "l2_n_seq")
	})
}

// The rules read the sequences the migration's columns own.
func TestRulesReadTheMigrationsSequences(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		script string
		code   pgcontract.Code
		// named and absent are relations of the schema the last statement has to name, or must not.
		named, absent []string
		// message is a part of the finding's message.
		message string
	}{
		{name: "RESTART IDENTITY resets a created table's serial sequence",
			script: "CREATE TABLE t (id serial); TRUNCATE t RESTART IDENTITY",
			code:   "R-MT-TRUNCATE", named: []string{"t_id_seq"}},
		{name: "RESTART IDENTITY resets the sequence of an identity the migration added",
			script: "ALTER TABLE orders ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY; TRUNCATE orders RESTART IDENTITY",
			code:   "R-MT-TRUNCATE", named: []string{"orders_id_seq"}},
		{name: "RESTART IDENTITY leaves a sequence OWNED BY NONE",
			script: "ALTER SEQUENCE users_id_seq OWNED BY NONE; TRUNCATE users RESTART IDENTITY CASCADE",
			code:   "R-MT-TRUNCATE", absent: []string{"users_id_seq"}},
		{name: "ALTER SEQUENCE names the column a created table's serial feeds",
			script: "CREATE TABLE t (id serial); ALTER TABLE t RENAME COLUMN id TO k; ALTER SEQUENCE t_id_seq RESTART",
			code:   "R-OB-ALTERSEQ", message: "apercu_snapshot_test.t.k"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			analyses := classifytest.Analyze(t, classifytest.Catalog(t), testCase.script)
			last := analyses[len(analyses)-1]
			assert.Empty(t, last.Errors)
			finding := classifytest.FindingOf(t, last, testCase.code)
			names := classifytest.TargetNames(&finding)
			for _, relation := range testCase.named {
				assert.Contains(t, names, "apercu_snapshot_test."+relation, finding.Message)
			}
			for _, relation := range testCase.absent {
				assert.NotContains(t, names, "apercu_snapshot_test."+relation, finding.Message)
			}
			assert.Contains(t, finding.Message, testCase.message)
		})
	}
}
