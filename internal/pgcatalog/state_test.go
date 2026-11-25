package pgcatalog

import (
	"testing"

	"apercu-cli/internal/pgparse/ir"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A name dropped and created again is a new relation with a key of its own, whether the first one was the
// snapshot's or the migration's.
func TestStateCreateDropRecreate(t *testing.T) {
	eachVersion(t, func(t *testing.T, state *State, _ int) {
		first := recorderOf(state).Declare(name("fresh"), "r")
		require.True(t, state.CreatedByMigration(name("fresh")))
		recorderOf(state).Drop(first)
		assert.False(t, state.Resolve(name("fresh"), nil).Exists())
		assert.False(t, state.CreatedByMigration(name("fresh")))

		second := recorderOf(state).Declare(name("fresh"), "r")
		assert.NotEqual(t, first, second)
		_, ok := state.RelationByKey(first)
		assert.False(t, ok, "the dropped relation's key leads nowhere")

		users := findKey(t, state, "users")
		recorderOf(state).Drop(users)
		assert.False(t, state.Resolve(name("users"), nil).Exists())
		_, ok = state.RelationByKey(users)
		assert.False(t, ok)

		recreated := recorderOf(state).Declare(name("users"), "r")
		info := state.Resolve(name("users"), nil)
		assert.Equal(t, OriginCreated, info.Origin)
		assert.Equal(t, recreated, info.Key())
		assert.Zero(t, info.Relation.OID)
	})
}

// Every name a relation went by but the last one leads nowhere, and the last one leads to the snapshot row.
func TestStateRenameChain(t *testing.T) {
	eachVersion(t, func(t *testing.T, state *State, _ int) {
		users := state.Resolve(name("users"), nil)
		recorderOf(state).Rename(users.Key(), name("members"))
		recorderOf(state).Rename(users.Key(), name("people"))

		assert.False(t, state.Resolve(name("users"), nil).Exists())
		assert.False(t, state.Resolve(name("members"), nil).Exists())
		people := state.Resolve(name("people"), nil)
		require.Equal(t, OriginExisting, people.Origin)
		assert.Equal(t, users.Relation.OID, people.Relation.OID)
		assert.Equal(t, users.PreviewBytes, people.PreviewBytes, "sizes stay keyed by the snapshot row")

		byKey, ok := state.RelationByKey(users.Key())
		require.True(t, ok)
		assert.Equal(t, name("people"), byKey.Name)

		recorderOf(state).Rename(users.Key(), name("users"))
		assert.Equal(t, users.Relation.OID, state.Resolve(name("users"), nil).Relation.OID, "renamed back to where it started")
		assert.False(t, state.Resolve(name("people"), nil).Exists())
	})
}

// A snapshot relation renamed onto the name of a dropped one answers to that name with its own row.
func TestStateRenameOntoADroppedName(t *testing.T) {
	eachVersion(t, func(t *testing.T, state *State, _ int) {
		orders := state.Resolve(name("orders"), nil)
		users := state.Resolve(name("users"), nil)
		recorderOf(state).Drop(orders.Key())
		recorderOf(state).Rename(users.Key(), name("orders"))

		now := state.Resolve(name("orders"), nil)
		require.True(t, now.Exists())
		assert.Equal(t, users.Relation.OID, now.Relation.OID)
		assert.Equal(t, users.Key(), now.Key())
		assert.False(t, state.Resolve(name("users"), nil).Exists())
	})
}

// Two columns that swap names each keep their snapshot column, the CHECKs read them under the new names, and the
// proofs follow the column rather than the name.
func TestStateColumnSwap(t *testing.T) {
	eachVersion(t, func(t *testing.T, state *State, _ int) {
		orders := findKey(t, state, "orders")
		total, ok := state.Column(orders, "total")
		require.True(t, ok)
		status, ok := state.Column(orders, "status")
		require.True(t, ok)

		totalState, ok := state.ColumnState(orders, "total")
		require.True(t, ok)
		statusState, ok := state.ColumnState(orders, "status")
		require.True(t, ok)
		recorderOf(state).SetColumn(orders, "total", statusState)
		recorderOf(state).SetColumn(orders, "status", totalState)

		swapped, ok := state.Column(orders, "total")
		require.True(t, ok)
		assert.Equal(t, status.Num, swapped.Num)
		assert.Equal(t, "total", swapped.Name, "the column answers to the name it goes by now")
		assert.Equal(t, status.FormattedType, swapped.FormattedType, "the type follows the column")
		other, ok := state.Column(orders, "status")
		require.True(t, ok)
		assert.Equal(t, total.FormattedType, other.FormattedType)
		assert.Equal(t, "status", state.CurrentColumn(orders, "total"))
		assert.Equal(t, "total", state.CurrentColumn(orders, "status"))

		// orders_total_check reads total >= 0, and total is now called status.
		check, ok := state.ConstraintByName(orders, "orders_total_check")
		require.True(t, ok)
		assert.Equal(t, []string{"status"}, check.Columns)
		require.NotNil(t, check.Expr)
		assert.Contains(t, columnRefs(check.Expr), "status")
		assert.NotContains(t, columnRefs(check.Expr), "total")

		// The CHECK proving the old status never null now proves the column called total.
		assert.Equal(t, NotNullCheck, state.NotNullProof(orders, "total"))
		assert.Equal(t, NotNullUnproven, state.NotNullProof(orders, "status"))

		var names []string
		for _, column := range state.Columns(orders) {
			names = append(names, column.Name)
		}
		assert.ElementsMatch(t, []string{"id", "user_id", "total", "status", "code"}, names)
	})
}

// A column renamed away leaves its old name to nothing, and the constraints on it under the new one.
func TestStateColumnRenamedAndDropped(t *testing.T) {
	eachVersion(t, func(t *testing.T, state *State, _ int) {
		orders := findKey(t, state, "orders")
		code, ok := state.Column(orders, "code")
		require.True(t, ok)

		codeState, ok := state.ColumnState(orders, "code")
		require.True(t, ok)
		recorderOf(state).SetColumn(orders, "reference", codeState)
		recorderOf(state).DeleteColumn(orders, "code")
		_, ok = state.Column(orders, "code")
		assert.False(t, ok)
		renamed, ok := state.Column(orders, "reference")
		require.True(t, ok)
		assert.Equal(t, code.Num, renamed.Num)

		recorderOf(state).DeleteColumn(orders, "reference")
		_, ok = state.Column(orders, "reference")
		assert.False(t, ok)
		assert.Equal(t, NotNullUnproven, state.NotNullProof(orders, "reference"))
	})
}

// A constraint dropped and added again under the same name is the migration's: nothing of the snapshot row shows
// through it.
func TestStateConstraintReplacedUnderTheSameName(t *testing.T) {
	eachVersion(t, func(t *testing.T, state *State, _ int) {
		orders := findKey(t, state, "orders")
		before, ok := state.ConstraintByName(orders, "orders_total_check")
		require.True(t, ok)
		require.False(t, before.Validated)
		require.NotZero(t, before.Snapshot.OID)

		recorderOf(state).DeleteConstraint(orders, "orders_total_check")
		_, ok = state.ConstraintByName(orders, "orders_total_check")
		assert.False(t, ok)
		assert.False(t, state.ConstraintNamesInSchema(testSchema)["orders_total_check"], "the name is free again")

		recorderOf(state).SetConstraint(orders, "orders_total_check", ConstraintState{Kind: "c", Validated: true, Enforced: true})
		after, ok := state.ConstraintByName(orders, "orders_total_check")
		require.True(t, ok)
		assert.True(t, after.Validated)
		assert.Zero(t, after.SnapshotOID)
		assert.Zero(t, after.Snapshot.OID, "the snapshot row is not linked to a constraint the migration created")

		count := 0
		for _, constraint := range state.Constraints(orders) {
			if constraint.Name == "orders_total_check" {
				count++
			}
		}
		assert.Equal(t, 1, count)
		assert.True(t, state.ConstraintNamesInSchema(testSchema)["orders_total_check"])
	})
}

// A renamed snapshot constraint keeps its snapshot row under the new name.
func TestStateConstraintRenamed(t *testing.T) {
	eachVersion(t, func(t *testing.T, state *State, _ int) {
		orders := findKey(t, state, "orders")
		before, ok := state.ConstraintByName(orders, "orders_user_id_fkey")
		require.True(t, ok)

		recorderOf(state).SetConstraint(orders, "orders_owner_fkey", before.ConstraintState)
		recorderOf(state).DeleteConstraint(orders, "orders_user_id_fkey")

		after, ok := state.ConstraintByName(orders, "orders_owner_fkey")
		require.True(t, ok)
		assert.Equal(t, before.Snapshot.OID, after.Snapshot.OID)
		assert.Equal(t, findKey(t, state, "users"), after.References)

		var referencing []string
		for _, reference := range state.ReferencesTo(findKey(t, state, "users")) {
			if reference.Table == orders {
				referencing = append(referencing, reference.Constraint.Name)
			}
		}
		assert.Equal(t, []string{"orders_owner_fkey"}, referencing)
	})
}

// Attaching a snapshot partition again with other flags replaces the edge rather than adding one.
func TestStateRelinkASnapshotEdge(t *testing.T) {
	eachVersion(t, func(t *testing.T, state *State, _ int) {
		events := findKey(t, state, "events")
		defaultPart := findKey(t, state, "events_default")
		_, ok := state.DefaultPartition(events)
		require.True(t, ok)

		recorderOf(state).Unlink(events, defaultPart)
		assert.Len(t, state.Children(events), 1)
		assert.Empty(t, state.Parents(defaultPart))

		recorderOf(state).Link(Link{Parent: events, Child: defaultPart, ParentIsPartitioned: true})
		assert.Len(t, state.Children(events), 2)
		_, ok = state.DefaultPartition(events)
		assert.False(t, ok, "attached again as a plain partition")
		assert.Contains(t, state.PartitionDescendants(events), defaultPart)
	})
}

// A created relation keeps its key, and so its links, whatever it is renamed to.
func TestStateRenamedCreatedRelationKeepsItsLinks(t *testing.T) {
	eachVersion(t, func(t *testing.T, state *State, _ int) {
		parent := recorderOf(state).Declare(name("p"), "p")
		child := recorderOf(state).Declare(name("p1"), "r")
		recorderOf(state).Link(Link{Parent: parent, Child: child, ParentIsPartitioned: true})

		recorderOf(state).Rename(parent, name("q"))
		q := state.Resolve(name("q"), nil)
		require.Equal(t, OriginCreated, q.Origin)
		assert.Equal(t, parent, q.Key())
		assert.False(t, state.Resolve(name("p"), nil).Exists())
		assert.Equal(t, []RelationKey{child}, state.PartitionDescendants(q.Key()))
	})
}

// A dropped relation takes its edges with it, in both directions.
func TestStateDropForgetsEdges(t *testing.T) {
	eachVersion(t, func(t *testing.T, state *State, _ int) {
		events := findKey(t, state, "events")
		partition := findKey(t, state, "events_2025")
		recorderOf(state).Drop(partition)
		assert.NotContains(t, state.PartitionDescendants(events), partition)
		assert.Empty(t, state.Parents(partition))
		assert.Len(t, state.Children(events), 1)
	})
}

// columnRefs lists the columns an expression reads.
func columnRefs(expr *ir.Expr) []string {
	if expr == nil {
		return nil
	}
	var out []string
	if expr.Kind == ir.ExprColumnRef {
		out = append(out, expr.Literal)
	}
	for i := range expr.Args {
		out = append(out, columnRefs(&expr.Args[i])...)
	}
	return out
}
