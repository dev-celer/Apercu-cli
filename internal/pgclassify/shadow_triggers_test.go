package pgclassify_test

import (
	"testing"

	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// triggerOn reports whether a table holds a trigger, and whether it fires per row.
func triggerOn(t *testing.T, state *pgcatalog.State, table, trigger string) (row, ok bool) {
	t.Helper()
	info := state.Resolve(relationName(table), nil)
	require.True(t, info.Exists(), table)
	found, ok := state.TriggerByName(info.Key(), trigger)
	return found.Row, ok
}

// A trigger the migration creates, renames or drops is one of its table's, and a row trigger on a partitioned table
// is cloned under its name onto every partition, the ones that come later included, until they leave.
func TestTriggersOfTheMigration(t *testing.T) {
	t.Parallel()

	const (
		rowOnEvents   = "CREATE TRIGGER r BEFORE UPDATE ON events FOR EACH ROW EXECUTE FUNCTION bump(); "
		stmtOnEvents  = "CREATE TRIGGER s AFTER UPDATE ON events FOR EACH STATEMENT EXECUTE FUNCTION bump(); "
		rowOnMetrics  = "CREATE TRIGGER r BEFORE UPDATE ON metrics FOR EACH ROW EXECUTE FUNCTION bump(); "
		stmtOnMetrics = "CREATE TRIGGER s AFTER UPDATE ON metrics FOR EACH STATEMENT EXECUTE FUNCTION bump(); "
	)

	t.Run("a trigger the migration creates fires as it was declared", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, "CREATE TRIGGER s AFTER INSERT ON orders FOR EACH STATEMENT EXECUTE FUNCTION bump();"+
			"CREATE TRIGGER r BEFORE INSERT ON orders FOR EACH ROW EXECUTE FUNCTION bump()")
		row, ok := triggerOn(t, state, "orders", "s")
		assert.True(t, ok)
		assert.False(t, row)
		row, ok = triggerOn(t, state, "orders", "r")
		assert.True(t, ok)
		assert.True(t, row)
	})

	t.Run("a row trigger is cloned onto every level of partitions, a statement trigger is not", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, rowOnMetrics+stmtOnMetrics)
		for _, partition := range []string{"metrics_2025", "metrics_default", "metrics_default_a", "metrics_default_other"} {
			row, ok := triggerOn(t, state, partition, "r")
			assert.True(t, ok, partition)
			assert.True(t, row, partition)
			_, ok = triggerOn(t, state, partition, "s")
			assert.False(t, ok, partition)
		}
	})

	t.Run("CREATE OR REPLACE changes how the trigger fires", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, "CREATE OR REPLACE TRIGGER orders_bump AFTER UPDATE ON orders FOR EACH STATEMENT EXECUTE FUNCTION bump()")
		row, ok := triggerOn(t, state, "orders", "orders_bump")
		assert.True(t, ok)
		assert.False(t, row)
	})

	t.Run("a snapshot trigger renamed goes by its new name", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, "ALTER TRIGGER orders_audit ON orders RENAME TO orders_audit2")
		_, ok := triggerOn(t, state, "orders", "orders_audit")
		assert.False(t, ok)
		row, ok := triggerOn(t, state, "orders", "orders_audit2")
		assert.True(t, ok)
		assert.False(t, row, "it is still a statement trigger")
	})

	t.Run("renaming a trigger renames its clones", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, rowOnEvents+"ALTER TRIGGER r ON events RENAME TO r2")
		for _, table := range []string{"events", "events_2025", "events_default"} {
			_, ok := triggerOn(t, state, table, "r")
			assert.False(t, ok, table)
			_, ok = triggerOn(t, state, table, "r2")
			assert.True(t, ok, table)
		}
	})

	t.Run("dropping a trigger drops its clones", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, rowOnEvents+"DROP TRIGGER r ON events; DROP TRIGGER orders_bump ON orders")
		for _, table := range []string{"events", "events_2025", "events_default"} {
			_, ok := triggerOn(t, state, table, "r")
			assert.False(t, ok, table)
		}
		_, ok := triggerOn(t, state, "orders", "orders_bump")
		assert.False(t, ok)
		_, ok = triggerOn(t, state, "orders", "orders_audit")
		assert.True(t, ok, "the other trigger stays")
	})

	t.Run("PARTITION OF clones the parent's row triggers", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, rowOnEvents+stmtOnEvents+
			"ALTER TABLE events DETACH PARTITION events_default; "+
			"CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')")
		_, ok := triggerOn(t, state, "events_2026", "r")
		assert.True(t, ok)
		_, ok = triggerOn(t, state, "events_2026", "s")
		assert.False(t, ok)
	})

	t.Run("ATTACH clones the parent's row triggers down the attached tree", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, rowOnEvents+
			"CREATE TABLE ev (id bigint, at timestamptz NOT NULL) PARTITION BY RANGE (at); "+
			"CREATE TABLE ev_1 PARTITION OF ev FOR VALUES FROM ('2026-01-01') TO ('2026-06-01'); "+
			"ALTER TABLE events DETACH PARTITION events_default; "+
			"ALTER TABLE events ATTACH PARTITION ev FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')")
		for _, table := range []string{"ev", "ev_1"} {
			row, ok := triggerOn(t, state, table, "r")
			assert.True(t, ok, table)
			assert.True(t, row, table)
		}
	})

	t.Run("DETACH takes the clones off every level of the detached tree", func(t *testing.T) {
		t.Parallel()
		state := stateAfter(t, rowOnMetrics+"ALTER TABLE metrics DETACH PARTITION metrics_default")
		for _, table := range []string{"metrics_default", "metrics_default_a"} {
			_, ok := triggerOn(t, state, table, "r")
			assert.False(t, ok, table)
		}
		_, ok := triggerOn(t, state, "metrics_2025", "r")
		assert.True(t, ok, "a partition still attached keeps its clone")
	})
}

// The rules read the triggers as the migration left them: a statement trigger reaches no partition.
func TestRulesReadTheMigrationsTriggers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		script string
		code   pgcontract.Code
		// named and absent are relations of the schema the last statement has to name, or must not.
		named, absent []string
	}{
		{name: "DROP TRIGGER of a statement trigger the migration created stays on the table",
			script: "CREATE TRIGGER s AFTER UPDATE ON events FOR EACH STATEMENT EXECUTE FUNCTION bump(); DROP TRIGGER s ON events",
			code:   "R-OB-DROPTRIG", named: []string{"events"}, absent: []string{"events_2025", "events_default"}},
		{name: "DROP TRIGGER of a row trigger the migration created reaches the partitions",
			script: "CREATE TRIGGER r BEFORE UPDATE ON events FOR EACH ROW EXECUTE FUNCTION bump(); DROP TRIGGER r ON events",
			code:   "R-OB-DROPTRIG", named: []string{"events", "events_2025", "events_default"}},
		{name: "DISABLE TRIGGER follows a statement trigger through its rename",
			script: "CREATE TRIGGER s AFTER UPDATE ON events FOR EACH STATEMENT EXECUTE FUNCTION bump(); " +
				"ALTER TRIGGER s ON events RENAME TO s2; ALTER TABLE events DISABLE TRIGGER s2",
			code: "R-AT-TRIG", named: []string{"events"}, absent: []string{"events_2025", "events_default"}},
		{name: "ENABLE TRIGGER reads the level CREATE OR REPLACE left",
			script: "CREATE TRIGGER t BEFORE UPDATE ON events FOR EACH ROW EXECUTE FUNCTION bump(); " +
				"CREATE OR REPLACE TRIGGER t AFTER UPDATE ON events FOR EACH STATEMENT EXECUTE FUNCTION bump(); " +
				"ALTER TABLE events ENABLE TRIGGER t",
			code: "R-AT-TRIG", named: []string{"events"}, absent: []string{"events_2025", "events_default"}},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			assertLastTargets(t, testCase.script, testCase.code, testCase.named, testCase.absent)
		})
	}
}
