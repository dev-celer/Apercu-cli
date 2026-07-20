package warning

import (
	"apercu-cli/config"
	"apercu-cli/helper"
	metricshelper "apercu-cli/helper/metrics"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// roundTrip serializes a warning's state, feeds it back through the
// converter registry (ConvertStatesToWarnings), and returns the
// reconstructed warning. It mirrors what happens when a warning is
// persisted to the state file and read back on the next run.
func roundTrip(t *testing.T, w Warning) Warning {
	t.Helper()

	state, err := w.GetStateValues()
	require.NoError(t, err)

	states := map[string]json.RawMessage{
		w.GetFullCode(): state,
	}

	got := ConvertStatesToWarnings(states)
	require.Len(t, got, 1, "expected exactly one reconstructed warning")
	return got[0]
}

func assertEquivalent(t *testing.T, want, got Warning) {
	t.Helper()
	require.NotNil(t, got)
	assert.Equal(t, want.GetFullCode(), got.GetFullCode(), "full code mismatch")
	assert.Equal(t, want.GetText(), got.GetText(), "text mismatch")
	assert.Equal(t, want.GetTextLong(), got.GetTextLong(), "long text mismatch")
	assert.Equal(t, want.GetLevel(), got.GetLevel(), "level mismatch")
	assert.Equal(t, want.GetIsIdempotent(), got.GetIsIdempotent(), "idempotency mismatch")
}

func TestConverterRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		w    Warning
	}{
		{"state_file", NewStateFileWarning("/tmp/state.json")},
		{"explain_path_not_found", NewExplainQueryFileWarning(CodeExplainQueryPathNotFound, "queries/a.sql")},
		{"explain_no_queries", NewExplainQueryFileWarning(CodeExplainQueryNoQueries, "queries/b.sql")},
		{"explain_read_error", NewExplainQueryFileWarning(CodeExplainQueryFailedToReadFile, "queries/c.sql")},
		{"explain_stat_statements_missing", NewExplainQueryProdFetchWarning(CodeExplainQueryStatStatementsMissing, "")},
		{"explain_prod_fetch_failed", NewExplainQueryProdFetchWarning(CodeExplainQueryProdFetchFailed, "")},
		{"plan_ordering_regression", NewExplainPlanOrderingRegressionWarning(WarningLevelMedium, "public.users")},
		{"plan_scan_regression", NewExplainPlanScanRegressionWarning(WarningLevelHigh, "public.users", "Index Scan", "Seq Scan")},
		{"missing_env_var", &MissingEnvVarsWarning{variable: "DATABASE_URL"}},
		{"wal_size", NewWALSizeWarning(2*1024*1024*1024, 1024*1024*1024)},
		{"migration_table_not_found", &MigrationTableNotFound{}},
		{"seed_file_open_err", NewSeedingError(CodeFailedToOpenSeedFile, "seeds/a.sql")},
		{"seed_file_not_found", NewSeedingError(CodeSeedFileNotFound, "seeds/b.sql")},
		{
			"table_rewritten",
			NewRewriteWarning(
				helper.FullTableName{Schema: "public", Table: "orders"},
				&metricshelper.TableMetrics{RowCount: 1000, TableSize: 5 * 1024 * 1024 * 1024},
			),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotNil(t, tc.w, "test setup: warning constructor returned nil")
			got := roundTrip(t, tc.w)
			assertEquivalent(t, tc.w, got)
		})
	}
}

// TestStatePersistenceRoundTrip exercises the full persistence path: a
// warning is stored in DatabaseState.LastWarnings, the State is marshaled
// to JSON (as config.State.Save does), parsed back, and the warning is
// reconstructed. This catches warnings whose GetStateValues produces
// JSON that cannot survive a marshal/unmarshal of the state file.
func TestStatePersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		w    Warning
	}{
		{"state_file", NewStateFileWarning("/tmp/state.json")},
		{"explain_stat_statements_missing", NewExplainQueryProdFetchWarning(CodeExplainQueryStatStatementsMissing, "")},
		{"migration_table_not_found", &MigrationTableNotFound{}},
		{"wal_size", NewWALSizeWarning(2*1024*1024*1024, 1024*1024*1024)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotNil(t, tc.w)

			value, err := tc.w.GetStateValues()
			require.NoError(t, err)

			dbState := config.NewDatabaseState()
			dbState.LastWarnings[tc.w.GetFullCode()] = value
			state := config.NewState()
			state.Databases["db"] = dbState

			// Marshal exactly like config.State.Save.
			data, err := json.Marshal(state)
			require.NoError(t, err, "state with this warning could not be marshaled")

			var parsed config.State
			require.NoError(t, json.Unmarshal(data, &parsed))

			got := ConvertStatesToWarnings(parsed.Databases["db"].LastWarnings)
			require.Len(t, got, 1)
			assertEquivalent(t, tc.w, got[0])
		})
	}
}

func TestConvertStatesToWarnings_UnknownCodeSkipped(t *testing.T) {
	t.Parallel()

	states := map[string]json.RawMessage{
		"TOTALLY_UNKNOWN_CODE.key":                  json.RawMessage(`{"path":"x"}`),
		NewStateFileWarning("/tmp/x").GetFullCode(): mustState(t, NewStateFileWarning("/tmp/x")),
	}

	got := ConvertStatesToWarnings(states)
	require.Len(t, got, 1)
	assert.Equal(t, CodeStateFileFailedToRead, got[0].GetCode())
}

func TestWarningStore_AddDedup(t *testing.T) {
	t.Parallel()

	store := NewWarningStore()
	w := NewStateFileWarning("/tmp/x")

	store.AddWarning(w)
	store.AddWarning(w)   // same pointer, must dedup
	store.AddWarning(nil) // must be ignored

	assert.Len(t, store.GetWarningsRaw(), 1)
}

func TestWarningStore_AddDedupByIdentity(t *testing.T) {
	t.Parallel()

	store := NewWarningStore()
	// Two distinct instances representing the same logical warning
	// (same full code). A logical de-duplication should collapse them.
	store.AddWarning(NewStateFileWarning("/tmp/x"))
	store.AddWarning(NewStateFileWarning("/tmp/x"))

	assert.Len(t, store.GetWarningsRaw(), 1,
		"warnings with the same full code should be de-duplicated")
}

func TestReconcile_NilState(t *testing.T) {
	t.Parallel()

	store := NewWarningStore()
	store.AddWarning(NewStateFileWarning("/tmp/x"))

	solved, added := store.ReconcileWarningsWithState(nil)
	assert.Zero(t, solved)
	assert.Zero(t, added)
}

func TestReconcile_NewWarningPersisted(t *testing.T) {
	t.Parallel()

	store := NewWarningStore()
	w := NewStateFileWarning("/tmp/x")
	store.AddWarning(w)

	dbState := config.NewDatabaseState()
	solved, added := store.ReconcileWarningsWithState(&dbState)

	assert.Equal(t, 0, solved)
	assert.Equal(t, 1, added, "a warning absent from last run is new")
	_, ok := dbState.LastWarnings[w.GetFullCode()]
	assert.True(t, ok, "current warning must be persisted to LastWarnings")
}

func TestReconcile_KnownWarningNotCountedNew(t *testing.T) {
	t.Parallel()

	w := NewStateFileWarning("/tmp/x")

	dbState := config.NewDatabaseState()
	dbState.LastWarnings[w.GetFullCode()] = mustState(t, w)

	store := NewWarningStore()
	store.AddWarning(w)

	solved, added := store.ReconcileWarningsWithState(&dbState)
	assert.Equal(t, 0, added, "a warning already in LastWarnings is not new")
	assert.Equal(t, 0, solved)
	_, ok := dbState.LastWarnings[w.GetFullCode()]
	assert.True(t, ok, "warning must still be persisted")
}

func TestReconcile_SolvedNonIdempotent(t *testing.T) {
	t.Parallel()

	// WAL warning is non-idempotent. It was present last run but is gone now.
	prev := NewWALSizeWarning(2*1024*1024*1024, 1024*1024*1024)
	require.NotNil(t, prev)
	require.False(t, prev.GetIsIdempotent())

	dbState := config.NewDatabaseState()
	dbState.LastWarnings[prev.GetFullCode()] = mustState(t, prev)

	store := NewWarningStore() // empty: warning no longer reproduced
	solved, added := store.ReconcileWarningsWithState(&dbState)

	assert.Equal(t, 1, solved, "a disappeared non-idempotent warning is solved")
	assert.Equal(t, 0, added)
	assert.Empty(t, store.GetWarningsRaw(), "solved warning must not be re-added")
	assert.Empty(t, dbState.LastWarnings, "solved warning must not be persisted again")
}

func TestReconcile_IdempotentReAddedWhenAbsent(t *testing.T) {
	t.Parallel()

	// State file warning is idempotent. It was present last run; even if not
	// reproduced this run it should be carried over (re-added & persisted).
	prev := NewStateFileWarning("/tmp/x")
	require.True(t, prev.GetIsIdempotent())

	dbState := config.NewDatabaseState()
	dbState.LastWarnings[prev.GetFullCode()] = mustState(t, prev)

	store := NewWarningStore() // empty this run
	solved, added := store.ReconcileWarningsWithState(&dbState)

	assert.Equal(t, 0, solved)
	assert.Equal(t, 0, added)
	require.Len(t, store.GetWarningsRaw(), 1, "idempotent warning should be carried over")
	_, ok := dbState.LastWarnings[prev.GetFullCode()]
	assert.True(t, ok, "carried-over warning must be persisted")
}

func TestReconcile_IgnoredIdempotentFilteredOut(t *testing.T) {
	t.Parallel()

	w := NewStateFileWarning("/tmp/x")
	require.True(t, w.GetIsIdempotent())

	dbState := config.NewDatabaseState()
	dbState.IgnoredWarnings[w.GetFullCode()] = mustState(t, w)

	store := NewWarningStore()
	store.AddWarning(w)

	solved, added := store.ReconcileWarningsWithState(&dbState)

	assert.Equal(t, 0, added)
	assert.Equal(t, 0, solved)
	assert.Empty(t, store.GetWarningsRaw(), "ignored idempotent warning must be filtered out")
	_, ok := dbState.LastWarnings[w.GetFullCode()]
	assert.False(t, ok, "ignored warning must not be persisted")
}

func mustState(t *testing.T, w Warning) json.RawMessage {
	t.Helper()
	v, err := w.GetStateValues()
	require.NoError(t, err)
	return v
}
