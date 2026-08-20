package pg_catalog

import (
	"apercu-cli/helper/pg_contract"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestItemRouting(t *testing.T) {
	t.Parallel()

	byID := map[string]item{}
	for _, it := range items {
		byID[it.id] = it
	}

	tests := []struct {
		name     string
		itemID   string
		opts     CollectOptions
		expected bool
	}{
		// Schema items live on the baseline and are captured on both sides so
		// the diff has something to compare.
		{name: "relations from baseline pre", itemID: "S-02",
			opts: CollectOptions{Source: SourceBaseline, PIT: PITPre}, expected: true},
		{name: "relations from baseline post", itemID: "S-02",
			opts: CollectOptions{Source: SourceBaseline, PIT: PITPost}, expected: true},
		{name: "relations not from prod", itemID: "S-02",
			opts: CollectOptions{Source: SourceProd, PIT: PITPre}, expected: false},

		// Activity statistics only mean something on production.
		{name: "stats from prod", itemID: "S-17",
			opts: CollectOptions{Source: SourceProd, PIT: PITPre}, expected: true},
		{name: "stats never from baseline", itemID: "S-17",
			opts: CollectOptions{Source: SourceBaseline, PIT: PITPre, ProdAvailable: false}, expected: false},

		// Publications prefer production but fall back to the baseline when
		// there is no production snapshot coming.
		{name: "publications from prod", itemID: "S-15",
			opts: CollectOptions{Source: SourceProd, PIT: PITPre}, expected: true},
		{name: "publications skipped on baseline when prod will provide them", itemID: "S-15",
			opts: CollectOptions{Source: SourceBaseline, PIT: PITPre, ProdAvailable: true}, expected: false},
		{name: "publications fall back to baseline without prod", itemID: "S-15",
			opts: CollectOptions{Source: SourceBaseline, PIT: PITPre, ProdAvailable: false}, expected: true},

		// Pre-only items are not repeated after the migration.
		{name: "settings not captured post", itemID: "S-16",
			opts: CollectOptions{Source: SourceBaseline, PIT: PITPost}, expected: false},
		{name: "roles captured post", itemID: "S-18",
			opts: CollectOptions{Source: SourceBaseline, PIT: PITPost}, expected: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			it, ok := byID[test.itemID]
			require.True(t, ok, "unknown item %s", test.itemID)
			assert.Equal(t, test.expected, it.wanted(test.opts))
		})
	}
}

func TestItemsCoverTheSpec(t *testing.T) {
	t.Parallel()

	// S-00 is deliberately absent: the header is always read, because the
	// collector needs the server version before it can build S-03 and S-05.
	expected := []string{
		"S-01", "S-02", "S-03", "S-04", "S-05", "S-06", "S-07", "S-08", "S-09",
		"S-10", "S-11", "S-12", "S-13", "S-14", "S-15", "S-16", "S-17", "S-18",
	}

	collected := make([]string, 0, len(items))
	for _, it := range items {
		collected = append(collected, it.id)
		assert.NotNil(t, it.collect, "%s has no collector", it.id)
	}
	assert.Equal(t, expected, collected)
}

func TestInventoryQueriesAreScoped(t *testing.T) {
	t.Parallel()

	// Inventory queries must carry the S-SCOPE filter; reference queries are
	// captured whole, because user objects use built-in types and functions.
	inventory := map[string]string{
		"S-01 schemas":     schemasQuery,
		"S-02 relations":   relationsQuery,
		"S-03 columns":     columnsQuery,
		"S-04 defaults":    defaultsQuery,
		"S-05 constraints": constraintsQuery(180000),
		"S-06 indexes":     indexesQuery,
		"S-07 inherits":    inheritsQuery,
		"S-08 sequences":   sequencesQuery,
		"S-10 triggers":    triggersQuery,
		"S-10 policies":    policiesQuery,
		"S-11 view deps":   viewDepsQuery,
		"S-12 depends":     dependsQuery,
		"S-18 relacls":     relACLsQuery,
	}
	reference := map[string]string{
		"S-09 types":     typesQuery,
		"S-13 procs":     procsQuery,
		"S-14 casts":     castsQuery,
		"S-14 operators": operatorsQuery,
		"S-16 settings":  settingsQuery,
		"S-17 stats":     tableStatsQuery,
		"S-18 roles":     rolesQuery,
	}

	for name, query := range inventory {
		assert.Contains(t, query, userNS, "%s must apply the scope filter", name)
	}
	for name, query := range reference {
		assert.NotContains(t, query, userNS, "%s is reference data and must not be filtered", name)
	}
}

func TestUserNSExcludesReservedSchemas(t *testing.T) {
	t.Parallel()

	// The single LIKE pattern is what covers pg_catalog, pg_toast, pg_temp_N and
	// pg_toast_temp_N at once.
	assert.Contains(t, userNS, `n.nspname <> 'information_schema'`)
	assert.Contains(t, userNS, `n.nspname NOT LIKE 'pg\_%'`)
}

func TestConstraintsQueryVersionColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		version      int
		wantPeriod   bool
		wantEnforced bool
	}{
		{name: "15 has neither", version: 150018},
		{name: "16 has neither", version: 160006},
		{name: "17 has neither, temporal support landed in 18", version: 170010},
		{name: "18 has both", version: 180004, wantPeriod: true, wantEnforced: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query := constraintsQuery(test.version)
			assert.Equal(t, test.wantPeriod, strings.Contains(query, "k.conperiod"))
			assert.Equal(t, test.wantEnforced, strings.Contains(query, "k.conenforced"))
			// The unconditional columns are there on every version.
			assert.Contains(t, query, "k.contype")
			assert.Contains(t, query, "pg_get_constraintdef(k.oid)")
		})
	}
}

func TestSnapshotHas(t *testing.T) {
	t.Parallel()

	snapshot := &Snapshot{Collected: []string{"S-00", "S-02", "S-17"}}
	assert.True(t, snapshot.Has("S-00"))
	assert.True(t, snapshot.Has("S-17"))
	// Not captured is not the same as captured and empty.
	assert.False(t, snapshot.Has("S-05"))
}

func TestFixtureRoundTrip(t *testing.T) {
	t.Parallel()

	statsTarget := int32(500)
	analyzed := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	original := &Snapshot{
		Source:     SourceBaseline,
		PIT:        PITPre,
		CapturedAt: time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC),
		Collected:  []string{"S-00", "S-02", "S-03"},
		Header: Header{
			ServerVersionNum: 180004,
			Version:          18,
			Database:         "app",
			SearchPath:       `"$user", public`,
			TimeZone:         "UTC",
		},
		Relations: []Relation{{
			OID: 16384, Namespace: "public", Name: "events", Kind: "p",
			Options: []string{"fillfactor=90"}, PartitionKey: "RANGE (at)",
		}},
		Columns: []Column{
			{RelID: 16384, Num: 1, Name: "id", StatsTarget: &statsTarget},
			{RelID: 16384, Num: 2, Name: "at"},
		},
		TableStats: []TableStat{{RelID: 16384, Name: "events", LastAnalyze: &analyzed}},
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	require.NoError(t, original.WriteJSON(path))

	loaded, err := LoadJSON(path)
	require.NoError(t, err)
	assert.Equal(t, original, loaded)

	// A default statistics target stays absent rather than becoming 0.
	encoded, err := json.Marshal(original.Columns[1])
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "stats_target")

	// Committed fixtures are compressed; the reader sniffs the content so a
	// rename cannot break them.
	compressed := filepath.Join(t.TempDir(), "snapshot.json.gz")
	require.NoError(t, original.WriteJSON(compressed))

	raw, err := os.ReadFile(compressed)
	require.NoError(t, err)
	assert.True(t, bytes.HasPrefix(raw, gzipMagic), "the .gz fixture is not compressed")

	fromGzip, err := LoadJSON(compressed)
	require.NoError(t, err)
	assert.Equal(t, original, fromGzip)
}

func TestGatingVersion(t *testing.T) {
	t.Parallel()

	baseline := &Snapshot{Source: SourceBaseline, Header: Header{Version: 16}}
	prod := &Snapshot{Source: SourceProd, Header: Header{Version: 17}}

	// Production decides, whatever the baseline runs.
	assert.Equal(t, pg_contract.Exactly(17), GatingVersion(baseline, prod))

	// Without production the baseline's version proves nothing about it, so the
	// range stays open and §8 has nothing to fire on yet.
	open := pg_contract.Between(pg_contract.MinSupportedVersion, pg_contract.MaxSupportedVersion)
	assert.Equal(t, open, GatingVersion(baseline))
	assert.Equal(t, open, GatingVersion())
	assert.False(t, open.Contains(pg_contract.VersionUnknown))

	// An unreadable or unsupported production version is no better than none.
	unsupported := &Snapshot{Source: SourceProd, Header: Header{Version: 14}}
	assert.Equal(t, open, GatingVersion(unsupported))
}
