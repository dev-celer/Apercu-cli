package pg_catalog

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
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
		// Schema items live on the baseline and are captured on both sides so the diff has something to compare.
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

		// Publications prefer production but fall back to the baseline when there is no production snapshot coming.
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

// Assert that the user_ns filter is applied to the required queries
func TestInventoryQueriesAreScoped(t *testing.T) {
	t.Parallel()

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
		"S-09 types":      typesQuery,
		"S-13 procs":      procsQuery,
		"S-14 casts":      castsQuery,
		"S-14 operators":  operatorsQuery,
		"S-16 settings":   settingsQuery,
		"S-17 stats":      tableStatsQuery,
		"S-18 roles":      rolesQuery,
		"S-19 collations": collationsQuery,
	}

	for name, query := range inventory {
		assert.Contains(t, query, userNS, "%s must apply the scope filter", name)
	}
	for name, query := range reference {
		assert.NotContains(t, query, userNS, "%s is reference data and must not be filtered", name)
	}
}

func TestSnapshotHas(t *testing.T) {
	t.Parallel()

	snapshot := &Snapshot{Collected: []string{"S-00", "S-02", "S-17"}}
	assert.True(t, snapshot.Has("S-00"))
	assert.True(t, snapshot.Has("S-17"))
	assert.False(t, snapshot.Has("S-05"))
}

func TestFixtureRoundTrip(t *testing.T) {
	t.Parallel()

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
			{RelID: 16384, Num: 1, Name: "id", StatsTarget: new(int32(500))},
			{RelID: 16384, Num: 2, Name: "at"},
		},
		TableStats: []TableStat{{RelID: 16384, Name: "events", LastAnalyze: new(time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC))}},
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

	// Committed fixtures are compressed; the reader sniffs the content so a rename cannot break them.
	compressed := filepath.Join(t.TempDir(), "snapshot.json.gz")
	require.NoError(t, original.WriteJSON(compressed))

	raw, err := os.ReadFile(compressed)
	require.NoError(t, err)
	assert.True(t, bytes.HasPrefix(raw, gzipMagic), "the .gz fixture is not compressed")

	fromGzip, err := LoadJSON(compressed)
	require.NoError(t, err)
	assert.Equal(t, original, fromGzip)
}
