package pg_catalog

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testSchema is the schema the integration fixture is built in.
const testSchema = "apercu_snapshot_test"

func findRelation(snapshot *Snapshot, name string) (Relation, bool) {
	for _, rel := range snapshot.Relations {
		if rel.Namespace == testSchema && rel.Name == name {
			return rel, true
		}
	}
	return Relation{}, false
}

func findColumn(snapshot *Snapshot, relid OID, name string) (Column, bool) {
	for _, col := range snapshot.Columns {
		if col.RelID == relid && col.Name == name {
			return col, true
		}
	}
	return Column{}, false
}

// fixtureVersions are the servers the committed fixtures were captured against.
// Regenerate them with: go test -tags integration ./helper/pg_catalog/ -run TestCollectAgainstServer
// with APERCU_UPDATE_FIXTURES=1 set.
var fixtureVersions = []int{15, 16, 17, 18}

func loadFixture(t *testing.T, version int, source Source) *Snapshot {
	t.Helper()

	path := filepath.Join("testdata", fmt.Sprintf("snapshot_pg%d_%s.json.gz", version, source))
	snapshot, err := LoadJSON(path)
	require.NoError(t, err)
	return snapshot
}

// TestFixturesAreUsable is what every later phase depends on: the committed
// snapshots load with no database and carry the facts the rules are written
// against.
func TestFixturesAreUsable(t *testing.T) {
	t.Parallel()

	for _, version := range fixtureVersions {
		t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
			t.Parallel()

			baseline := loadFixture(t, version, SourceBaseline)
			assert.Equal(t, version, int(baseline.Header.Version))
			assert.Equal(t, SourceBaseline, baseline.Source)
			assert.Equal(t, PITPre, baseline.PIT)

			assert.NotEmpty(t, baseline.Relations)
			assert.NotEmpty(t, baseline.Columns)
			assert.NotEmpty(t, baseline.Constraints)
			assert.NotEmpty(t, baseline.Procs, "volatility lookups need the whole of pg_proc")
			assert.NotEmpty(t, baseline.Casts, "binary coercibility needs the whole of pg_cast")
			assert.NotEmpty(t, baseline.Operators)

			events, ok := findRelation(baseline, "events")
			require.True(t, ok)
			assert.Equal(t, "p", events.Kind)
			assert.Zero(t, events.TotalBytes, "a partitioned parent has no size of its own")

			// The version-dependent shape of a NOT NULL, which P-14 reconciles.
			notNull := 0
			for _, c := range baseline.Constraints {
				if c.Type == "n" {
					notNull++
				}
			}
			if version >= 18 {
				assert.Positive(t, notNull)
			} else {
				assert.Zero(t, notNull)
			}

			prod := loadFixture(t, version, SourceProd)
			assert.Equal(t, SourceProd, prod.Source)
			assert.NotEmpty(t, prod.TableStats)
			assert.Empty(t, prod.Relations, "the schema comes from the baseline")
		})
	}
}
