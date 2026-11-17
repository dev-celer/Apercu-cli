package pgcatalog

import (
	"fmt"
	"path/filepath"
	"testing"

	"apercu-cli/internal/pgcatalog/snapshot"

	"github.com/stretchr/testify/require"
)

// testSchema is the schema the collector's integration test seeds; snapshot has its own copy.
const testSchema = "apercu_snapshot_test"

// fixtureVersions are the servers the committed fixtures were captured against.
var fixtureVersions = []int{15, 16, 17, 18}

// loadFixture reads one committed snapshot from the snapshot package's testdata.
func loadFixture(t *testing.T, version int, source snapshot.Source) *snapshot.Snapshot {
	t.Helper()

	path := filepath.Join("snapshot", "testdata", fmt.Sprintf("snapshot_pg%d_%s.json.gz", version, source))
	loaded, err := snapshot.LoadJSON(path)
	require.NoError(t, err)
	return loaded
}

// buildState is NewCatalog with an empty overlay on top, which is how everything outside the package reads it.
func buildState(opts CatalogOptions) (*State, error) {
	catalog, err := NewCatalog(opts)
	if err != nil {
		return nil, err
	}
	return NewRecorder(catalog).State(), nil
}

// recorderOf writes to a state a test built. Outside the package, only whoever built the state holds its recorder.
func recorderOf(state *State) *Recorder {
	return &Recorder{state: state}
}
