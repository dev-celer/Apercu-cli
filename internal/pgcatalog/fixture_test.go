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
