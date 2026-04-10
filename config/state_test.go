package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStateSaveAndLoad(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	original := &State{
		Databases: map[string]DatabaseState{
			"mydb": {
				AppliedSeeds: []SeedState{
					{Name: "seed1.sql", Hash: "abc123"},
					{Name: "seed2.sql", Hash: "def456"}},
			},
		},
	}

	err := original.Save(path)
	require.NoError(t, err)

	loaded, err := GetState(path)
	require.NoError(t, err)

	assert.Equal(t, original.Databases["mydb"].AppliedSeeds, loaded.Databases["mydb"].AppliedSeeds)
}

func TestGetState_NonExistent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "does-not-exist.json")

	state, err := GetState(path)
	require.NoError(t, err)
	assert.NotNil(t, state.Databases)
	assert.Empty(t, state.Databases)
}

func TestGetState_CorruptJSON(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.json")

	err := os.WriteFile(path, []byte("{invalid json"), 0644)
	require.NoError(t, err)

	_, err = GetState(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Failed to parse state file")
}
