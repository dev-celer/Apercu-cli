package seeding

import (
	"apercu-cli/config"
	"apercu-cli/helper/warning"
	"apercu-cli/output"
	"crypto/md5"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newTestDirectSeed() *DirectSeed {
	return &DirectSeed{
		output:   output.NewSeedingOutput(),
		warnings: make([]warning.Warning, 0),
	}
}

func TestCompareSeedContentFromHash(t *testing.T) {
	t.Parallel()

	t.Run("matching hash", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "seed.sql")
		content := []byte("INSERT INTO users (name) VALUES ('test');")
		err := os.WriteFile(filePath, content, 0644)
		assert.NoError(t, err)

		h := md5.Sum(content)
		hash := hex.EncodeToString(h[:])

		ds := newTestDirectSeed()
		match, err := ds.compareSeedContentFromHash(hash, filePath)
		assert.NoError(t, err)
		assert.True(t, match)
	})

	t.Run("non-matching hash", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "seed.sql")
		err := os.WriteFile(filePath, []byte("INSERT INTO users (name) VALUES ('test');"), 0644)
		assert.NoError(t, err)

		ds := newTestDirectSeed()
		match, err := ds.compareSeedContentFromHash("abcdef1234567890abcdef1234567890", filePath)
		assert.NoError(t, err)
		assert.False(t, match)
	})

	t.Run("file does not exist", func(t *testing.T) {
		t.Parallel()
		ds := newTestDirectSeed()
		match, err := ds.compareSeedContentFromHash("somehash", "/nonexistent/path/seed.sql")
		assert.Error(t, err)
		assert.False(t, match)
	})
}

func TestShouldSeedBeApplied(t *testing.T) {
	t.Parallel()

	t.Run("seed not in state with create mode", func(t *testing.T) {
		t.Parallel()
		state := []config.SeedState{
			{Name: "other.sql", Hash: "abc"},
		}

		ds := newTestDirectSeed()
		applied, err := ds.shouldSeedBeApplied("seed.sql", config.DatabaseSeedTypeCreate, state)
		assert.NoError(t, err)
		assert.True(t, applied)
	})

	t.Run("always mode applies even if already applied", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "seed.sql")
		content := []byte("SELECT 1;")
		err := os.WriteFile(filePath, content, 0644)
		assert.NoError(t, err)

		h := md5.Sum(content)
		hash := hex.EncodeToString(h[:])

		state := []config.SeedState{
			{Name: filePath, Hash: hash},
		}

		ds := newTestDirectSeed()
		applied, err := ds.shouldSeedBeApplied(filePath, config.DatabaseSeedTypeAlways, state)
		assert.NoError(t, err)
		assert.True(t, applied)
	})

	t.Run("create mode skips when already applied with same content", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "seed.sql")
		content := []byte("SELECT 1;")
		err := os.WriteFile(filePath, content, 0644)
		assert.NoError(t, err)

		h := md5.Sum(content)
		hash := hex.EncodeToString(h[:])

		state := []config.SeedState{
			{Name: filePath, Hash: hash},
		}

		ds := newTestDirectSeed()
		applied, err := ds.shouldSeedBeApplied(filePath, config.DatabaseSeedTypeCreate, state)
		assert.NoError(t, err)
		assert.False(t, applied)
	})

	t.Run("create mode skips when already applied with different content", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "seed.sql")
		err := os.WriteFile(filePath, []byte("SELECT 2;"), 0644)
		assert.NoError(t, err)

		originalContent := []byte("SELECT 1;")
		h := md5.Sum(originalContent)
		hash := hex.EncodeToString(h[:])

		state := []config.SeedState{
			{Name: filePath, Hash: hash},
		}

		ds := newTestDirectSeed()
		applied, err := ds.shouldSeedBeApplied(filePath, config.DatabaseSeedTypeCreate, state)
		assert.NoError(t, err)
		assert.False(t, applied)
	})

	t.Run("update mode applies when content changed", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "seed.sql")
		err := os.WriteFile(filePath, []byte("SELECT 2;"), 0644)
		assert.NoError(t, err)

		originalContent := []byte("SELECT 1;")
		h := md5.Sum(originalContent)
		hash := hex.EncodeToString(h[:])

		state := []config.SeedState{
			{Name: filePath, Hash: hash},
		}

		ds := newTestDirectSeed()
		applied, err := ds.shouldSeedBeApplied(filePath, config.DatabaseSeedTypeUpdate, state)
		assert.NoError(t, err)
		assert.True(t, applied)
	})

	t.Run("update mode skips when content unchanged", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "seed.sql")
		content := []byte("SELECT 1;")
		err := os.WriteFile(filePath, content, 0644)
		assert.NoError(t, err)

		h := md5.Sum(content)
		hash := hex.EncodeToString(h[:])

		state := []config.SeedState{
			{Name: filePath, Hash: hash},
		}

		ds := newTestDirectSeed()
		applied, err := ds.shouldSeedBeApplied(filePath, config.DatabaseSeedTypeUpdate, state)
		assert.NoError(t, err)
		assert.False(t, applied)
	})

	t.Run("empty state applies seed", func(t *testing.T) {
		t.Parallel()
		ds := newTestDirectSeed()
		applied, err := ds.shouldSeedBeApplied("seed.sql", config.DatabaseSeedTypeCreate, []config.SeedState{})
		assert.NoError(t, err)
		assert.True(t, applied)
	})
}

func TestGetSeedFilesToApply(t *testing.T) {
	t.Parallel()
	dirPath := t.TempDir()
	err := os.MkdirAll(filepath.Join(dirPath, "seed1"), 0755)
	assert.NoError(t, err)
	err = os.MkdirAll(filepath.Join(dirPath, "seed2"), 0755)
	assert.NoError(t, err)

	err = os.WriteFile(filepath.Join(dirPath, "seed1.sql"), []byte("SELECT 1;"), 0644)
	assert.NoError(t, err)
	err = os.WriteFile(filepath.Join(filepath.Join(dirPath, "seed1"), "seed2.sql"), []byte("SELECT 2;"), 0644)
	assert.NoError(t, err)

	t.Run("no seeds", func(t *testing.T) {
		t.Parallel()
		ds := &DirectSeed{
			output:    output.NewSeedingOutput(),
			warnings:  make([]warning.Warning, 0),
			state:     &config.DatabaseState{AppliedSeeds: make([]config.SeedState, 0)},
			seedFiles: nil,
		}

		ds.getSeedFilesToApply()
		assert.Len(t, ds.seedFilesPath, 0)
		assert.Len(t, ds.output.Errors, 0)
		assert.Len(t, ds.warnings, 0)
	})

	t.Run("seed file path", func(t *testing.T) {
		t.Parallel()
		ds := &DirectSeed{
			output:   output.NewSeedingOutput(),
			warnings: make([]warning.Warning, 0),
			state:    &config.DatabaseState{AppliedSeeds: make([]config.SeedState, 0)},
			seedFiles: []config.DatabaseSeed{
				{Path: filepath.Join(dirPath, "seed1.sql")},
			},
		}

		ds.getSeedFilesToApply()
		assert.Len(t, ds.seedFilesPath, 1)
		assert.Equal(t, ds.seedFilesPath[0], filepath.Join(dirPath, "seed1.sql"))
		assert.Len(t, ds.output.Errors, 0)
		assert.Len(t, ds.warnings, 0)
	})

	t.Run("seed folder path", func(t *testing.T) {
		t.Parallel()
		ds := &DirectSeed{
			output:   output.NewSeedingOutput(),
			warnings: make([]warning.Warning, 0),
			state:    &config.DatabaseState{AppliedSeeds: make([]config.SeedState, 0)},
			seedFiles: []config.DatabaseSeed{
				{Path: filepath.Join(dirPath, "seed1")},
			},
		}

		ds.getSeedFilesToApply()
		assert.Len(t, ds.seedFilesPath, 1)
		assert.Equal(t, ds.seedFilesPath[0], filepath.Join(dirPath, "seed1", "seed2.sql"))
		assert.Len(t, ds.output.Errors, 0)
		assert.Len(t, ds.warnings, 0)
	})

	t.Run("seed folder and file path", func(t *testing.T) {
		t.Parallel()
		ds := &DirectSeed{
			output:   output.NewSeedingOutput(),
			warnings: make([]warning.Warning, 0),
			state:    &config.DatabaseState{AppliedSeeds: make([]config.SeedState, 0)},
			seedFiles: []config.DatabaseSeed{
				{Path: filepath.Join(dirPath, "seed1")},
				{Path: filepath.Join(dirPath, "seed1.sql")},
			},
		}

		ds.getSeedFilesToApply()
		assert.Len(t, ds.seedFilesPath, 2)
		assert.Equal(t, ds.seedFilesPath[0], filepath.Join(dirPath, "seed1", "seed2.sql"))
		assert.Equal(t, ds.seedFilesPath[1], filepath.Join(dirPath, "seed1.sql"))
		assert.Len(t, ds.output.Errors, 0)
		assert.Len(t, ds.warnings, 0)
	})

	t.Run("missing path", func(t *testing.T) {
		t.Parallel()
		ds := &DirectSeed{
			output:   output.NewSeedingOutput(),
			warnings: make([]warning.Warning, 0),
			state:    &config.DatabaseState{AppliedSeeds: make([]config.SeedState, 0)},
			seedFiles: []config.DatabaseSeed{
				{Path: filepath.Join(dirPath, "nonexistent")},
				{Path: filepath.Join(dirPath, "seed1.sql")},
			},
		}

		ds.getSeedFilesToApply()
		assert.Len(t, ds.seedFilesPath, 1)
		assert.Equal(t, ds.seedFilesPath[0], filepath.Join(dirPath, "seed1.sql"))
		assert.Len(t, ds.output.Errors, 0)
		assert.Len(t, ds.warnings, 1)
	})

	t.Run("no valid path", func(t *testing.T) {
		t.Parallel()
		ds := &DirectSeed{
			output:   output.NewSeedingOutput(),
			warnings: make([]warning.Warning, 0),
			state:    &config.DatabaseState{AppliedSeeds: make([]config.SeedState, 0)},
			seedFiles: []config.DatabaseSeed{
				{Path: filepath.Join(dirPath, "nonexistent")},
				{Path: filepath.Join(dirPath, "seed2.sql")},
			},
		}

		ds.getSeedFilesToApply()
		assert.Len(t, ds.seedFilesPath, 0)
		assert.Len(t, ds.output.Errors, 0)
		assert.Len(t, ds.warnings, 2)
	})
}
