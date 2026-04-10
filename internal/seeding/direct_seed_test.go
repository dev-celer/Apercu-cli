package seeding

import (
	"apercu-cli/config"
	"crypto/md5"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDirectSeed_GetDuration(t *testing.T) {
	t.Parallel()

	t.Run("both times set", func(t *testing.T) {
		t.Parallel()
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2024, 1, 1, 0, 0, 3, 0, time.UTC)
		h := &DirectSeed{startTime: &start, endTime: &end}

		d := h.GetDuration()
		assert.NotNil(t, d)
		assert.Equal(t, 3*time.Second, *d)
	})

	t.Run("nil start time", func(t *testing.T) {
		t.Parallel()
		end := time.Now()
		h := &DirectSeed{endTime: &end}
		assert.Nil(t, h.GetDuration())
	})

	t.Run("nil end time", func(t *testing.T) {
		t.Parallel()
		start := time.Now()
		h := &DirectSeed{startTime: &start}
		assert.Nil(t, h.GetDuration())
	})

	t.Run("nil times", func(t *testing.T) {
		t.Parallel()
		h := &DirectSeed{}
		assert.Nil(t, h.GetDuration())
	})
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

		match, err := compareSeedContentFromHash(hash, filePath)
		assert.NoError(t, err)
		assert.True(t, match)
	})

	t.Run("non-matching hash", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "seed.sql")
		err := os.WriteFile(filePath, []byte("INSERT INTO users (name) VALUES ('test');"), 0644)
		assert.NoError(t, err)

		match, err := compareSeedContentFromHash("abcdef1234567890abcdef1234567890", filePath)
		assert.NoError(t, err)
		assert.False(t, match)
	})

	t.Run("file does not exist", func(t *testing.T) {
		t.Parallel()
		match, err := compareSeedContentFromHash("somehash", "/nonexistent/path/seed.sql")
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

		applied, err := shouldSeedBeApplied("seed.sql", config.DatabaseSeedTypeCreate, state)
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

		applied, err := shouldSeedBeApplied(filePath, config.DatabaseSeedTypeAlways, state)
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

		applied, err := shouldSeedBeApplied(filePath, config.DatabaseSeedTypeCreate, state)
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

		applied, err := shouldSeedBeApplied(filePath, config.DatabaseSeedTypeCreate, state)
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

		applied, err := shouldSeedBeApplied(filePath, config.DatabaseSeedTypeUpdate, state)
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

		applied, err := shouldSeedBeApplied(filePath, config.DatabaseSeedTypeUpdate, state)
		assert.NoError(t, err)
		assert.False(t, applied)
	})

	t.Run("empty state applies seed", func(t *testing.T) {
		t.Parallel()
		applied, err := shouldSeedBeApplied("seed.sql", config.DatabaseSeedTypeCreate, []config.SeedState{})
		assert.NoError(t, err)
		assert.True(t, applied)
	})
}
