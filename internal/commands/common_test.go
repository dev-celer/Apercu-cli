package commands

import (
	"apercu-cli/internal/migration"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Migration mock ---

type mockMigrationHandler struct {
	countResults []struct {
		count int
		err   error
	}
	countCall int
	applyErr  error
	duration  *time.Duration
	output    string
}

func (h *mockMigrationHandler) GetCount() (int, error) {
	if h.countCall >= len(h.countResults) {
		return 0, nil
	}
	r := h.countResults[h.countCall]
	h.countCall++
	return r.count, r.err
}

func (h *mockMigrationHandler) Apply(_ context.Context) error { return h.applyErr }
func (h *mockMigrationHandler) GetDuration() *time.Duration   { return h.duration }
func (h *mockMigrationHandler) GetOutput() string             { return h.output }

// --- Seeding mock ---

type mockSeedingHandler struct {
	failedCount  int
	appliedCount int
	duration     *time.Duration
	output       string
}

func (h *mockSeedingHandler) Close() error                { return nil }
func (h *mockSeedingHandler) Apply()                      {}
func (h *mockSeedingHandler) GetDuration() *time.Duration { return h.duration }
func (h *mockSeedingHandler) GetAppliedCount() int        { return h.appliedCount }
func (h *mockSeedingHandler) GetFailedCount() int         { return h.failedCount }
func (h *mockSeedingHandler) GetOutput() string           { return h.output }

// --- ApplyMigration tests ---

func TestApplyMigration_NilHandler(t *testing.T) {
	t.Parallel()
	msg, err := ApplyMigration(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, msg)
}

func TestApplyMigration_Success(t *testing.T) {
	t.Parallel()
	d := 2 * time.Second
	handler := &mockMigrationHandler{
		countResults: []struct {
			count int
			err   error
		}{
			{count: 5, err: nil},
			{count: 8, err: nil},
		},
		duration: &d,
	}

	msg, err := ApplyMigration(context.Background(), handler)
	require.NoError(t, err)
	assert.Contains(t, msg, "Migration completed successfully")
	assert.Contains(t, msg, "3 migrations applied")
	assert.Contains(t, msg, "2s")
}

func TestApplyMigration_InitialCountTableNotFound(t *testing.T) {
	t.Parallel()
	handler := &mockMigrationHandler{
		countResults: []struct {
			count int
			err   error
		}{
			{count: 0, err: migration.ErrMigrationTableNotFound},
			{count: 3, err: nil},
		},
	}

	msg, err := ApplyMigration(context.Background(), handler)
	require.NoError(t, err)
	assert.Contains(t, msg, "3 migrations applied")
}

func TestApplyMigration_ApplyError(t *testing.T) {
	t.Parallel()
	handler := &mockMigrationHandler{
		countResults: []struct {
			count int
			err   error
		}{
			{count: 0, err: nil},
		},
		applyErr: errors.New("docker crashed"),
	}

	_, err := ApplyMigration(context.Background(), handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "migration failed")
	assert.Contains(t, err.Error(), "docker crashed")
}

func TestApplyMigration_InitialCountError(t *testing.T) {
	t.Parallel()
	handler := &mockMigrationHandler{
		countResults: []struct {
			count int
			err   error
		}{
			{count: 0, err: errors.New("connection refused")},
		},
	}

	_, err := ApplyMigration(context.Background(), handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

func TestApplyMigration_NoDuration(t *testing.T) {
	t.Parallel()
	handler := &mockMigrationHandler{
		countResults: []struct {
			count int
			err   error
		}{
			{count: 0, err: nil},
			{count: 1, err: nil},
		},
	}

	msg, err := ApplyMigration(context.Background(), handler)
	require.NoError(t, err)
	assert.Contains(t, msg, "Migration completed successfully")
	assert.NotContains(t, msg, "completed in")
}

// --- ApplySeeding tests ---

func TestApplySeeding_NilHandler(t *testing.T) {
	t.Parallel()
	msg, err := ApplySeeding(nil)
	require.NoError(t, err)
	assert.Empty(t, msg)
}

func TestApplySeeding_Success(t *testing.T) {
	t.Parallel()
	d := 500 * time.Millisecond
	handler := &mockSeedingHandler{
		appliedCount: 3,
		failedCount:  0,
		duration:     &d,
	}

	msg, err := ApplySeeding(handler)
	require.NoError(t, err)
	assert.Contains(t, msg, "Seeding completed successfully")
	assert.Contains(t, msg, "3 files applied successfully")
	assert.Contains(t, msg, "500ms")
}

func TestApplySeeding_PartialFailures(t *testing.T) {
	t.Parallel()
	handler := &mockSeedingHandler{
		appliedCount: 2,
		failedCount:  1,
	}

	msg, err := ApplySeeding(handler)
	require.NoError(t, err)
	assert.Contains(t, msg, "Seeding completed with 1 errors")
	assert.Contains(t, msg, "2 files applied successfully")
}

func TestApplySeeding_NoDuration(t *testing.T) {
	t.Parallel()
	handler := &mockSeedingHandler{
		appliedCount: 1,
	}

	msg, err := ApplySeeding(handler)
	require.NoError(t, err)
	assert.Contains(t, msg, "Seeding completed successfully")
	assert.NotContains(t, msg, "completed in")
}
