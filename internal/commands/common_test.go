package commands

import (
	"apercu-cli/output"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Migration mock ---

type mockMigrationHandler struct {
	applyErr error
	output   *output.OutputDatabaseMigration
}

func (h *mockMigrationHandler) Apply(_ context.Context) error              { return h.applyErr }
func (h *mockMigrationHandler) GetOutput() *output.OutputDatabaseMigration { return h.output }

// --- Seeding mock ---

type mockSeedingHandler struct {
	output *output.OutputDatabaseSeeding
}

func (h *mockSeedingHandler) Close() error                             { return nil }
func (h *mockSeedingHandler) Apply()                                   {}
func (h *mockSeedingHandler) GetOutput() *output.OutputDatabaseSeeding { return h.output }

// --- ApplyMigration tests ---

func TestApplyMigration_NilHandler(t *testing.T) {
	t.Parallel()
	msg, err := ApplyMigration(context.Background(), nil, nil)
	require.NoError(t, err)
	assert.Empty(t, msg)
}

func TestApplyMigration_Success(t *testing.T) {
	t.Parallel()
	handler := &mockMigrationHandler{
		output: &output.OutputDatabaseMigration{
			Count:    3,
			Duration: "2s",
			Warnings: make([]string, 0),
			Errors:   make([]string, 0),
		},
	}

	msg, err := ApplyMigration(context.Background(), handler, nil)
	require.NoError(t, err)
	assert.Contains(t, msg, "Migration completed successfully")
	assert.Contains(t, msg, "3 migrations applied")
	assert.Contains(t, msg, "2s")
}

func TestApplyMigration_ApplyError(t *testing.T) {
	t.Parallel()
	handler := &mockMigrationHandler{
		applyErr: errors.New("docker crashed"),
		output: &output.OutputDatabaseMigration{
			Warnings: make([]string, 0),
			Errors:   make([]string, 0),
		},
	}

	_, err := ApplyMigration(context.Background(), handler, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "migration failed")
	assert.Contains(t, err.Error(), "docker crashed")
}

func TestApplyMigration_NoDuration(t *testing.T) {
	t.Parallel()
	handler := &mockMigrationHandler{
		output: &output.OutputDatabaseMigration{
			Count:    1,
			Duration: "",
			Warnings: make([]string, 0),
			Errors:   make([]string, 0),
		},
	}

	msg, err := ApplyMigration(context.Background(), handler, nil)
	require.NoError(t, err)
	assert.Contains(t, msg, "Migration completed successfully")
	assert.NotContains(t, msg, "completed in")
}

// --- ApplySeeding tests ---

func TestApplySeeding_NilHandler(t *testing.T) {
	t.Parallel()
	msg := ApplySeeding(nil)
	assert.Empty(t, msg)
}

func TestApplySeeding_Success(t *testing.T) {
	t.Parallel()
	handler := &mockSeedingHandler{
		output: &output.OutputDatabaseSeeding{
			SuccessCount: 3,
			FailedCount:  0,
			Duration:     "500ms",
			Warnings:     make([]string, 0),
			Errors:       make([]string, 0),
		},
	}

	msg := ApplySeeding(handler)
	assert.Contains(t, msg, "Seeding completed successfully")
	assert.Contains(t, msg, "3 files applied successfully")
	assert.Contains(t, msg, "500ms")
}

func TestApplySeeding_PartialFailures(t *testing.T) {
	t.Parallel()
	handler := &mockSeedingHandler{
		output: &output.OutputDatabaseSeeding{
			SuccessCount: 2,
			FailedCount:  1,
			Warnings:     make([]string, 0),
			Errors:       make([]string, 0),
		},
	}

	msg := ApplySeeding(handler)
	assert.Contains(t, msg, "Seeding completed with 1 errors")
	assert.Contains(t, msg, "2 files applied successfully")
}

func TestApplySeeding_NoDuration(t *testing.T) {
	t.Parallel()
	handler := &mockSeedingHandler{
		output: &output.OutputDatabaseSeeding{
			SuccessCount: 1,
			Duration:     "",
			Warnings:     make([]string, 0),
			Errors:       make([]string, 0),
		},
	}

	msg := ApplySeeding(handler)
	assert.Contains(t, msg, "Seeding completed successfully")
	assert.NotContains(t, msg, "completed in")
}
