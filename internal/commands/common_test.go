package commands

import (
	"apercu-cli/output"
	"testing"

	"github.com/stretchr/testify/assert"
)

// --- Seeding mock ---

type mockSeedingHandler struct {
	output *output.OutputDatabaseSeeding
}

func (h *mockSeedingHandler) Close() error                             { return nil }
func (h *mockSeedingHandler) Apply()                                   {}
func (h *mockSeedingHandler) GetOutput() *output.OutputDatabaseSeeding { return h.output }

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
