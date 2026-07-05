package output

import (
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockedWarning struct {
	desc         string
	isIdempotent bool
	keys         []string
}

const (
	MockedCode warning.Code = "MOCK_WARN"
)

func (w *mockedWarning) GetText() string {
	return w.desc
}

func (w *mockedWarning) GetTextLong() string { return w.GetText() }

func (w *mockedWarning) GetLevel() warning.Level {
	return warning.WarningLevelMedium
}

func (w *mockedWarning) GetCode() warning.Code {
	return MockedCode
}

func (w *mockedWarning) GetFullCode() string { return string(w.GetCode()) }

func (w *mockedWarning) GetIsIdempotent() bool { return w.isIdempotent }

func (w *mockedWarning) GetStateValues() (json.RawMessage, error) { return json.RawMessage{}, nil }

func (w *mockedWarning) PrintWarning() {}

func newMockedWarning(desc string, isIdempotent bool, keys []string) *mockedWarning {
	return &mockedWarning{
		desc:         desc,
		isIdempotent: isIdempotent,
		keys:         keys,
	}
}

func strPtr(s string) *string { return &s }

func TestRenderMarkdown_Empty(t *testing.T) {
	t.Parallel()
	o := PreviewOutput{Databases: map[string]PreviewOutputDatabase{}}
	md, err := o.RenderMarkdown()
	require.NoError(t, err)
	assert.Contains(t, md, "# Apercu Output")
}

func TestRenderMarkdown_MigrationAndSeeding(t *testing.T) {
	t.Parallel()
	warningStore := warning.NewWarningStore()
	warningStore.AddWarning(newMockedWarning("top-level warn", false, nil))
	o := PreviewOutput{
		Databases: map[string]PreviewOutputDatabase{
			"mydb": {
				Migration: &OutputDatabaseMigration{
					Count:    3,
					Duration: "2s",
					Logs:     strPtr("running migration 1\nrunning migration 2"),
					Errors:   []string{},
					Metrics: &OutputDatabaseMetrics{
						Prod:       metricshelper.DatabaseMetrics{},
						SchemaDiff: make(map[string]*metricshelper.SchemaDiff),
						Explains:   make([]OutputDatabaseExplainQuery, 0),
						Storage: &OutputDatabaseStorageMetrics{
							InitialSize: 10,
							FinalSize:   20,
							SizeDelta:   10,
							WALDelta:    5,
						},
					},
				},
				Seeding: &OutputDatabaseSeeding{
					SuccessCount: 4,
					FailedCount:  1,
					Duration:     "500ms",
					Errors:       []string{"seed x failed"},
				},
				Warnings: warningStore,
				Errors:   []string{"top-level error"},
			},
		},
	}

	md, err := o.RenderMarkdown()
	require.NoError(t, err)

	assert.Contains(t, md, "## mydb")
	assert.Contains(t, md, "### Migration")
	assert.Contains(t, md, "3 migration(s) ran in 2s")
	assert.Contains(t, md, "running migration 1")

	assert.Contains(t, md, "### Seeding")
	assert.Contains(t, md, "4 succeeded")
	assert.Contains(t, md, "1 failed")
	assert.Contains(t, md, "seed x failed")

	assert.Contains(t, md, "[!WARNING]")
	assert.Contains(t, md, "top-level warn")
	assert.Contains(t, md, "[!CAUTION]")
	assert.Contains(t, md, "top-level error")
}

func TestRenderMarkdown_SkipsEmptyLogs(t *testing.T) {
	t.Parallel()
	o := PreviewOutput{
		Databases: map[string]PreviewOutputDatabase{
			"mydb": {
				Migration: &OutputDatabaseMigration{
					Count:    1,
					Duration: "1s",
					Errors:   []string{},
				},
			},
		},
	}
	md, err := o.RenderMarkdown()
	require.NoError(t, err)
	assert.NotContains(t, md, "<details>")
}

func TestOutputConstructorsInitializeSlices(t *testing.T) {
	t.Parallel()

	preview := NewPreviewOutputDatabase()
	assert.NotNil(t, preview.Warnings)
	assert.NotNil(t, preview.Errors)

	migration := NewMigrationOutput()
	assert.NotNil(t, migration.Errors)
	assert.Nil(t, migration.Logs)

	seeding := NewSeedingOutput()
	assert.NotNil(t, seeding.Errors)

	anon := NewAnonymizationOutput()
	assert.NotNil(t, anon.Errors)
}
