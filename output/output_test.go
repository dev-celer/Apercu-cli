package output

import (
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockedWarning struct {
	desc string
}

const (
	MockedCode warning.Code = "MOCK_WARN"
)

func (w mockedWarning) GetWarningText() string {
	return w.desc
}

func (w mockedWarning) GetWarningLevel() warning.Level {
	return warning.WarningLevelMedium
}

func (w mockedWarning) GetWarningCode() warning.Code {
	return MockedCode
}

func (w mockedWarning) PrintWarning() {}

func newMockedWarning(desc string) mockedWarning {
	return mockedWarning{desc: desc}
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
	o := PreviewOutput{
		Databases: map[string]PreviewOutputDatabase{
			"mydb": {
				Migration: &OutputDatabaseMigration{
					Count:    3,
					Duration: "2s",
					Logs:     strPtr("running migration 1\nrunning migration 2"),
					Errors:   []string{},
					Metrics: &OutputDatabaseMetrics{
						Prod:       nil,
						SchemaDiff: make(map[string]*metricshelper.SchemaDiff),
						Locks:      map[metricshelper.QueryLock]map[string]metricshelper.LockMetrics{},
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
				Warnings: []warning.Warning{newMockedWarning("test")},
				Errors:   []string{"top-level error"},
			},
		},
	}

	md, err := o.RenderMarkdown()
	require.NoError(t, err)

	assert.Contains(t, md, "## mydb")
	assert.Contains(t, md, "### Migration")
	assert.Contains(t, md, "3 migration(s) ran in 2s")
	assert.Contains(t, md, "deprecated column")
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
