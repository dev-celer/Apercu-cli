package output

import (
	"apercu-cli/helper"
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

func (w *mockedWarning) GetQuery() *helper.QueryWithAffectedTables { return nil }

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
	o := NewPreviewOutput()
	md, err := o.RenderMarkdown()
	require.NoError(t, err)
	assert.Contains(t, md, "## Apercu migration report")
}

func TestRenderMarkdown_MigrationAndSeeding(t *testing.T) {
	t.Parallel()
	warningStore := warning.NewWarningStore()
	warningStore.AddWarning(newMockedWarning("top-level warn", false, nil))
	o := PreviewOutput{
		Migration: &OutputDatabaseMigration{
			Count:    3,
			Duration: "2s",
			Logs:     strPtr("running migration 1\nrunning migration 2"),
			Metrics: &OutputDatabaseMetrics{
				Prod:       metricshelper.DatabaseMetrics{},
				SchemaDiff: make(map[string]*metricshelper.SchemaDiff),
				Explains:   make(map[string][]OutputDatabaseExplainQuery),
				Storage: OutputDatabaseStorageMetrics{
					InitialSize: 10,
					FinalSize:   20,
					WALDelta:    5,
				},
			},
		},
		Seeding: &OutputDatabaseSeeding{
			Logs:         new("seeding..."),
			SuccessCount: 4,
			FailedCount:  1,
			Duration:     "500ms",
		},
		Warnings: warningStore,
	}

	md, err := o.RenderMarkdown()
	require.NoError(t, err)

	assert.Contains(t, md, "Migration took 2s")
	assert.Contains(t, md, "1 warning(s) generated across 3 migration(s)")

	assert.Contains(t, md, "4 succeeded - 1 failed")

	assert.Contains(t, md, "top-level warn")
}

func TestRenderMarkdown_SkipsEmptyLogs(t *testing.T) {
	t.Parallel()
	o := NewPreviewOutput()
	o.Migration = NewMigrationOutput()
	md, err := o.RenderMarkdown()
	require.NoError(t, err)
	assert.NotContains(t, md, "<details>")
}
