package output

import (
	"apercu-cli/helper"
	formathelper "apercu-cli/helper/format"
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

type PreviewOutput struct {
	Decision  OutputDecision          `yaml:"decision" json:"decision"`
	Migration OutputDatabaseMigration `yaml:"migration,omitempty" json:"migration,omitempty"`
	Seeding   OutputDatabaseSeeding   `yaml:"seeding,omitempty" json:"seeding,omitempty"`
	Warnings  *warning.WarningStore   `yaml:"warnings,omitempty" json:"warnings,omitempty"`
}

func NewPreviewOutput() *PreviewOutput {
	return &PreviewOutput{
		Warnings: warning.NewWarningStore(),
	}
}

type OutputDecision string

const (
	OutputDecisionSafe   OutputDecision = "safe"
	OutputDecisionReview OutputDecision = "review"
	OutputDecisionUnsafe OutputDecision = "unsafe"
)

type OutputDatabaseMigration struct {
	Logs     *string               `yaml:"logs,omitempty" json:"logs,omitempty"`
	Count    int                   `yaml:"count" json:"count"`
	Duration string                `yaml:"duration" json:"duration"`
	Metrics  OutputDatabaseMetrics `yaml:"metrics,omitempty" json:"metrics,omitempty"`
	// Temporary storage of the pg proxy logs, not produce in any output objects
	PgProxyLogs string
}

func NewMigrationOutput() OutputDatabaseMigration {
	return OutputDatabaseMigration{
		Logs:     nil,
		Count:    0,
		Duration: "",
	}
}

type OutputDatabaseMetrics struct {
	Prod           metricshelper.DatabaseMetrics           `yaml:"prod,omitempty" json:"prod,omitempty"`
	SchemaDiff     map[string]*metricshelper.SchemaDiff    `yaml:"schema_diff,omitempty" json:"schema_diff,omitempty"`
	RewrittenTable []helper.FullTableName                  `yaml:"rewritten_table,omitempty" json:"rewritten_table,omitempty"`
	Explains       map[string][]OutputDatabaseExplainQuery `yaml:"explains,omitempty" json:"explains,omitempty"`
	Storage        OutputDatabaseStorageMetrics            `yaml:"storage,omitempty" json:"storage,omitempty"`
}

func NewOutputDatabaseMetrics() *OutputDatabaseMetrics {
	return &OutputDatabaseMetrics{
		SchemaDiff:     make(map[string]*metricshelper.SchemaDiff),
		RewrittenTable: make([]helper.FullTableName, 0),
		Explains:       make(map[string][]OutputDatabaseExplainQuery),
	}
}

type OutputDatabaseStorageMetrics struct {
	InitialSize int64 `yaml:"initial_size" json:"initial_size"`
	FinalSize   int64 `yaml:"final_size" json:"final_size"`
	WALDelta    int64 `yaml:"wal_delta" json:"wal_delta"`
}

type OutputDatabaseExplainQuery struct {
	Query            string                                  `yaml:"query" json:"query"`
	PreMigrationRun  *OutputDatabaseMigrationExplainQueryRun `yaml:"pre_migration_run,omitempty" json:"pre_migration_run,omitempty"`
	PostMigrationRun *OutputDatabaseMigrationExplainQueryRun `yaml:"post_migration_run,omitempty" json:"post_migration_run,omitempty"`
	Regression       bool                                    `yaml:"regression" json:"regression"`
}

type OutputDatabaseMigrationExplainQueryRun struct {
	ExplainedQuery *metricshelper.ExplainResult `yaml:"explained_query,omitempty" json:"explained_query,omitempty"`
	Error          error                        `yaml:"error,omitempty" json:"error,omitempty"`
}

type OutputDatabaseSeeding struct {
	Logs         *string `yaml:"logs,omitempty" json:"logs,omitempty"`
	SuccessCount int     `yaml:"success_count" json:"success_count"`
	FailedCount  int     `yaml:"failed_count" json:"failed_count"`
	Duration     string  `yaml:"duration" json:"duration"`
}

func NewSeedingOutput() OutputDatabaseSeeding {
	return OutputDatabaseSeeding{
		Logs:         nil,
		SuccessCount: 0,
		FailedCount:  0,
		Duration:     "",
	}
}

type OutputDatabaseAnonymization struct {
	Logs     *string `yaml:"logs,omitempty" json:"logs,omitempty"`
	Duration string  `yaml:"duration" json:"duration"`
}

func NewAnonymizationOutput() *OutputDatabaseAnonymization {
	return &OutputDatabaseAnonymization{
		Logs:     nil,
		Duration: "",
	}
}

var templateFuncs = template.FuncMap{
	"deref": func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	},
	"get_decision_badge": func(decision OutputDecision) string {
		switch decision {
		case OutputDecisionSafe:
			return "safe_to_merge-green?style=for-the-badge"
		case OutputDecisionReview:
			return "review_recommended-orange?style=for-the-badge"
		case OutputDecisionUnsafe:
			return "do_not_merge-red?style=for-the-badge"
		}
		return ""
	},
	"get_migration_summary": func(migration OutputDatabaseMigration, warnings *warning.WarningStore) string {
		var s string
		if warnings != nil && len(warnings.GetWarnings()) > 0 {
			s = fmt.Sprintf("%d warning(s) generated", len(warnings.GetWarnings()))
			if migration.Count != 0 {
				s += fmt.Sprintf(" across %d migration(s)", migration.Count)
			}
		} else if migration.Count != 0 {
			s = fmt.Sprintf("%d migration(s) applied", migration.Count)
		}
		return s
	},
	"has_warnings": func(warnings *warning.WarningStore) bool {
		if warnings == nil {
			return false
		}
		return len(warnings.GetWarnings()) > 0
	},
	"get_warnings": func(warnings *warning.WarningStore) []warning.Warning {
		if warnings == nil {
			return nil
		}
		return warnings.GetWarnings()
	},
	"get_warning_severity": func(w warning.Warning) string {
		switch w.GetLevel() {
		case warning.WarningLevelLow:
			return "🔵 Low"
		case warning.WarningLevelMedium:
			return "🟠 Medium"
		case warning.WarningLevelHigh:
			return "🔴 High"
		}
		return ""
	},
	"get_warning_code": func(w warning.Warning) string {
		return string(w.GetCode())
	},
	"get_warning_description": func(w warning.Warning) string {
		return w.GetText()
	},
	"size_pretty": func(i int64) string {
		return formathelper.BytesSizePretty(i)
	},
	"sub": func(a, b int64) int64 {
		return a - b
	},
	"usize_pretty": func(i uint64) string {
		return formathelper.UBytesSizePretty(i)
	},
	"get_schema_diff_resume": func(diff map[string]*metricshelper.SchemaDiff) string {
		var created, deleted, updated int
		for _, i := range diff {
			if i == nil {
				continue
			}
			created += len(i.CreatedTables)
			deleted += len(i.DeletedTables)
			updated += len(i.UpdatedTables)
		}
		var s []string
		if created != 0 {
			s = append(s, fmt.Sprintf("%d created", created))
		}
		if deleted != 0 {
			s = append(s, fmt.Sprintf("%d deleted", deleted))
		}
		if updated != 0 {
			s = append(s, fmt.Sprintf("%d updated", updated))
		}
		return strings.Join(s, ", ")
	},
	"print_schema_diff": func(schemasDiff map[string]*metricshelper.SchemaDiff) string {
		text := metricshelper.GetSchemasDiffText(schemasDiff)
		if text == nil {
			return ""
		}
		return *text
	},
	"get_explain_resume": func(e map[string][]OutputDatabaseExplainQuery) string {
		var regression int
		for _, i := range e {
			for _, j := range i {
				if j.Regression {
					regression++
				}
			}
		}

		return fmt.Sprintf("%d total - %d regression", len(e), regression)
	},
	"explain_count_regression": func(e []OutputDatabaseExplainQuery) int {
		var regression int
		for _, i := range e {
			if i.Regression {
				regression++
			}
		}
		return regression
	},
	"get_seeding_resume": func(s OutputDatabaseSeeding) string {
		return fmt.Sprintf("%d succeeded - %d failed", s.SuccessCount, s.FailedCount)
	},
}

var markdownTmpl = template.Must(template.New("markdown").Funcs(templateFuncs).Parse(
	`## Apercu migration report ![status](https://img.shields.io/badge/{{get_decision_badge .Decision}}

> **Migration took {{.Migration.Duration}}**
>
{{- if get_migration_summary .Migration .Warnings}}
> **{{get_migration_summary .Migration .Warnings}}**
>
{{- end}}
> **{{size_pretty .Migration.Metrics.Storage.WALDelta}} WAL generated**
>
> **{{size_pretty (sub .Migration.Metrics.Storage.FinalSize .Migration.Metrics.Storage.InitialSize)}} database size increase**
{{- if has_warnings .Warnings}}

|Severity|Code|Description|
|---|---|---|
{{range $i, $warning := get_warnings .Warnings}}
|{{get_warning_severity $warning}}|{{get_warning_code $warning}}|{{get_warning_description $warning}}|
{{end}}

<details>
<summary><b>⚠ Warning details</b></summary>
</details>
{{- end}}
{{- if gt (len .Migration.Metrics.SchemaDiff) 0}}

<details>
<summary><b>🗄️ Schema diff</b><sub>{{get_schema_diff_resume .Migration.Metrics.SchemaDiff}}</sub></summary>

` + "```diff" + `
{{print_schema_diff .Migration.Metrics.SchemaDiff}}
` + "```" + `

</details>
{{- end}}
{{- if gt (len .Migration.Metrics.Explains) 0}}

<details>
<summary><b>Explained Queries</b><sub>{{get_explain_resume .Migration.Metrics.Explains}}</sub></summary>

{{range $source, $explains := .Migration.Metrics.Explains}}

<details>
{{- if gt (explain_count_regression $explains) 0 }}
<summary><span style="color:orange"><b>{{$source}}</b><sub>{{explain_count_regression $explains}}</sub></span></summary>
{{- else}}
<summary><b>{{$source}}</b></summary>
{{- end}}

{{range $i, $explain := $explains}}
` + "```sql" + `
{{$explain.Query}}
` + "```" + `

{{- if $explain.PreMigrationRun.ExplainedQuery}}
**Pre migration:**
` + "```" + `
{{$explain.PreMigrationRun.ExplainedQuery}}
` + "```" + `
{{- end}}
{{- if $explain.PostMigrationRun.ExplainedQuery}}
**Post migration:**
` + "```" + `
{{$explain.PostMigrationRun.ExplainedQuery}}
` + "```" + `
{{- end}}

{{end}}
</details>
{{end}}
</details>
{{- end}}

{{- if .Migration.Logs}}

<details>
<summary><b>🗃️ Migration logs</b></summary>

` + "```" + `
{{.Migration.Logs}}
` + "```" + `

</details>
{{- end}}
{{- if .Seeding.Logs}}

<details>
<summary><b>🌱 Seeding logs</b><sub>{{get_seeding_resume .Seeding}}</sub></summary>

` + "```" + `
{{.Seeding.Logs}}
` + "```" + `

</details>
{{- end}}
`))

func (o *PreviewOutput) RenderMarkdown() (string, error) {
	var buf bytes.Buffer
	if err := markdownTmpl.Execute(&buf, o); err != nil {
		return "", fmt.Errorf("failed to render markdown: %w", err)
	}
	return buf.String(), nil
}
