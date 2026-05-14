package output

import (
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"bytes"
	"fmt"
	"slices"
	"text/template"
)

type PreviewOutput struct {
	Databases map[string]PreviewOutputDatabase `yaml:"databases,omitempty" json:"databases,omitempty"`
}

type PreviewOutputDatabase struct {
	Migration *OutputDatabaseMigration `yaml:"migration,omitempty" json:"migration,omitempty"`
	Seeding   *OutputDatabaseSeeding   `yaml:"seeding,omitempty" json:"seeding,omitempty"`
	Warnings  []warning.Warning        `yaml:"warnings,omitempty" json:"warnings,omitempty"`
	Errors    []string                 `yaml:"errors,omitempty" json:"errors,omitempty"`
}

func NewPreviewOutputDatabase() *PreviewOutputDatabase {
	return &PreviewOutputDatabase{
		Warnings: make([]warning.Warning, 0),
		Errors:   make([]string, 0),
	}
}

type OutputDatabaseMigration struct {
	Logs        *string                `yaml:"logs,omitempty" json:"logs,omitempty"`
	Count       int                    `yaml:"count" json:"count"`
	Duration    string                 `yaml:"duration" json:"duration"`
	PgProxyLogs string                 `yaml:"pg_proxy_logs,omitempty" json:"pg_proxy_logs,omitempty"`
	Errors      []string               `yaml:"errors,omitempty" json:"errors,omitempty"`
	Metrics     *OutputDatabaseMetrics `yaml:"metrics,omitempty" json:"metrics,omitempty"`
}

func NewMigrationOutput() *OutputDatabaseMigration {
	return &OutputDatabaseMigration{
		Logs:     nil,
		Count:    0,
		Duration: "",
		Errors:   make([]string, 0),
	}
}

type OutputDatabaseMetrics struct {
	Prod       map[string]map[string]metricshelper.TableMetrics                 `yaml:"prod,omitempty" json:"prod,omitempty"`
	SchemaDiff map[string]*metricshelper.SchemaDiff                             `yaml:"schema_diff,omitempty" json:"schema_diff,omitempty"`
	Locks      map[metricshelper.QueryLock]map[string]metricshelper.LockMetrics `yaml:"locks,omitempty" json:"locks,omitempty"`
	Explains   []OutputDatabaseExplainQuery                                     `yaml:"explains,omitempty" json:"explains,omitempty"`
	Storage    *OutputDatabaseStorageMetrics                                    `yaml:"storage,omitempty" json:"storage,omitempty"`
}

func NewOutputDatabaseMetrics() *OutputDatabaseMetrics {
	return &OutputDatabaseMetrics{
		Prod:       make(map[string]map[string]metricshelper.TableMetrics),
		SchemaDiff: make(map[string]*metricshelper.SchemaDiff),
		Locks:      make(map[metricshelper.QueryLock]map[string]metricshelper.LockMetrics),
		Explains:   make([]OutputDatabaseExplainQuery, 0),
	}
}

type OutputDatabaseStorageMetrics struct {
	InitialSize int64  `yaml:"initial_size" json:"initial_size"`
	FinalSize   int64  `yaml:"final_size" json:"final_size"`
	SizeDelta   int64  `yaml:"size_delta" json:"size_delta"`
	WALDelta    uint64 `yaml:"wal_delta" json:"wal_delta"`
}

type OutputDatabaseExplainQuery struct {
	File             string                                  `yaml:"file" json:"file"`
	Query            string                                  `yaml:"query" json:"query"`
	Warnings         []warning.Warning                       `yaml:"warnings,omitempty" json:"warnings,omitempty"`
	MedianDelta      float64                                 `yaml:"median_delta" json:"median_delta"`
	Lo               float64                                 `yaml:"lo" json:"lo"`
	Hi               float64                                 `yaml:"hi" json:"hi"`
	PreMigrationRun  *OutputDatabaseMigrationExplainQueryRun `yaml:"pre_migration_run,omitempty" json:"pre_migration_run,omitempty"`
	PostMigrationRun *OutputDatabaseMigrationExplainQueryRun `yaml:"post_migration_run,omitempty" json:"post_migration_run,omitempty"`
}

type OutputDatabaseMigrationExplainQueryRun struct {
	ExecutionTimes []float64
	ExplainedQuery *metricshelper.ExplainResult `yaml:"explained_query,omitempty" json:"explained_query,omitempty"`
	Error          error                        `yaml:"error,omitempty" json:"error,omitempty"`
}

type OutputDatabaseSeeding struct {
	Logs         *string  `yaml:"logs,omitempty" json:"logs,omitempty"`
	SuccessCount int      `yaml:"success_count" json:"success_count"`
	FailedCount  int      `yaml:"failed_count" json:"failed_count"`
	Duration     string   `yaml:"duration" json:"duration"`
	Errors       []string `yaml:"errors,omitempty" json:"errors,omitempty"`
}

func NewSeedingOutput() *OutputDatabaseSeeding {
	return &OutputDatabaseSeeding{
		Logs:         nil,
		SuccessCount: 0,
		FailedCount:  0,
		Duration:     "",
		Errors:       make([]string, 0),
	}
}

type OutputDatabaseAnonymization struct {
	Logs     *string  `yaml:"logs,omitempty" json:"logs,omitempty"`
	Duration string   `yaml:"duration" json:"duration"`
	Errors   []string `yaml:"errors,omitempty" json:"errors,omitempty"`
}

func NewAnonymizationOutput() *OutputDatabaseAnonymization {
	return &OutputDatabaseAnonymization{
		Logs:     nil,
		Duration: "",
		Errors:   make([]string, 0),
	}
}

var templateFuncs = template.FuncMap{
	"deref": func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	},
	"size_pretty": func(i int64) string {
		if i < 1024 {
			return fmt.Sprintf("%d B", i)
		} else if i < 1024*1024 {
			return fmt.Sprintf("%.2f KB", float64(i)/1024)
		} else if i < 1024*1024*1024 {
			return fmt.Sprintf("%.2f MB", float64(i)/1024/1024)
		} else if i < 1024*1024*1024*1024 {
			return fmt.Sprintf("%.2f GB", float64(i)/1024/1024/1024)
		}
		return fmt.Sprintf("%.2f TB", float64(i)/1024/1024/1024/1024)
	},
	"usize_pretty": func(i uint64) string {
		if i < 1024 {
			return fmt.Sprintf("%d B", i)
		} else if i < 1024*1024 {
			return fmt.Sprintf("%.2f KB", float64(i)/1024)
		} else if i < 1024*1024*1024 {
			return fmt.Sprintf("%.2f MB", float64(i)/1024/1024)
		} else if i < 1024*1024*1024*1024 {
			return fmt.Sprintf("%.2f GB", float64(i)/1024/1024/1024)
		}
		return fmt.Sprintf("%.2f TB", float64(i)/1024/1024/1024/1024)
	},
	"print_schemas_diff": func(schemasDiff map[string]*metricshelper.SchemaDiff) string {
		text := metricshelper.GetSchemasDiffText(schemasDiff)
		if text == nil {
			return ""
		}
		return *text
	},
	"print_explain": func(e []OutputDatabaseExplainQuery) string {
		displayedFile := make([]string, 0)
		var outputStr string

		for _, explain := range e {
			if slices.Contains(displayedFile, explain.File) {
				continue
			}
			currentFile := explain.File
			displayedFile = append(displayedFile, currentFile)
			outputStr += "<details>\n"

			// Check if file contain warnings
			bWarning := false
			for _, explain := range e {
				if explain.File == currentFile && len(explain.Warnings) > 0 {
					bWarning = true
					break
				}
			}
			if bWarning {
				outputStr += fmt.Sprintf("<summary><span style=\"color:orange\"><b>%s</b></span></summary>\n", currentFile)
			} else {
				outputStr += fmt.Sprintf("<summary><b>%s</b></summary>\n\n", currentFile)
			}

			for _, explain := range e {
				if explain.File != currentFile {
					continue
				}

				// Display details header
				outputStr += "<details>\n"
				var query string
				if len(explain.Query) > 120 {
					query = explain.Query[:120] + "..."
				} else {
					query = explain.Query
				}
				if len(explain.Warnings) > 0 {
					outputStr += fmt.Sprintf("<summary><span style=\"color:orange\"><b>%s</b></span></summary>\n", query)
				} else {
					outputStr += fmt.Sprintf("<summary><b>%s</b></summary>\n\n", query)
				}

				// Display warnings
				if len(explain.Warnings) > 0 {
					outputStr += "> [!WARNING]\n"
				}
				for _, warning := range explain.Warnings {
					outputStr += fmt.Sprintf("> - %s", warning.GetWarningText())
				}

				// Display delta
				if explain.MedianDelta != 0 && explain.Hi != 0 && explain.Lo != 0 {
					outputStr += fmt.Sprintf("median %+.1f (95%% CI: %+.1f to %+.1f)", explain.MedianDelta*100, explain.Lo*100, explain.Hi*100)
				}

				// Display explained query
				if explain.PreMigrationRun != nil {
					outputStr += fmt.Sprintf("**Pre migration:**\n```\n")
					if explain.PreMigrationRun.Error != nil {
						outputStr += fmt.Sprintf("ERROR: %s\n", explain.PreMigrationRun.Error)
					} else if explain.PreMigrationRun.ExplainedQuery != nil {
						outputStr += explain.PreMigrationRun.ExplainedQuery.String()
					}
					outputStr += "```\n"
				}
				if explain.PostMigrationRun != nil {
					outputStr += fmt.Sprintf("**Post migration:**\n```\n")
					if explain.PostMigrationRun.Error != nil {
						outputStr += fmt.Sprintf("ERROR: %s\n", explain.PostMigrationRun.Error)
					} else if explain.PostMigrationRun.ExplainedQuery != nil {
						outputStr += explain.PostMigrationRun.ExplainedQuery.String()
					}
					outputStr += "```\n"
				}

				outputStr += "\n</details>\n"
			}

			outputStr += "\n</details>\n"
		}
		return outputStr
	},
}

var markdownTmpl = template.Must(template.New("markdown").Funcs(templateFuncs).Parse(
	`# Apercu Output
{{range $name, $db := .Databases}}
## {{$name}}
{{- if $db.Migration}}
{{- if $db.Warnings}}

> [!WARNING]
{{range $db.Warnings}}> - {{.}}
{{end}}
{{- end}}
{{- if $db.Errors}}

> [!CAUTION]
{{range $db.Errors}}> - {{.}}
{{end}}
{{- end}}

### Migration

{{$db.Migration.Count}} migration(s) ran in {{$db.Migration.Duration}}
{{- if $db.Migration.Errors}}

> [!CAUTION]
{{range $db.Migration.Errors}}> - {{.}}
{{end}}
{{- end}}
{{- if $db.Migration.Metrics}}
{{- if $db.Migration.Metrics.SchemaDiff }}

<details>
<summary><b>Schema Diff</b></summary>

` + "```diff" + `
{{print_schemas_diff $db.Migration.Metrics.SchemaDiff}}
` + "```" + `

</details>
{{- end}}

<details>
<summary><b>Stats</b></summary>

` + "```" + `
{{- if $db.Migration.Metrics.Storage}}
--- Size detail ---
Before Migration Size: {{size_pretty $db.Migration.Metrics.Storage.InitialSize}}
After Migration Size: {{size_pretty $db.Migration.Metrics.Storage.FinalSize}}
Size Delta: {{size_pretty $db.Migration.Metrics.Storage.SizeDelta}}
--- WAL Detail ---
WAL Size Delta: {{usize_pretty $db.Migration.Metrics.Storage.WALDelta}}
{{- end}}
{{- if $db.Migration.Metrics.Locks}}
--- Locks detail ---
{{range $lockType, $tables := $db.Migration.Metrics.Locks}}{{$lockType}}:
{{range $table, $stats := $tables}}{{$table}} | count {{$stats.LockCount}} | mean {{$stats.MeanDuration}} | max {{$stats.MaxDuration}}
{{end}}{{end}}
{{- end}}
` + "```" + `

</details>


{{- if $db.Migration.Metrics.Explains}}
<details>
<summary><b>Explained Queries</b></summary>

{{print_explain $db.Migration.Metrics.Explains}}

</details>
{{- end }}
{{end}}

{{- if $db.Migration.Logs}}

<details>
<summary><b>Logs</b></summary>

` + "```" + `
{{deref $db.Migration.Logs}}
` + "```" + `

</details>
{{- end}}
{{- end}}
{{- if $db.Seeding}}

### Seeding

{{$db.Seeding.SuccessCount}} succeeded · {{$db.Seeding.FailedCount}} failed · {{$db.Seeding.Duration}}
{{- if $db.Seeding.Errors}}

> [!CAUTION]
{{range $db.Seeding.Errors}}> - {{.}}
{{end}}
{{- end}}
{{- if $db.Seeding.Logs}}

<details>
<summary><b>Logs</b></summary>

` + "```" + `
{{deref $db.Seeding.Logs}}
` + "```" + `

</details>
{{- end}}
{{- end}}
{{- end}}
`))

func (o *PreviewOutput) RenderMarkdown() (string, error) {
	var buf bytes.Buffer
	if err := markdownTmpl.Execute(&buf, o); err != nil {
		return "", fmt.Errorf("failed to render markdown: %w", err)
	}
	return buf.String(), nil
}
