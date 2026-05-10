package output

import (
	"apercu-cli/helper/metrics"
	"apercu-cli/helper/pgproxy"
	"apercu-cli/helper/schema_diff"
	"bytes"
	"fmt"
	"slices"
	"text/template"
	"time"
)

type PreviewOutput struct {
	Databases map[string]PreviewOutputDatabase `yaml:"databases,omitempty" json:"databases,omitempty"`
}

type PreviewOutputDatabase struct {
	Migration *OutputDatabaseMigration `yaml:"migration,omitempty" json:"migration,omitempty"`
	Seeding   *OutputDatabaseSeeding   `yaml:"seeding,omitempty" json:"seeding,omitempty"`
	Warnings  []string                 `yaml:"warnings,omitempty" json:"warnings,omitempty"`
	Errors    []string                 `yaml:"errors,omitempty" json:"errors,omitempty"`
}

func NewPreviewOutputDatabase() *PreviewOutputDatabase {
	return &PreviewOutputDatabase{
		Warnings: make([]string, 0),
		Errors:   make([]string, 0),
	}
}

type OutputDatabaseMigration struct {
	ProdStats   map[string]map[string]metrics.TableStats `yaml:"prod_stats,omitempty" json:"prod_stats,omitempty"`
	Logs        *string                                  `yaml:"logs,omitempty" json:"logs,omitempty"`
	Count       int                                      `yaml:"count" json:"count"`
	Duration    string                                   `yaml:"duration" json:"duration"`
	SchemaDiff  map[string]*schema_diff.SchemaDiff       `yaml:"schema_diff,omitempty" json:"schema_diff,omitempty"`
	Stats       *OutputDatabaseMigrationStats            `yaml:"stats,omitempty" json:"stats,omitempty"`
	Explains    []OutputDatabaseMigrationExplainQuery    `yaml:"explains,omitempty" json:"explains,omitempty"`
	PgProxyLogs []pgproxy.QueryEvent                     `yaml:"pg_proxy_logs,omitempty" json:"pg_proxy_logs,omitempty"`
	Warnings    []string                                 `yaml:"warnings,omitempty" json:"warnings,omitempty"`
	Errors      []string                                 `yaml:"errors,omitempty" json:"errors,omitempty"`
}

type OutputDatabaseMigrationStats struct {
	InitialSize int64                                                             `yaml:"initial_size" json:"initial_size"`
	FinalSize   int64                                                             `yaml:"final_size" json:"final_size"`
	SizeDelta   int64                                                             `yaml:"size_delta" json:"size_delta"`
	WALDelta    int64                                                             `yaml:"wal_delta" json:"wal_delta"`
	LockStats   map[pgproxy.QueryLock]map[string]OutputDatabaseMigrationLockStats `yaml:"lock_stats,omitempty" json:"lock_stats,omitempty"`
}

type OutputDatabaseMigrationExplainQuery struct {
	File             string                                  `yaml:"file" json:"file"`
	Query            string                                  `yaml:"query" json:"query"`
	Warnings         []string                                `yaml:"warnings,omitempty" json:"warnings,omitempty"`
	MedianDelta      float64                                 `yaml:"median_delta" json:"median_delta"`
	Lo               float64                                 `yaml:"lo" json:"lo"`
	Hi               float64                                 `yaml:"hi" json:"hi"`
	PreMigrationRun  *OutputDatabaseMigrationExplainQueryRun `yaml:"pre_migration_run,omitempty" json:"pre_migration_run,omitempty"`
	PostMigrationRun *OutputDatabaseMigrationExplainQueryRun `yaml:"post_migration_run,omitempty" json:"post_migration_run,omitempty"`
}

type OutputDatabaseMigrationExplainQueryRun struct {
	ExecutionTimes []float64
	ExplainedQuery *metrics.ExplainResult `yaml:"explained_query,omitempty" json:"explained_query,omitempty"`
	Error          error                  `yaml:"error,omitempty" json:"error,omitempty"`
}

func NewOutputDatabaseMigrationStats(initialSize int64, finalSize int64, initialWalSize int64, finalWalSize int64, lockStats map[pgproxy.QueryLock]map[string]OutputDatabaseMigrationLockStats) *OutputDatabaseMigrationStats {
	return &OutputDatabaseMigrationStats{
		InitialSize: initialSize,
		FinalSize:   finalSize,
		SizeDelta:   finalSize - initialSize,
		WALDelta:    finalWalSize - initialWalSize,
		LockStats:   lockStats,
	}
}

func GetTableLockStats(queries []pgproxy.QueryEvent) map[pgproxy.QueryLock]map[string]OutputDatabaseMigrationLockStats {
	lockStats := make(map[pgproxy.QueryLock]map[string]OutputDatabaseMigrationLockStats)

	for _, query := range queries {
		if query.Stats.Lock == nil || query.Stats.Table == "" {
			continue
		}
		// Filter locks type
		switch *query.Stats.Lock {
		case pgproxy.QueryLockAccessExclusive:
		case pgproxy.QueryLockShareRowExclusive:
		case pgproxy.QueryLockShareUpdateExclusive:
		default:
			continue
		}

		// Get lock map
		l, ok := lockStats[*query.Stats.Lock]
		if !ok {
			l = make(map[string]OutputDatabaseMigrationLockStats)
		}

		// Get table map
		t, ok := l[query.Stats.Table]
		if !ok {
			t = OutputDatabaseMigrationLockStats{
				LockCount:     1,
				TotalDuration: query.Duration,
				MeanDuration:  query.Duration,
				MaxDuration:   query.Duration,
			}
		} else {
			t.LockCount++
			t.TotalDuration += query.Duration
			t.MeanDuration = t.TotalDuration / time.Duration(t.LockCount)
			if t.MaxDuration < query.Duration {
				t.MaxDuration = query.Duration
			}
		}

		l[query.Stats.Table] = t
		lockStats[*query.Stats.Lock] = l
	}

	return lockStats
}

type OutputDatabaseMigrationLockStats struct {
	LockCount     int64         `yaml:"lock_count" json:"lock_count"`
	TotalDuration time.Duration `yaml:"total_duration" json:"total_duration"`
	MeanDuration  time.Duration `yaml:"mean_duration" json:"mean_duration"`
	MaxDuration   time.Duration `yaml:"max_duration" json:"max_duration"`
}

func NewMigrationOutput() *OutputDatabaseMigration {
	return &OutputDatabaseMigration{
		Logs:     nil,
		Count:    0,
		Duration: "",
		Warnings: make([]string, 0),
		Errors:   make([]string, 0),
	}
}

type OutputDatabaseSeeding struct {
	Logs         *string  `yaml:"logs,omitempty" json:"logs,omitempty"`
	SuccessCount int      `yaml:"success_count" json:"success_count"`
	FailedCount  int      `yaml:"failed_count" json:"failed_count"`
	Duration     string   `yaml:"duration" json:"duration"`
	Warnings     []string `yaml:"warnings,omitempty" json:"warnings,omitempty"`
	Errors       []string `yaml:"errors,omitempty" json:"errors,omitempty"`
}

func NewSeedingOutput() *OutputDatabaseSeeding {
	return &OutputDatabaseSeeding{
		Logs:         nil,
		SuccessCount: 0,
		FailedCount:  0,
		Duration:     "",
		Warnings:     make([]string, 0),
		Errors:       make([]string, 0),
	}
}

type OutputDatabaseAnonymization struct {
	Logs     *string  `yaml:"logs,omitempty" json:"logs,omitempty"`
	Duration string   `yaml:"duration" json:"duration"`
	Warnings []string `yaml:"warnings,omitempty" json:"warnings,omitempty"`
	Errors   []string `yaml:"errors,omitempty" json:"errors,omitempty"`
}

func NewAnonymizationOutput() *OutputDatabaseAnonymization {
	return &OutputDatabaseAnonymization{
		Logs:     nil,
		Duration: "",
		Warnings: make([]string, 0),
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
	"print_schemas_diff": func(schemasDiff map[string]*schema_diff.SchemaDiff) string {
		text := schema_diff.GetSchemasDiffText(schemasDiff)
		if text == nil {
			return ""
		}
		return *text
	},
	"print_explain": func(e []OutputDatabaseMigrationExplainQuery) string {
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
					outputStr += fmt.Sprintf("> - %s", warning)
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

### Migration

{{$db.Migration.Count}} migration(s) ran in {{$db.Migration.Duration}}
{{- if $db.Migration.Warnings}}

> [!WARNING]
{{range $db.Migration.Warnings}}> - {{.}}
{{end}}
{{- end}}
{{- if $db.Migration.Errors}}

> [!CAUTION]
{{range $db.Migration.Errors}}> - {{.}}
{{end}}
{{- end}}
{{- if $db.Migration.SchemaDiff }}

<details>
<summary><b>Schema Diff</b></summary>

` + "```diff" + `
{{print_schemas_diff $db.Migration.SchemaDiff}}
` + "```" + `

</details>
{{- end}}
{{- if $db.Migration.Stats}}

<details>
<summary><b>Stats</b></summary>

` + "```" + `
--- Size detail ---
Before Migration Size: {{size_pretty $db.Migration.Stats.InitialSize}}
After Migration Size: {{size_pretty $db.Migration.Stats.FinalSize}}
Size Delta: {{size_pretty $db.Migration.Stats.SizeDelta}}
--- WAL Detail ---
WAL Size Delta: {{size_pretty $db.Migration.Stats.WALDelta}}
{{- if $db.Migration.Stats.LockStats}}
--- Locks detail ---
{{range $lockType, $tables := $db.Migration.Stats.LockStats}}{{$lockType}}:
{{range $table, $stats := $tables}}{{$table}} | count {{$stats.LockCount}} | mean {{$stats.MeanDuration}} | max {{$stats.MaxDuration}}
{{end}}{{end}}
{{- end}}
` + "```" + `

</details>

{{- end }}
{{- if $db.Migration.Explains}}
<details>
<summary><b>Explained Queries</b></summary>

{{print_explain $db.Migration.Explains}}

</details>
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
{{- if $db.Seeding.Warnings}}

> [!WARNING]
{{range $db.Seeding.Warnings}}> - {{.}}
{{end}}
{{- end}}
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
{{- end}}
`))

func (o *PreviewOutput) RenderMarkdown() (string, error) {
	var buf bytes.Buffer
	if err := markdownTmpl.Execute(&buf, o); err != nil {
		return "", fmt.Errorf("failed to render markdown: %w", err)
	}
	return buf.String(), nil
}
