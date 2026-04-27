package output

import (
	"apercu-cli/helper/pgproxy"
	"bytes"
	"fmt"
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
	Logs        *string                       `yaml:"logs,omitempty" json:"logs,omitempty"`
	Count       int                           `yaml:"count" json:"count"`
	Duration    string                        `yaml:"duration" json:"duration"`
	SchemaDiff  *string                       `yaml:"schema_diff,omitempty" json:"schema_diff,omitempty"`
	Stats       *OutputDatabaseMigrationStats `yaml:"stats,omitempty" json:"stats,omitempty"`
	PgProxyLogs []pgproxy.QueryEvent          `yaml:"pg_proxy_logs,omitempty" json:"pg_proxy_logs,omitempty"`
	Warnings    []string                      `yaml:"warnings,omitempty" json:"warnings,omitempty"`
	Errors      []string                      `yaml:"errors,omitempty" json:"errors,omitempty"`
}

type OutputDatabaseMigrationStats struct {
	InitialSize int64                                                             `yaml:"initial_size" json:"initial_size"`
	FinalSize   int64                                                             `yaml:"final_size" json:"final_size"`
	SizeDelta   int64                                                             `yaml:"size_delta" json:"size_delta"`
	LockStats   map[pgproxy.QueryLock]map[string]OutputDatabaseMigrationLockStats `yaml:"lock_stats,omitempty" json:"lock_stats,omitempty"`
}

func NewOutputDatabaseMigrationStats(initialSize int64, finalSize int64, lockStats map[pgproxy.QueryLock]map[string]OutputDatabaseMigrationLockStats) *OutputDatabaseMigrationStats {
	return &OutputDatabaseMigrationStats{
		InitialSize: initialSize,
		FinalSize:   finalSize,
		SizeDelta:   finalSize - initialSize,
		LockStats:   lockStats,
	}
}

func GetTableLockStats(queries []pgproxy.QueryEvent) map[pgproxy.QueryLock]map[string]OutputDatabaseMigrationLockStats {
	lockStats := make(map[pgproxy.QueryLock]map[string]OutputDatabaseMigrationLockStats)

	for _, query := range queries {
		if query.Stats.Lock == nil || query.Stats.Table == "" {
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
<summary>Schema Diff</summary>

` + "```diff" + `
{{deref $db.Migration.SchemaDiff}}
` + "```" + `

</details>
{{- end}}
{{- if $db.Migration.Stats}}

<details>
<summary>Stats</summary>

` + "```" + `
Before Migration Size: {{size_pretty $db.Migration.Stats.InitialSize}}
After Migration Size: {{size_pretty $db.Migration.Stats.FinalSize}}
Size Delta: {{size_pretty $db.Migration.Stats.SizeDelta}}
` + "```" + `

</details>

{{- end}}
{{- if $db.Migration.Logs}}

<details>
<summary>Logs</summary>

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
<summary>Logs</summary>

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
