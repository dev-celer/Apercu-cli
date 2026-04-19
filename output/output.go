package output

import (
	"bytes"
	"fmt"
	"text/template"
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
	Logs       *string  `yaml:"logs,omitempty" json:"logs,omitempty"`
	Count      int      `yaml:"count" json:"count"`
	Duration   string   `yaml:"duration" json:"duration"`
	SchemaDiff *string  `yaml:"schema_diff,omitempty" json:"schema_diff,omitempty"`
	Warnings   []string `yaml:"warnings,omitempty" json:"warnings,omitempty"`
	Errors     []string `yaml:"errors,omitempty" json:"errors,omitempty"`
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
