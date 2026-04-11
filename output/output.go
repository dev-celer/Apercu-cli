package output

import (
	"bytes"
	"fmt"
	"text/template"
)

type Output struct {
	Databases map[string]OutputDatabase `yaml:"databases,omitempty" json:"databases,omitempty"`
}

type OutputDatabase struct {
	Migration *OutputDatabaseMigration `yaml:"migration,omitempty" json:"migration,omitempty"`
	Seeding   *OutputDatabaseSeeding   `yaml:"seeding,omitempty" json:"seeding,omitempty"`
	Warnings  []string                 `yaml:"warnings,omitempty" json:"warnings,omitempty"`
	Errors    []string                 `yaml:"errors,omitempty" json:"errors,omitempty"`
}

func NewOutputDatabase() *OutputDatabase {
	return &OutputDatabase{
		Warnings: make([]string, 0),
		Errors:   make([]string, 0),
	}
}

type OutputDatabaseMigration struct {
	Logs     *string  `yaml:"logs,omitempty" json:"logs,omitempty"`
	Count    int      `yaml:"count" json:"count"`
	Duration string   `yaml:"duration" json:"duration"`
	Warnings []string `yaml:"warnings,omitempty" json:"warnings,omitempty"`
	Errors   []string `yaml:"errors,omitempty" json:"errors,omitempty"`
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
## Database: {{$name}}
{{if $db.Migration}}
### Migration
| Field | Value |
|-------|-------|
| Duration | {{$db.Migration.Duration}} |
| Count | {{$db.Migration.Count}} |
{{if $db.Migration.Warnings}}
**Warnings:**
{{range $db.Migration.Warnings}}- {{.}}
{{end}}{{end}}
{{- if $db.Migration.Errors}}
**Errors:**
{{range $db.Migration.Errors}}- {{.}}
{{end}}{{end}}
{{- if $db.Migration.Logs}}
<details>
<summary>Logs</summary>

` + "```" + `
{{deref $db.Migration.Logs}}
` + "```" + `

</details>
{{end}}
{{- end}}
{{- if $db.Seeding}}
### Seeding
| Field | Value |
|-------|-------|
| Duration | {{$db.Seeding.Duration}} |
| Success | {{$db.Seeding.SuccessCount}} |
| Failed | {{$db.Seeding.FailedCount}} |
{{if $db.Seeding.Warnings}}
**Warnings:**
{{range $db.Seeding.Warnings}}- {{.}}
{{end}}{{end}}
{{- if $db.Seeding.Errors}}
**Errors:**
{{range $db.Seeding.Errors}}- {{.}}
{{end}}{{end}}
{{- if $db.Seeding.Logs}}
<details>
<summary>Logs</summary>

` + "```" + `
{{deref $db.Seeding.Logs}}
` + "```" + `

</details>
{{end}}
{{- end}}
{{- if $db.Warnings}}
### Warnings
{{range $db.Warnings}}- {{.}}
{{end}}{{end}}
{{- if $db.Errors}}
### Errors
{{range $db.Errors}}- {{.}}
{{end}}{{end}}
{{- end}}
`))

func (o *Output) RenderMarkdown() (string, error) {
	var buf bytes.Buffer
	if err := markdownTmpl.Execute(&buf, o); err != nil {
		return "", fmt.Errorf("failed to render markdown: %w", err)
	}
	return buf.String(), nil
}
