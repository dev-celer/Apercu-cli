package output

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
