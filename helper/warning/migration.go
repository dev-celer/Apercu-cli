package warning

const (
	CodeMigrationTableNotFound Code = "NO_MIGRATION_TABLE"
)

type MigrationTableNotFound struct{}

func (w MigrationTableNotFound) GetWarningText() string {
	return "Migration table not found, cannot determine migration count"
}

func (w MigrationTableNotFound) GetWarningLevel() Level {
	return WarningLevelLow
}

func (w MigrationTableNotFound) GetWarningCode() Code {
	return CodeMigrationTableNotFound
}
