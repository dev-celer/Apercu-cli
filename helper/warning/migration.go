package warning

const (
	CodeMigrationTableNotFound Code = "NO_MIGRATION_TABLE"
)

type MigrationTableNotFound struct{}

func (w *MigrationTableNotFound) GetText() string {
	return "Migration table not found, cannot determine migration count"
}

func (w *MigrationTableNotFound) GetTextLong() string {
	return w.GetText()
}

func (w *MigrationTableNotFound) GetLevel() Level {
	return WarningLevelLow
}

func (w *MigrationTableNotFound) GetCode() Code {
	return CodeMigrationTableNotFound
}

func (w *MigrationTableNotFound) GetIsIdempotent() bool {
	return true
}

func (w *MigrationTableNotFound) GetKeys() []string {
	return nil
}
