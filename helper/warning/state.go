package warning

import "fmt"

const (
	CodeStateFileFailedToRead Code = "STATE_FILE_OPEN_ERR"
)

type StateFileWarning struct {
	path string
}

func (w *StateFileWarning) GetText() string {
	return fmt.Sprintf("Failed to open state file (%s)", w.path)
}

func (w *StateFileWarning) GetTextLong() string {
	return w.GetText()
}

func (w *StateFileWarning) GetLevel() Level {
	return WarningLevelLow
}

func (w *StateFileWarning) GetCode() Code {
	return CodeStateFileFailedToRead
}

func (w *StateFileWarning) GetIsIdempotent() bool {
	return true
}

func (w *StateFileWarning) GetKeys() []string {
	return []string{w.path}
}

func NewStateFileWarning(filePath string) *StateFileWarning {
	return &StateFileWarning{
		path: filePath,
	}
}
