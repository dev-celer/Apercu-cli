package warning

import "fmt"

const (
	CodeStateFileFailedToRead Code = "STATE_FILE_OPEN_ERR"
)

type StateFileWarning struct {
	path string
}

func (s StateFileWarning) GetWarningText() string {
	return fmt.Sprintf("Failed to open state file (%s)", s.path)
}

func (s StateFileWarning) GetWarningTextLong() string {
	return s.GetWarningText()
}

func (s StateFileWarning) GetWarningLevel() Level {
	return WarningLevelLow
}

func (s StateFileWarning) GetWarningCode() Code {
	return CodeStateFileFailedToRead
}

func NewStateFileWarning(filePath string) StateFileWarning {
	return StateFileWarning{
		path: filePath,
	}
}
