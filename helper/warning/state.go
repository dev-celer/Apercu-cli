package warning

import "fmt"

const (
	CodeStateFileFailedToRead Code = "STATE_FILE_OPEN_ERR"
)

type StateFileWarning struct {
	msg string
}

func (s StateFileWarning) GetWarningText() string {
	return s.msg
}

func (s StateFileWarning) GetWarningLevel() Level {
	return WarningLevelLow
}

func (s StateFileWarning) GetWarningCode() Code {
	return CodeStateFileFailedToRead
}

func NewStateFileWarning(filePath string) StateFileWarning {
	return StateFileWarning{
		msg: fmt.Sprintf("Failed to open state file (%s)", filePath),
	}
}
