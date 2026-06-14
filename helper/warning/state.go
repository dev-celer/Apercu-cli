package warning

import (
	"encoding/json"
	"fmt"
	"log/slog"
)

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

func (w *StateFileWarning) GetFullCode() string {
	return fmt.Sprintf("%s.%s", w.GetCode(), FormatKey(w.path))
}

func (w *StateFileWarning) GetIsIdempotent() bool {
	return true
}

type StateFileWarningState struct {
	Path string `json:"path"`
}

func (w *StateFileWarning) GetStateValues() (json.RawMessage, error) {
	v := StateFileWarningState{Path: w.path}
	return json.Marshal(v)
}

func NewStateFileWarning(filePath string) *StateFileWarning {
	return &StateFileWarning{
		path: filePath,
	}
}

func init() {
	warningConverter[CodeStateFileFailedToRead] = func(state json.RawMessage) Warning {
		s := StateFileWarningState{}
		err := json.Unmarshal(state, &s)
		if err != nil {
			slog.Debug("Failed to unmarshal state", "error", err)
			return nil
		}
		return NewStateFileWarning(s.Path)
	}
}
