package warning

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
)

const (
	CodeMissingEnvironmentVariable Code = "MISSING_ENV_VAR"
)

type MissingEnvVarsWarning struct {
	variable string
}

func (w *MissingEnvVarsWarning) GetText() string {
	return fmt.Sprintf("Missing environment variable used in config (%s)", w.variable)
}

func (w *MissingEnvVarsWarning) GetTextLong() string {
	return w.GetText()
}

func (w *MissingEnvVarsWarning) GetLevel() Level {
	return WarningLevelLow
}

func (w *MissingEnvVarsWarning) GetCode() Code {
	return CodeMissingEnvironmentVariable
}

func (w *MissingEnvVarsWarning) GetFullCode() string {
	return fmt.Sprintf("%s.%s", w.GetCode(), FormatKey(w.variable))
}

func (w *MissingEnvVarsWarning) GetIsIdempotent() bool {
	return true
}

type MissingEnvVarsWarningState struct {
	Variable string `json:"variable"`
}

func (w *MissingEnvVarsWarning) GetStateValues() (json.RawMessage, error) {
	v := MissingEnvVarsWarningState{w.variable}
	return json.Marshal(v)
}

func NewMissingEnvVarsWarnings(variables ...string) []MissingEnvVarsWarning {
	if len(variables) == 0 {
		return nil
	}

	// Filter out duplicate variables
	vars := make([]string, len(variables))
	for _, v := range variables {
		if slices.Index(variables, v) == -1 {
			vars = append(vars, v)
		}
	}

	warnings := make([]MissingEnvVarsWarning, len(vars))
	for _, v := range vars {
		warnings = append(warnings, MissingEnvVarsWarning{variable: v})
	}
	return warnings
}

func init() {
	warningConverter[CodeMissingEnvironmentVariable] = func(state json.RawMessage) Warning {
		s := MissingEnvVarsWarningState{}
		err := json.Unmarshal(state, &s)
		if err != nil {
			slog.Debug("Failed to unmarshal state", "error", err)
			return nil
		}

		return &MissingEnvVarsWarning{s.Variable}
	}
}
