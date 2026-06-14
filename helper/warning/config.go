package warning

import (
	"encoding/json"
	"fmt"
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
	return fmt.Sprintf("%s.%s", w.GetCode(), EscapeKey(w.variable))
}

func (w *MissingEnvVarsWarning) GetIsIdempotent() bool {
	return true
}

func (w *MissingEnvVarsWarning) GetStateValues() (json.RawMessage, error) {
	return json.RawMessage{}, nil
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
