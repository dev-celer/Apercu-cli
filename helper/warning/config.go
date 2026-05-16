package warning

import (
	"fmt"
	"slices"
	"strings"
)

const (
	CodeMissingEnvironmentVariable Code = "MISSING_ENV_VAR"
)

type MissingEnvVarsWarning struct {
	variables []string
}

func (w *MissingEnvVarsWarning) GetWarningText() string {
	if len(w.variables) > 1 {
		return fmt.Sprintf("Missing environment variables used in config (%s)", strings.Join(w.variables, ", "))
	}
	return fmt.Sprintf("Missing environment variable used in config (%s)", strings.Join(w.variables, ", "))
}

func (w *MissingEnvVarsWarning) GetWarningLevel() Level {
	return WarningLevelLow
}

func (w *MissingEnvVarsWarning) GetWarningCode() Code {
	return CodeMissingEnvironmentVariable
}

func NewMissingEnvVarsWarning(variables ...string) *MissingEnvVarsWarning {
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

	return &MissingEnvVarsWarning{
		variables: vars,
	}
}

func (w *MissingEnvVarsWarning) UpdateGlobalEnvVarsWarning(globalWarnings []Warning) {
	for _, i := range globalWarnings {
		if gw, ok := i.(*MissingEnvVarsWarning); ok {
			// Filter out variables already inside the global warning
			for _, j := range gw.variables {
				if slices.Index(w.variables, j) == -1 {
					gw.variables = append(gw.variables, j)
				}
			}
			return
		}
	}

	// If not found in global variable, create it
	globalWarnings = append(globalWarnings, w)
}

func MergeEnvVarsWarning(warnings ...*MissingEnvVarsWarning) *MissingEnvVarsWarning {
	envVars := make([]string, 0)
	for _, w := range warnings {
		if w == nil {
			continue
		}
		envVars = append(envVars, w.variables...)
	}
	return NewMissingEnvVarsWarning(envVars...)
}
