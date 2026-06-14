package warning

import (
	"apercu-cli/config"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"reflect"
	"slices"
	"strings"
)

type Warning interface {
	GetText() string
	GetTextLong() string
	GetLevel() Level
	GetCode() Code
	GetFullCode() string
	GetIsIdempotent() bool
	GetStateValues() (json.RawMessage, error)
}

type Code string

type Level uint8

func (l Level) String() string {
	switch l {
	case WarningLevelLow:
		return "low"
	case WarningLevelMedium:
		return "medium"
	case WarningLevelHigh:
		return "high"
	}
	return "unknown"
}

const (
	WarningLevelLow    Level = 1
	WarningLevelMedium Level = 2
	WarningLevelHigh   Level = 3
)

func PrintWarning(w Warning) {
	if w == nil {
		return
	}
	// Guard against a typed-nil pointer wrapped in a non-nil interface
	if v := reflect.ValueOf(w); v.Kind() == reflect.Ptr && v.IsNil() {
		return
	}
	_, _ = fmt.Fprintln(log.Writer(), fmt.Sprintf("WARNING: %s", w.GetTextLong()))
}

func FormatKey(key string) string {
	key = strings.Replace(key, " ", "_", -1)
	key = strings.Replace(key, "/", "_", -1)
	return key
}

func ConvertStatesToWarnings(states map[string]json.RawMessage) []Warning {
	warnings := make([]Warning, 0)
	slog.Debug("Converting state to warnings")

	for fullCode, state := range states {
		// Extract code / keys
		code, key, _ := strings.Cut(fullCode, ".")
		slog.Debug("handling warning", "code", code, "key", key)

		f, ok := warningConverter[Code(code)]
		if !ok {
			slog.Debug("unknown code", "code", code)
			continue
		}

		warnings = append(warnings, f(state))
	}

	return warnings
}

var warningConverter map[Code]func(state json.RawMessage) Warning = make(map[Code]func(state json.RawMessage) Warning)

type WarningStore struct {
	warnings []Warning
}

func NewWarningStore() *WarningStore {
	return &WarningStore{warnings: make([]Warning, 0)}
}

func (s *WarningStore) AddWarning(w Warning) {
	if w != nil && !slices.Contains(s.warnings, w) {
		s.warnings = append(s.warnings, w)
	}
}

func (s *WarningStore) AddWarnings(w []Warning) {
	for _, w := range w {
		s.AddWarning(w)
	}
}

func (s *WarningStore) AddWarningAndPrint(w Warning) {
	PrintWarning(w)
	s.AddWarning(w)
}

func (s *WarningStore) AddWarningsAndPrint(w []Warning) {
	for _, w := range w {
		s.AddWarningAndPrint(w)
	}
}

func (s *WarningStore) GetWarnings() []Warning {
	return s.warnings
}

// ReconcileWarningsWithState will read previous warnings, ignored warnings from state.
// Filter out the ignored warnings, count the solved / new warnings and add the last warnings that are not idempotent.
// It will also mutate the state to replace the last warnings by current warnings
func (s *WarningStore) ReconcileWarningsWithState(state *config.DatabaseState) (solved, new int) {
	solved = 0
	new = 0
	if state == nil {
		return
	}

	// Filter out ignored warning from the present warnings
	s.warnings = slices.DeleteFunc(s.warnings, func(w Warning) bool {
		// Check if warning is present in state ignored warnings
		_, ignored := state.IgnoredWarnings[w.GetFullCode()]

		if !ignored {
			// If warning is not present in ignored, keep it
			return false
		}

		// If warning was ignored, is not idempotent and is still present, consider as a new warning
		if !w.GetIsIdempotent() {
			// If present in last warning, remove it and consider as solved
			delete(state.LastWarnings, w.GetFullCode())
			solved++
			return false
		}

		// Else if warning is idempotent, filter out ignored keys
		return true
	})

	// Count the new warnings
	for _, w := range s.warnings {
		// Check if warning is present in state last warnings
		_, last := state.LastWarnings[w.GetFullCode()]
		// If no, consider as new
		if !last {
			new++
			continue
		}
		// Else remove it from last warnings
		delete(state.LastWarnings, w.GetFullCode())
	}

	// Handle the remaining warnings from last run
	for _, w := range ConvertStatesToWarnings(state.LastWarnings) {
		// If warning is not idempotent, consider as solved
		if !w.GetIsIdempotent() {
			solved++
			continue
		}

		// Else, add it to the warnings
		s.AddWarning(w)
	}

	// Update the state
	state.LastWarnings = make(map[string]json.RawMessage)
	for _, w := range s.warnings {
		v, err := w.GetStateValues()
		if err != nil {
			slog.Error("Failed to store warnings to state", "code", w.GetFullCode(), "error", err)
			continue
		}
		state.LastWarnings[w.GetFullCode()] = v
	}
	return
}
