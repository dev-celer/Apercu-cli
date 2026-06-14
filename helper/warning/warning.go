package warning

import (
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

func EscapeKey(key string) string {
	key = strings.Replace(key, " ", "_", -1)
	key = strings.Replace(key, "/", "_", -1)
	return key
}

func ConvertStateToWarnings(states map[string]json.RawMessage) []Warning {
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

var warningConverter map[Code]func(state json.RawMessage) Warning

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
