package warning

import (
	"apercu-cli/config"
	"apercu-cli/helper"
	"apercu-cli/helper/warning_interface"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"reflect"
	"slices"
	"strings"
)

type Warning = warning_interface.Warning

type Code = warning_interface.Code

type Level = warning_interface.Level

const (
	WarningLevelLow    = warning_interface.WarningLevelLow
	WarningLevelMedium = warning_interface.WarningLevelMedium
	WarningLevelHigh   = warning_interface.WarningLevelHigh
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
	key = strings.ReplaceAll(key, " ", "_")
	key = strings.ReplaceAll(key, "/", "_")
	key = strings.ReplaceAll(key, "\"", "")
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
	Warnings []Warning `json:"warnings" yaml:"warnings"`
}

func NewWarningStore() *WarningStore {
	return &WarningStore{Warnings: make([]Warning, 0)}
}

func (s *WarningStore) AddWarning(w Warning) {
	if w == nil {
		return
	}
	// Guard against a typed-nil pointer wrapped in a non-nil interface
	if v := reflect.ValueOf(w); v.Kind() == reflect.Ptr && v.IsNil() {
		return
	}

	if !slices.ContainsFunc(s.Warnings, func(warning Warning) bool {
		return w.GetFullCode() == warning.GetFullCode()
	}) {
		s.Warnings = append(s.Warnings, w)
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

type WarningPerQuery struct {
	Query          string
	AffectedTables []helper.FullTableName
	Warnings       []Warning
}

func (s *WarningStore) GetWarningsPerQuery() []WarningPerQuery {
	out := make([]WarningPerQuery, 0)
	for _, w := range s.Warnings {
		// Ignore all warning that aren't linked to a query
		if w.GetQuery() == nil {
			continue
		}

		idx := slices.IndexFunc(out, func(q WarningPerQuery) bool { return q.Query == w.GetQuery().Query })
		if idx == -1 {
			out = append(out, WarningPerQuery{
				Query:          w.GetQuery().Query,
				AffectedTables: w.GetQuery().AffectedTables,
				Warnings:       []Warning{w},
			})
			continue
		}

		out[idx].Warnings = append(out[idx].Warnings, w)
	}

	return out
}

func (s *WarningStore) GetWarningsRaw() []Warning {
	return s.Warnings
}

func (s *WarningStore) GetWarnings() []Warning {
	w := s.Warnings
	w = collapseLockTimeoutWarnings(w)
	return w
}

func collapseLockTimeoutWarnings(warnings []Warning) []Warning {
	var filteredWarnings []Warning
	var lockWarnings []*LockTimeout
	for _, w := range warnings {
		x, ok := w.(*LockTimeout)
		if !ok {
			filteredWarnings = append(filteredWarnings, w)
			continue
		}
		lockWarnings = append(lockWarnings, x)
	}

	if len(lockWarnings) == 0 {
		return filteredWarnings
	}
	if len(lockWarnings) == 1 {
		// If a single lock warning is found, don't collapse it
		return append(filteredWarnings, lockWarnings[0])
	}

	return append(filteredWarnings, NewLockTimeoutCollapsed(lockWarnings...))
}

// ReconcileWarningsWithState will read previous warnings, ignored warnings from state.
// Filter out the ignored warnings, count the solved / new warnings and add the last warnings that are not idempotent.
// It will also mutate the state to replace the last warnings by current warnings
func (s *WarningStore) ReconcileWarningsWithState(state *config.DatabaseState) (solved, added int) {
	solved = 0
	added = 0
	if state == nil {
		return
	}

	// Filter out ignored warning from the present warnings
	s.Warnings = slices.DeleteFunc(s.Warnings, func(w Warning) bool {
		// Check if warning is present in state ignored warnings
		_, ignored := state.IgnoredWarnings[w.GetFullCode()]

		if !ignored {
			// If warning is not present in ignored, keep it
			return false
		}

		// If warning was ignored, is not idempotent and is still present, preserve the warning
		if !w.GetIsIdempotent() {
			// If present in last warning, consider the last one as solved and remove it from the last warning map so it can be considered as new
			delete(state.LastWarnings, w.GetFullCode())
			solved++
			return false
		}

		// Else if warning is idempotent, filter out ignored keys
		return true
	})

	// Count the new warnings
	for _, w := range s.Warnings {
		// Check if warning is present in state last warnings
		_, last := state.LastWarnings[w.GetFullCode()]
		// If no, consider as new
		if !last {
			added++
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
	for _, w := range s.Warnings {
		v, err := w.GetStateValues()
		if err != nil {
			slog.Error("Failed to store warnings to state", "code", w.GetFullCode(), "error", err)
			continue
		}
		state.LastWarnings[w.GetFullCode()] = v
	}
	return
}

func (s *WarningStore) toState() (map[string]json.RawMessage, error) {
	state := make(map[string]json.RawMessage, len(s.Warnings))
	for _, w := range s.Warnings {
		v, err := w.GetStateValues()
		if err != nil {
			return nil, fmt.Errorf("failed to serialize warning %s: %w", w.GetFullCode(), err)
		}
		state[w.GetFullCode()] = v
	}
	return state, nil
}

func (s *WarningStore) MarshalJSON() (data []byte, err error) {
	state, err := s.toState()
	if err != nil {
		return nil, err
	}
	return json.Marshal(state)
}

func (s *WarningStore) UnmarshalJSON(data []byte) error {
	var state map[string]json.RawMessage
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	s.Warnings = ConvertStatesToWarnings(state)
	return nil
}

func (s *WarningStore) MarshalYAML() (any, error) {
	state, err := s.toState()
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, len(state))
	for fullCode, raw := range state {
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, fmt.Errorf("failed to serialize warning %s: %w", fullCode, err)
		}
		out[fullCode] = v
	}
	return out, nil
}

func (s *WarningStore) UnmarshalYAML(unmarshal func(any) error) error {
	var decoded map[string]any
	if err := unmarshal(&decoded); err != nil {
		return err
	}
	state := make(map[string]json.RawMessage, len(decoded))
	for fullCode, v := range decoded {
		raw, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("failed to deserialize warning %s: %w", fullCode, err)
		}
		state[fullCode] = raw
	}
	s.Warnings = ConvertStatesToWarnings(state)
	return nil
}
