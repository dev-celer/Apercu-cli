package warning

import (
	"encoding/json"
	"fmt"
	"log/slog"
)

const (
	CodeFailedToOpenSeedFile Code = "SEED_FILE_OPEN_ERR"
	CodeSeedFileNotFound     Code = "SEED_FILE_NOT_FOUND"
)

type SeedingError struct {
	path string
	code Code
}

func (w *SeedingError) GetText() string {
	switch w.code {
	case CodeFailedToOpenSeedFile:
		return fmt.Sprintf("Failed to open seed file (%s)", w.path)
	case CodeSeedFileNotFound:
		return fmt.Sprintf("Seed file not found (%s)", w.path)
	default:
		return ""
	}
}

func (w *SeedingError) GetTextLong() string { return w.GetText() }

func (w *SeedingError) GetLevel() Level {
	return WarningLevelLow
}

func (w *SeedingError) GetCode() Code {
	return w.code
}

func (w *SeedingError) GetFullCode() string {
	return fmt.Sprintf("%s.%s", w.GetCode(), FormatKey(w.path))
}

func (w *SeedingError) GetIsIdempotent() bool {
	return true
}

type SeedingErrorState struct {
	Path string `json:"path"`
}

func (w *SeedingError) GetStateValues() (json.RawMessage, error) {
	v := SeedingErrorState{Path: w.path}
	return json.Marshal(v)
}

func NewSeedingError(code Code, filepath string) *SeedingError {
	switch code {
	case CodeFailedToOpenSeedFile:
	case CodeSeedFileNotFound:
	default:
		return nil
	}

	return &SeedingError{
		code: code,
		path: filepath,
	}
}

func init() {
	validateState := func(state json.RawMessage) (*SeedingErrorState, error) {
		s := &SeedingErrorState{}
		err := json.Unmarshal(state, s)
		if err != nil {
			slog.Debug("failed to unmarshal state", "error", err)
			return nil, err
		}
		return s, nil
	}
	warningConverter[CodeFailedToOpenSeedFile] = func(state json.RawMessage) Warning {
		s, err := validateState(state)
		if err != nil {
			return nil
		}
		return NewSeedingError(CodeFailedToOpenSeedFile, s.Path)
	}
	warningConverter[CodeSeedFileNotFound] = func(state json.RawMessage) Warning {
		s, err := validateState(state)
		if err != nil {
			return nil
		}
		return NewSeedingError(CodeSeedFileNotFound, s.Path)
	}
}
