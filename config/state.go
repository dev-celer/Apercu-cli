package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
)

type State struct {
	Databases map[string]DatabaseState `yaml:"databases" json:"databases"`
}

type DatabaseState struct {
	AppliedSeeds    []SeedState                `json:"applied_seeds"`
	LastWarnings    map[string]json.RawMessage `json:"last_warnings"`
	IgnoredWarnings map[string]json.RawMessage `json:"ignored_warnings"`
}

func NewDatabaseState() DatabaseState {
	return DatabaseState{
		AppliedSeeds:    make([]SeedState, 0),
		LastWarnings:    make(map[string]json.RawMessage),
		IgnoredWarnings: make(map[string]json.RawMessage),
	}
}

type SeedState struct {
	Name string `yaml:"name" json:"seed_file"`
	Hash string `yaml:"hash" json:"seed_hash"`
}

func NewState() *State {
	return &State{
		Databases: make(map[string]DatabaseState),
	}
}

func (s *State) Save(path string) error {
	content, err := json.Marshal(s)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to save state file: %v", err))
	}

	if err := os.WriteFile(path, content, 0644); err != nil {
		return errors.New(fmt.Sprintf("Failed to save state file: %v", err))
	}

	slog.Debug("State file saved", "path", path)
	return nil
}

func GetState(path string) (State, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Debug("State file not found, creating new one", "path", path)
			return *NewState(), nil
		}
		return State{}, errors.New(fmt.Sprintf("Failed to read state file: %v", err))
	}

	var state State
	if err := json.Unmarshal(content, &state); err != nil {
		return State{}, errors.New(fmt.Sprintf("Failed to parse state file: %v", err))
	}

	slog.Debug("State file found", "path", path)
	return state, nil
}
