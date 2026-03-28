package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"

	"go.yaml.in/yaml/v3"
)

func LoadConfig(path string) (Config, error) {
	path = filepath.Join(path, "apercu.yaml")
	slog.Debug("Loading config file from", "path", path)

	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, errors.New(fmt.Sprintf("Failed to read config file: %v", err))
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return Config{}, errors.New(fmt.Sprintf("Failed to parse config file: %v", err))
	}
	return config, nil
}

func ReplaceVariables(data string, internalVariables map[string]string) string {
	var reg = regexp.MustCompile(`\${{\s*(\w+)\s*}}`)

	return reg.ReplaceAllStringFunc(data, func(match string) string {
		submatches := reg.FindStringSubmatch(match)
		varName := submatches[1]
		envValue := internalVariables[varName]
		if envValue == "" {
			envValue = os.Getenv(varName)
		}
		if envValue == "" {
			fmt.Println(fmt.Sprintf("WARNING: Variable %s not set", varName))
		}

		slog.Debug("Replacing variable", "variable", match, "value", envValue)
		return envValue
	})
}

type Config struct {
	Databases map[string]Database `yaml:"databases"`
}

type Database struct {
	Source    *DatabaseSource    `yaml:"source"`
	Migration *DatabaseMigration `yaml:"migration"`
}

type DatabaseSource struct {
	Provider DatabaseProvider    `yaml:"provider"`
	Neon     *DatabaseNeonSource `yaml:"neon"`
}

type DatabaseProvider string

const (
	DatabaseProviderNeon DatabaseProvider = "neon"
)

type DatabaseNeonSource struct {
	ProjectId     string `yaml:"project_id"`
	ApiKey        string `yaml:"api_key"`
	ParentBranch  string `yaml:"parent_branch"`
	PreviewBranch string `yaml:"preview_branch"`
}

type DatabaseMigration struct {
	Runner  string            `yaml:"runner"`
	Command []string          `yaml:"command"`
	Env     map[string]string `yaml:"env"`
}
