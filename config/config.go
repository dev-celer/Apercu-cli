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

		return envValue
	})
}

type Config struct {
	Databases map[string]Database `yaml:"databases"`
}

type Database struct {
	Source        *DatabaseSource        `yaml:"source"`
	Anonymization *DatabaseAnonymization `yaml:"anonymization,omitempty"`
	Migration     *DatabaseMigration     `yaml:"migration,omitempty"`
	Seed          []DatabaseSeed         `yaml:"seed,omitempty"`
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
	ProjectId     string                    `yaml:"project_id"`
	ApiKey        string                    `yaml:"api_key"`
	ParentBranch  string                    `yaml:"parent_branch"`
	PreviewBranch string                    `yaml:"preview_branch"`
	BranchingType DatabaseNeonBranchingType `yaml:"branching_type" default:"parent_data"`
}

type DatabaseNeonBranchingType string

const (
	DatabaseNeonBranchingTypeSchemaOnly DatabaseNeonBranchingType = "schema_only"
	DatabaseNeonBranchingTypeParentData DatabaseNeonBranchingType = "parent_data"
)

func (b DatabaseNeonBranchingType) Valid() bool {
	_, ok := map[DatabaseNeonBranchingType]struct{}{
		DatabaseNeonBranchingTypeSchemaOnly: {},
		DatabaseNeonBranchingTypeParentData: {},
	}[b]
	return ok
}

type DatabaseMigration struct {
	Image    string            `yaml:"image"`
	Command  []string          `yaml:"command"`
	Env      map[string]string `yaml:"env"`
	LocalDir string            `yaml:"local_dir,omitempty"`
	WorkDir  *string           `yaml:"work_dir"`
}

type DatabaseSeed struct {
	Path   string           `yaml:"path"`
	SeedOn DatabaseSeedType `yaml:"seed_on"`
}

type DatabaseSeedType string

const (
	DatabaseSeedTypeAlways DatabaseSeedType = "always"
	DatabaseSeedTypeUpdate DatabaseSeedType = "update"
	DatabaseSeedTypeCreate DatabaseSeedType = "create"
)

type DatabaseAnonymization struct {
	GreenmaskConfig string                       `yaml:"greenmask_config"`
	Storage         DatabaseAnonymizationStorage `yaml:"storage"`
	Env             map[string]string            `yaml:"env"`
}

type DatabaseAnonymizationStorage struct {
	Neon *DatabaseAnonymizationStorageNeon `yaml:"neon,omitempty"`
}

type DatabaseAnonymizationStorageNeon struct {
	ProjectId  *string `yaml:"project_id,omitempty"`
	ApiKey     *string `yaml:"api_key,omitempty"`
	BranchName string  `yaml:"branch_name"`
}
