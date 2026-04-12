package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig_Valid(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig("testdata")
	require.NoError(t, err)

	assert.Contains(t, cfg.Databases, "mydb")
	db := cfg.Databases["mydb"]

	assert.Equal(t, DatabaseProviderNeon, db.Source.Provider)
	assert.Equal(t, "test-project", db.Source.Neon.ProjectId)
	assert.Equal(t, "test-key", db.Source.Neon.ApiKey)
	assert.Equal(t, "main", db.Source.Neon.ParentBranch)
	assert.Equal(t, "preview-${{ PR_NUMBER }}", db.Source.Neon.PreviewBranch)
	assert.Equal(t, DatabaseNeonBranchingTypeSchemaOnly, db.Source.Neon.BranchingType)

	assert.Equal(t, "./greenmask.yaml", db.Anonymization.GreenmaskConfig)
	assert.Nil(t, db.Anonymization.Storage.Neon.ProjectId)
	assert.Nil(t, db.Anonymization.Storage.Neon.ApiKey)
	assert.Equal(t, "main-anonymized", db.Anonymization.Storage.Neon.BranchName)

	assert.Equal(t, "flyway", db.Migration.Image)
	assert.Equal(t, []string{"migrate"}, db.Migration.Command)
	assert.Equal(t, "./migrations", db.Migration.LocalDir)

	assert.Equal(t, []DatabaseSeed{{Path: "./seeds/data.sql", SeedOn: DatabaseSeedTypeAlways}}, db.Seed)
}

func TestLoadConfig_Minimal(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig("testdata/minimal")
	require.NoError(t, err)

	db := cfg.Databases["mydb"]
	assert.Nil(t, db.Migration)
	assert.Empty(t, db.Seed)
}

func TestLoadConfig_MissingFile(t *testing.T) {
	t.Parallel()
	_, err := LoadConfig("testdata/nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Failed to read config file")
}

func TestLoadConfig_MalformedYAML(t *testing.T) {
	t.Parallel()
	_, err := LoadConfig("testdata/malformed")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Failed to parse config file")
}

func TestReplaceVariables(t *testing.T) {

	tests := []struct {
		name     string
		input    string
		vars     map[string]string
		envKey   string
		envVal   string
		expected string
	}{
		{
			name:     "single variable from map",
			input:    "preview-${{ PR_NUMBER }}",
			vars:     map[string]string{"PR_NUMBER": "42"},
			expected: "preview-42",
		},
		{
			name:     "env var fallback",
			input:    "key-${{ MY_ENV_VAR }}",
			vars:     map[string]string{},
			envKey:   "MY_ENV_VAR",
			envVal:   "from-env",
			expected: "key-from-env",
		},
		{
			name:     "map takes precedence over env",
			input:    "${{ VAR1 }}",
			vars:     map[string]string{"VAR1": "from-map"},
			envKey:   "VAR1",
			envVal:   "from-env",
			expected: "from-map",
		},
		{
			name:     "multiple variables",
			input:    "${{ A }}-${{ B }}",
			vars:     map[string]string{"A": "hello", "B": "world"},
			expected: "hello-world",
		},
		{
			name:     "unknown variable returns empty",
			input:    "prefix-${{ UNKNOWN }}",
			vars:     map[string]string{},
			expected: "prefix-",
		},
		{
			name:     "no variables in string",
			input:    "plain-string",
			vars:     map[string]string{},
			expected: "plain-string",
		},
		{
			name:     "variable with spaces in braces",
			input:    "${{  SPACED  }}",
			vars:     map[string]string{"SPACED": "val"},
			expected: "val",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envKey != "" {
				t.Setenv(tt.envKey, tt.envVal)
			}
			result := ReplaceVariables(tt.input, tt.vars)
			assert.Equal(t, tt.expected, result)
		})
	}
}
