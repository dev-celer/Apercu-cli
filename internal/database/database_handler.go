package database

import (
	"apercu-cli/config"
	"errors"
	"fmt"
	"log/slog"
)

type ConnectionFields struct {
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	User     string `json:"user" yaml:"user"`
	Password string `json:"password" yaml:"password"`
	Database string `json:"database" yaml:"database"`
	Url      string `json:"url" yaml:"url"`
}

type HandlerInterface interface {
	Apply() error
	Cleanup() error
	Reset() error
	GetParentConnectionFields() (ConnectionFields, error)
	GetPreviewConnectionFields() (ConnectionFields, error)
	PrunePreviewDatabases(openedPullRequestNumber []string) ([]string, error)
	GetWarnings() []string
}

func GetSourceDatabaseHandler(dbConfig config.Database) (HandlerInterface, error) {
	if dbConfig.Source == nil {
		slog.Debug("No source database specified")
		return nil, nil
	}

	switch dbConfig.Source.Provider {
	case config.DatabaseProviderNeon:
		if dbConfig.Source.Neon == nil {
			return nil, errors.New("missing neon source database configuration")
		}

		// Prepare branching type
		branchingTypeStr := config.ReplaceVariables(string(dbConfig.Source.Neon.BranchingType), map[string]string{})
		branchingType := config.DatabaseNeonBranchingType(branchingTypeStr)
		if !branchingType.Valid() {
			branchingType = config.DatabaseNeonBranchingTypeParentData
		}

		var projectId string
		var apiKey string
		var parentBranch string

		// If anonymization is configured with storage on neon use those values
		if dbConfig.Anonymization != nil && dbConfig.Anonymization.Storage.Neon != nil {
			if dbConfig.Anonymization.Storage.Neon.ProjectId != nil {
				projectId = config.ReplaceVariables(*dbConfig.Anonymization.Storage.Neon.ProjectId, map[string]string{})
			} else {
				projectId = config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, map[string]string{})
			}

			if dbConfig.Anonymization.Storage.Neon.ApiKey != nil {
				apiKey = config.ReplaceVariables(*dbConfig.Anonymization.Storage.Neon.ApiKey, map[string]string{})
			} else {
				apiKey = config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, map[string]string{})
			}

			parentBranch = config.ReplaceVariables(dbConfig.Anonymization.Storage.Neon.BranchName, map[string]string{})
		} else {
			projectId = config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, map[string]string{})
			apiKey = config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, map[string]string{})
			parentBranch = config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, map[string]string{})
		}

		return NewNeonBranchHandler(
			projectId,
			apiKey,
			parentBranch,
			config.ReplaceVariables(dbConfig.Source.Neon.PreviewBranch, map[string]string{}),
			branchingType,
		)
	}

	return nil, errors.New(fmt.Sprintf("unsupported source database provider: %s", dbConfig.Source.Provider))
}

func GetDatabaseHandlerForPruning(dbConfig config.Database) (HandlerInterface, error) {
	if dbConfig.Source == nil {
		slog.Debug("No source database specified")
		return nil, nil
	}

	switch dbConfig.Source.Provider {
	case config.DatabaseProviderNeon:
		if dbConfig.Source.Neon == nil {
			return nil, errors.New("missing neon source database configuration")
		}

		// Prepare branching type
		branchingTypeStr := config.ReplaceVariables(string(dbConfig.Source.Neon.BranchingType), map[string]string{})
		branchingType := config.DatabaseNeonBranchingType(branchingTypeStr)
		if !branchingType.Valid() {
			branchingType = config.DatabaseNeonBranchingTypeParentData
		}

		var projectId string
		var apiKey string
		var parentBranch string

		// If anonymization is configured with storage on neon use those values
		if dbConfig.Anonymization != nil && dbConfig.Anonymization.Storage.Neon != nil {
			if dbConfig.Anonymization.Storage.Neon.ProjectId != nil {
				projectId = config.ReplaceVariables(*dbConfig.Anonymization.Storage.Neon.ProjectId, map[string]string{})
			} else {
				projectId = config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, map[string]string{})
			}

			if dbConfig.Anonymization.Storage.Neon.ApiKey != nil {
				apiKey = config.ReplaceVariables(*dbConfig.Anonymization.Storage.Neon.ApiKey, map[string]string{})
			} else {
				apiKey = config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, map[string]string{})
			}

			parentBranch = config.ReplaceVariables(dbConfig.Anonymization.Storage.Neon.BranchName, map[string]string{})
		} else {
			projectId = config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, map[string]string{})
			apiKey = config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, map[string]string{})
			parentBranch = config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, map[string]string{})
		}

		return NewNeonBranchHandler(
			projectId,
			apiKey,
			parentBranch,
			config.ReplaceVariables(dbConfig.Source.Neon.PreviewBranch, map[string]string{}),
			branchingType,
		)
	}

	return nil, errors.New(fmt.Sprintf("unsupported database provider: %s", dbConfig.Source.Provider))
}
