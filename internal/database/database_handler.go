package database

import (
	"apercu-cli/config"
	"apercu-cli/helper"
	"errors"
	"fmt"
	"log/slog"
)

type HandlerInterface interface {
	Exists() (bool, error)
	Create() error
	Delete() error
	Reset() error
	GetConnectionFields() (helper.ConnectionFields, error)
	GetWarnings() []string
}

func GetPreviewDatabaseHandler(dbConfig config.Database) (HandlerInterface, error) {
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

		return NewNeonHandler(
			projectId,
			apiKey,
			&parentBranch,
			config.ReplaceVariables(dbConfig.Source.Neon.PreviewBranch, map[string]string{}),
			&branchingType,
		)
	}

	return nil, errors.New(fmt.Sprintf("unsupported source database provider: %s", dbConfig.Source.Provider))
}

// GetAnonymizationDatabaseHandlers return (source, storage, error)
func GetAnonymizationDatabaseHandlers(dbConfig config.Database) (HandlerInterface, HandlerInterface, error) {
	if dbConfig.Source == nil || dbConfig.Anonymization == nil {
		slog.Debug("No source database specified")
		return nil, nil, nil
	}

	switch dbConfig.Source.Provider {
	case config.DatabaseProviderNeon:
		if dbConfig.Anonymization.Storage.Neon == nil {
			return nil, nil, errors.New("missing neon anonymization storage configuration")
		}
		if dbConfig.Source.Neon == nil {
			return nil, nil, errors.New("missing neon source database configuration")
		}

		parentProjectId := config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, map[string]string{})
		var storageProjectId string
		if dbConfig.Anonymization.Storage.Neon.ProjectId != nil {
			storageProjectId = config.ReplaceVariables(*dbConfig.Anonymization.Storage.Neon.ProjectId, map[string]string{})
		} else {
			storageProjectId = parentProjectId
		}

		sourceApiKey := config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, map[string]string{})
		var storageApiKey string
		if dbConfig.Anonymization.Storage.Neon.ApiKey != nil {
			storageApiKey = config.ReplaceVariables(*dbConfig.Anonymization.Storage.Neon.ApiKey, map[string]string{})
		} else {
			storageApiKey = sourceApiKey
		}

		env := make(map[string]string, len(dbConfig.Anonymization.Env))
		for k, v := range dbConfig.Anonymization.Env {
			env[k] = config.ReplaceVariables(v, map[string]string{})
		}

		sourceBranch := config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, map[string]string{})
		sourceHandler, err := NewNeonHandler(
			config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, map[string]string{}),
			sourceApiKey,
			nil,
			sourceBranch,
			nil,
		)
		if err != nil {
			return nil, nil, err
		}

		storageHandler, err := NewNeonHandler(
			storageProjectId,
			storageApiKey,
			&sourceBranch,
			config.ReplaceVariables(dbConfig.Anonymization.Storage.Neon.BranchName, map[string]string{}),
			new(config.DatabaseNeonBranchingTypeSchemaOnly),
		)
		if err != nil {
			return nil, nil, err
		}

		return sourceHandler, storageHandler, nil
	}

	return nil, nil, errors.New(fmt.Sprintf("unsupported database provider: %s", dbConfig.Source.Provider))
}

type PruningHandlerInterface interface {
	Prune([]string) ([]string, error)
	GetWarnings() []string
}

func GetPruningDatabaseHandler(dbConfig config.Database) (PruningHandlerInterface, error) {
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

		return NewNeonPruneHandler(
			projectId,
			apiKey,
			parentBranch,
			config.ReplaceVariables(dbConfig.Source.Neon.PreviewBranch, map[string]string{}),
		)
	}

	return nil, errors.New(fmt.Sprintf("unsupported database provider: %s", dbConfig.Source.Provider))
}
