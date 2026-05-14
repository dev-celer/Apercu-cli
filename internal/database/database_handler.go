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

func getProdDatabaseConn(dbConfig config.Database) (helper.ConnectionFields, error) {
	if dbConfig.Source == nil {
		slog.Debug("No source database specified")
		return helper.ConnectionFields{}, nil
	}

	switch dbConfig.Source.Provider {
	case config.DatabaseProviderNeon:
		if dbConfig.Source.Neon == nil {
			return helper.ConnectionFields{}, errors.New("missing neon source database configuration")
		}
		neonHandler, err := NewNeonHandler(
			config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, nil),
			config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, nil),
			nil,
			config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, nil),
			nil,
		)
		if err != nil {
			return helper.ConnectionFields{}, err
		}

		return neonHandler.GetConnectionFields()
	case config.DatabaseProviderGeneric:
		if dbConfig.Source.Generic == nil {
			return helper.ConnectionFields{}, errors.New("missing generic source database configuration")
		}
		return helper.ExtractConnectionFieldsFromUrl(dbConfig.Source.Generic.DatabaseUrl)
	}

	return helper.ConnectionFields{}, errors.New(fmt.Sprintf("unsupported source database provider: %s", dbConfig.Source.Provider))
}

func GetPreviewDatabaseHandler(dbConfig config.Database) (*helper.ConnectionFields, HandlerInterface, error) {
	if dbConfig.Source == nil {
		slog.Debug("No source database specified")
		return nil, nil, nil
	}
	if dbConfig.PreviewBranch == "" {
		slog.Debug("No preview branch pattern specified")
		return nil, nil, nil
	}

	// If anonymization is enabled, use the storage database information
	if dbConfig.Anonymization != nil {
		if dbConfig.Anonymization.Storage.Neon == nil {
			return nil, nil, fmt.Errorf("no storage Neon configuration specified")
		}

		branchParent := config.ReplaceVariables(dbConfig.Anonymization.Storage.Neon.BranchName, nil)
		apiKey := dbConfig.Anonymization.Storage.Neon.ApiKey
		projectId := dbConfig.Anonymization.Storage.Neon.ProjectId

		// If projectId or ApiKey are nil, try to retrieve them from neon source configuration
		if apiKey == nil || projectId == nil {
			if dbConfig.Source.Provider != config.DatabaseProviderNeon || dbConfig.Source.Neon == nil {
				return nil, nil, fmt.Errorf("missing neon ApiKey / ProjectId configuration in anonymization")
			}

			if apiKey == nil {
				apiKey = new(dbConfig.Source.Neon.ApiKey)
			}
			if projectId == nil {
				projectId = new(dbConfig.Source.Neon.ProjectId)
			}
		}

		neonHandler, err := NewNeonHandler(
			config.ReplaceVariables(*projectId, nil),
			config.ReplaceVariables(*apiKey, nil),
			&branchParent,
			config.ReplaceVariables(dbConfig.PreviewBranch, nil),
			new(config.DatabaseNeonBranchingTypeParentData),
		)
		if err != nil {
			return nil, nil, err
		}

		prodConn, err := getProdDatabaseConn(dbConfig)
		if err != nil {
			return nil, nil, err
		}

		return &prodConn, neonHandler, nil
	}

	// No anonymization specified, basing everything on source configuration
	switch dbConfig.Source.Provider {
	case config.DatabaseProviderNeon:
		if dbConfig.Source.Neon == nil {
			return nil, nil, errors.New("missing neon source database configuration")
		}

		// Prepare branching type
		branchingTypeStr := config.ReplaceVariables(string(dbConfig.Source.Neon.BranchingType), nil)
		branchingType := config.DatabaseNeonBranchingType(branchingTypeStr)
		if !branchingType.Valid() {
			branchingType = config.DatabaseNeonBranchingTypeParentData
		}

		projectId := config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, nil)
		apiKey := config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, nil)
		parentBranch := config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, nil)

		neonHandler, err := NewNeonHandler(
			projectId,
			apiKey,
			&parentBranch,
			config.ReplaceVariables(dbConfig.PreviewBranch, nil),
			&branchingType,
		)
		if err != nil {
			return nil, nil, err
		}

		prodConn, err := getProdDatabaseConn(dbConfig)
		if err != nil {
			return nil, nil, err
		}

		return &prodConn, neonHandler, nil
	case config.DatabaseProviderGeneric:
		return nil, nil, errors.New("generic provider without anonymization is currently unsupported")
	}

	return nil, nil, errors.New(fmt.Sprintf("unsupported source database provider: %s", dbConfig.Source.Provider))
}

// GetAnonymizationDatabaseHandlers return (sourceConnection, storage, error)
func GetAnonymizationDatabaseHandlers(dbConfig config.Database) (*helper.ConnectionFields, HandlerInterface, error) {
	if dbConfig.Source == nil || dbConfig.Anonymization == nil {
		slog.Debug("No source database specified")
		return nil, nil, nil
	}

	// Getting source database connection
	var sourceConnection *helper.ConnectionFields
	switch dbConfig.Source.Provider {
	case config.DatabaseProviderNeon:
		if dbConfig.Source.Neon == nil {
			if dbConfig.Source.Neon == nil {
				return nil, nil, errors.New("missing neon source database configuration")
			}

			sourceHandler, err := NewNeonHandler(
				config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, nil),
				config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, nil),
				nil,
				config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, nil),
				nil,
			)
			if err != nil {
				return nil, nil, err
			}

			sourceConn, err := sourceHandler.GetConnectionFields()
			if err != nil {
				return nil, nil, err
			}
			sourceConnection = &sourceConn
		}
	case config.DatabaseProviderGeneric:
		if dbConfig.Source.Generic == nil {
			return nil, nil, errors.New("missing generic source database configuration")
		}
		sourceConn, err := helper.ExtractConnectionFieldsFromUrl(dbConfig.Source.Generic.DatabaseUrl)
		if err != nil {
			return nil, nil, err
		}
		sourceConnection = &sourceConn
	default:
		return nil, nil, fmt.Errorf("unsupported source database provider: %s", dbConfig.Source.Provider)
	}

	// Get storage database handler
	if dbConfig.Anonymization.Storage.Neon == nil {
		return nil, nil, errors.New("missing storage database configuration")
	}

	apiKey := dbConfig.Anonymization.Storage.Neon.ApiKey
	projectId := dbConfig.Anonymization.Storage.Neon.ProjectId

	// If projectId or ApiKey are nil, try to retrieve them from neon source configuration
	if apiKey == nil || projectId == nil {
		if dbConfig.Source.Provider != config.DatabaseProviderNeon || dbConfig.Source.Neon == nil {
			return nil, nil, fmt.Errorf("missing neon ApiKey / ProjectId configuration in anonymization")
		}

		if apiKey == nil {
			apiKey = new(dbConfig.Source.Neon.ApiKey)
		}
		if projectId == nil {
			projectId = new(dbConfig.Source.Neon.ProjectId)
		}
	}

	storageHandler, err := NewNeonHandler(
		config.ReplaceVariables(*projectId, nil),
		config.ReplaceVariables(*apiKey, nil),
		new(config.ReplaceVariables(dbConfig.Anonymization.Storage.Neon.BranchName, nil)),
		config.ReplaceVariables(dbConfig.PreviewBranch, nil),
		new(config.DatabaseNeonBranchingTypeParentData),
	)
	if err != nil {
		return nil, nil, err
	}

	return sourceConnection, storageHandler, nil
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
	if dbConfig.PreviewBranch == "" {
		slog.Debug("No preview branch pattern specified")
		return nil, nil
	}

	// If anonymization is enabled, use the storage database information
	if dbConfig.Anonymization != nil {
		if dbConfig.Anonymization.Storage.Neon == nil {
			return nil, fmt.Errorf("no storage Neon configuration specified")
		}

		branchParent := config.ReplaceVariables(dbConfig.Anonymization.Storage.Neon.BranchName, nil)
		apiKey := dbConfig.Anonymization.Storage.Neon.ApiKey
		projectId := dbConfig.Anonymization.Storage.Neon.ProjectId

		// If projectId or ApiKey are nil, try to retrieve them from neon source configuration
		if apiKey == nil || projectId == nil {
			if dbConfig.Source.Provider != config.DatabaseProviderNeon || dbConfig.Source.Neon == nil {
				return nil, fmt.Errorf("missing neon ApiKey / ProjectId configuration in anonymization")
			}

			if apiKey == nil {
				apiKey = new(dbConfig.Source.Neon.ApiKey)
			}
			if projectId == nil {
				projectId = new(dbConfig.Source.Neon.ProjectId)
			}
		}

		return NewNeonPruneHandler(
			config.ReplaceVariables(*projectId, nil),
			config.ReplaceVariables(*apiKey, nil),
			branchParent,
			dbConfig.PreviewBranch,
		)
	}

	// No anonymization specified, basing everything on source configuration
	switch dbConfig.Source.Provider {
	case config.DatabaseProviderNeon:
		if dbConfig.Source.Neon == nil {
			return nil, errors.New("missing neon source database configuration")
		}

		// Prepare branching type
		branchingTypeStr := config.ReplaceVariables(string(dbConfig.Source.Neon.BranchingType), nil)
		branchingType := config.DatabaseNeonBranchingType(branchingTypeStr)
		if !branchingType.Valid() {
			branchingType = config.DatabaseNeonBranchingTypeParentData
		}

		projectId := config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, nil)
		apiKey := config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, nil)
		parentBranch := config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, nil)

		return NewNeonPruneHandler(
			projectId,
			apiKey,
			parentBranch,
			dbConfig.PreviewBranch,
		)
	case config.DatabaseProviderGeneric:
		return nil, errors.New("generic provider without anonymization is currently unsupported")
	}

	return nil, errors.New(fmt.Sprintf("unsupported source database provider: %s", dbConfig.Source.Provider))
}
