package database

import (
	"apercu-cli/config"
	"apercu-cli/helper"
	"apercu-cli/helper/warning"
	"errors"
	"fmt"
	"log/slog"
	"slices"
)

type HandlerInterface interface {
	Exists() (bool, error)
	Create() error
	Delete() error
	Reset() error
	GetConnectionFields() (helper.ConnectionFields, error)
}

// getProdDatabaseConn return a ConnectionFields object to the prod database or an error
func getProdDatabaseConn(dbConfig config.Database, store *warning.WarningStore) (helper.ConnectionFields, error) {
	if dbConfig.Source == nil {
		slog.Debug("No source database specified")
		return helper.ConnectionFields{}, nil
	}

	switch dbConfig.Source.Provider {
	case config.DatabaseProviderNeon:
		if dbConfig.Source.Neon == nil {
			return helper.ConnectionFields{}, errors.New("missing neon source database configuration")
		}
		projectId, m1 := config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, nil)
		apiKey, m2 := config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, nil)
		parentBranch, m3 := config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, nil)
		for _, w := range warning.NewMissingEnvVarsWarnings(slices.Concat(m1, m2, m3)...) {
			store.AddWarningAndPrint(&w)
		}

		neonHandler, err := NewNeonHandler(
			projectId,
			apiKey,
			nil,
			parentBranch,
			nil,
			store,
		)
		if err != nil {
			return helper.ConnectionFields{}, err
		}

		conn, err := neonHandler.GetConnectionFields()
		return conn, err
	case config.DatabaseProviderGeneric:
		if dbConfig.Source.Generic == nil {
			return helper.ConnectionFields{}, errors.New("missing generic source database configuration")
		}
		dbUrl, m := config.ReplaceVariables(dbConfig.Source.Generic.DatabaseUrl, nil)
		for _, w := range warning.NewMissingEnvVarsWarnings(m...) {
			store.AddWarningAndPrint(&w)
		}

		conn, err := helper.ExtractConnectionFieldsFromUrl(dbUrl)
		return conn, err
	}

	return helper.ConnectionFields{}, errors.New(fmt.Sprintf("unsupported source database provider: %s", dbConfig.Source.Provider))
}

// GetPreviewDatabaseHandler return a ConnectionFields object to the prod database, a database.HandlerInterface Object for the preview branch or an error
func GetPreviewDatabaseHandler(dbConfig config.Database, store *warning.WarningStore) (*helper.ConnectionFields, HandlerInterface, error) {
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

		branchParent, m1 := config.ReplaceVariables(dbConfig.Anonymization.Storage.Neon.BranchName, nil)
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

		projectIdStr, m2 := config.ReplaceVariables(*projectId, nil)
		apiKeyStr, m3 := config.ReplaceVariables(*apiKey, nil)
		previewBranch, m4 := config.ReplaceVariables(dbConfig.PreviewBranch, nil)
		for _, w := range warning.NewMissingEnvVarsWarnings(slices.Concat(m1, m2, m3, m4)...) {
			store.AddWarningAndPrint(&w)
		}

		neonHandler, err := NewNeonHandler(
			projectIdStr,
			apiKeyStr,
			&branchParent,
			previewBranch,
			new(config.DatabaseNeonBranchingTypeParentData),
			store,
		)
		if err != nil {
			return nil, nil, err
		}

		prodConn, err := getProdDatabaseConn(dbConfig, store)
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
		branchingTypeStr, m1 := config.ReplaceVariables(string(dbConfig.Source.Neon.BranchingType), nil)
		branchingType := config.DatabaseNeonBranchingType(branchingTypeStr)
		if !branchingType.Valid() {
			branchingType = config.DatabaseNeonBranchingTypeParentData
		}

		projectId, m2 := config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, nil)
		apiKey, m3 := config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, nil)
		parentBranch, m4 := config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, nil)
		previewBranch, m5 := config.ReplaceVariables(dbConfig.PreviewBranch, nil)
		for _, w := range warning.NewMissingEnvVarsWarnings(slices.Concat(m1, m2, m3, m4, m5)...) {
			store.AddWarningAndPrint(&w)
		}

		neonHandler, err := NewNeonHandler(
			projectId,
			apiKey,
			&parentBranch,
			previewBranch,
			&branchingType,
			store,
		)
		if err != nil {
			return nil, nil, err
		}

		prodConn, err := getProdDatabaseConn(dbConfig, store)
		if err != nil {
			return nil, nil, err
		}

		return &prodConn, neonHandler, nil
	case config.DatabaseProviderGeneric:
		return nil, nil, errors.New("generic provider without anonymization is currently unsupported")
	}

	return nil, nil, errors.New(fmt.Sprintf("unsupported source database provider: %s", dbConfig.Source.Provider))
}

// GetAnonymizationDatabaseHandlers return a ConnectionFields object to the prod database, a database.HandlerInterface Object for the storage branch or an error
func GetAnonymizationDatabaseHandlers(dbConfig config.Database, store *warning.WarningStore) (*helper.ConnectionFields, HandlerInterface, error) {
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

			projectId, m1 := config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, nil)
			apiKey, m2 := config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, nil)
			parentBranch, m3 := config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, nil)
			for _, w := range warning.NewMissingEnvVarsWarnings(slices.Concat(m1, m2, m3)...) {
				store.AddWarningAndPrint(&w)
			}

			sourceHandler, err := NewNeonHandler(
				projectId,
				apiKey,
				nil,
				parentBranch,
				nil,
				store,
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
		dbUrl, m := config.ReplaceVariables(dbConfig.Source.Generic.DatabaseUrl, nil)
		for _, w := range warning.NewMissingEnvVarsWarnings(m...) {
			store.AddWarningAndPrint(&w)
		}

		sourceConn, err := helper.ExtractConnectionFieldsFromUrl(dbUrl)
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

	projectIdStr, m1 := config.ReplaceVariables(*projectId, nil)
	apiKeyStr, m2 := config.ReplaceVariables(*apiKey, nil)
	branchName, m3 := config.ReplaceVariables(dbConfig.Anonymization.Storage.Neon.BranchName, nil)
	previewBranch, m4 := config.ReplaceVariables(dbConfig.PreviewBranch, nil)
	for _, w := range warning.NewMissingEnvVarsWarnings(slices.Concat(m1, m2, m3, m4)...) {
		store.AddWarningAndPrint(&w)
	}

	storageHandler, err := NewNeonHandler(
		projectIdStr,
		apiKeyStr,
		&branchName,
		previewBranch,
		new(config.DatabaseNeonBranchingTypeParentData),
		store,
	)
	if err != nil {
		return nil, nil, err
	}

	return sourceConnection, storageHandler, nil
}

type PruningHandlerInterface interface {
	Prune([]string) ([]string, error)
}

// GetPruningDatabaseHandler return a database.PruningHandlerInterface Object for the project to prune or an error
func GetPruningDatabaseHandler(dbConfig config.Database, store *warning.WarningStore) (PruningHandlerInterface, error) {
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

		branchParent, m1 := config.ReplaceVariables(dbConfig.Anonymization.Storage.Neon.BranchName, nil)
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

		projectIdStr, m2 := config.ReplaceVariables(*projectId, nil)
		apiKeyStr, m3 := config.ReplaceVariables(*apiKey, nil)
		for _, w := range warning.NewMissingEnvVarsWarnings(slices.Concat(m1, m2, m3)...) {
			store.AddWarningAndPrint(&w)
		}

		handler, err := NewNeonPruneHandler(
			projectIdStr,
			apiKeyStr,
			branchParent,
			dbConfig.PreviewBranch,
			store,
		)
		return handler, err
	}

	// No anonymization specified, basing everything on source configuration
	switch dbConfig.Source.Provider {
	case config.DatabaseProviderNeon:
		if dbConfig.Source.Neon == nil {
			return nil, errors.New("missing neon source database configuration")
		}

		// Prepare branching type
		branchingTypeStr, m1 := config.ReplaceVariables(string(dbConfig.Source.Neon.BranchingType), nil)
		branchingType := config.DatabaseNeonBranchingType(branchingTypeStr)
		if !branchingType.Valid() {
			branchingType = config.DatabaseNeonBranchingTypeParentData
		}

		projectId, m2 := config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, nil)
		apiKey, m3 := config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, nil)
		parentBranch, m4 := config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, nil)
		for _, w := range warning.NewMissingEnvVarsWarnings(slices.Concat(m1, m2, m3, m4)...) {
			store.AddWarningAndPrint(&w)
		}

		handler, err := NewNeonPruneHandler(
			projectId,
			apiKey,
			parentBranch,
			dbConfig.PreviewBranch,
			store,
		)
		return handler, err
	case config.DatabaseProviderGeneric:
		return nil, errors.New("generic provider without anonymization is currently unsupported")
	}

	return nil, errors.New(fmt.Sprintf("unsupported source database provider: %s", dbConfig.Source.Provider))
}
