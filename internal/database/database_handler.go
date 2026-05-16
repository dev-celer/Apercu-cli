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
	GetWarnings() []warning.Warning
}

// getProdDatabaseConn return a ConnectionFields object to the prod database, an missingEnvVarsWarning (optionally), or an error
func getProdDatabaseConn(dbConfig config.Database) (helper.ConnectionFields, *warning.MissingEnvVarsWarning, error) {
	if dbConfig.Source == nil {
		slog.Debug("No source database specified")
		return helper.ConnectionFields{}, nil, nil
	}

	switch dbConfig.Source.Provider {
	case config.DatabaseProviderNeon:
		if dbConfig.Source.Neon == nil {
			return helper.ConnectionFields{}, nil, errors.New("missing neon source database configuration")
		}
		projectId, m1 := config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, nil)
		apiKey, m2 := config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, nil)
		parentBranch, m3 := config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, nil)
		w := warning.NewMissingEnvVarsWarning(slices.Concat(m1, m2, m3)...)
		warning.PrintWarning(w)

		neonHandler, err := NewNeonHandler(
			projectId,
			apiKey,
			nil,
			parentBranch,
			nil,
		)
		if err != nil {
			return helper.ConnectionFields{}, w, err
		}

		conn, err := neonHandler.GetConnectionFields()
		return conn, w, err
	case config.DatabaseProviderGeneric:
		if dbConfig.Source.Generic == nil {
			return helper.ConnectionFields{}, nil, errors.New("missing generic source database configuration")
		}
		dbUrl, m := config.ReplaceVariables(dbConfig.Source.Generic.DatabaseUrl, nil)
		w := warning.NewMissingEnvVarsWarning(m...)
		warning.PrintWarning(w)

		conn, err := helper.ExtractConnectionFieldsFromUrl(dbUrl)
		return conn, w, err
	}

	return helper.ConnectionFields{}, nil, errors.New(fmt.Sprintf("unsupported source database provider: %s", dbConfig.Source.Provider))
}

// GetPreviewDatabaseHandler return a ConnectionFields object to the prod database, a database.HandlerInterface Object for the preview branch, an missingEnvVarsWarning (optionally), or an error
func GetPreviewDatabaseHandler(dbConfig config.Database) (*helper.ConnectionFields, HandlerInterface, *warning.MissingEnvVarsWarning, error) {
	if dbConfig.Source == nil {
		slog.Debug("No source database specified")
		return nil, nil, nil, nil
	}
	if dbConfig.PreviewBranch == "" {
		slog.Debug("No preview branch pattern specified")
		return nil, nil, nil, nil
	}

	// If anonymization is enabled, use the storage database information
	if dbConfig.Anonymization != nil {
		if dbConfig.Anonymization.Storage.Neon == nil {
			return nil, nil, nil, fmt.Errorf("no storage Neon configuration specified")
		}

		branchParent, m1 := config.ReplaceVariables(dbConfig.Anonymization.Storage.Neon.BranchName, nil)
		apiKey := dbConfig.Anonymization.Storage.Neon.ApiKey
		projectId := dbConfig.Anonymization.Storage.Neon.ProjectId

		// If projectId or ApiKey are nil, try to retrieve them from neon source configuration
		if apiKey == nil || projectId == nil {
			if dbConfig.Source.Provider != config.DatabaseProviderNeon || dbConfig.Source.Neon == nil {
				return nil, nil, nil, fmt.Errorf("missing neon ApiKey / ProjectId configuration in anonymization")
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
		w := warning.NewMissingEnvVarsWarning(slices.Concat(m1, m2, m3, m4)...)
		warning.PrintWarning(w)

		neonHandler, err := NewNeonHandler(
			projectIdStr,
			apiKeyStr,
			&branchParent,
			previewBranch,
			new(config.DatabaseNeonBranchingTypeParentData),
		)
		if err != nil {
			return nil, nil, w, err
		}

		prodConn, w2, err := getProdDatabaseConn(dbConfig)
		if err != nil {
			return nil, nil, warning.MergeEnvVarsWarning(w, w2), err
		}

		return &prodConn, neonHandler, warning.MergeEnvVarsWarning(w, w2), nil
	}

	// No anonymization specified, basing everything on source configuration
	switch dbConfig.Source.Provider {
	case config.DatabaseProviderNeon:
		if dbConfig.Source.Neon == nil {
			return nil, nil, nil, errors.New("missing neon source database configuration")
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
		w := warning.NewMissingEnvVarsWarning(slices.Concat(m1, m2, m3, m4, m5)...)
		warning.PrintWarning(w)

		neonHandler, err := NewNeonHandler(
			projectId,
			apiKey,
			&parentBranch,
			previewBranch,
			&branchingType,
		)
		if err != nil {
			return nil, nil, w, err
		}

		prodConn, w2, err := getProdDatabaseConn(dbConfig)
		if err != nil {
			return nil, nil, warning.MergeEnvVarsWarning(w, w2), err
		}

		return &prodConn, neonHandler, warning.MergeEnvVarsWarning(w, w2), nil
	case config.DatabaseProviderGeneric:
		return nil, nil, nil, errors.New("generic provider without anonymization is currently unsupported")
	}

	return nil, nil, nil, errors.New(fmt.Sprintf("unsupported source database provider: %s", dbConfig.Source.Provider))
}

// GetAnonymizationDatabaseHandlers return a ConnectionFields object to the prod database, a database.HandlerInterface Object for the storage branch, an missingEnvVarsWarning (optionally), or an error
func GetAnonymizationDatabaseHandlers(dbConfig config.Database) (*helper.ConnectionFields, HandlerInterface, *warning.MissingEnvVarsWarning, error) {
	if dbConfig.Source == nil || dbConfig.Anonymization == nil {
		slog.Debug("No source database specified")
		return nil, nil, nil, nil
	}

	var w *warning.MissingEnvVarsWarning

	// Getting source database connection
	var sourceConnection *helper.ConnectionFields
	switch dbConfig.Source.Provider {
	case config.DatabaseProviderNeon:
		if dbConfig.Source.Neon == nil {
			if dbConfig.Source.Neon == nil {
				return nil, nil, nil, errors.New("missing neon source database configuration")
			}

			projectId, m1 := config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, nil)
			apiKey, m2 := config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, nil)
			parentBranch, m3 := config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, nil)
			w = warning.NewMissingEnvVarsWarning(slices.Concat(m1, m2, m3)...)
			warning.PrintWarning(w)

			sourceHandler, err := NewNeonHandler(
				projectId,
				apiKey,
				nil,
				parentBranch,
				nil,
			)
			if err != nil {
				return nil, nil, w, err
			}

			sourceConn, err := sourceHandler.GetConnectionFields()
			if err != nil {
				return nil, nil, w, err
			}
			sourceConnection = &sourceConn
		}
	case config.DatabaseProviderGeneric:
		if dbConfig.Source.Generic == nil {
			return nil, nil, nil, errors.New("missing generic source database configuration")
		}
		dbUrl, m := config.ReplaceVariables(dbConfig.Source.Generic.DatabaseUrl, nil)
		w = warning.NewMissingEnvVarsWarning(m...)
		warning.PrintWarning(w)
		sourceConn, err := helper.ExtractConnectionFieldsFromUrl(dbUrl)
		if err != nil {
			return nil, nil, w, err
		}
		sourceConnection = &sourceConn
	default:
		return nil, nil, nil, fmt.Errorf("unsupported source database provider: %s", dbConfig.Source.Provider)
	}

	// Get storage database handler
	if dbConfig.Anonymization.Storage.Neon == nil {
		return nil, nil, nil, errors.New("missing storage database configuration")
	}

	apiKey := dbConfig.Anonymization.Storage.Neon.ApiKey
	projectId := dbConfig.Anonymization.Storage.Neon.ProjectId

	// If projectId or ApiKey are nil, try to retrieve them from neon source configuration
	if apiKey == nil || projectId == nil {
		if dbConfig.Source.Provider != config.DatabaseProviderNeon || dbConfig.Source.Neon == nil {
			return nil, nil, w, fmt.Errorf("missing neon ApiKey / ProjectId configuration in anonymization")
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
	w2 := warning.NewMissingEnvVarsWarning(slices.Concat(m1, m2, m3, m4)...)
	warning.PrintWarning(w2)
	w = warning.MergeEnvVarsWarning(w, w2)

	storageHandler, err := NewNeonHandler(
		projectIdStr,
		apiKeyStr,
		&branchName,
		previewBranch,
		new(config.DatabaseNeonBranchingTypeParentData),
	)
	if err != nil {
		return nil, nil, w, err
	}

	return sourceConnection, storageHandler, w, nil
}

type PruningHandlerInterface interface {
	Prune([]string) ([]string, error)
	GetWarnings() []warning.Warning
}

// GetPruningDatabaseHandler return a database.PruningHandlerInterface Object for the project to prune, an missingEnvVarsWarning (optionally), or an error
func GetPruningDatabaseHandler(dbConfig config.Database) (PruningHandlerInterface, *warning.MissingEnvVarsWarning, error) {
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
		w := warning.NewMissingEnvVarsWarning(slices.Concat(m1, m2, m3)...)
		warning.PrintWarning(w)

		handler, err := NewNeonPruneHandler(
			projectIdStr,
			apiKeyStr,
			branchParent,
			dbConfig.PreviewBranch,
		)
		return handler, w, err
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
		w := warning.NewMissingEnvVarsWarning(slices.Concat(m1, m2, m3, m4)...)
		warning.PrintWarning(w)

		handler, err := NewNeonPruneHandler(
			projectId,
			apiKey,
			parentBranch,
			dbConfig.PreviewBranch,
		)
		return handler, w, err
	case config.DatabaseProviderGeneric:
		return nil, nil, errors.New("generic provider without anonymization is currently unsupported")
	}

	return nil, nil, errors.New(fmt.Sprintf("unsupported source database provider: %s", dbConfig.Source.Provider))
}
