package anonymization

import (
	"apercu-cli/config"
	"apercu-cli/output"
	"context"
	"errors"
	"fmt"
	"log/slog"
)

const GREENMASK_IMAGE = "greenmask/greenmask:v0.2.18"

type HandlerInterface interface {
	Anonymize(ctx context.Context) error
	GetOutput() *output.OutputDatabaseAnonymization
}

func GetDatabaseAnonymizer(dbConfig config.Database) (HandlerInterface, error) {
	if dbConfig.Source == nil || dbConfig.Anonymization == nil {
		slog.Debug("No source database specified")
		return nil, nil
	}

	switch dbConfig.Source.Provider {
	case config.DatabaseProviderNeon:
		if dbConfig.Anonymization.Storage.Neon == nil {
			return nil, errors.New("missing neon anonymization storage configuration")
		}
		if dbConfig.Source.Neon == nil {
			return nil, errors.New("missing neon source database configuration")
		}

		parentProjectId := config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, map[string]string{})
		var storageProjectId string
		if dbConfig.Anonymization.Storage.Neon.ProjectId != nil {
			storageProjectId = config.ReplaceVariables(*dbConfig.Anonymization.Storage.Neon.ProjectId, map[string]string{})
		} else {
			storageProjectId = parentProjectId
		}

		var storageApiKey *string
		if dbConfig.Anonymization.Storage.Neon.ApiKey != nil {
			storageApiKey = new(config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, map[string]string{}))
		}

		env := make(map[string]string, len(dbConfig.Anonymization.Env))
		for k, v := range dbConfig.Anonymization.Env {
			env[k] = config.ReplaceVariables(v, map[string]string{})
		}

		return NewNeonBranchAnonymizerHandler(
			config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, map[string]string{}),
			config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, map[string]string{}),
			config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, map[string]string{}),
			config.ReplaceVariables(dbConfig.Anonymization.Storage.Neon.BranchName, map[string]string{}),
			storageApiKey,
			storageProjectId,
			env,
			config.ReplaceVariables(dbConfig.Anonymization.GreenmaskConfig, map[string]string{}),
		)
	}

	return nil, errors.New(fmt.Sprintf("unsupported database provider: %s", dbConfig.Source.Provider))
}
