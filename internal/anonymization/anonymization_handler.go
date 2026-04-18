package anonymization

import (
	"apercu-cli/config"
	"apercu-cli/helper"
	"apercu-cli/output"
	"context"
	"log/slog"
)

type HandlerInterface interface {
	Anonymize(ctx context.Context) error
	GetOutput() *output.OutputDatabaseAnonymization
}

func GetDatabaseAnonymizer(dbConfig config.Database, sourceConnection helper.ConnectionFields, storageConnection helper.ConnectionFields) HandlerInterface {
	if dbConfig.Source == nil || dbConfig.Anonymization == nil {
		slog.Debug("No source database specified")
		return nil
	}

	env := make(map[string]string, len(dbConfig.Anonymization.Env))
	for k, v := range dbConfig.Anonymization.Env {
		env[k] = config.ReplaceVariables(v, map[string]string{})
	}

	return NewGreenmaskHandler(
		sourceConnection,
		storageConnection,
		env,
		config.ReplaceVariables(dbConfig.Anonymization.GreenmaskConfig, map[string]string{}),
	)
}
