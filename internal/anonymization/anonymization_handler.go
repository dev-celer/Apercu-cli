package anonymization

import (
	"apercu-cli/config"
	"apercu-cli/helper"
	"apercu-cli/helper/warning"
	"apercu-cli/output"
	"context"
	"log/slog"
	"slices"
)

type HandlerInterface interface {
	Anonymize(ctx context.Context) error
	GetOutput() *output.OutputDatabaseAnonymization
}

// GetDatabaseAnonymizer return a anonymization.HandlerInterface Object and optionally a Missing EnvVars warning
func GetDatabaseAnonymizer(dbConfig config.Database, sourceConnection helper.ConnectionFields, storageConnection helper.ConnectionFields) (HandlerInterface, *warning.MissingEnvVarsWarning) {
	if dbConfig.Source == nil || dbConfig.Anonymization == nil {
		slog.Debug("No source database specified")
		return nil, nil
	}

	env := make(map[string]string, len(dbConfig.Anonymization.Env))
	m1 := make([]string, 0)
	for k, v := range dbConfig.Anonymization.Env {
		var m []string
		env[k], m = config.ReplaceVariables(v, map[string]string{})
		m1 = append(m1, m...)
	}

	greenmaskConfig, m2 := config.ReplaceVariables(dbConfig.Anonymization.GreenmaskConfig, map[string]string{})
	w := warning.NewMissingEnvVarsWarning(slices.Concat(m1, m2)...)
	warning.PrintWarning(w)

	return NewGreenmaskHandler(
		sourceConnection,
		storageConnection,
		env,
		greenmaskConfig,
	), w
}
