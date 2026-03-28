package migration

import (
	"apercu-cli/config"
	"context"
	"log/slog"
	"time"
)

type HandlerInterface interface {
	GetCount() (int, error)
	Apply(ctx context.Context) error
	GetDuration() *time.Duration
	GetOutput() string
}

func GetMigrationHandler(dbConfig config.Database, databaseUrl string) HandlerInterface {
	if dbConfig.Migration == nil {
		slog.Debug("No migration specified")
		return nil
	}

	internalEnv := map[string]string{
		"PREVIEW_DATABASE_URL": databaseUrl,
	}

	commands := make([]string, len(dbConfig.Migration.Command))
	for i, command := range dbConfig.Migration.Command {
		commands[i] = config.ReplaceVariables(command, internalEnv)
	}

	env := make(map[string]string)
	for k, v := range dbConfig.Migration.Env {
		env[k] = config.ReplaceVariables(v, internalEnv)
	}

	return NewDockerHandler(
		config.ReplaceVariables(dbConfig.Migration.Runner, internalEnv),
		commands,
		env,
	)
}
