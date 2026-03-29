package migration

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"context"
	"log/slog"
	"strconv"
	"time"
)

type HandlerInterface interface {
	GetCount() (int, error)
	Apply(ctx context.Context) error
	GetDuration() *time.Duration
	GetOutput() string
}

func GetMigrationHandler(dbConfig config.Database, connection database.ConnectionFields) HandlerInterface {
	if dbConfig.Migration == nil {
		slog.Debug("No migration specified")
		return nil
	}

	internalEnv := map[string]string{
		"PREVIEW_DATABASE_URL": connection.Url,
		"PREVIEW_USER":         connection.User,
		"PREVIEW_PASSWORD":     connection.Password,
		"PREVIEW_HOST":         connection.Host,
		"PREVIEW_DATABASE":     connection.Database,
		"PREVIEW_PORT":         strconv.Itoa(connection.Port),
	}

	commands := make([]string, len(dbConfig.Migration.Command))
	for i, command := range dbConfig.Migration.Command {
		commands[i] = config.ReplaceVariables(command, internalEnv)
	}

	env := make(map[string]string)
	for k, v := range dbConfig.Migration.Env {
		env[k] = config.ReplaceVariables(v, internalEnv)
	}

	workDir := "/data"
	if dbConfig.Migration.WorkDir != nil {
		workDir = config.ReplaceVariables(*dbConfig.Migration.WorkDir, internalEnv)
	}

	return NewDockerHandler(
		config.ReplaceVariables(dbConfig.Migration.Runner, internalEnv),
		commands,
		env,
		workDir,
		config.ReplaceVariables(dbConfig.Migration.LocalFolder, internalEnv),
		connection.Url,
	)
}
