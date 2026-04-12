package migration

import (
	"apercu-cli/config"
	"apercu-cli/helper"
	"apercu-cli/output"
	"context"
	"log/slog"
	"strconv"
)

type HandlerInterface interface {
	Apply(ctx context.Context) error
	GetOutput() *output.OutputDatabaseMigration
}

func GetMigrationHandler(dbConfig config.Database, connection helper.ConnectionFields) HandlerInterface {
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
		config.ReplaceVariables(dbConfig.Migration.Image, internalEnv),
		commands,
		env,
		workDir,
		config.ReplaceVariables(dbConfig.Migration.LocalDir, internalEnv),
		connection.Url,
	)
}
