package migration

import (
	"apercu-cli/config"
	"apercu-cli/helper"
	"apercu-cli/helper/database_url"
	"apercu-cli/helper/warning"
	"apercu-cli/output"
	"context"
	"fmt"
	"log/slog"
	"slices"
)

type HandlerInterface interface {
	Apply(ctx context.Context) error
	GetOutput() *output.OutputDatabaseMigration
	GetWarnings() []warning.Warning
}

// GetMigrationHandler return an migration.HandlerInterface Object, a Missing EnvVars Warning (optionally), or an error
func GetMigrationHandler(dbConfig config.Database, connection *helper.ConnectionFields) (HandlerInterface, *warning.MissingEnvVarsWarning, error) {
	if dbConfig.Migration == nil {
		slog.Debug("No migration specified")
		return nil, nil, nil
	}

	proxyHost, proxyPort := "apercu-pgproxy", "5432"
	proxyUrl, err := database_url.RewriteDatabaseUrlHostAndPort(connection.Url, proxyHost, proxyPort)
	if err != nil {
		return nil, nil, fmt.Errorf("could not rewrite database url: %w", err)
	}
	internalEnv := map[string]string{
		"PREVIEW_DATABASE_URL": proxyUrl,
		"PREVIEW_USER":         connection.User,
		"PREVIEW_PASSWORD":     connection.Password,
		"PREVIEW_HOST":         proxyHost,
		"PREVIEW_DATABASE":     connection.Database,
		"PREVIEW_PORT":         proxyPort,
	}

	m1 := make([]string, 0)
	commands := make([]string, len(dbConfig.Migration.Command))
	for i, command := range dbConfig.Migration.Command {
		var m []string
		commands[i], m = config.ReplaceVariables(command, internalEnv)
		m1 = append(m1, m...)
	}

	env := make(map[string]string)
	for k, v := range dbConfig.Migration.Env {
		var m []string
		env[k], m = config.ReplaceVariables(v, internalEnv)
		m1 = append(m1, m...)
	}

	workDir := "/data"
	var m2 []string
	if dbConfig.Migration.WorkDir != nil {
		workDir, m2 = config.ReplaceVariables(*dbConfig.Migration.WorkDir, internalEnv)
	}

	image, m3 := config.ReplaceVariables(dbConfig.Migration.Image, internalEnv)
	localDir, m4 := config.ReplaceVariables(dbConfig.Migration.LocalDir, internalEnv)
	w := warning.NewMissingEnvVarsWarning(slices.Concat(m1, m2, m3, m4)...)
	warning.PrintWarning(w)

	return NewDockerHandler(
		image,
		commands,
		env,
		workDir,
		localDir,
		connection,
	), w, nil
}
