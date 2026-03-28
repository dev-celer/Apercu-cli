package database

import (
	"apercu-cli/config"
	"errors"
	"fmt"
	"log/slog"
)

type HandlerInterface interface {
	Apply() error
	Cleanup() error
	Reset() error
	GetDatabaseUrl() string
}

func GetSourceDatabaseHandler(dbConfig config.Database) (HandlerInterface, error) {
	if dbConfig.Source == nil {
		slog.Debug("No source database specified")
		return nil, nil
	}

	switch dbConfig.Source.Provider {
	case config.DatabaseProviderNeon:
		if dbConfig.Source.Neon == nil {
			return nil, errors.New("missing neon source database configuration")
		}
		return NewNeonBranchHandler(
			config.ReplaceVariables(dbConfig.Source.Neon.ProjectId, map[string]string{}),
			config.ReplaceVariables(dbConfig.Source.Neon.ApiKey, map[string]string{}),
			config.ReplaceVariables(dbConfig.Source.Neon.ParentBranch, map[string]string{}),
			config.ReplaceVariables(dbConfig.Source.Neon.PreviewBranch, map[string]string{}),
		)
	}

	return nil, errors.New(fmt.Sprintf("unsupported source database provider: %s", dbConfig.Source.Provider))
}
