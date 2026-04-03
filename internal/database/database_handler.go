package database

import (
	"apercu-cli/config"
	"errors"
	"fmt"
	"log/slog"
)

type ConnectionFields struct {
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	User     string `json:"user" yaml:"user"`
	Password string `json:"password" yaml:"password"`
	Database string `json:"database" yaml:"database"`
	Url      string `json:"url" yaml:"url"`
}

type HandlerInterface interface {
	Apply() error
	Cleanup() error
	Reset() error
	GetConnectionFields() (ConnectionFields, error)
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
