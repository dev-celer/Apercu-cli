package seeding

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"apercu-cli/output"
	"log/slog"
)

type HandlerInterface interface {
	Close() error
	Apply()
	GetOutput() *output.OutputDatabaseSeeding
}

func GetSeedingHandler(dbConfig config.Database, state *config.DatabaseState, connection database.ConnectionFields) (HandlerInterface, error) {
	if len(dbConfig.Seed) == 0 {
		slog.Debug("No seed specified")
		return nil, nil
	}

	return NewDirectSeed(
		connection,
		dbConfig.Seed,
		state,
	)
}
