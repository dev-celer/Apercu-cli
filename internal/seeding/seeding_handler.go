package seeding

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"log/slog"
	"time"
)

type HandlerInterface interface {
	Close() error
	Apply()
	GetDuration() *time.Duration
	GetAppliedCount() int
	GetFailedCount() int
	GetOutput() string
}

func GetSeedingHandler(dbConfig config.Database, connection database.ConnectionFields) (HandlerInterface, error) {
	if len(dbConfig.Seed) == 0 {
		slog.Debug("No seed specified")
		return nil, nil
	}

	return NewDirectSeed(
		connection,
		dbConfig.Seed,
	)
}
