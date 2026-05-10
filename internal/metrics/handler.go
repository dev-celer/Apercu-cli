package metrics

import (
	helperpgproxy "apercu-cli/helper/pgproxy"
	"apercu-cli/internal/metrics/engines"
	"apercu-cli/output"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq"
)

type MetricsHandler struct {
	engines []engines.MetricEngine
	output  *output.OutputDatabaseMetrics
	db      *sql.DB
}

func NewMetricsHandler(databaseUrl string) (*MetricsHandler, error) {
	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to connect to database: %v", err))
	}

	enginesObj, err := initializeEngines(db)
	if err != nil {
		return nil, err
	}

	return &MetricsHandler{
		engines: enginesObj,
		output:  output.NewOutputDatabaseMetrics(),
		db:      db,
	}, nil
}

func (h *MetricsHandler) Close() {
	_ = h.db.Close()
}

// initializeEngines is used to call all constructor for every metrics engines
// any new metrics engines should be added to this function
func initializeEngines(db *sql.DB) ([]engines.MetricEngine, error) {
	return []engines.MetricEngine{}, nil
}

func (h *MetricsHandler) Setup() error {
	prodMetrics, err := GetDatabaseStats(h.db)
	if err != nil {
		return err
	}

	for _, engine := range h.engines {
		engine.SetDatabase(h.db)
		engine.SendProdStats(prodMetrics)
	}
	return nil
}

func (h *MetricsHandler) CollectPreMigrationMetrics() error {
	for _, engine := range h.engines {
		err := engine.CollectPreMigrationMetrics()
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *MetricsHandler) SendPgProxyEvents(event []helperpgproxy.QueryEvent) error {
	for _, engine := range h.engines {
		err := engine.SendPgProxyEvents(event)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *MetricsHandler) CollectPostMigrationMetrics() error {
	for _, engine := range h.engines {
		err := engine.CollectPostMigrationMetrics()
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *MetricsHandler) GetOutput() (*output.OutputDatabaseMetrics, error) {
	for _, engine := range h.engines {
		err := engine.StoreMetricsToOutput(h.output)
		if err != nil {
			return nil, err
		}
	}
	return h.output, nil
}
