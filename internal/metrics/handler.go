package metrics

import (
	"apercu-cli/config"
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"apercu-cli/internal/metrics/engines"
	"apercu-cli/output"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq"
)

type MetricsHandler struct {
	engines   []engines.MetricEngine
	output    *output.OutputDatabaseMetrics
	prodDb    *sql.DB
	previewDb *sql.DB
}

func NewMetricsHandler(prodDbUrl, previewDbUrl string, dbConfig *config.Database, fullConfig *config.Config) (*MetricsHandler, error) {
	prodDb, err := sql.Open("postgres", prodDbUrl)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to connect to prod database: %v", err))
	}

	prodMetrics, err := GetDatabaseStats(prodDb)
	if err != nil {
		_ = prodDb.Close()
		return nil, err
	}
	metricsOutput := output.NewOutputDatabaseMetrics()
	metricsOutput.Prod = prodMetrics

	previewDb, err := sql.Open("postgres", previewDbUrl)
	if err != nil {
		_ = prodDb.Close()
		return nil, errors.New(fmt.Sprintf("Failed to connect to preview database: %v", err))
	}
	enginesObj, err := initializeEngines(prodDb, previewDb, prodMetrics, dbConfig, fullConfig)
	if err != nil {
		_ = prodDb.Close()
		_ = previewDb.Close()
		return nil, err
	}

	return &MetricsHandler{
		engines:   enginesObj,
		output:    metricsOutput,
		prodDb:    prodDb,
		previewDb: previewDb,
	}, nil
}

func (h *MetricsHandler) Close() {
	_ = h.prodDb.Close()
	_ = h.previewDb.Close()
}

// initializeEngines is used to call all constructor for every metrics engines
// any new metrics engines should be added to this function
func initializeEngines(prodDb, previewDb *sql.DB, prodStats metricshelper.DatabaseMetrics, dbConfig *config.Database, fullConfig *config.Config) ([]engines.MetricEngine, error) {
	enginesList := make([]engines.MetricEngine, 0)

	enginesList = append(enginesList, engines.NewLocksEngine())

	queryEngine, err := engines.NewExplainQueryEngine(previewDb, dbConfig)
	if err != nil {
		return nil, err
	}
	enginesList = append(enginesList, queryEngine)

	enginesList = append(enginesList, engines.NewSchemaDiffEngine(previewDb))

	enginesList = append(enginesList, engines.NewSizeEngine(previewDb, prodStats))

	enginesList = append(enginesList, engines.NewRewriteEngine(previewDb, prodStats))

	return enginesList, nil
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

func (h *MetricsHandler) CollectPostMigrationMetrics(pgProxyLogs string) error {
	for _, engine := range h.engines {
		engine.SendPgProxyLogs(pgProxyLogs)
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

func (h *MetricsHandler) GetWarnings() []warning.Warning {
	warnings := make([]warning.Warning, 0)
	for _, engine := range h.engines {
		warnings = append(warnings, engine.GetWarnings()...)
	}
	return warnings
}
