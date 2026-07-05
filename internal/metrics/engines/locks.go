package engines

import (
	metricshelper "apercu-cli/helper/metrics"
	parsinghelper "apercu-cli/helper/sql_parsing"
	"apercu-cli/helper/warning"
	"apercu-cli/output"
	"encoding/json"
	"log/slog"
	"strings"
)

type LocksEngine struct {
	ProdStats     metricshelper.DatabaseMetrics
	PgProxyEvents []metricshelper.QueryEventAnalysis
	WarningStore  *warning.WarningStore
}

func NewLocksEngine(prodStats metricshelper.DatabaseMetrics, warningStore *warning.WarningStore) *LocksEngine {
	return &LocksEngine{
		ProdStats:     prodStats,
		PgProxyEvents: make([]metricshelper.QueryEventAnalysis, 0),
		WarningStore:  warningStore,
	}
}

func (e *LocksEngine) CollectPreMigrationMetrics() error { return nil }

func (e *LocksEngine) SendPgProxyLogs(logs string) {
	slog.Debug("Start pg proxy logs parsing for locks detection")
	e.PgProxyEvents = make([]metricshelper.QueryEventAnalysis, 0)

	for line := range strings.Lines(logs) {
		query := metricshelper.QueryEvent{}
		err := json.Unmarshal([]byte(line), &query)
		if err != nil {
			slog.Debug("Error parsing query line", "line", line, "error", err)
			continue
		}

		lock := parsinghelper.GetLockType(query.SQL)
		// Filter locks type
		if lock == nil {
			continue
		}
		switch *lock {
		case metricshelper.QueryLockAccessExclusive:
		case metricshelper.QueryLockExclusive:
		case metricshelper.QueryLockShareRowExclusive:
		case metricshelper.QueryLockShare:
		default:
			continue
		}

		e.PgProxyEvents = append(e.PgProxyEvents, metricshelper.QueryEventAnalysis{
			Event:          &query,
			Type:           metricshelper.EventOperationTypeNonBlocking,
			AffectedTables: parsinghelper.ParseTables(query.SQL),
			Warnings:       make([]warning.Warning, 0),
			Lock:           *lock,
		})
	}
	slog.Debug("Pg proxy logs parsing for locks detection complete", "locks_count", len(e.PgProxyEvents))
}

func (e *LocksEngine) CollectPostMigrationMetrics() error {
	// Analyze all query and infer operation type and remediation step
	for i := range e.PgProxyEvents {
		query := &e.PgProxyEvents[i]
		if query.Event == nil {
			continue
		}
		parsinghelper.ClassifyOperation(query, e.ProdStats.ServerVersion, e.WarningStore, &e.ProdStats)
	}
	return nil
}

func (e *LocksEngine) StoreMetricsToOutput(metrics *output.OutputDatabaseMetrics) error {
	return nil
}
