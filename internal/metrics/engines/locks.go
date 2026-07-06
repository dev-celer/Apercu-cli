package engines

import (
	metricshelper "apercu-cli/helper/metrics"
	parsinghelper "apercu-cli/helper/sql_parsing"
	"apercu-cli/helper/warning"
	"apercu-cli/output"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
)

type LocksEngine struct {
	prodDb             *sql.DB
	ProdStats          metricshelper.DatabaseMetrics
	PgProxyEvents      []metricshelper.QueryEventAnalysis
	WarningStore       *warning.WarningStore
	initialLockTimeout int64
}

func NewLocksEngine(prodDb *sql.DB, prodStats metricshelper.DatabaseMetrics, warningStore *warning.WarningStore) *LocksEngine {
	return &LocksEngine{
		prodDb:             prodDb,
		ProdStats:          prodStats,
		PgProxyEvents:      make([]metricshelper.QueryEventAnalysis, 0),
		WarningStore:       warningStore,
		initialLockTimeout: 0,
	}
}

func (e *LocksEngine) CollectPreMigrationMetrics() error {
	// Get the current value of lock_timeout from the production database before any migration statement
	err := e.prodDb.QueryRow("SELECT setting::int FROM pg_settings WHERE name = 'lock_timeout';").Scan(&e.initialLockTimeout)
	if err != nil {
		return err
	}
	return nil
}

func (e *LocksEngine) SendPgProxyLogs(logs string) {
	slog.Debug("Start pg proxy logs parsing for locks detection")
	e.PgProxyEvents = make([]metricshelper.QueryEventAnalysis, 0)

	currentLockTimeout := e.initialLockTimeout

	for line := range strings.Lines(logs) {
		query := metricshelper.QueryEvent{}
		err := json.Unmarshal([]byte(line), &query)
		if err != nil {
			slog.Debug("Error parsing query line", "line", line, "error", err)
			continue
		}

		// Detect if the lock_timeout value was changed
		hasChanged, lockTimeout := parsinghelper.GetLockTimeoutValue(query.SQL)
		if hasChanged {
			if lockTimeout == nil {
				currentLockTimeout = e.initialLockTimeout
			} else {
				currentLockTimeout = *lockTimeout
			}
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

		tables := parsinghelper.ParseTables(query.SQL)

		// Ensure that lock_timeout is set
		if currentLockTimeout == 0 {
			for _, table := range tables {
				w := warning.NewLockTimeoutWarning(table)
				e.WarningStore.AddWarning(w)
			}
		}

		e.PgProxyEvents = append(e.PgProxyEvents, metricshelper.QueryEventAnalysis{
			Event:          &query,
			Type:           metricshelper.EventOperationTypeNonBlocking,
			AffectedTables: tables,
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
