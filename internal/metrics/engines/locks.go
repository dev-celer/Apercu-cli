package engines

import (
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"apercu-cli/output"
	"encoding/json"
	"log/slog"
	"strings"
	"time"
)

type LocksEngine struct {
	PgProxyEvents []metricshelper.QueryEvent
}

func NewLocksEngine() *LocksEngine {
	return &LocksEngine{PgProxyEvents: make([]metricshelper.QueryEvent, 0)}
}

func (e *LocksEngine) CollectPreMigrationMetrics() error { return nil }

func (e *LocksEngine) SendPgProxyLogs(logs string) {
	e.PgProxyEvents = make([]metricshelper.QueryEvent, 0)

	for line := range strings.Lines(logs) {
		query := metricshelper.QueryEvent{}
		err := json.Unmarshal([]byte(line), &query)
		if err != nil {
			slog.Debug("Error parsing query line", "line", line, "error", err)
			continue
		}

		// Filter locks type
		if query.Stats.Lock == nil {
			continue
		}
		switch *query.Stats.Lock {
		case metricshelper.QueryLockAccessExclusive:
		case metricshelper.QueryLockShareRowExclusive:
		case metricshelper.QueryLockShareUpdateExclusive:
		default:
			continue
		}

		e.PgProxyEvents = append(e.PgProxyEvents, query)
	}
}

func (e *LocksEngine) CollectPostMigrationMetrics() error {
	return nil
}

func (e *LocksEngine) StoreMetricsToOutput(metrics *output.OutputDatabaseMetrics) error {
	if metrics.Locks == nil {
		metrics.Locks = make(map[metricshelper.QueryLock]map[string]metricshelper.LockMetrics)
	}

	for _, query := range e.PgProxyEvents {
		if query.Stats.Lock == nil || query.Stats.Table == "" {
			continue
		}

		// Get lock map
		l, ok := metrics.Locks[*query.Stats.Lock]
		if !ok {
			l = make(map[string]metricshelper.LockMetrics)
		}

		// Get table map
		t, ok := l[query.Stats.Table]
		if !ok {
			t = metricshelper.LockMetrics{
				LockCount:     1,
				TotalDuration: query.Duration,
				MeanDuration:  query.Duration,
				MaxDuration:   query.Duration,
			}
		} else {
			t.LockCount++
			t.TotalDuration += query.Duration
			t.MeanDuration = t.TotalDuration / time.Duration(t.LockCount)
			if t.MaxDuration < query.Duration {
				t.MaxDuration = query.Duration
			}
		}

		l[query.Stats.Table] = t
		metrics.Locks[*query.Stats.Lock] = l
	}

	return nil
}

func (e *LocksEngine) GetWarnings() []warning.Warning {
	return nil
}
