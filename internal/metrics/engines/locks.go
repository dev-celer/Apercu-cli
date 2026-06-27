package engines

import (
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/output"
	"encoding/json"
	"log/slog"
	"strings"
	"time"
)

type LocksEngine struct {
	ProdStats     metricshelper.DatabaseMetrics
	PgProxyEvents []metricshelper.QueryEventAnalysis
}

func NewLocksEngine(prodStats metricshelper.DatabaseMetrics) *LocksEngine {
	return &LocksEngine{
		ProdStats:     prodStats,
		PgProxyEvents: make([]metricshelper.QueryEventAnalysis, 0),
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

		// Filter locks type
		if query.Stats.Lock == nil {
			continue
		}
		switch *query.Stats.Lock {
		case metricshelper.QueryLockAccessExclusive:
		case metricshelper.QueryLockExclusive:
		case metricshelper.QueryLockShareRowExclusive:
		case metricshelper.QueryLockShare:
		default:
			continue
		}

		e.PgProxyEvents = append(e.PgProxyEvents, metricshelper.QueryEventAnalysis{
			Event: &query,
			Type:  metricshelper.EventOperationTypeNonBlocking,
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
		kind, remediation := metricshelper.ClassifyOperation(query.Event.SQL, e.ProdStats.ServerVersion)
		query.Type = kind
		query.Remediation = remediation
	}
	return nil
}

func (e *LocksEngine) StoreMetricsToOutput(metrics *output.OutputDatabaseMetrics) error {
	if metrics.Locks == nil {
		metrics.Locks = make(map[metricshelper.QueryLock]map[string]metricshelper.LockMetrics)
	}

	for _, query := range e.PgProxyEvents {
		if query.Event.Stats.Lock == nil || query.Event.Stats.Table == "" {
			continue
		}

		// Get lock map
		l, ok := metrics.Locks[*query.Event.Stats.Lock]
		if !ok {
			l = make(map[string]metricshelper.LockMetrics)
		}

		// Get table map
		t, ok := l[query.Event.Stats.Table]
		if !ok {
			t = metricshelper.LockMetrics{
				LockCount:     1,
				TotalDuration: query.Event.Duration,
				MeanDuration:  query.Event.Duration,
				MaxDuration:   query.Event.Duration,
			}
		} else {
			t.LockCount++
			t.TotalDuration += query.Event.Duration
			t.MeanDuration = t.TotalDuration / time.Duration(t.LockCount)
			if t.MaxDuration < query.Event.Duration {
				t.MaxDuration = query.Event.Duration
			}
		}

		l[query.Event.Stats.Table] = t
		metrics.Locks[*query.Event.Stats.Lock] = l
	}

	return nil
}
