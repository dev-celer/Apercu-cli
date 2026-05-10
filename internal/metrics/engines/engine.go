package engines

import (
	metricshelper "apercu-cli/helper/metrics"
	helperpgproxy "apercu-cli/helper/pgproxy"
	"apercu-cli/output"
	"database/sql"
)

type MetricEngine interface {
	SetDatabase(*sql.DB)
	SendProdStats(map[string]map[string]metricshelper.TableStats)
	CollectPreMigrationMetrics() error
	SendPgProxyEvents([]helperpgproxy.QueryEvent) error
	CollectPostMigrationMetrics() error
	StoreMetricsToOutput(*output.OutputDatabaseMetrics) error
}
