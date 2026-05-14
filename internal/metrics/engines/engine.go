package engines

import (
	"apercu-cli/helper/warning"
	"apercu-cli/output"
)

type MetricEngine interface {
	CollectPreMigrationMetrics() error
	SendPgProxyLogs(string)
	CollectPostMigrationMetrics() error
	StoreMetricsToOutput(*output.OutputDatabaseMetrics) error
	GetWarnings() []warning.Warning
}
