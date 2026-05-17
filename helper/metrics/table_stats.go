package metrics

import "apercu-cli/helper"

type TableMetrics struct {
	RowCount int64
	// TableSize in bytes
	TableSize int64
}

type DatabaseMetrics struct {
	// DatabaseSize in bytes
	DatabaseSize  int64
	TablesMetrics map[helper.FullTableName]TableMetrics
}
