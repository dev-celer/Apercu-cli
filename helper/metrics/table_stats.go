package metrics

import "apercu-cli/helper"

type TableMetrics struct {
	RowCount int64 `json:"row_count"`
	// TableSize in bytes
	TableSize int64 `json:"table_size"`
}

type DatabaseMetrics struct {
	// DatabaseSize in bytes
	DatabaseSize  int64
	TablesMetrics map[helper.FullTableName]TableMetrics
}
