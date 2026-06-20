package metrics

import "apercu-cli/helper"

type TableMetrics struct {
	RowCount int64 `json:"row_count"`
	// TableSize in bytes
	TableSize       int64    `json:"table_size"`
	WritesPerSecond *float64 `json:"writes_per_second,omitempty"`
	ScanPerSecond   *float64 `json:"reads_per_second,omitempty"`
}

type DatabaseMetrics struct {
	// DatabaseSize in bytes
	DatabaseSize  int64
	TablesMetrics map[helper.FullTableName]TableMetrics
}
