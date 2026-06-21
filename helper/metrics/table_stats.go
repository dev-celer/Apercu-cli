package metrics

import "apercu-cli/helper"

type TableMetrics struct {
	RowCount int64 `json:"row_count"`
	// TableSize in bytes
	TableSize       int64         `json:"table_size"`
	WritesPerSecond *float64      `json:"writes_per_second,omitempty"`
	WriteActivity   TableActivity `json:"write_activity"`
	ScanPerSecond   *float64      `json:"scans_per_second,omitempty"`
	ReadActivity    TableActivity `json:"read_activity"`
}

type DatabaseMetrics struct {
	// DatabaseSize in bytes
	DatabaseSize  int64
	TablesMetrics map[helper.FullTableName]TableMetrics
}

type TableActivity string

var (
	TableActivityNone TableActivity = "NONE"
	TableActivityHot  TableActivity = "HOT"
	TableActivityWarm TableActivity = "WARM"
	TableActivityCold TableActivity = "COLD"
)
