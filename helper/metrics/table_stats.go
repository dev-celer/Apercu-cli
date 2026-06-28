package metrics

import "apercu-cli/helper"

type TableMetrics struct {
	RowCount int64 `json:"row_count"`
	// TableSize in bytes
	TableSize       int64            `json:"table_size"`
	WritesPerSecond *float64         `json:"writes_per_second,omitempty"`
	WriteActivity   TableActivity    `json:"write_activity"`
	WriteDecision   ActivityDecision `json:"write_decision"`
	ScanPerSecond   *float64         `json:"scans_per_second,omitempty"`
	ReadActivity    TableActivity    `json:"read_activity"`
	ReadDecision    ActivityDecision `json:"read_decision"`
}

type ActivityDecision string

var (
	ActivityDecisionPercentile ActivityDecision = "percentile"
	ActivityDecisionFloor      ActivityDecision = "floor"
	ActivityDecisionCeiling    ActivityDecision = "ceiling"
	ActivityDecisionLowCount   ActivityDecision = "low_count"
	ActivityDecisionNone       ActivityDecision = "none"
)

type DatabaseMetrics struct {
	// DatabaseSize in bytes
	DatabaseSize  int64
	ServerVersion float32 `json:"server_version"`
	TablesMetrics map[helper.FullTableName]TableMetrics
}

type TableActivity string

var (
	TableActivityNone TableActivity = "NONE"
	TableActivityHot  TableActivity = "HOT"
	TableActivityWarm TableActivity = "WARM"
	TableActivityCold TableActivity = "COLD"
)
