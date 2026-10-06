package pg_contract

import (
	"apercu-cli/helper"
	"apercu-cli/helper/metrics"
	"apercu-cli/helper/warning_interface"
	"time"
)

type QueryEvent struct {
	SQL          string        `json:"sql"`
	StartedAt    time.Time     `json:"started_at"`
	Cycle        int           `json:"cycle"`
	Duration     time.Duration `json:"duration"`
	CommandTag   string        `json:"command_tag"`
	RowsAffected int64         `json:"rows_affected"`
	Error        string        `json:"error,omitempty"`
}

type QueryEventAnalysis struct {
	Event          *QueryEvent                 `json:"event"`
	Type           metrics.EventOperationType  `json:"type"`
	AffectedTables []helper.FullRelationName   `json:"affected_tables"`
	Warnings       []warning_interface.Warning `json:"warnings"`
	Lock           metrics.QueryLock           `json:"lock,omitempty"`
}
