package metrics

import "time"

type QueryEvent struct {
	SQL          string          `json:"sql"`
	StartedAt    time.Time       `json:"started_at"`
	Duration     time.Duration   `json:"duration"`
	CommandTag   string          `json:"command_tag"`
	RowsAffected int64           `json:"rows_affected"`
	LocksTimeout *int64          `json:"locks_timeout,omitempty"`
	Error        string          `json:"error,omitempty"`
	Stats        QueryEventStats `json:"stats,omitempty"`
}

type QueryEventStats struct {
	Lock  *QueryLock `json:"lock,omitempty"`
	Table string     `json:"table,omitempty"`
}

type QueryLock string

const (
	QueryLockAccessExclusive      QueryLock = "ACCESS_EXCLUSIVE"
	QueryLockExclusive            QueryLock = "EXCLUSIVE"
	QueryLockShareRowExclusive    QueryLock = "SHARE_ROW_EXCLUSIVE"
	QueryLockShare                QueryLock = "SHARE"
	QueryLockShareUpdateExclusive QueryLock = "SHARE_UPDATE_EXCLUSIVE"
	QueryLockRowExclusive         QueryLock = "ROW_EXCLUSIVE"
	QueryLockRowShare             QueryLock = "ROW_SHARE"
	QueryLockAccessShare          QueryLock = "ACCESS_SHARE"
)

type LockMetrics struct {
	LockCount     int64         `yaml:"lock_count" json:"lock_count"`
	TotalDuration time.Duration `yaml:"total_duration" json:"total_duration"`
	MeanDuration  time.Duration `yaml:"mean_duration" json:"mean_duration"`
	MaxDuration   time.Duration `yaml:"max_duration" json:"max_duration"`
}
