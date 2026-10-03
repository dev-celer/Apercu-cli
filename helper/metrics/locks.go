package metrics

import (
	"apercu-cli/helper"
	"apercu-cli/helper/warning_interface"
	"time"
)

const DefaultHotFloorReadActivity float64 = 1
const DefaultColdCeilingReadActivity float64 = 100
const DefaultHotFloorWriteActivity float64 = 1
const DefaultColdCeilingWriteActivity float64 = 100
const DefaultHotPercentile float64 = 0.75
const DefaultWarmPercentile float64 = 0.25

type QueryEvent struct {
	SQL          string        `json:"sql"`
	StartedAt    time.Time     `json:"started_at"`
	Duration     time.Duration `json:"duration"`
	CommandTag   string        `json:"command_tag"`
	RowsAffected int64         `json:"rows_affected"`
	Error        string        `json:"error,omitempty"`
}

type QueryEventAnalysis struct {
	Event          *QueryEvent                 `json:"event"`
	Type           EventOperationType          `json:"type"`
	AffectedTables []helper.FullRelationName   `json:"affected_tables"`
	Warnings       []warning_interface.Warning `json:"warnings"`
	Lock           QueryLock                   `json:"lock,omitempty"`
}

type EventOperationType string

var (
	EventOperationTypeMetadataOnly     EventOperationType = "metadata_only"
	EventOperationTypeScanUnderLock    EventOperationType = "scan_under_lock"
	EventOperationTypeRewriteUnderLock EventOperationType = "rewrite_under_lock"
	EventOperationTypeNonBlocking      EventOperationType = "non_blocking"
)

func (t EventOperationType) Severity() int {
	switch t {
	case EventOperationTypeRewriteUnderLock:
		return 3
	case EventOperationTypeScanUnderLock:
		return 2
	case EventOperationTypeMetadataOnly:
		return 1
	default:
		return 0
	}
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

// Strength ranks lock modes from weakest to strongest, following the ordering of
// the Postgres lock-mode table (ACCESS SHARE = 1 … ACCESS EXCLUSIVE = 8). An
// unknown or empty mode ranks below every real one.
//
// The ranking is a total order over a conflict table that is only a partial
// order (SHARE and SHARE UPDATE EXCLUSIVE do not dominate one another), so it is
// meant for "which of these is the worst" comparisons, not for deciding whether
// two modes conflict.
func (l QueryLock) Strength() int {
	switch l {
	case QueryLockAccessExclusive:
		return 8
	case QueryLockExclusive:
		return 7
	case QueryLockShareRowExclusive:
		return 6
	case QueryLockShare:
		return 5
	case QueryLockShareUpdateExclusive:
		return 4
	case QueryLockRowExclusive:
		return 3
	case QueryLockRowShare:
		return 2
	case QueryLockAccessShare:
		return 1
	default:
		return 0
	}
}

func (l QueryLock) IsReadBlocking() bool {
	return l == QueryLockAccessExclusive
}

func (l QueryLock) IsWriteBlocking() bool {
	return l == QueryLockAccessExclusive || l == QueryLockExclusive || l == QueryLockShareRowExclusive || l == QueryLockShare
}

type LockMetrics struct {
	LockCount     int64         `yaml:"lock_count" json:"lock_count"`
	TotalDuration time.Duration `yaml:"total_duration" json:"total_duration"`
	MeanDuration  time.Duration `yaml:"mean_duration" json:"mean_duration"`
	MaxDuration   time.Duration `yaml:"max_duration" json:"max_duration"`
}
