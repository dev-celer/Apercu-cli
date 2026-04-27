package pgproxy

import (
	"encoding/json"
	"log/slog"
	"strings"
	"time"
)

type QueryEvent struct {
	SQL          string          `json:"sql"`
	StartedAt    time.Time       `json:"started_at"`
	Duration     time.Duration   `json:"duration"`
	CommandTag   string          `json:"command_tag"`
	RowsAffected int64           `json:"rows_affected"`
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

func ParseQueriesFromLogs(logs string) []QueryEvent {
	queries := make([]QueryEvent, 0)

	for line := range strings.Lines(logs) {
		query := QueryEvent{}
		err := json.Unmarshal([]byte(line), &query)
		if err != nil {
			slog.Debug("Error parsing query line", "line", line, "error", err)
			continue
		}

		queries = append(queries, query)
	}

	return queries
}
