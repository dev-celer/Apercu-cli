package metrics

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

func GetWALBytes(databaseUrl string) (int64, error) {
	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to database: %v", err)
	}
	defer func() { _ = db.Close() }()

	var wal_lsn string
	err = db.QueryRow("SELECT pg_current_wal_lsn()").Scan(&wal_lsn)
	if err != nil {
		return 0, fmt.Errorf("failed to query database for WAL LSN: %v", err)
	}

	parts := strings.SplitN(wal_lsn, "/", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid WAL LSN format, expected hex/hex, got %v", wal_lsn)
	}
	high, err := strconv.ParseInt(parts[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse WAL LSN high half %v: %v", parts[0], err)
	}
	low, err := strconv.ParseInt(parts[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse WAL LSN low half %v: %v", parts[1], err)
	}

	return high<<32 | low, nil
}
