package engines

import (
	"apercu-cli/output"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

type SizeEngine struct {
	db                           *sql.DB
	initialSize, finalSize       int64
	initialWALSize, finalWALSize uint64
}

func NewSizeEngine(db *sql.DB) *SizeEngine {
	return &SizeEngine{
		db: db,
	}
}

func (e *SizeEngine) CollectPreMigrationMetrics() error {
	initialSize, err := e.getDatabaseStorageInBytes()
	if err != nil {
		return err
	}
	initialWALSize, err := e.getWALBytes()
	if err != nil {
		return err
	}
	e.initialSize = initialSize
	e.initialWALSize = initialWALSize
	return nil
}

func (e *SizeEngine) SendPgProxyLogs(s string) {}

func (e *SizeEngine) CollectPostMigrationMetrics() error {
	finalSize, err := e.getDatabaseStorageInBytes()
	if err != nil {
		return err
	}
	finalWALSize, err := e.getWALBytes()
	if err != nil {
		return err
	}
	e.finalSize = finalSize
	e.finalWALSize = finalWALSize
	return nil
}

func (e *SizeEngine) StoreMetricsToOutput(metrics *output.OutputDatabaseMetrics) error {
	metrics.Storage = &output.OutputDatabaseStorageMetrics{
		InitialSize: e.initialSize,
		FinalSize:   e.finalSize,
		SizeDelta:   e.finalSize - e.initialSize,
		WALDelta:    e.finalWALSize - e.initialWALSize,
	}
	return nil
}

func (e *SizeEngine) getDatabaseStorageInBytes() (int64, error) {
	var size int64
	err := e.db.QueryRow("SELECT pg_database_size(current_database())").Scan(&size)
	if err != nil {
		return 0, fmt.Errorf("failed to query database for size: %v", err)
	}

	return size, nil
}

func (e *SizeEngine) getWALBytes() (uint64, error) {
	var wal_lsn string
	err := e.db.QueryRow("SELECT pg_current_wal_lsn()").Scan(&wal_lsn)
	if err != nil {
		return 0, fmt.Errorf("failed to query database for WAL LSN: %v", err)
	}

	parts := strings.SplitN(wal_lsn, "/", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid WAL LSN format, expected hex/hex, got %v", wal_lsn)
	}
	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse WAL LSN high half %v: %v", parts[0], err)
	}
	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse WAL LSN low half %v: %v", parts[1], err)
	}

	return high<<32 | low, nil
}
