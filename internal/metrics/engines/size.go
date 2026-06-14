package engines

import (
	"apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"apercu-cli/output"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

type SizeEngine struct {
	db                               *sql.DB
	prodMetrics                      metrics.DatabaseMetrics
	initialSize, finalSize           int64
	initialWAL, finalWAL             int64
	initialTempBytes, finalTempBytes int64
	warningStore                     *warning.WarningStore
}

func NewSizeEngine(db *sql.DB, prodMetrics metrics.DatabaseMetrics, warningStore *warning.WarningStore) *SizeEngine {
	return &SizeEngine{
		db:           db,
		prodMetrics:  prodMetrics,
		warningStore: warningStore,
	}
}

func (e *SizeEngine) CollectPreMigrationMetrics() error {
	slog.Debug("Start to collect pre-migration size metrics")
	initialSize, err := e.getDatabaseStorageInBytes()
	if err != nil {
		return err
	}
	initialWAL, err := e.getCurrentWALPosition()
	if err != nil {
		return err
	}
	initialTempBytes, err := e.getTempBytes()
	if err != nil {
		return err
	}
	e.initialSize = initialSize
	e.initialWAL = initialWAL
	e.initialTempBytes = initialTempBytes
	return nil
}

func (e *SizeEngine) SendPgProxyLogs(s string) {}

func (e *SizeEngine) CollectPostMigrationMetrics() error {
	slog.Debug("Start to collect post-migration size metrics")
	finalSize, err := e.getDatabaseStorageInBytes()
	if err != nil {
		return err
	}
	finalWAL, err := e.getCurrentWALPosition()
	if err != nil {
		return err
	}
	finalTempBytes, err := e.getTempBytes()
	if err != nil {
		return err
	}
	e.finalSize = finalSize
	e.finalWAL = finalWAL
	e.finalTempBytes = finalTempBytes
	return nil
}

func (e *SizeEngine) StoreMetricsToOutput(m *output.OutputDatabaseMetrics) error {
	sizeDelta := e.finalSize - e.initialSize
	walDelta := e.finalWAL - e.initialWAL
	tempDelta := e.finalTempBytes - e.initialTempBytes

	var diffFromProd float64
	if e.initialSize > 0 && e.initialSize < e.prodMetrics.DatabaseSize {
		diffFromProd = float64(e.prodMetrics.DatabaseSize) / float64(e.initialSize)
	} else {
		diffFromProd = 1
	}

	estimatedProdWALDelta := int64(float64(walDelta) * diffFromProd)

	m.Storage = &output.OutputDatabaseStorageMetrics{
		InitialSize:            e.initialSize,
		FinalSize:              e.finalSize,
		SizeDelta:              sizeDelta,
		WALDelta:               walDelta,
		TempDelta:              tempDelta,
		EstimatedTempDelta:     int64(float64(tempDelta) * diffFromProd),
		EstimatedProdWALDelta:  estimatedProdWALDelta,
		EstimatedProdSizeDelta: int64(float64(sizeDelta) * diffFromProd),
	}

	e.warningStore.AddWarning(warning.NewWALSizeWarning(estimatedProdWALDelta, e.prodMetrics.DatabaseSize))

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

func (e *SizeEngine) getCurrentWALPosition() (int64, error) {
	var walLsn string
	err := e.db.QueryRow("SELECT pg_current_wal_lsn()").Scan(&walLsn)
	if err != nil {
		return 0, fmt.Errorf("failed to query database for WAL LSN: %v", err)
	}

	parts := strings.SplitN(walLsn, "/", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid WAL LSN format, expected hex/hex, got %v", walLsn)
	}
	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse WAL LSN high half %v: %v", parts[0], err)
	}
	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse WAL LSN low half %v: %v", parts[1], err)
	}

	return int64(high<<32 | low), nil
}

func (e *SizeEngine) getTempBytes() (int64, error) {
	var temp_bytes int64
	err := e.db.QueryRow("SELECT temp_bytes from pg_stat_database WHERE datname = current_database()").Scan(&temp_bytes)
	if err != nil {
		return 0, fmt.Errorf("failed to query database for temp bytes: %v", err)
	}

	return temp_bytes, nil
}
