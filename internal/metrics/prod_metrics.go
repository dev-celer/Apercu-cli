package metrics

import (
	"apercu-cli/helper"
	metricshelper "apercu-cli/helper/metrics"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

const AnalyzeDeltaThreshold = time.Hour * 24

type TablePgClassStats struct {
	RelId           int64
	TableName       string
	SchemaName      string
	RowCount        int64
	LastAnalyze     time.Time
	LastAutoAnalyze time.Time
}

func getPgClassDatabaseStats(db *sql.DB) ([]TablePgClassStats, error) {
	rows, err := db.Query("select s.relid, c.relname as table_name, s.schemaname as schema_name, c.reltuples::bigint as row_count, s.last_analyze, s.last_autoanalyze from pg_class c inner join pg_stat_user_tables s on s.relid = c.oid")
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to query prod database for stats: %v", err))
	}
	defer func() { _ = rows.Close() }()

	stats := make([]TablePgClassStats, 0)
	for rows.Next() {
		var s TablePgClassStats
		if err := rows.Scan(&s.RelId, &s.TableName, &s.SchemaName, &s.RowCount, &s.LastAnalyze, &s.LastAutoAnalyze); err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to scan returned rows: %v", err))
		}
		stats = append(stats, s)
	}

	return stats, nil
}

func getExactRowCount(db *sql.DB, schemaName string, tableName string) (int64, error) {
	var rowCount int64
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM \"%s\".\"%s\"", schemaName, tableName)).Scan(&rowCount)
	if err != nil {
		return 0, fmt.Errorf("failed to get prod database row count for table %s.%s: %v", schemaName, tableName, err)
	}
	return rowCount, nil
}

func GetDatabaseStats(db *sql.DB) (metricshelper.DatabaseMetrics, error) {
	tablesStats := make(map[helper.FullTableName]metricshelper.TableMetrics)

	pgClassStats, err := getPgClassDatabaseStats(db)
	if err != nil {
		return metricshelper.DatabaseMetrics{}, err
	}
	isReadOnly := false
	for _, s := range pgClassStats {
		// If in read only mode, request the row count from SELECT COUNT(*)
		if isReadOnly {
			s.RowCount, err = getExactRowCount(db, s.SchemaName, s.TableName)
			if err != nil {
				return metricshelper.DatabaseMetrics{}, err
			}
		}

		// If last analyze delta time exceed threshold, try to call analyze
		lastAnalyze := min(time.Now().Sub(s.LastAutoAnalyze), time.Now().Sub(s.LastAnalyze))
		if lastAnalyze.Hours() > AnalyzeDeltaThreshold.Hours() {
			_, err := db.Exec(fmt.Sprintf("ANALYZE \"%s\".\"%s\"", s.SchemaName, s.TableName))
			if err != nil {
				if pqErr, ok := errors.AsType[*pq.Error](err); ok && (pqErr.Code == "25006" || pqErr.Code == "42501") {
					isReadOnly = true
					s.RowCount, err = getExactRowCount(db, s.SchemaName, s.TableName)
					if err != nil {
						return metricshelper.DatabaseMetrics{}, err
					}
				} else {
					return metricshelper.DatabaseMetrics{}, fmt.Errorf("failed to call ANALYZE on prod database table %s.%s: %v", s.SchemaName, s.TableName, err)
				}
			}

			// Recall the pg_class request for row count
			if !isReadOnly {
				err = db.QueryRow("select c.reltuples::bigint as row_count from pg_class c inner join pg_stat_user_tables s on s.relname = c.relname where c.relkind = 'r' and s.relid = ?", s.RelId).Scan(&s.RowCount)
				if err != nil {
					return metricshelper.DatabaseMetrics{}, fmt.Errorf("failed to get prod database row count for table %s.%s: %v", s.SchemaName, s.TableName, err)
				}
			}
		}

		// Retrieve the table size
		var tableSize int64
		err := db.QueryRow(fmt.Sprintf("SELECT pg_total_relation_size(%d)", s.RelId)).Scan(&tableSize)
		if err != nil {
			return metricshelper.DatabaseMetrics{}, fmt.Errorf("failed to get prod database table size for table %s.%s: %v", s.SchemaName, s.TableName, err)
		}

		tablesStats[helper.FullTableName{
			Schema: s.SchemaName,
			Table:  s.TableName,
		}] = metricshelper.TableMetrics{
			RowCount:  s.RowCount,
			TableSize: tableSize,
		}
	}

	// Retrieve the full database size
	var databaseSize int64
	err = db.QueryRow("SELECT pg_database_size(current_database())").Scan(&databaseSize)
	if err != nil {
		return metricshelper.DatabaseMetrics{}, fmt.Errorf("failed to get prod database size: %v", err)
	}

	return metricshelper.DatabaseMetrics{
		TablesMetrics: tablesStats,
		DatabaseSize:  databaseSize,
	}, nil
}
