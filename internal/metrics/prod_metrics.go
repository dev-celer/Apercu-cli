package metrics

import (
	"apercu-cli/helper"
	metricshelper "apercu-cli/helper/metrics"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/lib/pq"
)

const AnalyzeDeltaThreshold = time.Hour * 24
const DefaultHotFloorReadActivity float64 = 1
const DefaultColdCeilingReadActivity float64 = 100
const DefaultHotFloorWriteActivity float64 = 1
const DefaultColdCeilingWriteActivity float64 = 100
const DefaultHotPercentile float64 = 0.75
const DefaultWarmPercentile float64 = 0.25
const MinTableCountForPercentile int = 10

type TablePgClassStats struct {
	RelId           int64
	TableName       string
	SchemaName      string
	RowCount        int64
	Writes          int64
	Scans           int64
	LastAnalyze     sql.NullTime
	LastAutoAnalyze sql.NullTime
}

func getPgClassDatabaseStats(db *sql.DB) ([]TablePgClassStats, error) {
	rows, err := db.Query("/* apercu */" +
		"select s.relid, c.relname as table_name, s.schemaname as schema_name, c.reltuples::bigint as row_count, s.last_analyze, s.last_autoanalyze," +
		"(s.n_tup_ins + s.n_tup_upd + s.n_tup_del) as writes, (s.seq_scan + COALESCE(s.idx_scan, 0)) as scans " +
		"from pg_class c inner join pg_stat_user_tables s on s.relid = c.oid")
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to query prod database for stats: %v", err))
	}
	defer func() { _ = rows.Close() }()

	stats := make([]TablePgClassStats, 0)
	for rows.Next() {
		var s TablePgClassStats
		if err := rows.Scan(&s.RelId, &s.TableName, &s.SchemaName, &s.RowCount, &s.Writes, &s.Scans, &s.LastAnalyze, &s.LastAutoAnalyze); err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to scan returned rows: %v", err))
		}
		stats = append(stats, s)
	}

	return stats, nil
}

func getExactRowCount(db *sql.DB, schemaName string, tableName string) (int64, error) {
	var rowCount int64
	err := db.QueryRow(fmt.Sprintf("/* apercu */SELECT COUNT(*) FROM \"%s\".\"%s\"", schemaName, tableName)).Scan(&rowCount)
	if err != nil {
		return 0, fmt.Errorf("failed to get prod database row count for table %s.%s: %v", schemaName, tableName, err)
	}
	return rowCount, nil
}

func GetDatabaseStats(db *sql.DB) (metricshelper.DatabaseMetrics, error) {
	tablesStats := make(map[helper.FullTableName]metricshelper.TableMetrics)

	// Get stats age in seconds
	var statsAge sql.NullFloat64
	err := db.QueryRow("/* apercu */SELECT EXTRACT(EPOCH FROM (now() - stats_reset)) AS stats_age_s FROM pg_stat_database WHERE datname = current_database()").Scan(&statsAge)
	if err != nil {
		return metricshelper.DatabaseMetrics{}, fmt.Errorf("failed to get prod database stats age: %v", err)
	}

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
		lastAnalyze := min(time.Now().Sub(s.LastAutoAnalyze.Time), time.Now().Sub(s.LastAnalyze.Time))
		if (!s.LastAutoAnalyze.Valid && !s.LastAnalyze.Valid) || lastAnalyze.Hours() > AnalyzeDeltaThreshold.Hours() {
			_, err := db.Exec(fmt.Sprintf("/* apercu */ANALYZE \"%s\".\"%s\"", s.SchemaName, s.TableName))
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
				err = db.QueryRow("/* apercu */select c.reltuples::bigint as row_count from pg_class c inner join pg_stat_user_tables s on s.relname = c.relname where c.relkind = 'r' and s.relid = $1", s.RelId).Scan(&s.RowCount)
				if err != nil {
					return metricshelper.DatabaseMetrics{}, fmt.Errorf("failed to get prod database row count for table %s.%s: %v", s.SchemaName, s.TableName, err)
				}
			}
		}

		// Retrieve the table size
		var tableSize int64
		err := db.QueryRow(fmt.Sprintf("/* apercu */SELECT pg_total_relation_size(%d)", s.RelId)).Scan(&tableSize)
		if err != nil {
			return metricshelper.DatabaseMetrics{}, fmt.Errorf("failed to get prod database table size for table %s.%s: %v", s.SchemaName, s.TableName, err)
		}

		var wps, sps *float64
		if statsAge.Valid && statsAge.Float64 >= 0 {
			wps = new(float64(s.Writes) / statsAge.Float64)
			sps = new(float64(s.Scans) / statsAge.Float64)
		}

		tablesStats[helper.FullTableName{
			Schema: s.SchemaName,
			Table:  s.TableName,
		}] = metricshelper.TableMetrics{
			RowCount:        s.RowCount,
			TableSize:       tableSize,
			WritesPerSecond: wps,
			WriteActivity:   metricshelper.TableActivityNone,
			ScanPerSecond:   sps,
			ReadActivity:    metricshelper.TableActivityNone,
		}
	}

	// Retrieve the full database size
	var databaseSize int64
	err = db.QueryRow("/* apercu */SELECT pg_database_size(current_database())").Scan(&databaseSize)
	if err != nil {
		return metricshelper.DatabaseMetrics{}, fmt.Errorf("failed to get prod database size: %v", err)
	}

	metrics := metricshelper.DatabaseMetrics{
		TablesMetrics: tablesStats,
		DatabaseSize:  databaseSize,
	}
	injectTableActivity(&metrics)

	return metrics, nil
}

func injectTableActivity(databaseMetrics *metricshelper.DatabaseMetrics) {
	// Guard for small schema
	if len(databaseMetrics.TablesMetrics) < MinTableCountForPercentile {
		// Percentile analysis is unreliable on small amount of data point
		// For small schema apply analysis based on floor / ceiling values only
		for k, i := range databaseMetrics.TablesMetrics {
			i.WriteActivity = getActivityFromGuards(i.WritesPerSecond, DefaultHotFloorWriteActivity, DefaultColdCeilingWriteActivity)
			i.ReadActivity = getActivityFromGuards(i.ScanPerSecond, DefaultHotFloorReadActivity, DefaultColdCeilingReadActivity)
			databaseMetrics.TablesMetrics[k] = i
		}
		return
	}

	// Extract all data points that pass the floor threshold
	var rDataPoint, wDataPoint []float64
	for _, i := range databaseMetrics.TablesMetrics {
		if i.ScanPerSecond != nil && *i.ScanPerSecond >= DefaultHotFloorReadActivity {
			rDataPoint = append(rDataPoint, *i.ScanPerSecond)
		}
		if i.WritesPerSecond != nil && *i.WritesPerSecond >= DefaultHotFloorWriteActivity {
			wDataPoint = append(wDataPoint, *i.WritesPerSecond)
		}
	}
	slices.Sort(rDataPoint)
	slices.Sort(wDataPoint)

	// Extract percentile reference values
	var rHotValuePercentile, rWarmValuePercentile, wHotValuePercentile, wWarmValuePercentile float64
	if len(rDataPoint) >= MinTableCountForPercentile {
		rHotValuePercentile = extractPercentileFromDatapoint(rDataPoint, DefaultHotPercentile)
		rWarmValuePercentile = extractPercentileFromDatapoint(rDataPoint, DefaultWarmPercentile)
	}
	if len(wDataPoint) >= MinTableCountForPercentile {
		wHotValuePercentile = extractPercentileFromDatapoint(wDataPoint, DefaultHotPercentile)
		wWarmValuePercentile = extractPercentileFromDatapoint(wDataPoint, DefaultWarmPercentile)
	}

	// Assign activity to all values
	for k, i := range databaseMetrics.TablesMetrics {
		if len(wDataPoint) >= MinTableCountForPercentile {
			i.WriteActivity = getActivityFromRules(i.WritesPerSecond, wHotValuePercentile, wWarmValuePercentile, DefaultHotFloorWriteActivity, DefaultColdCeilingWriteActivity)
		} else {
			i.WriteActivity = getActivityFromGuards(i.WritesPerSecond, DefaultHotFloorWriteActivity, DefaultColdCeilingWriteActivity)
		}

		if len(rDataPoint) >= MinTableCountForPercentile {
			i.ReadActivity = getActivityFromRules(i.ScanPerSecond, rHotValuePercentile, rWarmValuePercentile, DefaultHotFloorReadActivity, DefaultColdCeilingReadActivity)
		} else {
			i.ReadActivity = getActivityFromGuards(i.ScanPerSecond, DefaultHotFloorReadActivity, DefaultColdCeilingReadActivity)
		}

		databaseMetrics.TablesMetrics[k] = i
	}
}

func getActivityFromGuards(ops *float64, hotFloor, coldCeiling float64) metricshelper.TableActivity {
	if ops == nil {
		return metricshelper.TableActivityNone
	}
	if *ops >= coldCeiling {
		return metricshelper.TableActivityHot
	} else if *ops >= hotFloor {
		return metricshelper.TableActivityWarm
	}
	return metricshelper.TableActivityCold
}

func getActivityFromRules(ops *float64, hotPercentileValue, warmPercentileValue, hotFloor, coldCeiling float64) metricshelper.TableActivity {
	if ops == nil {
		return metricshelper.TableActivityNone
	}

	if *ops >= hotPercentileValue && *ops >= hotFloor {
		return metricshelper.TableActivityHot
	}
	if *ops >= warmPercentileValue || *ops >= coldCeiling {
		return metricshelper.TableActivityWarm
	}
	return metricshelper.TableActivityCold
}

func extractPercentileFromDatapoint(datapoint []float64, percent float64) float64 {
	if len(datapoint) == 0 {
		return 0
	}

	pos := float64(len(datapoint)-1) * percent
	i, frac := math.Modf(pos)
	j := int(i) + 1
	if j >= len(datapoint) {
		return datapoint[int(i)]
	}
	x := datapoint[j] * frac
	x += datapoint[int(i)] * (1 - frac)
	return x
}
