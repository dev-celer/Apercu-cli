package warning

import (
	"apercu-cli/helper"
	"apercu-cli/helper/format"
	"apercu-cli/helper/metrics"
	"encoding/json"
	"fmt"
)

type LockWarning struct {
	code          Code
	query         string
	lock          metrics.QueryLock
	operationType metrics.EventOperationType
	pgVersion     float32
	table         helper.FullTableName
	tableStats    metrics.TableMetrics
	remediation   string
}

func (w *LockWarning) getTableActivityText() string {
	var t string
	readDetails := w.lock.IsReadBlocking() && w.tableStats.ReadActivity != metrics.TableActivityNone && w.tableStats.ScanPerSecond != nil
	writeDetails := w.lock.IsWriteBlocking() && w.tableStats.WriteActivity != metrics.TableActivityNone && w.tableStats.WritesPerSecond != nil
	if readDetails || writeDetails {
		t += "this table was deemed to be "
		if readDetails {
			var s, r string
			switch w.tableStats.ReadActivity {
			case metrics.TableActivityHot:
				s = "read-hot"
				r = fmt.Sprintf("%.2f Read/s, Top %d%% read load", *w.tableStats.ScanPerSecond, int((1-metrics.DefaultHotPercentile)*100))
			case metrics.TableActivityWarm:
				s = "read-warm"
				switch w.tableStats.ReadDecision {
				case metrics.ActivityDecisionPercentile:
					r = fmt.Sprintf("%.2f Read/s, Top %d%% read load", *w.tableStats.ScanPerSecond, int((1-metrics.DefaultWarmPercentile)*100))
				case metrics.ActivityDecisionCeiling:
					r = fmt.Sprintf("%.2f Read/s > %.2f", *w.tableStats.ScanPerSecond, metrics.DefaultColdCeilingReadActivity)
				case metrics.ActivityDecisionFloor:
					r = fmt.Sprintf("%.2f Read/s < %.2f", *w.tableStats.ScanPerSecond, metrics.DefaultHotFloorReadActivity)
				case metrics.ActivityDecisionLowCount:
					r = fmt.Sprintf("%.2f Read/s", *w.tableStats.ScanPerSecond)
				}
			case metrics.TableActivityCold:
				s = "read-cold"
				r = fmt.Sprintf("%.2f Read/s, Bottom %d%% read load", *w.tableStats.ScanPerSecond, int(metrics.DefaultWarmPercentile*100))
			}
			t += fmt.Sprintf("%s (%s) ", s, r)
		}
		if readDetails && writeDetails {
			t += "& "
		}
		if writeDetails {
			var s, r string
			switch w.tableStats.WriteActivity {
			case metrics.TableActivityHot:
				s = "write-hot"
				r = fmt.Sprintf("%.2f Writes/s, Top %d%% write load", *w.tableStats.WritesPerSecond, int((1-metrics.DefaultHotPercentile)*100))
			case metrics.TableActivityWarm:
				s = "write-warm"
				switch w.tableStats.WriteDecision {
				case metrics.ActivityDecisionPercentile:
					r = fmt.Sprintf("%.2f Writes/s, Top %d%% write load", *w.tableStats.WritesPerSecond, int((1-metrics.DefaultWarmPercentile)*100))
				case metrics.ActivityDecisionCeiling:
					r = fmt.Sprintf("%.2f Writes/s > %.2f", *w.tableStats.WritesPerSecond, metrics.DefaultColdCeilingWriteActivity)
				case metrics.ActivityDecisionFloor:
					r = fmt.Sprintf("%.2f Writes/s < %.2f", *w.tableStats.WritesPerSecond, metrics.DefaultHotFloorWriteActivity)
				case metrics.ActivityDecisionLowCount:
					r = fmt.Sprintf("%.2f Writes/s", *w.tableStats.WritesPerSecond)
				}
			case metrics.TableActivityCold:
				s = "write-cold"
				r = fmt.Sprintf("%.2f Writes/s, Bottom %d%% write load", *w.tableStats.WritesPerSecond, int(metrics.DefaultWarmPercentile*100))
			}
			t += fmt.Sprintf("%s (%s)", s, r)
		}
	}
	return t
}

func (w *LockWarning) recommendMaintenance() bool {
	return w.operationType == metrics.EventOperationTypeRewriteUnderLock || w.operationType == metrics.EventOperationTypeScanUnderLock
}

func (w *LockWarning) GetText() string {
	// Get top level warning text

	t := fmt.Sprintf("%s event detected for the table %s (%d rows, %s)",
		w.operationType, w.table.String(), w.tableStats.RowCount, format.BytesSizePretty(w.tableStats.TableSize))

	activity := w.getTableActivityText()
	if activity != "" {
		t += fmt.Sprintf(", %s", activity)
	}

	if w.remediation != "" {
		t += ", a recommendation is available"
	}

	if w.recommendMaintenance() {
		if w.remediation != "" {
			t += ", else try to plan a maintenance window for this migration"
		} else {
			t += ", try to plan a maintenance window for this migration"
		}
	}

	return t
}

func (w *LockWarning) GetTextLong() string {
	t := fmt.Sprintf("this query will cause a %s event on table %s (%d rows, %s)",
		w.operationType, w.table.String(), w.tableStats.RowCount, format.BytesSizePretty(w.tableStats.TableSize))
	if w.remediation != "" {
		t += fmt.Sprintf("\nremediation: %s", w.remediation)
	}
	return t
}

func (w *LockWarning) GetLevel() Level {
	// Write lock matrix
	writeLevel := WarningLevelLow
	if w.lock.IsWriteBlocking() {
		switch w.operationType {
		case metrics.EventOperationTypeMetadataOnly:
			if w.tableStats.WriteActivity == metrics.TableActivityHot {
				writeLevel = WarningLevelMedium
			}
		case metrics.EventOperationTypeScanUnderLock:
			switch w.tableStats.WriteActivity {
			case metrics.TableActivityHot:
				writeLevel = WarningLevelHigh
			case metrics.TableActivityWarm:
				writeLevel = WarningLevelMedium
			case metrics.TableActivityCold:
				writeLevel = WarningLevelLow
			}
		case metrics.EventOperationTypeRewriteUnderLock:
			if w.tableStats.WriteActivity == metrics.TableActivityCold {
				writeLevel = WarningLevelMedium
			} else {
				writeLevel = WarningLevelHigh
			}
		}
	}

	// Read lock matrix
	readLevel := WarningLevelLow
	if w.lock.IsReadBlocking() {
		switch w.operationType {
		case metrics.EventOperationTypeMetadataOnly:
			if w.tableStats.ReadActivity != metrics.TableActivityCold {
				readLevel = WarningLevelMedium
			}
		case metrics.EventOperationTypeScanUnderLock:
			if w.tableStats.ReadActivity == metrics.TableActivityHot {
				readLevel = WarningLevelHigh
			} else {
				readLevel = WarningLevelMedium
			}
		case metrics.EventOperationTypeRewriteUnderLock:
			if w.tableStats.ReadActivity == metrics.TableActivityCold {
				readLevel = WarningLevelMedium
			} else {
				readLevel = WarningLevelHigh
			}
		}
	}

	return max(writeLevel, readLevel)
}

func (w *LockWarning) GetCode() Code {
	return w.code
}

func (w *LockWarning) GetFullCode() string {
	return fmt.Sprintf("%s.%s", w.GetCode(), FormatKey(w.table.String()))
}

func (w *LockWarning) GetIsIdempotent() bool {
	return false
}

type LockWarningState struct {
	Query      string               `json:"query"`
	TableStats metrics.TableMetrics `json:"table_stats"`
}

func (w *LockWarning) GetStateValues() (json.RawMessage, error) {
	v := LockWarningState{
		Query:      w.query,
		TableStats: w.tableStats,
	}
	return json.Marshal(v)
}

var (
	CodeCreateIndexWithoutConcurrently             Code = "CREATE_INDEX_WITHOUT_CONCURRENTLY"
	CodeDropIndexCascadeWithoutConcurrently        Code = "DROP_INDEX_CASCADE_WITHOUT_CONCURRENTLY"
	CodeDropIndexWithoutConcurrently               Code = "DROP_INDEX_WITHOUT_CONCURRENTLY"
	CodeReindexWithoutConcurrently                 Code = "REINDEX_WITHOUT_CONCURRENTLY"
	CodeRefreshMaterializedViewWithoutConcurrently Code = "REFRESH_MATERIALIZED_VIEW_WITHOUT_CONCURRENTLY"
	CodeCluster                                    Code = "CLUSTER"
	CodeVacuumFull                                 Code = "VACUUM_FULL"
	CodeCreateTrigger                              Code = "CREATE_TRIGGER"
	CodeCreateRule                                 Code = "CREATE_RULE"
	CodeAlterRule                                  Code = "ALTER_RULE"
	CodeAlterType                                  Code = "ALTER_TYPE"
	CodeAlterTableDropConstraint                   Code = "ALTER_TABLE_DROP_CONSTRAINT"
	CodeAlterTableDropColumn                       Code = "ALTER_TABLE_DROP_COLUMN"
	CodeAlterTableLogged                           Code = "ALTER_TABLE_LOGGED"
	CodeAlterTableTablespace                       Code = "ALTER_TABLE_TABLESPACE"
	CodeAlterTableFillFactor                       Code = "ALTER_TABLE_FILL_FACTOR"
	CodeAlterTableReset                            Code = "ALTER_TABLE_RESET"
	CodeAlterTableSwitchTrigger                    Code = "ALTER_TABLE_SWITCH_TRIGGER"
	CodeAlterTableRename                           Code = "ALTER_TABLE_RENAME"
	CodeAddColumnGeneratedAlwaysAsStored           Code = "ADD_COLUMN_GENERATED_ALWAYS_AS_STORED"
	CodeAddColumnGeneratedAlwaysAsIdentity         Code = "ADD_COLUMN_GENERATED_ALWAYS_AS_IDENTITY"
	CodeAddColumnVolatileDefault                   Code = "ADD_COLUMN_VOLATILE_DEFAULT"
	CodeAddColumn                                  Code = "ADD_COLUMN"
	CodeAlterColumnDefault                         Code = "ALTER_COLUMN_DEFAULT"
	CodeAlterColumnDropNotNull                     Code = "ALTER_COLUMN_DROP_NOT_NULL"
	CodeAlterColumnSetNotNull                      Code = "ALTER_COLUMN_SET_NOT_NULL"
	CodeAlterColumnSetTypeNotWidening              Code = "ALTER_COLUMN_SET_TYPE_NOT_WIDENING"
	CodeAlterColumnSetTypeWidening                 Code = "ALTER_COLUMN_SET_TYPE_WIDENING"
	CodeAlterColumnSetStorage                      Code = "ALTER_COLUMN_SET_STORAGE"
	CodeAlterColumnSetStatistics                   Code = "ALTER_COLUMN_SET_STATISTICS"
	CodeAlterTableAddConstraintNotValid            Code = "ALTER_TABLE_ADD_CONSTRAINT_NOT_VALID"
	CodeAlterTableAddConstraint                    Code = "ALTER_TABLE_ADD_CONSTRAINT"
	CodeAlterTableAddUniqueWithoutIndex            Code = "ALTER_TABLE_ADD_UNIQUE_WITHOUT_INDEX"
	CodeAlterTableAddUniqueWithIndex               Code = "ALTER_TABLE_ADD_UNIQUE_WITH_INDEX"
	CodeAlterTableAddConstraintExclude             Code = "ALTER_TABLE_ADD_CONSTRAINT_EXCLUDE"
)

func NewLockWarnings(query *metrics.QueryEventAnalysis, code Code, pgVersion float32, prodStats *metrics.DatabaseMetrics) []*LockWarning {
	if query == nil {
		return nil
	}

	var remediation string
	switch code {
	case CodeCreateIndexWithoutConcurrently:
		remediation = "Use the keyword 'CONCURRENTLY' with 'CREATE INDEX' to prevent a lock from taking place"
	case CodeDropIndexCascadeWithoutConcurrently:
		remediation = "Use the keyword 'CONCURRENTLY' with 'DROP INDEX' to prevent a lock from taking place, note that 'CASCADE' is not supported with 'CONCURRENTLY', you will need to drop each objects that depend on this index manually"
	case CodeDropIndexWithoutConcurrently:
		remediation = "Use the keyword 'CONCURRENTLY' with 'DROP INDEX' to prevent a lock from taking place"
	case CodeReindexWithoutConcurrently:
		remediation = "You can use the keywork 'CONCURRENTLY' with 'REINDEX' to prevent a lock from taking place, note that if 'REINDEX CONCURRENTLY' fail, it will leave behind a broken index that need to be cleaned up manually"
	case CodeRefreshMaterializedViewWithoutConcurrently:
		remediation = "Use the keyword 'CONCURRENTLY' with 'REFRESH MATERIALIZED VIEW' to prevent a lock from taking place"
	case CodeCluster:
	case CodeVacuumFull:
	case CodeCreateTrigger:
	case CodeCreateRule:
	case CodeAlterRule:
	case CodeAlterType:
	case CodeAlterTableDropConstraint:
	case CodeAlterTableDropColumn:
	case CodeAlterTableLogged:
	case CodeAlterTableTablespace:
	case CodeAlterTableFillFactor:
	case CodeAlterTableReset:
	case CodeAlterTableSwitchTrigger:
	case CodeAlterTableRename:
	case CodeAddColumnGeneratedAlwaysAsStored:
		remediation = "Avoid adding a new column with 'GENERATED ALWAYS AS ... STORED' where possible, instead you can create a simple nullable column, create a trigger 'BEFORE INSERT OR UPDATE' with your default, backfill the rows than set to NOT NULL"
	case CodeAddColumnGeneratedAlwaysAsIdentity:
		remediation = "Avoid adding directly a new column with 'GENERATED ALWAYS AS IDENTITY', instead you can create a nullable column, backfill the rows, set 'NOT NULL', than attach the 'GENERATED ALWAYS AS IDENTITY' property"
	case CodeAddColumnVolatileDefault:
		remediation = "When adding a new column with volatile default, we recommend to first add a nullable column without default, set default for new rows then backfill existing rows in batches"
	case CodeAddColumn:
	case CodeAlterColumnSetNotNull:
	case CodeAlterColumnDropNotNull:
	case CodeAlterColumnDefault:
	case CodeAlterColumnSetStatistics:
	case CodeAlterColumnSetStorage:
	case CodeAlterColumnSetTypeNotWidening:
		remediation = "Add a new column of the target type, backfill it, swap via RENAME then dropping the old column"
	case CodeAlterColumnSetTypeWidening:
	case CodeAlterTableAddConstraintNotValid:
	case CodeAlterTableAddConstraint:
		remediation = "Add your constraint with 'NOT VALID', then VALIDATE CONSTRAINT in a separate statement"
	case CodeAlterTableAddUniqueWithIndex:
	case CodeAlterTableAddUniqueWithoutIndex:
		remediation = "First create a unique index concurrently then add your UNIQUE CONSTRAINT / PRIMARY KEY with 'USING INDEX <idx>'"
	case CodeAlterTableAddConstraintExclude:
	default:
		return nil
	}

	warnings := make([]*LockWarning, 0, len(query.AffectedTables))
	for _, table := range query.AffectedTables {
		warnings = append(warnings, &LockWarning{
			code:          code,
			operationType: query.Type,
			query:         query.Event.SQL,
			table:         table,
			tableStats:    prodStats.TablesMetrics[table],
			lock:          query.Lock,
			pgVersion:     pgVersion,
			remediation:   remediation,
		})
	}

	return warnings
}
