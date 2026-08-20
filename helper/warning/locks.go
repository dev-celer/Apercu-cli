package warning

import (
	"apercu-cli/helper"
	"apercu-cli/helper/format"
	"apercu-cli/helper/metrics"
	"encoding/json"
	"fmt"
	"log/slog"
)

type LockWarning struct {
	code          Code
	description   string
	query         *helper.QueryWithAffectedTables
	lock          metrics.QueryLock
	operationType metrics.EventOperationType
	pgVersion     float32
	table         helper.FullRelationName
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

	var t string
	if w.description == "" {
		t = fmt.Sprintf("%s event detected for the table %s (%d rows, %s)",
			w.operationType, w.table.String(), w.tableStats.RowCount, format.BytesSizePretty(w.tableStats.TableSize))
	} else {
		t = fmt.Sprintf("%s, this will cause a %s event on the table %s (%d rows, %s)",
			w.description, w.operationType, w.table.String(), w.tableStats.RowCount, format.BytesSizePretty(w.tableStats.TableSize))
	}

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
	var t string
	if w.description == "" {
		t = fmt.Sprintf("this query will cause a %s event on table %s (%d rows, %s)",
			w.operationType, w.table.String(), w.tableStats.RowCount, format.BytesSizePretty(w.tableStats.TableSize))
	} else {
		t = fmt.Sprintf("%s, this will cause a %s event on table %s (%d rows, %s)",
			w.description, w.operationType, w.table.String(), w.tableStats.RowCount, format.BytesSizePretty(w.tableStats.TableSize))
	}

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
	Query         *helper.QueryWithAffectedTables `json:"query"`
	Lock          metrics.QueryLock               `json:"lock"`
	OperationType metrics.EventOperationType      `json:"operation_type"`
	Table         helper.FullRelationName         `json:"table"`
}

func (w *LockWarning) GetStateValues() (json.RawMessage, error) {
	v := LockWarningState{
		Query:         w.query,
		Lock:          w.lock,
		OperationType: w.operationType,
		Table:         w.table,
	}
	return json.Marshal(v)
}

func (w *LockWarning) GetQuery() *helper.QueryWithAffectedTables {
	return w.query
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

// lockWarningRemediation holds every code handled by LockWarning, mapped to its
// remediation. A code with no remediation is still expected to be present here,
// a code absent from this map is not a lock warning.
var lockWarningRemediation = map[Code]string{
	CodeCreateIndexWithoutConcurrently:             "Use the keyword 'CONCURRENTLY' with 'CREATE INDEX' to prevent a lock from taking place",
	CodeDropIndexCascadeWithoutConcurrently:        "Use the keyword 'CONCURRENTLY' with 'DROP INDEX' to prevent a lock from taking place, note that 'CASCADE' is not supported with 'CONCURRENTLY', you will need to drop each objects that depend on this index manually",
	CodeDropIndexWithoutConcurrently:               "Use the keyword 'CONCURRENTLY' with 'DROP INDEX' to prevent a lock from taking place",
	CodeReindexWithoutConcurrently:                 "You can use the keywork 'CONCURRENTLY' with 'REINDEX' to prevent a lock from taking place, note that if 'REINDEX CONCURRENTLY' fail, it will leave behind a broken index that need to be cleaned up manually",
	CodeRefreshMaterializedViewWithoutConcurrently: "Use the keyword 'CONCURRENTLY' with 'REFRESH MATERIALIZED VIEW' to prevent a lock from taking place",
	CodeCluster:                            "",
	CodeVacuumFull:                         "",
	CodeCreateTrigger:                      "",
	CodeCreateRule:                         "",
	CodeAlterRule:                          "",
	CodeAlterType:                          "",
	CodeAlterTableDropConstraint:           "",
	CodeAlterTableDropColumn:               "",
	CodeAlterTableLogged:                   "",
	CodeAlterTableTablespace:               "",
	CodeAlterTableFillFactor:               "",
	CodeAlterTableReset:                    "",
	CodeAlterTableSwitchTrigger:            "",
	CodeAlterTableRename:                   "",
	CodeAddColumnGeneratedAlwaysAsStored:   "Avoid adding a new column with 'GENERATED ALWAYS AS ... STORED' where possible, instead you can create a simple nullable column, create a trigger 'BEFORE INSERT OR UPDATE' with your default, backfill the rows than set to NOT NULL",
	CodeAddColumnGeneratedAlwaysAsIdentity: "Avoid adding directly a new column with 'GENERATED ALWAYS AS IDENTITY', instead you can create a nullable column, backfill the rows, set 'NOT NULL', than attach the 'GENERATED ALWAYS AS IDENTITY' property",
	CodeAddColumnVolatileDefault:           "When adding a new column with volatile default, we recommend to first add a nullable column without default, set default for new rows then backfill existing rows in batches",
	CodeAddColumn:                          "",
	CodeAlterColumnSetNotNull:              "",
	CodeAlterColumnDropNotNull:             "",
	CodeAlterColumnDefault:                 "",
	CodeAlterColumnSetStatistics:           "",
	CodeAlterColumnSetStorage:              "",
	CodeAlterColumnSetTypeNotWidening:      "Add a new column of the target type, backfill it, swap via RENAME then dropping the old column",
	CodeAlterColumnSetTypeWidening:         "",
	CodeAlterTableAddConstraintNotValid:    "",
	CodeAlterTableAddConstraint:            "Add your constraint with 'NOT VALID', then VALIDATE CONSTRAINT in a separate statement",
	CodeAlterTableAddUniqueWithIndex:       "",
	CodeAlterTableAddUniqueWithoutIndex:    "First create a unique index concurrently then add your UNIQUE CONSTRAINT / PRIMARY KEY with 'USING INDEX <idx>'",
	CodeAlterTableAddConstraintExclude:     "",
}

func NewLockWarnings(query *metrics.QueryEventAnalysis, code Code, prodStats *metrics.DatabaseMetrics) []*LockWarning {
	if query == nil {
		return nil
	}

	remediation, known := lockWarningRemediation[code]
	if !known {
		return nil
	}

	var pgVersion float32
	if prodStats != nil {
		pgVersion = prodStats.ServerVersion
	}

	q := helper.QueryWithAffectedTables{
		Query:          query.Event.SQL,
		AffectedTables: query.AffectedTables,
	}

	warnings := make([]*LockWarning, 0, len(query.AffectedTables))
	for _, table := range query.AffectedTables {
		var prodTableMetric metrics.TableMetrics
		if prodStats != nil {
			// If the table wasn't found on the prod database, ignore the warning
			x, ok := prodStats.TablesMetrics[table]
			prodTableMetric = x
			if !ok {
				continue
			}
		}

		warnings = append(warnings, &LockWarning{
			code:          code,
			operationType: query.Type,
			query:         &q,
			table:         table,
			tableStats:    prodTableMetric,
			lock:          query.Lock,
			pgVersion:     pgVersion,
			remediation:   remediation,
		})
	}

	return warnings
}

// newLockWarningConverter builds the state converter of a single lock warning code.
// The table statistics and the server version are never persisted, they are read back
// from the metrics of the current run, exactly like NewLockWarnings does.
func newLockWarningConverter(code Code) func(state json.RawMessage, prodMetrics *metrics.DatabaseMetrics) Warning {
	return func(state json.RawMessage, prodMetrics *metrics.DatabaseMetrics) Warning {
		v := LockWarningState{}
		err := json.Unmarshal(state, &v)
		if err != nil {
			slog.Debug("Failed to unmarshal state", "error", err)
			return nil
		}

		var prodTableMetric metrics.TableMetrics
		var pgVersion float32
		if prodMetrics != nil {
			pgVersion = prodMetrics.ServerVersion

			// If the table wasn't found on the prod database, ignore the warning
			x, ok := prodMetrics.TablesMetrics[v.Table]
			prodTableMetric = x
			if !ok {
				return nil
			}
		}

		return &LockWarning{
			code:          code,
			operationType: v.OperationType,
			query:         v.Query,
			table:         v.Table,
			tableStats:    prodTableMetric,
			lock:          v.Lock,
			pgVersion:     pgVersion,
			remediation:   lockWarningRemediation[code],
		}
	}
}

func init() {
	for code := range lockWarningRemediation {
		warningConverter[code] = newLockWarningConverter(code)
	}
}
