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
