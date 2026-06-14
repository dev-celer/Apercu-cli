package warning

import (
	"apercu-cli/helper"
	"apercu-cli/helper/format"
	metricshelper "apercu-cli/helper/metrics"
	"encoding/json"
	"fmt"
	"log/slog"
)

const (
	CodeTableRewritten Code = "TABLE_REWRITTEN"
)

type TableRewriteWarning struct {
	table       helper.FullTableName
	prodMetrics *metricshelper.TableMetrics
}

func NewRewriteWarning(table helper.FullTableName, metrics *metricshelper.TableMetrics) *TableRewriteWarning {
	return &TableRewriteWarning{
		table:       table,
		prodMetrics: metrics,
	}
}

func (w *TableRewriteWarning) GetText() string {
	if w.prodMetrics == nil {
		return fmt.Sprintf("Table %s was rewritten, this table was not found in the production database", w.table.String())
	}
	return fmt.Sprintf("Table %s was rewritten, in production this table is %s (%s rows)", w.table.String(), format.BytesSizePretty(w.prodMetrics.TableSize), format.CountPretty(w.prodMetrics.RowCount))
}

func (w *TableRewriteWarning) GetTextLong() string {
	return w.GetText()
}

func (w *TableRewriteWarning) GetLevel() Level {
	// If table is missing in prod - Low
	if w.prodMetrics == nil {
		return WarningLevelLow
	}

	// If > 1GiB - High
	if w.prodMetrics.TableSize > 1024*1024*1024 {
		return WarningLevelHigh
	}

	// If > 100MiB - Med
	if w.prodMetrics.TableSize > 100*1024*1024 {
		return WarningLevelMedium
	}

	// Else - Low
	return WarningLevelLow
}

func (w *TableRewriteWarning) GetCode() Code {
	return CodeTableRewritten
}

func (w *TableRewriteWarning) GetFullCode() string {
	return fmt.Sprintf("%s.%s", w.GetCode(), EscapeKey(w.table.String()))
}

func (w *TableRewriteWarning) GetIsIdempotent() bool {
	return false
}

type TableRewriteWarningState struct {
	Table       helper.FullTableName        `json:"table"`
	ProdMetrics *metricshelper.TableMetrics `json:"prod_metrics,omitempty"`
}

func (w *TableRewriteWarning) GetStateValues() (json.RawMessage, error) {
	v := TableRewriteWarningState{
		Table:       w.table,
		ProdMetrics: w.prodMetrics,
	}
	return json.Marshal(v)
}

func init() {
	warningConverter[CodeTableRewritten] = func(state json.RawMessage) Warning {
		v := TableRewriteWarning{}
		err := json.Unmarshal(state, &v)
		if err != nil {
			slog.Debug("Failed to unmarshal state", "error", err)
			return nil
		}

		return NewRewriteWarning(v.table, v.prodMetrics)
	}
}
