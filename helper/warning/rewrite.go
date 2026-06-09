package warning

import (
	"apercu-cli/helper"
	"apercu-cli/helper/format"
	metricshelper "apercu-cli/helper/metrics"
	"fmt"
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

func (w *TableRewriteWarning) GetIsIdempotent() bool {
	return false
}

func (w *TableRewriteWarning) GetKeys() []string {
	return []string{w.table.String()}
}
