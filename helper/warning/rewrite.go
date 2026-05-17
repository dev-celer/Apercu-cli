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

func NewRewriteWarning(table helper.FullTableName, metrics *metricshelper.TableMetrics) TableRewriteWarning {
	return TableRewriteWarning{
		table:       table,
		prodMetrics: metrics,
	}
}

func (t TableRewriteWarning) GetWarningText() string {
	if t.prodMetrics == nil {
		return fmt.Sprintf("Table %s was rewritten, this table was not found in the production database", t.table.String())
	}
	return fmt.Sprintf("Table %s was rewritten, in production this table is %s (%s rows)", t.table.String(), format.BytesSizePretty(t.prodMetrics.TableSize), format.CountPretty(t.prodMetrics.RowCount))
}

func (t TableRewriteWarning) GetWarningLevel() Level {
	// If table is missing in prod - Low
	if t.prodMetrics == nil {
		return WarningLevelLow
	}

	// If > 1GiB - High
	if t.prodMetrics.TableSize > 1024*1024*1024 {
		return WarningLevelHigh
	}

	// If > 100MiB - Med
	if t.prodMetrics.TableSize > 100*1024*1024 {
		return WarningLevelMedium
	}

	// Else - Low
	return WarningLevelLow
}

func (t TableRewriteWarning) GetWarningCode() Code {
	return CodeTableRewritten
}
