package warning

import (
	"fmt"
)

const (
	CodeExplainQueryPathNotFound           Code = "EXPL_QUERY_PATH_NOT_FOUND"
	CodeExplainQueryNoQueries              Code = "EXPL_QUERY_NO_QUERIES"
	CodeExplainQueryFailedToReadFile       Code = "EXPL_QUERY_ERR_READING_FILE"
	CodeExplainQueryStatStatementsMissing  Code = "EXPL_QUERY_PG_STAT_STATEMENTS_MISSING"
	CodeExplainQueryProdFetchFailed        Code = "EXPL_QUERY_PROD_FETCH_FAILED"
	CodeExplainQueryPlanScanRegression     Code = "EXPL_QUERY_PLAN_SCAN_REGR"
	CodeExplainQueryPlanOrderingRegression Code = "EXPL_QUERY_PLAN_ORDER_REGR"
)

type ExplainQueryFile struct {
	path string
	code Code
}

func NewExplainQueryFileWarning(code Code, path string) *ExplainQueryFile {
	switch code {
	case CodeExplainQueryPathNotFound:
	case CodeExplainQueryNoQueries:
	case CodeExplainQueryFailedToReadFile:
	default:
		return nil
	}
	return &ExplainQueryFile{
		path: path,
		code: code,
	}
}

func (w *ExplainQueryFile) GetText() string {
	switch w.code {
	case CodeExplainQueryPathNotFound:
		return fmt.Sprintf("Explain query path not found: %s", w.path)
	case CodeExplainQueryNoQueries:
		return fmt.Sprintf("No queries to explain found in file: %s", w.path)
	case CodeExplainQueryFailedToReadFile:
		return fmt.Sprintf("Failed to read for explain queries file: %s", w.path)
	}
	return ""
}

func (w *ExplainQueryFile) GetTextLong() string {
	return w.GetText()
}

func (w *ExplainQueryFile) GetLevel() Level {
	return WarningLevelLow
}

func (w *ExplainQueryFile) GetCode() Code {
	return w.code
}

func (w *ExplainQueryFile) GetIsIdempotent() bool {
	return true
}

func (w *ExplainQueryFile) GetKeys() []string {
	return []string{w.path}
}

type ExplainQueryProdFetch struct {
	detail string
	code   Code
}

func NewExplainQueryProdFetchWarning(code Code, detail string) *ExplainQueryProdFetch {
	switch code {
	case CodeExplainQueryStatStatementsMissing:
	case CodeExplainQueryProdFetchFailed:
	default:
		return nil
	}
	return &ExplainQueryProdFetch{
		detail: detail,
		code:   code,
	}
}

func (w *ExplainQueryProdFetch) GetText() string {
	switch w.code {
	case CodeExplainQueryStatStatementsMissing:
		return "pg_stat_statements extension is not installed on the prod database; auto-fetch skipped"
	case CodeExplainQueryProdFetchFailed:
		return "Failed to fetch queries from prod database"
	}
	return ""
}

func (w *ExplainQueryProdFetch) GetTextLong() string {
	switch w.code {
	case CodeExplainQueryProdFetchFailed:
		return fmt.Sprintf("Failed to fetch queries from prod database: %s", w.detail)
	}
	return w.GetText()
}

func (w *ExplainQueryProdFetch) GetLevel() Level {
	return WarningLevelLow
}

func (w *ExplainQueryProdFetch) GetCode() Code {
	return w.code
}

func (w *ExplainQueryProdFetch) GetIsIdempotent() bool {
	return true
}

func (w *ExplainQueryProdFetch) GetKeys() []string {
	return nil
}

type ExplainPlanRegression struct {
	short string
	long  string
	level Level
	code  Code
	key   string
}

func NewExplainPlanScanRegressionWarning(level Level, rel, before, after string) *ExplainPlanRegression {
	return &ExplainPlanRegression{
		level: level,
		code:  CodeExplainQueryPlanScanRegression,
		short: fmt.Sprintf("Plan scan regression on %s: %s -> %s", rel, before, after),
		long:  fmt.Sprintf("Query plan regression on relation %s: access method changed from %s to %s after the migration. This usually means an index the query relied on is no longer usable.", rel, before, after),
		key:   rel,
	}
}

func NewExplainPlanOrderingRegressionWarning(level Level, key string) *ExplainPlanRegression {
	return &ExplainPlanRegression{
		level: level,
		code:  CodeExplainQueryPlanOrderingRegression,
		short: fmt.Sprintf("Plan ordering regression: new sort on %s", key),
		long:  fmt.Sprintf("Query plan regression: the post-migration plan introduces a new sort on %s that was not present before, suggesting an index that previously provided ordering was dropped.", key),
		key:   key,
	}
}

func (w *ExplainPlanRegression) GetText() string {
	return w.short
}

func (w *ExplainPlanRegression) GetTextLong() string {
	if w.long == "" {
		return w.short
	}
	return w.long
}

func (w *ExplainPlanRegression) GetLevel() Level {
	return w.level
}

func (w *ExplainPlanRegression) GetCode() Code {
	return w.code
}

func (w *ExplainPlanRegression) GetIsIdempotent() bool {
	return true
}

func (w *ExplainPlanRegression) GetKeys() []string {
	return []string{w.key}
}
