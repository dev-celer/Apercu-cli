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

func (e ExplainQueryFile) GetWarningText() string {
	switch e.code {
	case CodeExplainQueryPathNotFound:
		return fmt.Sprintf("Explain query path not found: %s", e.path)
	case CodeExplainQueryNoQueries:
		return fmt.Sprintf("No queries to explain found in file: %s", e.path)
	case CodeExplainQueryFailedToReadFile:
		return fmt.Sprintf("Failed to read for explain queries file: %s", e.path)
	}
	return ""
}

func (e ExplainQueryFile) GetWarningTextLong() string {
	return e.GetWarningText()
}

func (e ExplainQueryFile) GetWarningLevel() Level {
	return WarningLevelLow
}

func (e ExplainQueryFile) GetWarningCode() Code {
	return e.code
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

func (e ExplainQueryProdFetch) GetWarningText() string {
	switch e.code {
	case CodeExplainQueryStatStatementsMissing:
		return "pg_stat_statements extension is not installed on the prod database; auto-fetch skipped"
	case CodeExplainQueryProdFetchFailed:
		return "Failed to fetch queries from prod database"
	}
	return ""
}

func (e ExplainQueryProdFetch) GetWarningTextLong() string {
	switch e.code {
	case CodeExplainQueryProdFetchFailed:
		return fmt.Sprintf("Failed to fetch queries from prod database: %s", e.detail)
	}
	return e.GetWarningText()
}

func (e ExplainQueryProdFetch) GetWarningLevel() Level {
	return WarningLevelLow
}

func (e ExplainQueryProdFetch) GetWarningCode() Code {
	return e.code
}

type ExplainPlanRegression struct {
	short string
	long  string
	level Level
	code  Code
}

func NewExplainPlanScanRegressionWarning(level Level, rel, before, after string) ExplainPlanRegression {
	return ExplainPlanRegression{
		level: level,
		code:  CodeExplainQueryPlanScanRegression,
		short: fmt.Sprintf("Plan scan regression on %s: %s -> %s", rel, before, after),
		long:  fmt.Sprintf("Query plan regression on relation %s: access method changed from %s to %s after the migration. This usually means an index the query relied on is no longer usable.", rel, before, after),
	}
}

func NewExplainPlanOrderingRegressionWarning(level Level, key string) ExplainPlanRegression {
	return ExplainPlanRegression{
		level: level,
		code:  CodeExplainQueryPlanOrderingRegression,
		short: fmt.Sprintf("Plan ordering regression: new sort on %s", key),
		long:  fmt.Sprintf("Query plan regression: the post-migration plan introduces a new sort on %s that was not present before, suggesting an index that previously provided ordering was dropped.", key),
	}
}

func (e ExplainPlanRegression) GetWarningText() string {
	return e.short
}

func (e ExplainPlanRegression) GetWarningTextLong() string {
	if e.long == "" {
		return e.short
	}
	return e.long
}

func (e ExplainPlanRegression) GetWarningLevel() Level {
	return e.level
}

func (e ExplainPlanRegression) GetWarningCode() Code {
	return e.code
}
