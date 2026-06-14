package warning

import (
	"encoding/json"
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

func (w *ExplainQueryFile) GetFullCode() string {
	return fmt.Sprintf("%s.%s", w.code, EscapeKey(w.path))
}

func (w *ExplainQueryFile) GetIsIdempotent() bool {
	return true
}

type ExplainQueryStateValues struct {
	Path string `json:"path"`
	Code Code   `json:"code"`
}

func (w *ExplainQueryFile) GetStateValues() (json.RawMessage, error) {
	v := ExplainQueryStateValues{
		Path: w.path,
		Code: w.code,
	}
	return json.Marshal(v)
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
		if len(w.detail) > 0 {
			return fmt.Sprintf("Failed to fetch queries from prod database: %s", w.detail)
		}
		return "Failed to fetch queries from prod database"
	}
	return w.GetText()
}

func (w *ExplainQueryProdFetch) GetLevel() Level {
	return WarningLevelLow
}

func (w *ExplainQueryProdFetch) GetCode() Code {
	return w.code
}

func (w *ExplainQueryProdFetch) GetFullCode() string {
	return string(w.code)
}

func (w *ExplainQueryProdFetch) GetIsIdempotent() bool {
	return true
}

func (w *ExplainQueryProdFetch) GetStateValues() (json.RawMessage, error) {
	return json.RawMessage{}, nil
}

type ExplainPlanOrderingRegression struct {
	level Level
	key   string
}

func NewExplainPlanOrderingRegressionWarning(level Level, key string) *ExplainPlanOrderingRegression {
	return &ExplainPlanOrderingRegression{
		level: level,
		key:   key,
	}
}

func (w *ExplainPlanOrderingRegression) GetText() string {
	return fmt.Sprintf("Plan ordering regression: new sort on %s", w.key)
}

func (w *ExplainPlanOrderingRegression) GetTextLong() string {
	return fmt.Sprintf("Query plan regression: the post-migration plan introduces a new sort on %s that was not present before, suggesting an index that previously provided ordering was dropped.", w.key)
}

func (w *ExplainPlanOrderingRegression) GetLevel() Level {
	return w.level
}

func (w *ExplainPlanOrderingRegression) GetCode() Code {
	return CodeExplainQueryPlanOrderingRegression
}

func (w *ExplainPlanOrderingRegression) GetFullCode() string {
	return fmt.Sprintf("%s.%s", w.GetCode(), EscapeKey(w.key))
}

func (w *ExplainPlanOrderingRegression) GetIsIdempotent() bool {
	return true
}

type ExplainPlanOrderingRegressionState struct {
	Level Level `json:"level"`
}

func (w *ExplainPlanOrderingRegression) GetStateValues() (json.RawMessage, error) {
	v := ExplainPlanOrderingRegressionState{
		Level: w.level,
	}
	return json.Marshal(v)
}

type ExplainPlanScanRegression struct {
	level  Level
	key    string
	before string
	after  string
}

func NewExplainPlanScanRegressionWarning(level Level, rel, before, after string) *ExplainPlanScanRegression {
	return &ExplainPlanScanRegression{
		level:  level,
		key:    rel,
		before: before,
		after:  after,
	}
}

func (w *ExplainPlanScanRegression) GetText() string {
	return fmt.Sprintf("Plan scan regression on %s: %s -> %s", w.key, w.before, w.after)
}

func (w *ExplainPlanScanRegression) GetTextLong() string {
	return fmt.Sprintf("Query plan regression on relation %s: access method changed from %s to %s after the migration. This usually means an index the query relied on is no longer usable.", w.key, w.before, w.after)
}

func (w *ExplainPlanScanRegression) GetLevel() Level {
	return w.level
}

func (w *ExplainPlanScanRegression) GetCode() Code {
	return CodeExplainQueryPlanScanRegression
}

func (w *ExplainPlanScanRegression) GetFullCode() string {
	return fmt.Sprintf("%s.%s", w.GetCode(), EscapeKey(w.key))
}

func (w *ExplainPlanScanRegression) GetIsIdempotent() bool {
	return true
}

type ExplainPlanScanRegressionState struct {
	Level  Level  `json:"level"`
	Before string `json:"before"`
	After  string `json:"after"`
}

func (w *ExplainPlanScanRegression) GetStateValues() (json.RawMessage, error) {
	v := ExplainPlanScanRegressionState{
		Level:  w.level,
		Before: w.before,
		After:  w.after,
	}
	return json.Marshal(v)
}
