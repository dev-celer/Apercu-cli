package warning

import (
	"fmt"
)

const (
	CodeExplainQueryTimeRegression   Code = "EXPL_QUERY_REGR"
	CodeExplainQueryPathNotFound     Code = "EXPL_QUERY_PATH_NOT_FOUND"
	CodeExplainQueryNoQueries        Code = "EXPL_QUERY_NO_QUERIES"
	CodeExplainQueryFailedToReadFile Code = "EXPL_QUERY_ERR_READING_FILE"
)

type ExplainQueryTime struct {
	Hi, Lo, Med float64
	Level       Level
}

func (w *ExplainQueryTime) GetWarningText() string {
	return fmt.Sprintf("Query execution time regression (med: %+.1f 95%% CI: %+.1f to %+.1f)", w.Med, w.Lo, w.Hi)
}

func (w *ExplainQueryTime) GetWarningLevel() Level {
	return w.Level
}

func (w *ExplainQueryTime) GetWarningCode() Code {
	return CodeExplainQueryTimeRegression
}

type ExplainQueryCount struct {
	Level Level
	Count uint64
}

func (w ExplainQueryCount) GetWarningText() string {
	return fmt.Sprintf("Query execution time regression on %d queries", w.Count)
}

func (w ExplainQueryCount) GetWarningLevel() Level {
	return w.Level
}

func (w ExplainQueryCount) GetWarningCode() Code {
	return CodeExplainQueryTimeRegression
}

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

func (e ExplainQueryFile) GetWarningLevel() Level {
	return WarningLevelLow
}

func (e ExplainQueryFile) GetWarningCode() Code {
	return e.code
}
