package warning

import (
	"fmt"
)

const (
	CodeExplainQueryTimeRegression Code = "EXPL_QUERY_REGR"
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
