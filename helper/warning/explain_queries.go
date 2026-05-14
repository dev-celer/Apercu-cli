package warning

import (
	"fmt"
)

const (
	ExplainQueryTimeRegression = "EXPL_QUERY_REGR"
)

type ExplainQuery struct {
	Hi, Lo, Med float64
	Level       Level
}

func (w *ExplainQuery) GetWarningText() string {
	return fmt.Sprintf("Query execution time regression threshold (med: %+.1f 95%% CI: %+.1f to %+.1f)", w.Med, w.Lo, w.Hi)
}

func (w *ExplainQuery) GetWarningLevel() Level {
	return w.Level
}

func (w *ExplainQuery) GetWarningCode() Code {
	return ExplainQueryTimeRegression
}
