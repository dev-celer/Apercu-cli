package warning

import (
	"fmt"
)

const (
	ExplainQueryRealRegression     = "EXPL_QUERY_REGR_R"
	ExplainQueryPlanningRegression = "EXPL_QUERY_REGR_P"
	ExplainQueryBothRegression     = "EXPL_QUERY_REGR_RP"
)

type ExplainQuery struct {
	planned *ExplainQueryPlannedValue
	real    *ExplainQueryRealValue
}

func (w *ExplainQuery) GetWarningText() string {
	if w.planned == nil && w.real == nil {
		return ""
	}

	switch {
	case w.planned == nil:
		return fmt.Sprintf("Query execution time regression exceeded real execution time increase threshold (med: %.2f, range: %.2f-%.2f)", w.real.Med, w.real.Lo, w.real.Hi)
	case w.real == nil:
		return fmt.Sprintf("Query execution time regression exceeded planning cost increase threshold (before: %.2f, after: %.2f)", w.planned.InitialCost, w.planned.FinalCost)
	default:
		return fmt.Sprintf("Query execution time regression exceeded planning and real threshold (planning cost: before: %.2f, after: %.2f | execution time delta: med: %.2fms, range: %.2fms-%.2fms)", w.planned.InitialCost, w.planned.FinalCost, w.real.Med, w.real.Lo, w.real.Hi)
	}
}

func (w *ExplainQuery) GetWarningLevel() Level {
	if w.planned == nil && w.real == nil {
		return 0
	}

	switch {
	case w.real == nil:
		return w.planned.Level
	case w.planned == nil:
		return w.real.Level
	case w.planned.Level < w.real.Level:
		return w.real.Level
	case w.planned.Level > w.real.Level:
		return w.planned.Level
	default:
		return w.planned.Level
	}
}

func (w *ExplainQuery) GetWarningCode() Code {
	if w.planned == nil && w.real == nil {
		return ""
	}

	switch {
	case w.real == nil:
		return ExplainQueryPlanningRegression
	case w.planned == nil:
		return ExplainQueryRealRegression
	default:
		return ExplainQueryBothRegression
	}
}

type ExplainQueryPlannedValue struct {
	InitialCost, FinalCost float64
	Level                  Level
}

type ExplainQueryRealValue struct {
	Hi, Lo, Med float64
	Level       Level
}

func NewExplainQueryWarning(planned *ExplainQueryPlannedValue, real *ExplainQueryRealValue) Warning {
	if planned == nil && real == nil {
		return nil
	}
	return &ExplainQuery{
		planned: planned,
		real:    real,
	}
}
