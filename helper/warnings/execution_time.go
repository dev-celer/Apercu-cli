package warnings

import (
	"apercu-cli/output"
	"fmt"
	"log/slog"
)

const PlanningTimeThreshold = 0.1
const ExecutionTimeThreshold = 0.1

func GenerateExecutionTimeWarnings(q *output.OutputDatabaseMigrationExplainQuery) string {
	bPlannedTimeRegression := false
	bRealTimeRegression := false

	// Check for planned time regression
	if q.PreMigrationRun.ExplainedQuery.PlanningTime/q.PostMigrationRun.ExplainedQuery.PlanningTime-1 > PlanningTimeThreshold {
		bPlannedTimeRegression = true
	}

	// Check for execution time regression
	if q.Lo >= ExecutionTimeThreshold {
		bRealTimeRegression = true
	}

	var warningText string
	if bPlannedTimeRegression && bRealTimeRegression {
		slog.Debug("Query exceeded time regression threshold exceeded for planned time and real execution time", "median", q.MedianDelta, "hi", q.Hi, "Lo", q.Lo, "prePlannedTime", q.PreMigrationRun.ExplainedQuery.PlanningTime, "postPlannedTime", q.PostMigrationRun.ExplainedQuery.PlanningTime)
		warningText = fmt.Sprintf("Query execution time regression exceeded %d%% threshold", int(PlanningTimeThreshold*100))
	} else {
		if bPlannedTimeRegression {
			slog.Debug("Query exceeded planned time regression threshold exceeded for planned time", "prePlannedTime", q.PreMigrationRun.ExplainedQuery.PlanningTime, "postPlannedTime", q.PostMigrationRun.ExplainedQuery.PlanningTime)
			warningText = fmt.Sprintf("Query planned execution time regression exceeded %d%% threshold", int(PlanningTimeThreshold*100))
		}
		if bRealTimeRegression {
			slog.Debug("Query exceeded real time regression threshold exceeded for real time", "median", q.MedianDelta, "hi", q.Hi, "Lo", q.Lo)
			warningText = fmt.Sprintf("Query real execution time regression exceeded %d%% threshold", int(PlanningTimeThreshold*100))
		}
	}

	return warningText
}
