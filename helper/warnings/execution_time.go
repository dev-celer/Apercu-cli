package warnings

import (
	"apercu-cli/output"
	"fmt"
	"log/slog"
)

const ExecutionTimeThreshold = 0.2

func GenerateExecutionTimeWarnings(preMigrationRun *output.OutputDatabaseMigrationExplainQueryRun, postMigrationRun *output.OutputDatabaseMigrationExplainQueryRun) string {
	// Planned time regression
	// If planned time increased by more than 20%, warn about query planned time regression
	bPlannedTimeRegression := false
	prePlannedTime := preMigrationRun.PlannedTime.Microseconds()
	postPlannedTime := postMigrationRun.PlannedTime.Microseconds()
	diff := postPlannedTime - prePlannedTime
	if diff > 0 && diff >= int64(float64(prePlannedTime)*ExecutionTimeThreshold) {
		bPlannedTimeRegression = true
	}

	// Real time regression
	// If real time increased by more than 20%, warn about query real time regression
	bRealTimeRegression := false
	preRealTime := preMigrationRun.RealTime.Microseconds()
	postRealTime := postMigrationRun.RealTime.Microseconds()
	diff = postRealTime - preRealTime
	if diff > 0 && diff > int64(float64(preRealTime)*ExecutionTimeThreshold) {
		bRealTimeRegression = true
	}

	warningText := ""
	if bPlannedTimeRegression && bRealTimeRegression {
		slog.Debug("Query exceeded time regression threshold", "prePlannedTime", prePlannedTime, "postPlannedTime", postPlannedTime, "preRealTime", preRealTime, "postRealTime", postRealTime)
		warningText = fmt.Sprintf("Query execution time regression exceeded %d%% threshold", int(ExecutionTimeThreshold*100))
	} else {
		if bRealTimeRegression {
			slog.Debug("Query exceeded real time regression threshold", "preRealTime", preRealTime, "postRealTime", postRealTime)
			warningText = fmt.Sprintf("Query real execution time regression exceeded %d%% threshold", int(ExecutionTimeThreshold*100))
		}
		if bPlannedTimeRegression {
			slog.Debug("Query exceeded planned time regression threshold", "prePlannedTime", prePlannedTime, "postPlannedTime", postPlannedTime)
			warningText = fmt.Sprintf("Query planned execution time regression exceeded %d%% threshold", int(ExecutionTimeThreshold*100))
		}
	}
	return warningText
}
