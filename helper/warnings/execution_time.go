package warnings

import (
	"apercu-cli/output"
	"fmt"
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
	if diff > 0 && diff >= int64(float64(preRealTime)*ExecutionTimeThreshold) {
		bRealTimeRegression = true
	}

	warningText := ""
	if bPlannedTimeRegression && bRealTimeRegression {
		warningText = fmt.Sprintf("Query execution time regression exceeded %d%% threshold", ExecutionTimeThreshold*100)
	} else {
		if bRealTimeRegression {
			warningText = fmt.Sprintf("Query real execution time regression exceeded %d%% threshold", ExecutionTimeThreshold*100)
		}
		if bPlannedTimeRegression {
			warningText = fmt.Sprintf("Query planned execution time regression exceeded %d%% threshold", ExecutionTimeThreshold*100)
		}
	}
	return warningText
}
