package commands

import (
	"apercu-cli/internal/migration"
	"apercu-cli/internal/seeding"
	"apercu-cli/output"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func ApplySeeding(seedHandler seeding.HandlerInterface) string {
	if seedHandler == nil {
		return ""
	}

	seedHandler.Apply()

	if runnerOutput || seedHandler.GetOutput().FailedCount > 0 {
		_, _ = fmt.Fprintln(log.Writer(), "\n-----Seeding output-----")
		_, _ = fmt.Fprintln(log.Writer(), seedHandler.GetOutput())
		_, _ = fmt.Fprintln(log.Writer(), "---------------------")
	}

	var seedingMessage string
	if errCount := seedHandler.GetOutput().FailedCount; errCount > 0 {
		seedingMessage = fmt.Sprintf("Seeding completed with %d errors", errCount)
	} else {
		seedingMessage = "Seeding completed successfully"
	}

	if duration := seedHandler.GetOutput().Duration; duration != "" {
		seedingMessage += fmt.Sprintf(", completed in %s", duration)
	}
	seedingMessage += fmt.Sprintf(", %d files applied successfully", seedHandler.GetOutput().SuccessCount)

	return seedingMessage
}

func ApplyMigration(ctx context.Context, migrationHandler migration.HandlerInterface) (string, error) {
	if migrationHandler == nil {
		return "", nil
	}

	output := migrationHandler.GetOutput()

	// Apply the migrations
	if err := migrationHandler.Apply(ctx); err != nil {
		if output != nil && output.Logs != nil {
			_, _ = fmt.Fprintln(log.Writer(), "\n-----Migration runner output-----")
			_, _ = fmt.Fprintln(log.Writer(), output.Logs)
			_, _ = fmt.Fprintln(log.Writer(), "---------------------------------")
			output.Errors = append(output.Errors, err.Error())
		}
		return "", fmt.Errorf("migration failed: %w", err)
	}

	if runnerOutput {
		if output != nil && output.Logs != nil {
			_, _ = fmt.Fprintln(log.Writer(), "\n-----Migration runner output-----")
			_, _ = fmt.Fprintln(log.Writer(), output.Logs)
			_, _ = fmt.Fprintln(log.Writer(), "---------------------------------")
		}
	}

	// Generate the migration message
	migrationMessage := "Migration completed successfully"
	if output.Duration != "" {
		migrationMessage += fmt.Sprintf(", completed in %s", output.Duration)
	}
	migrationMessage += fmt.Sprintf(", %d migrations applied", output.Count)

	return migrationMessage, nil
}

func ErrorAndExit(err error, dbOutput *output.OutputDatabase, dbName string) {
	if jsonOutput {
		outputData := output.Output{
			Databases: map[string]output.OutputDatabase{
				dbName: *dbOutput,
			},
		}
		jsonData, err := json.Marshal(outputData)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("Failed to marshal database json output: %v", err))
			os.Exit(1)
		}

		fmt.Println(fmt.Sprintf("OUTPUT=%s", string(jsonData)))
	}
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
