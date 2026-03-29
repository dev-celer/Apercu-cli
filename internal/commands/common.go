package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"apercu-cli/internal/migration"
	"context"
	"errors"
	"fmt"
	"os"
)

func ApplyMigration(ctx context.Context, dbConfig config.Database, connectionFields database.ConnectionFields) string {
	var migrationMessage string
	migrationHandler := migration.GetMigrationHandler(dbConfig, connectionFields)
	if migrationHandler != nil {
		// Get the current migration count
		initCount, initCountErr := migrationHandler.GetCount()
		if initCountErr != nil && !errors.Is(initCountErr, migration.ErrMigrationTableNotFound) {
			fmt.Println(initCountErr)
			os.Exit(1)
		}

		// Apply the migrations
		if err := migrationHandler.Apply(ctx); err != nil {
			fmt.Println("Migration failed")
			if output := migrationHandler.GetOutput(); output != "" {
				fmt.Println(migrationHandler.GetOutput())
			}
			fmt.Println(err)
			os.Exit(1)
		}
		if runnerOutput {
			if output := migrationHandler.GetOutput(); output != "" {
				fmt.Println("\n-----Migration runner output-----")
				fmt.Println(migrationHandler.GetOutput())
				fmt.Println("---------------------------------")
			}
		}

		// Get the new migration count
		var migrationCount int
		finalCount, finalCountErr := migrationHandler.GetCount()
		if finalCountErr != nil {
			if errors.Is(finalCountErr, migration.ErrMigrationTableNotFound) {
				fmt.Println("WARNING: migration table not found, cannot determine migration count")
			} else {
				fmt.Println(finalCountErr)
				os.Exit(1)
			}
		} else {
			if initCountErr != nil {
				initCount = 0
			}
			migrationCount = finalCount - initCount
		}

		// Generate the migration message
		migrationMessage = "Migration completed successfully"
		if duration := migrationHandler.GetDuration(); duration != nil {
			migrationMessage += fmt.Sprintf(", completed in %s", duration.String())
		}
		if finalCountErr == nil {
			migrationMessage += fmt.Sprintf(", %d migrations applied", migrationCount)
		}
	}
	return migrationMessage
}
