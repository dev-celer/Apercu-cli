package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"apercu-cli/internal/migration"
	"apercu-cli/internal/seeding"
	"context"
	"errors"
	"fmt"
	"os"
)

func ApplySeeding(dbConfig config.Database, state *config.DatabaseState, connectionFields database.ConnectionFields) string {
	var seedingMessage string
	seedHandler, err := seeding.GetSeedingHandler(dbConfig, state, connectionFields)
	defer func() { _ = seedHandler.Close() }()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if seedHandler != nil {
		seedHandler.Apply()

		if runnerOutput {
			fmt.Println("\n-----Seeding output-----")
			fmt.Println(seedHandler.GetOutput())
			fmt.Println("---------------------")
		}

		if errCount := seedHandler.GetFailedCount(); errCount > 0 {
			seedingMessage = fmt.Sprintf("Seeding completed with %d errors", errCount)
		} else {
			seedingMessage = "Seeding completed successfully"
		}

		if duration := seedHandler.GetDuration(); duration != nil {
			seedingMessage += fmt.Sprintf(", completed in %s", duration.String())
		}
		seedingMessage += fmt.Sprintf(", %d files applied successfully", seedHandler.GetAppliedCount())
	}

	return seedingMessage
}

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
