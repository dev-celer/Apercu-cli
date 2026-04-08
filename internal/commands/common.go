package commands

import (
	"apercu-cli/internal/migration"
	"apercu-cli/internal/seeding"
	"context"
	"errors"
	"fmt"
	"log"
)

func ApplySeeding(seedHandler seeding.HandlerInterface) (string, error) {
	if seedHandler == nil {
		return "", nil
	}

	seedHandler.Apply()

	if runnerOutput || seedHandler.GetFailedCount() > 0 {
		_, _ = fmt.Fprintln(log.Writer(), "\n-----Seeding output-----")
		_, _ = fmt.Fprintln(log.Writer(), seedHandler.GetOutput())
		_, _ = fmt.Fprintln(log.Writer(), "---------------------")
	}

	var seedingMessage string
	if errCount := seedHandler.GetFailedCount(); errCount > 0 {
		seedingMessage = fmt.Sprintf("Seeding completed with %d errors", errCount)
	} else {
		seedingMessage = "Seeding completed successfully"
	}

	if duration := seedHandler.GetDuration(); duration != nil {
		seedingMessage += fmt.Sprintf(", completed in %s", duration.String())
	}
	seedingMessage += fmt.Sprintf(", %d files applied successfully", seedHandler.GetAppliedCount())

	return seedingMessage, nil
}

func ApplyMigration(ctx context.Context, migrationHandler migration.HandlerInterface) (string, error) {
	if migrationHandler == nil {
		return "", nil
	}

	// Get the current migration count
	initCount, initCountErr := migrationHandler.GetCount()
	if initCountErr != nil && !errors.Is(initCountErr, migration.ErrMigrationTableNotFound) {
		return "", initCountErr
	}

	// Apply the migrations
	if err := migrationHandler.Apply(ctx); err != nil {
		if output := migrationHandler.GetOutput(); output != "" {
			_, _ = fmt.Fprintln(log.Writer(), "\n-----Migration runner output-----")
			_, _ = fmt.Fprintln(log.Writer(), output)
			_, _ = fmt.Fprintln(log.Writer(), "---------------------------------")
		}
		return "", fmt.Errorf("migration failed: %w", err)
	}

	if runnerOutput {
		if output := migrationHandler.GetOutput(); output != "" {
			_, _ = fmt.Fprintln(log.Writer(), "\n-----Migration runner output-----")
			_, _ = fmt.Fprintln(log.Writer(), migrationHandler.GetOutput())
			_, _ = fmt.Fprintln(log.Writer(), "---------------------------------")
		}
	}

	// Get the new migration count
	var migrationCount int
	finalCount, finalCountErr := migrationHandler.GetCount()
	if finalCountErr != nil {
		if errors.Is(finalCountErr, migration.ErrMigrationTableNotFound) {
			_, _ = fmt.Fprintln(log.Writer(), "WARNING: migration table not found, cannot determine migration count")
		} else {
			return "", finalCountErr
		}
	} else {
		if initCountErr != nil {
			initCount = 0
		}
		migrationCount = finalCount - initCount
	}

	// Generate the migration message
	migrationMessage := "Migration completed successfully"
	if duration := migrationHandler.GetDuration(); duration != nil {
		migrationMessage += fmt.Sprintf(", completed in %s", duration.String())
	}
	if finalCountErr == nil {
		migrationMessage += fmt.Sprintf(", %d migrations applied", migrationCount)
	}

	return migrationMessage, nil
}
