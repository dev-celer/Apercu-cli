package commands

import (
	"apercu-cli/internal/migration"
	"apercu-cli/internal/seeding"
	"apercu-cli/output"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
)

func ApplySeeding(seedHandler seeding.HandlerInterface) string {
	if seedHandler == nil {
		return ""
	}

	seedHandler.Apply()

	if runnerOutput || seedHandler.GetOutput().FailedCount > 0 {
		_, _ = fmt.Fprintln(log.Writer(), "\n-----Seeding output-----")
		if seedHandler.GetOutput().Logs != nil {
			_, _ = fmt.Fprintln(log.Writer(), *seedHandler.GetOutput().Logs)
		}
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
			_, _ = fmt.Fprintln(log.Writer(), *output.Logs)
			_, _ = fmt.Fprintln(log.Writer(), "---------------------------------")
			output.Errors = append(output.Errors, err.Error())
		}
		return "", fmt.Errorf("migration failed: %w", err)
	}

	if runnerOutput {
		if output != nil && output.Logs != nil {
			_, _ = fmt.Fprintln(log.Writer(), "\n-----Migration runner output-----")
			_, _ = fmt.Fprintln(log.Writer(), *output.Logs)
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

func SaveOutputInFile(path string, output *output.PreviewOutput) error {
	content, err := json.Marshal(output)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to save output file: %v", err))
	}

	if err := os.WriteFile(path, content, 0644); err != nil {
		return errors.New(fmt.Sprintf("Failed to save output file: %v", err))
	}

	slog.Debug("Output file saved", "path", path)
	return nil
}

func SaveMarkdownFile(path string, output *output.PreviewOutput) error {
	content, err := output.RenderMarkdown()
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to render markdown output: %v", err))
	}

	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		return errors.New(fmt.Sprintf("Failed to save markdown file: %v", err))
	}

	slog.Debug("Markdown file saved", "path", path)
	return nil
}

func ErrorAndExit(err error, dbOutput *output.PreviewOutputDatabase, dbName string) {
	outputData := output.PreviewOutput{
		Databases: map[string]output.PreviewOutputDatabase{
			dbName: *dbOutput,
		},
	}

	if markdownOutput != "" {
		if err := SaveMarkdownFile(markdownOutput, &outputData); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
		}
	}

	if outputFile != "" {
		if err := SaveOutputInFile(outputFile, &outputData); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(0)
	}
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
