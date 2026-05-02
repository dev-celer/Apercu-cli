package commands

import (
	"apercu-cli/helper"
	"apercu-cli/helper/metrics"
	"apercu-cli/helper/pgproxy"
	"apercu-cli/helper/schema_diff"
	warningshelper "apercu-cli/helper/warnings"
	"apercu-cli/internal/migration"
	"apercu-cli/internal/seeding"
	"apercu-cli/output"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"slices"
	"time"

	_ "github.com/lib/pq"
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

func ApplyMigration(ctx context.Context, migrationHandler migration.HandlerInterface, databaseConn *helper.ConnectionFields, explainQuery []string) (string, error) {
	if migrationHandler == nil {
		return "", nil
	}

	migrationOutput := migrationHandler.GetOutput()

	queriesExtractOutput, err := metrics.ExtractAllQueriesToExplain(explainQuery)
	if err != nil {
		migrationOutput.Errors = append(migrationOutput.Errors, err.Error())
		return "", err
	}
	migrationOutput.Warnings = append(migrationOutput.Warnings, queriesExtractOutput.Warnings...)
	explainQueriesStats := make([]output.OutputDatabaseMigrationExplainQuery, 0)

	var initialSchema map[string]schema_diff.Schema
	var initialSize int64
	var initialWALSize int64
	if databaseConn != nil {
		db, err := sql.Open("postgres", databaseConn.Url)
		if err != nil {
			return "", errors.New(fmt.Sprintf("Failed to connect to database: %v", err))
		}
		defer func() { _ = db.Close() }()

		initialSchema, err = schema_diff.GetSchema(db)
		if err != nil {
			return "", err
		}
		initialSize, err = metrics.GetDatabaseStorageInBytes(db, databaseConn.Database)
		if err != nil {
			return "", err
		}
		initialWALSize, err = metrics.GetWALBytes(db)
		if err != nil {
			return "", err
		}

		for file, queries := range queriesExtractOutput.Queries {
			for _, query := range queries {
				explainResult, err := metrics.ExplainQuery(db, query)
				run := output.OutputDatabaseMigrationExplainQueryRun{}

				if err != nil {
					run.Error = new(err.Error())
				} else {
					run.ExplainedQuery = explainResult
					run.PlannedTime = new(time.Duration(explainResult.PlanningTime * float64(time.Millisecond)))
					run.RealTime = new(time.Duration(explainResult.ExecutionTime * float64(time.Millisecond)))
				}

				explainQueriesStats = append(explainQueriesStats, output.OutputDatabaseMigrationExplainQuery{
					File:            file,
					Query:           query,
					PreMigrationRun: &run,
				})
			}
		}
	}

	// Apply the migrations
	if err := migrationHandler.Apply(ctx); err != nil {
		if migrationOutput != nil && migrationOutput.Logs != nil {
			_, _ = fmt.Fprintln(log.Writer(), "\n-----Migration runner output-----")
			_, _ = fmt.Fprintln(log.Writer(), *migrationOutput.Logs)
			_, _ = fmt.Fprintln(log.Writer(), "---------------------------------")
			migrationOutput.Errors = append(migrationOutput.Errors, err.Error())
		}
		return "", fmt.Errorf("migration failed: %w", err)
	}

	if runnerOutput {
		if migrationOutput != nil && migrationOutput.Logs != nil {
			_, _ = fmt.Fprintln(log.Writer(), "\n-----Migration runner output-----")
			_, _ = fmt.Fprintln(log.Writer(), *migrationOutput.Logs)
			_, _ = fmt.Fprintln(log.Writer(), "---------------------------------")
		}
	}

	if databaseConn != nil && migrationOutput != nil {
		db, err := sql.Open("postgres", databaseConn.Url)
		if err != nil {
			return "", errors.New(fmt.Sprintf("Failed to connect to database: %v", err))
		}
		defer func() { _ = db.Close() }()

		// Get schema diff
		finalSchema, err := schema_diff.GetSchema(db)
		if err != nil {
			return "", err
		}
		migrationOutput.SchemaDiff = schema_diff.GetSchemaDiffText(initialSchema, finalSchema)

		// Get Database size metrics
		finalSize, err := metrics.GetDatabaseStorageInBytes(db, databaseConn.Database)
		if err != nil {
			return "", err
		}

		// Get WAL Size metrics
		finalWALSize, err := metrics.GetWALBytes(db)
		if err != nil {
			return "", err
		}

		// Get Locks metrics
		locks := output.GetTableLockStats(migrationOutput.PgProxyLogs)

		// Get Explain queries stats
		for file, queries := range queriesExtractOutput.Queries {
			for _, query := range queries {
				idx := slices.IndexFunc(explainQueriesStats, func(s output.OutputDatabaseMigrationExplainQuery) bool {
					return s.Query == query && s.File == file
				})
				if idx == -1 {
					continue
				}

				explainResult, err := metrics.ExplainQuery(db, query)
				run := output.OutputDatabaseMigrationExplainQueryRun{}

				if err != nil {
					run.Error = new(err.Error())
				} else {
					run.ExplainedQuery = explainResult
					run.PlannedTime = new(time.Duration(explainResult.PlanningTime * float64(time.Millisecond)))
					run.RealTime = new(time.Duration(explainResult.ExecutionTime * float64(time.Millisecond)))
				}

				explainQueriesStats[idx].PostMigrationRun = &run

				if explainQueriesStats[idx].PreMigrationRun == nil || explainQueriesStats[idx].PostMigrationRun == nil ||
					explainQueriesStats[idx].PreMigrationRun.Error != nil || explainQueriesStats[idx].PostMigrationRun.Error != nil {
					continue
				}
				if len(query) > 120 {
					query = query[:120] + "..."
				}

				// Generate warnings
				if warningText := warningshelper.GenerateExecutionTimeWarnings(explainQueriesStats[idx].PreMigrationRun, explainQueriesStats[idx].PostMigrationRun, file, query); warningText != "" {
					_, _ = fmt.Fprintln(log.Writer(), fmt.Sprintf("WARNING: %s", warningText))
					explainQueriesStats[idx].Warnings = append(explainQueriesStats[idx].Warnings, warningText)
				}
			}
		}

		migrationOutput.Stats = output.NewOutputDatabaseMigrationStats(initialSize, finalSize, initialWALSize, finalWALSize, locks, explainQueriesStats)

		// Handle Warnings
		if migrationOutput.Stats.WALDelta > 1024*1024*1024 {
			migrationOutput.Warnings = append(migrationOutput.Warnings, "WAL size generated over 1GB, risk of replication lag")
		}

		AELocks, ok := migrationOutput.Stats.LockStats[pgproxy.QueryLockAccessExclusive]
		if ok {
			for table, lock := range AELocks {
				if lock.MaxDuration >= time.Second {
					migrationOutput.Warnings = append(migrationOutput.Warnings, fmt.Sprintf("Access Exclusive lock on table %s exceeded 1 second", table))
				}
			}
		}
	}

	// Generate the migration message
	migrationMessage := "Migration completed successfully"
	if migrationOutput.Duration != "" {
		migrationMessage += fmt.Sprintf(", completed in %s", migrationOutput.Duration)
	}
	migrationMessage += fmt.Sprintf(", %d migrations applied", migrationOutput.Count)

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
