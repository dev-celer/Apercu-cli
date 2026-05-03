package commands

import (
	"apercu-cli/config"
	"apercu-cli/helper"
	greenmaskhelper "apercu-cli/helper/greenmask"
	"apercu-cli/helper/metrics"
	"apercu-cli/helper/pgproxy"
	"apercu-cli/helper/pii"
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
				explainQueriesStats = append(explainQueriesStats, output.OutputDatabaseMigrationExplainQuery{
					File:            file,
					Query:           query,
					PreMigrationRun: new(generateQueryRun(db, query)),
				})
			}
		}
	}

	// Apply the migrations
	if err := migrationHandler.Apply(ctx); err != nil {
		if migrationOutput.Logs != nil {
			_, _ = fmt.Fprintln(log.Writer(), "\n-----Migration runner output-----")
			_, _ = fmt.Fprintln(log.Writer(), *migrationOutput.Logs)
			_, _ = fmt.Fprintln(log.Writer(), "---------------------------------")
			migrationOutput.Errors = append(migrationOutput.Errors, err.Error())
		}
		return "", fmt.Errorf("migration failed: %w", err)
	}

	if runnerOutput {
		if migrationOutput.Logs != nil {
			_, _ = fmt.Fprintln(log.Writer(), "\n-----Migration runner output-----")
			_, _ = fmt.Fprintln(log.Writer(), *migrationOutput.Logs)
			_, _ = fmt.Fprintln(log.Writer(), "---------------------------------")
		}
	}

	if databaseConn != nil {
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
		migrationOutput.SchemaDiff = schema_diff.GetSchemasDiff(initialSchema, finalSchema)

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
			regressionWarningInFile := 0
			for _, query := range queries {
				idx := slices.IndexFunc(explainQueriesStats, func(s output.OutputDatabaseMigrationExplainQuery) bool {
					return s.Query == query && s.File == file
				})
				if idx == -1 {
					continue
				}

				explainQueriesStats[idx].PostMigrationRun = new(generateQueryRun(db, query))

				if explainQueriesStats[idx].PreMigrationRun == nil || explainQueriesStats[idx].PostMigrationRun == nil ||
					explainQueriesStats[idx].PreMigrationRun.Error != nil || explainQueriesStats[idx].PostMigrationRun.Error != nil {
					continue
				}
				if len(query) > 120 {
					query = query[:120] + "..."
				}

				// Generate warnings
				if warningText := warningshelper.GenerateExecutionTimeWarnings(explainQueriesStats[idx].PreMigrationRun, explainQueriesStats[idx].PostMigrationRun); warningText != "" {
					_, _ = fmt.Fprintln(log.Writer(), fmt.Sprintf("WARNING: %s\nfile:%s\nquery:%s", warningText, file, query))
					explainQueriesStats[idx].Warnings = append(explainQueriesStats[idx].Warnings, warningText)
					regressionWarningInFile++
				}
			}
			// Add top level migration warning if a regression warning has been issued in this file
			if regressionWarningInFile > 0 {
				warningText := fmt.Sprintf("Regression detected for %d queries inside this file %s, see migration stats for more details", regressionWarningInFile, file)
				migrationOutput.Warnings = append(migrationOutput.Warnings, warningText)
			}
		}

		migrationOutput.Explains = explainQueriesStats
		migrationOutput.Stats = output.NewOutputDatabaseMigrationStats(initialSize, finalSize, initialWALSize, finalWALSize, locks)

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

func GenerateWarningsOnSchemaDiff(dbConfig *config.Database, schemasDiff map[string]*schema_diff.SchemaDiff) []string {
	if len(schemasDiff) == 0 {
		return nil
	}

	// Parse GreenMask config for handled rows
	modifiedTables := make([]greenmaskhelper.ModifiedTable, 0)
	if dbConfig != nil && dbConfig.Anonymization != nil && dbConfig.Anonymization.GreenmaskConfig != "" {
		c, err := greenmaskhelper.ParseConfig(dbConfig.Anonymization.GreenmaskConfig)
		if err != nil {
			return []string{
				err.Error(),
			}
		}
		if c != nil {
			modifiedTables = c.ModifiedTables()
		}
	}

	warnings := make([]string, 0)

	// PII fields added checks
	piiFields := detectPIIFieldsFromSchemasDiff(schemasDiff)
	for schema, t := range piiFields {
		for table, columns := range t {
			detectedColumns := make([]string, 0)
			for _, column := range columns {
				if !greenmaskhelper.IsRowModified(modifiedTables, schema, table, column) {
					detectedColumns = append(detectedColumns, column)
				}
			}

			if len(detectedColumns) > 0 {
				fullTableName := fmt.Sprintf("%s.%s", schema, table)
				var columnsList string
				for i := range detectedColumns {
					columnsList += detectedColumns[i]
					if i < len(detectedColumns)-1 {
						columnsList += ", "
					}
				}
				warnings = append(warnings, fmt.Sprintf("PII fields addition detected without anonymization in table %s (%s)", fullTableName, columnsList))
			}
		}
	}

	return warnings
}

// detectPIIFieldsFromSchemasDiff return map[Schemas]map[Table][]columns
func detectPIIFieldsFromSchemasDiff(schemasDiff map[string]*schema_diff.SchemaDiff) map[string]map[string][]string {
	piiFields := make(map[string]map[string][]string)

	for schema, diff := range schemasDiff {
		schemaFields := make(map[string][]string)

		// Handle created tables
		for _, t := range diff.CreatedTables {
			columns := make([]string, 0)
			for _, c := range t.Columns {
				if pii.IsPII(t.Name, c.Name) {
					columns = append(columns, c.Name)
				}
			}
			if len(columns) > 0 {
				schemaFields[t.Name] = columns
			}
		}

		// Handle updated tables
		for _, t := range diff.UpdatedTables {
			columns := make([]string, 0)
			for _, c := range t.CreatedColumns {
				if pii.IsPII(t.Name, c.Name) {
					columns = append(columns, c.Name)
				}
			}
			if len(columns) > 0 {
				schemaFields[t.Name] = columns
			}
		}

		if schemaFields != nil {
			piiFields[schema] = schemaFields
		}
	}

	return piiFields
}

func generateQueryRun(db *sql.DB, query string) output.OutputDatabaseMigrationExplainQueryRun {
	explainQueryResults := make([]*metrics.ExplainResult, 5)

	run := output.OutputDatabaseMigrationExplainQueryRun{}

	// Run the query 5 time
	for i := range explainQueryResults {
		explainResult, err := metrics.ExplainQuery(db, query)
		if err != nil {
			run.Error = new(err.Error())
			return run
		}

		explainQueryResults[i] = explainResult
	}

	// Discard first query as it may a served to wake up the instance or warm the cache
	explainQueryResults = explainQueryResults[1:]

	// Get average plannedTime and realTime
	avgPlannedTime := time.Duration(0)
	avgRealTime := time.Duration(0)
	for _, explainResult := range explainQueryResults {
		avgPlannedTime += time.Duration(explainResult.PlanningTime * float64(time.Millisecond))
		avgRealTime += time.Duration(explainResult.ExecutionTime * float64(time.Millisecond))
	}
	avgPlannedTime /= time.Duration(len(explainQueryResults))
	avgRealTime /= time.Duration(len(explainQueryResults))

	// Select result closest to average real time
	var closestResult *metrics.ExplainResult
	var closestDiff time.Duration
	for _, explainResult := range explainQueryResults {
		diff := avgRealTime - time.Duration(explainResult.ExecutionTime*float64(time.Millisecond))
		if diff < 0 {
			diff = diff * -1
		}

		if closestResult == nil {
			closestResult = explainResult
			closestDiff = diff
		} else {
			if diff < closestDiff {
				closestResult = explainResult
				closestDiff = diff
			}
		}
	}

	run.ExplainedQuery = closestResult
	run.PlannedTime = &avgPlannedTime
	run.RealTime = &avgRealTime
	return run
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
