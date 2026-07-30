package commands

import (
	"apercu-cli/config"
	"apercu-cli/helper"
	"apercu-cli/helper/warning"
	"apercu-cli/internal/database"
	"apercu-cli/internal/metrics"
	"apercu-cli/internal/migration"
	"apercu-cli/internal/seeding"
	"apercu-cli/output"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var previewCmd = &cobra.Command{
	Use:   "preview",
	Short: "Create or update the preview database",
	Args:  cobra.NoArgs,
	RunE:  preview,
}

func init() {
	rootCmd.AddCommand(previewCmd)
}

func preview(cmd *cobra.Command, args []string) error {
	// Get config
	configFile, err := config.LoadConfig(".")
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var dbConfig config.Database
	var dbName string
	for name, db := range configFile.Databases {
		dbConfig = db
		dbName = name
		break
	}
	previewOutput := output.NewPreviewOutput()

	// Get state
	var state config.State
	if statePath != "" {
		state, err = config.GetState(statePath)
		if err != nil {
			slog.Debug("Error loading state file", "path", statePath, "error", err)
			w := warning.NewStateFileWarning(statePath)
			previewOutput.Warnings.AddWarningAndPrint(w)
		}
	} else {
		state = *config.NewState()
	}
	dbState, ok := state.Databases[dbName]
	if ok {
		slog.Debug("State found for database", "database", dbName, "state", dbState)
	} else {
		slog.Debug("State not found for database", "database", dbName)
		dbState = config.NewDatabaseState()
	}

	// Create the preview database if it doesn't exist
	prodConn, dbHandler, err := database.GetPreviewDatabaseHandler(dbConfig, previewOutput.Warnings)
	if err != nil {
		return ErrorAndExit(err, previewOutput, dbName)
	}
	if dbHandler == nil {
		return nil
	}

	exist, err := dbHandler.Exists()
	if err != nil {
		return ErrorAndExit(err, previewOutput, dbName)
	}
	if !exist {
		if err := dbHandler.Create(); err != nil {
			return ErrorAndExit(err, previewOutput, dbName)
		}
	}
	previewConn, err := dbHandler.GetConnectionFields()
	if err != nil {
		return ErrorAndExit(err, previewOutput, dbName)
	}

	// Initialize migration handler
	ctx := cmd.Context()
	migrationHandler, err := migration.GetMigrationHandler(dbConfig, &previewConn, previewOutput.Warnings)
	if err != nil {
		return ErrorAndExit(err, previewOutput, dbName)
	}

	// Initialize metrics handler
	metricHandler, err := metrics.NewMetricsHandler(prodConn.Url, previewConn.Url, &dbConfig, &configFile, previewOutput.Warnings)
	if err != nil {
		return ErrorAndExit(err, previewOutput, dbName)
	}

	// Apply migration
	migrationMessage, err := ApplyMigration(ctx, migrationHandler, metricHandler)
	if err != nil {
		return ErrorAndExit(err, previewOutput, dbName)
	}
	if migrationHandler != nil {
		previewOutput.Migration = migrationHandler.GetOutput()
	}

	// Apply the seeding
	seedHandler, err := seeding.GetSeedingHandler(dbConfig, &dbState, previewConn, previewOutput.Warnings)
	if err != nil {
		previewOutput.Seeding = output.NewSeedingOutput()
		return ErrorAndExit(err, previewOutput, dbName)
	}
	defer func() {
		if seedHandler != nil {
			_ = seedHandler.Close()
		}
	}()
	seedingMessage := ApplySeeding(seedHandler)
	if seedHandler != nil {
		previewOutput.Seeding = seedHandler.GetOutput()
	}

	// Reconcile warnings with the state
	previewOutput.Warnings.ReconcileWarningsWithState(&dbState, metricHandler.GetProdMetrics())

	// Save the state
	state.Databases[dbName] = dbState
	if statePath != "" {
		if err := state.Save(statePath); err != nil {
			return ErrorAndExit(err, previewOutput, dbName)
		}
	}

	if migrationMessage != "" {
		_, _ = fmt.Fprintln(log.Writer(), "\n"+migrationMessage)
	}
	if seedingMessage != "" {
		_, _ = fmt.Fprintln(log.Writer(), seedingMessage)
	}
	_, _ = fmt.Fprintln(log.Writer())

	if markdownOutput != "" {
		if err := SaveMarkdownFile(markdownOutput, previewOutput); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	if outputFile != "" {
		if err := SaveOutputInFile(outputFile, previewOutput); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	if jsonOutput {
		// Print the connection json output
		connectionOutput := map[string]helper.ConnectionFields{
			dbName: previewConn,
		}
		connJsonData, err := json.Marshal(connectionOutput)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("Failed to marshal database connection json output: %v", err))
			os.Exit(1)
		}
		fmt.Println(fmt.Sprintf("DATABASE_CONNECTIONS=%s", string(connJsonData)))
	} else {
		fmt.Println(fmt.Sprintf("DATABASE_URL: %s", previewConn.Url))
	}
	return nil
}
