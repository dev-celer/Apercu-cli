package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"apercu-cli/internal/migration"
	"apercu-cli/internal/seeding"
	"apercu-cli/output"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
)

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset the preview database",
	Args:  cobra.NoArgs,
	RunE:  reset,
}

func init() {
	rootCmd.AddCommand(resetCmd)
}

func reset(cmd *cobra.Command, args []string) error {
	// Get config
	configFile, err := config.LoadConfig(".")
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var dbConfig config.Database
	var dbName string
	for name, db := range configFile.Databases {
		dbName = name
		dbConfig = db
		break
	}
	dbOutput := output.NewOutputDatabase()

	// Initialize new state
	var dbState config.DatabaseState

	// Reset the database
	dbHandler, err := database.GetSourceDatabaseHandler(dbConfig)
	if err != nil {
		dbOutput.Errors = append(dbOutput.Errors, err.Error())
		ErrorAndExit(err, dbOutput, dbName)
	}
	if err := dbHandler.Reset(); err != nil {
		dbOutput.Errors = append(dbOutput.Errors, err.Error())
		ErrorAndExit(err, dbOutput, dbName)
	}
	conn, err := dbHandler.GetConnectionFields()
	if err != nil {
		dbOutput.Errors = append(dbOutput.Errors, err.Error())
		ErrorAndExit(err, dbOutput, dbName)
	}

	// Apply the migrations
	ctx := cmd.Context()
	migrationHandler := migration.GetMigrationHandler(dbConfig, conn)
	migrationMessage, err := ApplyMigration(ctx, migrationHandler)
	if err != nil {
		dbOutput.Migration = migrationHandler.GetOutput()
		ErrorAndExit(err, dbOutput, dbName)
	}
	if migrationHandler != nil {
		dbOutput.Migration = migrationHandler.GetOutput()
	}

	// Apply the seeding
	seedHandler, err := seeding.GetSeedingHandler(dbConfig, &dbState, conn)
	if err != nil {
		dbOutput.Seeding = output.NewSeedingOutput()
		dbOutput.Seeding.Errors = append(dbOutput.Seeding.Errors, err.Error())
		ErrorAndExit(err, dbOutput, dbName)
	}
	defer func() {
		if seedHandler != nil {
			_ = seedHandler.Close()
		}
	}()
	seedingMessage := ApplySeeding(seedHandler)
	if seedHandler != nil {
		dbOutput.Seeding = seedHandler.GetOutput()
	}

	// Save a new state
	state := config.NewState()
	state.Databases[dbName] = dbState
	if statePath != "" {
		if err := state.Save(statePath); err != nil {
			dbOutput.Warnings = append(dbOutput.Warnings, err.Error())
			_, _ = fmt.Fprintln(os.Stderr, err)
		}
	}

	if migrationMessage != "" {
		_, _ = fmt.Fprintln(log.Writer(), "\n"+migrationMessage)
	}
	if seedingMessage != "" {
		_, _ = fmt.Fprintln(log.Writer(), seedingMessage)
	}
	_, _ = fmt.Fprintln(log.Writer())

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

		connectionOutput := map[string]database.ConnectionFields{
			dbName: conn,
		}
		connJsonData, err := json.Marshal(connectionOutput)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("Failed to marshal database connection json output: %v", err))
			os.Exit(1)
		}

		fmt.Println(fmt.Sprintf("DATABASE_CONNECTIONS=%s", string(connJsonData)))
		fmt.Println(fmt.Sprintf("OUTPUT=%s", string(jsonData)))
	} else {
		fmt.Println(fmt.Sprintf("DATABASE_URL: %s", conn.Url))
	}
	return nil
}
