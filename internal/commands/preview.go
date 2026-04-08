package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"apercu-cli/internal/migration"
	"apercu-cli/internal/seeding"
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

	// Get state
	var state config.State
	if statePath != "" {
		state, err = config.GetState(statePath)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	} else {
		state = *config.NewState()
	}
	dbState, ok := state.Databases[dbName]
	if ok {
		slog.Debug("State found for database", "database", dbName, "state", dbState)
	} else {
		slog.Debug("State not found for database", "database", dbName)
	}

	// Apply the database
	dbHandler, err := database.GetSourceDatabaseHandler(dbConfig)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := dbHandler.Apply(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	conn, err := dbHandler.GetConnectionFields()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Apply the migrations
	ctx := cmd.Context()
	migrationHandler := migration.GetMigrationHandler(dbConfig, conn)
	migrationMessage, err := ApplyMigration(ctx, migrationHandler)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Apply the seeding
	seedHandler, err := seeding.GetSeedingHandler(dbConfig, &dbState, conn)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() {
		if seedHandler != nil {
			_ = seedHandler.Close()
		}
	}()
	seedingMessage, err := ApplySeeding(seedHandler)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Save the state
	state.Databases[dbName] = dbState
	if statePath != "" {
		if err := state.Save(statePath); err != nil {
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
		databaseConnections := map[string]database.ConnectionFields{
			dbName: conn,
		}
		jsonData, err := json.Marshal(databaseConnections)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("Failed to marshal database connections: %v", err))
			os.Exit(1)
		}

		fmt.Println(fmt.Sprintf("DATABASE_CONNECTIONS=%s", string(jsonData)))
	} else {
		fmt.Println(fmt.Sprintf("DATABASE_URL: %s", conn.Url))
	}
	return nil
}
