package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"encoding/json"
	"fmt"
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
		return err
	}

	var dbConfig config.Database
	var dbName string
	for name, db := range configFile.Databases {
		dbName = name
		dbConfig = db
		break
	}

	// Initialize new state
	var dbState config.DatabaseState

	// Reset the database
	dbHandler, err := database.GetSourceDatabaseHandler(dbConfig)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := dbHandler.Reset(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	conn, err := dbHandler.GetConnectionFields()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Apply the migrations
	ctx := cmd.Context()
	migrationMessage := ApplyMigration(ctx, dbConfig, conn)

	// Apply the seeding
	seedingMessage := ApplySeeding(dbConfig, &dbState, conn)

	// Save a new state
	state := config.NewState()
	state.Databases[dbName] = dbState
	if statePath != "" {
		if err := state.Save(statePath); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
		}
	}

	if migrationMessage != "" {
		fmt.Println(migrationMessage)
	}
	if seedingMessage != "" {
		fmt.Println(seedingMessage)
	}

	if jsonOutput {
		databaseConnections := map[string]database.ConnectionFields{
			dbName: conn,
		}
		jsonData, err := json.Marshal(databaseConnections)
		if err != nil {
			fmt.Println(fmt.Sprintf("Failed to marshal database connections: %v", err))
			os.Exit(1)
		}

		fmt.Println(fmt.Sprintf("\nDATABASE_CONNECTIONS=%s", string(jsonData)))
	} else {
		fmt.Println(fmt.Sprintf("\nDATABASE_URL: %s", conn.Url))
	}
	return nil
}
