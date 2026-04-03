package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"fmt"
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
		return err
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
			return err
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
		fmt.Println(err)
		os.Exit(1)
	}
	if err := dbHandler.Apply(); err != nil {
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

	// Save the state
	state.Databases[dbName] = dbState
	if statePath != "" {
		if err := state.Save(statePath); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
		}
	}

	if migrationMessage != "" {
		fmt.Println("\n" + migrationMessage)
	}
	if seedingMessage != "" {
		fmt.Println(seedingMessage)
	}
	fmt.Println(fmt.Sprintf("\nDATABASE_URL: %s", conn.Url))
	return nil
}
