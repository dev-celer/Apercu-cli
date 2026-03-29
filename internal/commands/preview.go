package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"fmt"
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
	configFile, err := config.LoadConfig(".")
	if err != nil {
		return err
	}

	var dbConfig config.Database
	for _, db := range configFile.Databases {
		dbConfig = db
		break
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
	seedingMessage := ApplySeeding(dbConfig, conn)

	if migrationMessage != "" {
		fmt.Println("\n" + migrationMessage)
	}
	if seedingMessage != "" {
		fmt.Println(seedingMessage)
	}
	fmt.Println(fmt.Sprintf("\nDATABASE_URL: %s", conn.Url))
	return nil
}
