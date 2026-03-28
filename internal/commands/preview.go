package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"apercu-cli/internal/migration"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var previewCmd = &cobra.Command{
	Use:   "preview",
	Short: "Create or update the preview database for this preview name",
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

	// Apply the migrations
	ctx := cmd.Context()
	migrationHandler := migration.GetMigrationHandler(dbConfig, dbHandler.GetDatabaseUrl())
	if migrationHandler != nil {
		if err := migrationHandler.Apply(ctx); err != nil {
			fmt.Println("Migration failed")
			fmt.Println(migrationHandler.GetOutput())
			fmt.Println(err)
			os.Exit(1)
		}
		slog.Debug(migrationHandler.GetOutput())
		fmt.Println(fmt.Sprintf("Migration completed successfully, completed in %s", migrationHandler.GetDuration().String()))
	}

	fmt.Println(fmt.Sprintf("DATABASE_URL: %s", dbHandler.GetDatabaseUrl()))
	return nil
}
