package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"apercu-cli/internal/migration"
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

	// Apply the migrations
	ctx := cmd.Context()
	migrationHandler := migration.GetMigrationHandler(dbConfig, dbHandler.GetConnectionFields())
	if migrationHandler != nil {
		if err := migrationHandler.Apply(ctx); err != nil {
			fmt.Println("Migration failed")
			if output := migrationHandler.GetOutput(); output != "" {
				fmt.Println(migrationHandler.GetOutput())
			}
			fmt.Println(err)
			os.Exit(1)
		}
		if output := migrationHandler.GetOutput(); output != "" {
			fmt.Println(migrationHandler.GetOutput())
		}
		if duration := migrationHandler.GetDuration(); duration != nil {
			fmt.Println(fmt.Sprintf("Migration completed successfully, completed in %s", duration.String()))
		} else {
			fmt.Println("Migration completed successfully")
		}
	}

	fmt.Println(fmt.Sprintf("DATABASE_URL: %s", dbHandler.GetConnectionFields().Url))
	return nil
}
