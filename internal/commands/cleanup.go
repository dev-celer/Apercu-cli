package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"

	"github.com/spf13/cobra"
)

var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Cleanup the preview database",
	Args:  cobra.NoArgs,
	RunE:  cleanup,
}

func init() {
	rootCmd.AddCommand(cleanupCmd)
}

func cleanup(cmd *cobra.Command, args []string) error {
	configFile, err := config.LoadConfig(".")
	if err != nil {
		return err
	}

	var dbConfig config.Database
	for _, db := range configFile.Databases {
		dbConfig = db
		break
	}

	_, handler, _, err := database.GetPreviewDatabaseHandler(dbConfig)
	if err != nil {
		return err
	}

	if handler != nil {
		exist, err := handler.Exists()
		if err != nil {
			return err
		}

		if exist {
			if err := handler.Delete(); err != nil {
				return err
			}
		}
	}

	return nil
}
