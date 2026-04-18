package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"fmt"
	"os"

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

	handler, err := database.GetPreviewDatabaseHandler(dbConfig)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if handler != nil {
		exist, err := handler.Exists()
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		if exist {
			if err := handler.Delete(); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
	}

	return nil
}
