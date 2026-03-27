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

	handler, err := database.GetSourceDatabaseHandler(dbConfig)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := handler.Apply(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println(fmt.Sprintf("DATABASE_URL: %s", handler.GetDatabaseUrl()))
	return nil
}
