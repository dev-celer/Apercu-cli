package commands

import (
	"apercu-cli/internal/database"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var previewCmd = &cobra.Command{
	Use:   "preview <name>",
	Short: "Create or update the preview database for this preview name",
	Args:  cobra.ExactArgs(1),
	RunE:  preview,
}

func init() {
	rootCmd.AddCommand(previewCmd)
}

func preview(cmd *cobra.Command, args []string) error {
	previewName := args[0]
	apiKey := ""

	handler, err := database.NewNeonBranchHandler("long-scene-56324284", apiKey, "production", previewName)
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
