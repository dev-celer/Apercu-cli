package commands

import (
	"apercu-cli/internal/database"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var resetCmd = &cobra.Command{
	Use:   "reset <name>",
	Short: "Reset the preview database for this preview name",
	Args:  cobra.ExactArgs(1),
	RunE:  reset,
}

func init() {
	rootCmd.AddCommand(resetCmd)
}

func reset(cmd *cobra.Command, args []string) error {
	previewName := args[0]
	apiKey := os.Getenv("API_KEY")
	projectId := os.Getenv("PROJECT_ID")

	handler, err := database.NewNeonBranchHandler(projectId, apiKey, "production", previewName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := handler.Reset(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return nil
}
