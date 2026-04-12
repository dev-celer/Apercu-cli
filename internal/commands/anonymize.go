package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/anonymization"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var anonymizeCmd = &cobra.Command{
	Use:   "anonymize",
	Short: "Dump the parent database, anonymize it using GreenMask store it on based on the storage configuration",
	Args:  cobra.NoArgs,
	RunE:  anonymize,
}

func init() {
	rootCmd.AddCommand(anonymizeCmd)
}

func anonymize(cmd *cobra.Command, args []string) error {
	configFile, err := config.LoadConfig(".")
	if err != nil {
		return err
	}

	var dbConfig config.Database
	for _, db := range configFile.Databases {
		dbConfig = db
		break
	}

	handler, err := anonymization.GetDatabaseAnonymizer(dbConfig)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := handler.Anonymize(cmd.Context()); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	output := handler.GetOutput()

	fmt.Println("\nAnonymized database successfully in", output.Duration)

	return nil
}
