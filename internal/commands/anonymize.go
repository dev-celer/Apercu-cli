package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/anonymization"
	"apercu-cli/internal/database"
	"fmt"
	"log"
	"log/slog"
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

	// Get the databases handlers
	sourceHandler, storageHandler, err := database.GetAnonymizationDatabaseHandlers(dbConfig)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Create the storage database if missing
	exist, err := storageHandler.Exists()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if !exist {
		if err := storageHandler.Create(); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	// Get the databases connection fields
	sourceConn, err := sourceHandler.GetConnectionFields()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	storageConn, err := storageHandler.GetConnectionFields()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Anonymize the database
	handler := anonymization.GetDatabaseAnonymizer(dbConfig, sourceConn, storageConn)
	if err := handler.Anonymize(cmd.Context()); err != nil {
		if handler.GetOutput() != nil && handler.GetOutput().Logs != nil {
			_, _ = fmt.Println(log.Writer(), "-------Greenmask output-------")
			_, _ = fmt.Println(log.Writer(), *handler.GetOutput().Logs)
			_, _ = fmt.Println(log.Writer(), "-----------------------------")
		}

		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	output := handler.GetOutput()
	if output != nil && output.Logs != nil {
		slog.Debug("-------Greenmask output-------")
		slog.Debug(*output.Logs)
		slog.Debug("-----------------------------")
	}

	if output != nil {
		fmt.Println("\nAnonymized database successfully in", output.Duration)
	}

	return nil
}
