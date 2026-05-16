package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/anonymization"
	"apercu-cli/internal/database"
	"fmt"
	"log"
	"log/slog"

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
	sourceConn, storageHandler, err := database.GetAnonymizationDatabaseHandlers(dbConfig)
	if err != nil {
		return err
	}
	if sourceConn == nil || storageHandler == nil {
		return fmt.Errorf("no anonymization configuration specified")
	}

	// Create the storage database if missing
	exist, err := storageHandler.Exists()
	if err != nil {
		return err
	}
	if !exist {
		if err := storageHandler.Create(); err != nil {
			return err
		}
	}

	// Get the database connection fields
	storageConn, err := storageHandler.GetConnectionFields()
	if err != nil {
		return err
	}

	// Anonymize the database
	handler := anonymization.GetDatabaseAnonymizer(dbConfig, *sourceConn, storageConn)
	if err := handler.Anonymize(cmd.Context()); err != nil {
		if handler.GetOutput() != nil && handler.GetOutput().Logs != nil {
			_, _ = fmt.Println(log.Writer(), "-------Greenmask output-------")
			_, _ = fmt.Println(log.Writer(), *handler.GetOutput().Logs)
			_, _ = fmt.Println(log.Writer(), "-----------------------------")
		}

		return err
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
