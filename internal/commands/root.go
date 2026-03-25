package commands

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var debug bool

var rootCmd = &cobra.Command{
	Use:   "apercu",
	Short: "Aperçu CLI",
	Long:  "Aperçu CLI",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if debug {
			slog.SetLogLoggerLevel(slog.LevelDebug)
		}
	},
}

func init() {
	rootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "enable debug output")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
