package commands

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var debug bool
var runnerOutput bool
var jsonOutput bool
var statePath string

var rootCmd = &cobra.Command{
	Use:   "apercu",
	Short: "Aperçu CLI",
	Long:  "Aperçu CLI",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if debug {
			slog.SetLogLoggerLevel(slog.LevelDebug)
			runnerOutput = true
		}
	},
}

func init() {
	rootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "enable debug output")
	rootCmd.PersistentFlags().BoolVarP(&runnerOutput, "runner-output", "o", false, "enable runner output")
	rootCmd.PersistentFlags().StringVarP(&statePath, "state-path", "s", "", "path to state file")
	rootCmd.PersistentFlags().BoolVarP(&jsonOutput, "json", "j", false, "enable JSON output")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
