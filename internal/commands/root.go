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
var outputFile string

var rootCmd = &cobra.Command{
	Use:   "apercu",
	Short: "Aperçu CLI",
	Long:  "Aperçu CLI",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if debug {
			slog.SetLogLoggerLevel(slog.LevelDebug)
			runnerOutput = true
		}
		if outputFile != "" {
			jsonOutput = true
		}
	},
}

func init() {
	rootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "enable debug output")
	rootCmd.PersistentFlags().BoolVarP(&runnerOutput, "output", "o", false, "enable migration output and seeding output")
	rootCmd.PersistentFlags().StringVarP(&statePath, "state-path", "s", "", "path to state file")
	rootCmd.PersistentFlags().BoolVarP(&jsonOutput, "json", "j", false, "enable JSON output")
	rootCmd.PersistentFlags().StringVar(&outputFile, "output-file", "", "path to output file")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
