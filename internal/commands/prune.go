package commands

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"apercu-cli/internal/repository"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var githubRepository string

var pruneCmd = &cobra.Command{
	Use:   "prune",
	Short: "Prune all the preview database without a ongoing pull request",
	Args:  cobra.NoArgs,
	RunE:  prune,
}

func init() {
	pruneCmd.PersistentFlags().StringVar(&githubRepository, "github-repository", "", "Github repository to use prune against")
	rootCmd.AddCommand(pruneCmd)
}

func prune(cmd *cobra.Command, args []string) error {
	// Get config
	configFile, err := config.LoadConfig(".")
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var dbConfig config.Database
	for _, db := range configFile.Databases {
		dbConfig = db
		break
	}

	// Create repository handler
	var repositoryHandler repository.HandlerInterface
	switch {
	case githubRepository != "":
		// Extract owner / repository name from flag argument
		values := strings.Split(githubRepository, "/")
		if len(values) != 2 {
			_, _ = fmt.Fprintln(os.Stderr, "Invalid repository structure, please use owner/repository")
			os.Exit(1)
		}

		// Retrieve github token
		ghToken := os.Getenv("GH_TOKEN")
		if ghToken == "" {
			_, _ = fmt.Fprintln(os.Stderr, "GH_TOKEN environment variable not set")
			os.Exit(1)
		}

		repositoryHandler = repository.NewGithubHandler(cmd.Context(), ghToken, values[0], values[1])
	default:
		_, _ = fmt.Fprintln(os.Stderr, "Missing repository flag, please use --github-repository")
		os.Exit(1)
	}

	// Get database handler
	databaseHandler, err := database.GetDatabaseHandlerForPruning(dbConfig)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	_, _ = fmt.Fprintln(log.Writer(), "Pruning databases...")

	// Retrieve the list of opened pull requests
	prNumbers, err := repositoryHandler.GetOpenedPullRequestsNumber()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Prune the database
	prunedDatabase, err := databaseHandler.PrunePreviewDatabases(prNumbers)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	_, _ = fmt.Fprintln(log.Writer(), fmt.Sprintf("Pruned %d database(s):\n%s", len(prunedDatabase), strings.Join(prunedDatabase, "\n")))
	return nil
}
