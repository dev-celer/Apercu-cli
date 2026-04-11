package database

import (
	"apercu-cli/config"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"regexp"
	"strconv"
	"time"

	neon "github.com/kislerdm/neon-sdk-go"
)

type NeonBranchHandler struct {
	projectId        string
	apiKey           string
	parentBranch     string
	parentBranchId   string
	client           *neon.Client
	previewBranch    string
	connectionFields ConnectionFields
	warnings         []string
}

func NewNeonBranchHandler(projectId string, apiKey string, parentBranch string, previewBranch string) (*NeonBranchHandler, error) {
	client, err := neon.NewClient(neon.Config{Key: apiKey})
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to connect to Neon API: %v", err))
	}

	return &NeonBranchHandler{
		projectId:     projectId,
		apiKey:        apiKey,
		parentBranch:  parentBranch,
		previewBranch: previewBranch,
		client:        client,
		warnings:      make([]string, 0),
	}, nil
}

func (h *NeonBranchHandler) extractConnectionFieldsFromUrl(databaseUrl string) (ConnectionFields, error) {
	reg := regexp.MustCompile(`postgresql:\/\/(.+?):(.+?)@(.+?)[\/:](\d*)\/?(.+?)\?`)
	matches := reg.FindStringSubmatch(databaseUrl)

	portStr := matches[4]
	if portStr == "" {
		portStr = "5432"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return ConnectionFields{}, errors.New(fmt.Sprintf("Failed to parse port from database url: %v", err))
	}

	return ConnectionFields{
		Host:     matches[3],
		Port:     port,
		User:     matches[1],
		Password: matches[2],
		Database: matches[5],
		Url:      databaseUrl,
	}, nil
}

func (h *NeonBranchHandler) getBranchByName(branchName string) (*neon.Branch, error) {
	slog.Debug("Getting branch by name", "branch_name", branchName)
	var branches []neon.Branch
	var cursor *string
	for {
		resp, err := h.client.ListProjectBranches(h.projectId, &branchName, nil, cursor, nil, nil)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to list project branches: %v", err))
		}
		branches = append(branches, resp.Branches...)
		if resp.Pagination != nil && resp.Pagination.Next != nil {
			cursor = resp.Pagination.Next
		} else {
			break
		}
	}

	var previewBranch *neon.Branch
	for _, branch := range branches {
		if branch.Name == branchName {
			previewBranch = &branch
		}
	}

	if previewBranch != nil {
		slog.Debug("Found branch with id", "branch_id", previewBranch.ID)
	} else {
		slog.Debug("No branch with name", "branch_name", branchName)
	}

	return previewBranch, nil
}

func (h *NeonBranchHandler) resetRolePassword() error {
	_, _ = fmt.Fprintln(log.Writer(), "Resetting role password")

	// Get preview branch
	previewBranch, err := h.getBranchByName(h.previewBranch)
	if err != nil {
		return err
	}
	if previewBranch == nil {
		return errors.New(fmt.Sprintf("Failed to find preview branch with name: %v", h.previewBranch))
	}

	// Get role
	resp, err := h.client.ListProjectBranchRoles(h.projectId, previewBranch.ID)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to list project branch roles: %v", err))
	}
	if len(resp.Roles) == 0 {
		return errors.New(fmt.Sprintf("No role found in branch: %v", h.previewBranch))
	}

	// Reset role password
	_, err = h.client.ResetProjectBranchRolePassword(h.projectId, previewBranch.ID, resp.Roles[0].Name)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to reset project branch role password: %v", err))
	}
	h.connectionFields = ConnectionFields{}

	return nil
}

func (h *NeonBranchHandler) Apply() error {
	_, _ = fmt.Fprintln(log.Writer(), "Branching from parent branch", h.parentBranch+"...")

	// Find parent branch id from name
	if h.parentBranchId == "" {
		parentBranch, err := h.getBranchByName(h.parentBranch)
		if err != nil {
			return err
		}
		if parentBranch == nil {
			return errors.New(fmt.Sprintf("Failed to find parent branch with name: %v", h.parentBranch))
		}
		h.parentBranchId = parentBranch.ID
	}

	// Check if preview branch exists
	previewBranch, err := h.getBranchByName(h.previewBranch)
	if err != nil {
		return err
	}
	if previewBranch != nil {
		// Preview branch already exist
		slog.Debug("Preview branch already exist, nothing to do")
		return nil
	}

	slog.Debug("Preview branch does not exist, creating new one")

	// Create preview branch
	resp, err := h.client.CreateProjectBranch(h.projectId, &neon.CreateProjectBranchReqObj{
		BranchCreateRequest: neon.BranchCreateRequest{
			Branch: &neon.BranchCreateRequestBranch{
				Name:     &h.previewBranch,
				ParentID: &h.parentBranchId,
			},
			Endpoints: &[]neon.BranchCreateRequestEndpointOptions{
				{Type: neon.EndpointTypeReadWrite},
			},
		},
	})
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to create project branch: %v", err))
	}
	slog.Debug("Preview branch created, id", "branch_id", resp.Branch.ID)

	// Retrieve database_url
	if resp.ConnectionURIs != nil && len(*resp.ConnectionURIs) > 0 {
		// Extract values from database url
		connection, err := h.extractConnectionFieldsFromUrl((*resp.ConnectionURIs)[0].ConnectionURI)
		if err != nil {
			return err
		}

		h.connectionFields = connection
		slog.Debug("Preview branch database_url found", "database_url", h.connectionFields.Url)
	} else {
		slog.Debug("Preview branch database_url not found")
	}

	// Wait for the branch to finish resetting before proceeding
	if err := h.waitForReady(resp.Branch.ID); err != nil {
		return err
	}

	return h.resetRolePassword()
}

func (h *NeonBranchHandler) Cleanup() error {
	_, _ = fmt.Fprintln(log.Writer(), "Cleaning up preview branch", h.previewBranch+"...")

	// Find branch id by name
	previewBranch, err := h.getBranchByName(h.previewBranch)
	if err != nil {
		return err
	}
	if previewBranch == nil {
		slog.Debug("Preview branch not found, nothing to cleanup")
		return nil
	}

	// Delete branch
	if _, err := h.client.DeleteProjectBranch(h.projectId, previewBranch.ID); err != nil {
		return errors.New(fmt.Sprintf("Failed to delete project branch: %v", err))
	}

	return nil
}

func (h *NeonBranchHandler) waitForReady(branchID string) error {
	slog.Debug("Waiting for branch to be ready", "branch_id", branchID)
	timeout := time.After(2 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeout:
			return errors.New("timed out waiting for branch to be ready")
		case <-ticker.C:
			resp, err := h.client.GetProjectBranch(h.projectId, branchID)
			if err != nil {
				return errors.New(fmt.Sprintf("Failed to get branch state: %v", err))
			}
			slog.Debug("Branch state", "state", resp.Branch.CurrentState)
			if resp.Branch.CurrentState == "ready" {
				return nil
			}
		}
	}
}

func (h *NeonBranchHandler) Reset() error {
	_, _ = fmt.Fprintln(log.Writer(), "Resetting preview branch", h.previewBranch, "to it's parent state...")

	// Find branch id by name
	previewBranch, err := h.getBranchByName(h.previewBranch)
	if err != nil {
		return err
	}
	if previewBranch == nil {
		slog.Debug("Preview branch not found, creating")
		return h.Apply()
	}

	// Reset branch to parent state
	_, err = h.client.RestoreProjectBranch(h.projectId, previewBranch.ID, neon.BranchRestoreRequest{
		SourceBranchID: *previewBranch.ParentID,
	})
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to restore project branch: %v", err))
	}

	// Wait for the branch to finish resetting before proceeding
	if err := h.waitForReady(previewBranch.ID); err != nil {
		return err
	}

	return h.resetRolePassword()
}

func (h *NeonBranchHandler) GetConnectionFields() (ConnectionFields, error) {
	if h.connectionFields.Url == "" {
		slog.Debug("Connection fields not found, retrieving from database")
		previewBranch, err := h.getBranchByName(h.previewBranch)
		if err != nil {
			return ConnectionFields{}, err
		}
		if previewBranch == nil {
			return ConnectionFields{}, errors.New(fmt.Sprintf("Failed to find preview branch with name: %v", h.previewBranch))
		}

		slog.Debug("Getting database from project branch")
		database, err := h.client.ListProjectBranchDatabases(h.projectId, previewBranch.ID)
		if err != nil {
			return ConnectionFields{}, errors.New(fmt.Sprintf("Failed to list project branch databases: %v", err))
		}
		if len(database.Databases) == 0 {
			return ConnectionFields{}, errors.New(fmt.Sprintf("No database found in branch: %v", h.previewBranch))
		}
		slog.Debug("Found database with name", "database_name", database.Databases[0].Name)

		slog.Debug("Getting role from project branch")
		roles, err := h.client.ListProjectBranchRoles(h.projectId, previewBranch.ID)
		if err != nil {
			return ConnectionFields{}, errors.New(fmt.Sprintf("Failed to list project branch roles: %v", err))
		}
		if len(roles.Roles) == 0 {
			return ConnectionFields{}, errors.New(fmt.Sprintf("No role found in branch: %v", h.previewBranch))
		}
		slog.Debug("Found role with name", "role_name", roles.Roles[0].Name)

		slog.Debug("Getting database url")
		resp, err := h.client.GetConnectionURI(h.projectId, &previewBranch.ID, nil, database.Databases[0].Name, roles.Roles[0].Name, nil)
		if err != nil {
			return ConnectionFields{}, errors.New(fmt.Sprintf("Failed to get branch connection uri: %v", err))
		}
		slog.Debug("Database url found", "database_url", resp.URI)

		// Extract values from database url
		connection, err := h.extractConnectionFieldsFromUrl(resp.URI)
		if err != nil {
			return ConnectionFields{}, err
		}

		h.connectionFields = connection
	}

	return h.connectionFields, nil
}

func (h *NeonBranchHandler) getAllPreviewBranches(previewPattern string) ([]neon.Branch, error) {
	slog.Debug("Getting all preview branches")

	// Extract start of the preview pattern, before any variable
	reg := regexp.MustCompile(`(\S*)\${{\s*\w+\s*}}`)
	matches := reg.FindStringSubmatch(previewPattern)

	// List branches based on preview pattern
	slog.Debug("Listing branches starting with", "pattern_start", matches[1])
	branches := make([]neon.Branch, 0)
	var cursor *string
	for {
		resp, err := h.client.ListProjectBranches(h.projectId, &matches[1], nil, cursor, nil, nil)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to list project branches: %v", err))
		}
		branches = append(branches, resp.Branches...)
		if resp.Pagination != nil && resp.Pagination.Next != nil {
			cursor = resp.Pagination.Next
		} else {
			break
		}
	}
	slog.Debug(fmt.Sprintf("Found %d branches starting with", len(branches)), "pattern_start", matches[1])

	// Filter out branches that are not child of the parent branch or does not match the preview pattern
	patternRegex, err := previewPatternToRegex(previewPattern)
	if err != nil {
		return nil, err
	}

	filteredBranches := filterBranchesByParentAndPattern(branches, h.parentBranchId, patternRegex)

	return filteredBranches, nil
}

func (h *NeonBranchHandler) PrunePreviewDatabases(openedPullRequestNumber []string) ([]string, error) {
	// Find parent branch id from name
	if h.parentBranchId == "" {
		parentBranch, err := h.getBranchByName(h.parentBranch)
		if err != nil {
			return nil, err
		}
		if parentBranch == nil {
			return nil, errors.New(fmt.Sprintf("Failed to find parent branch with name: %v", h.parentBranch))
		}
		h.parentBranchId = parentBranch.ID
	}

	previewBranches, err := h.getAllPreviewBranches(h.previewBranch)
	if err != nil {
		return nil, err
	}

	branchesToPrune := selectBranchesForPruning(previewBranches, h.previewBranch, openedPullRequestNumber)

	prunedBranches := make([]string, 0, len(branchesToPrune))
	for _, branch := range branchesToPrune {
		slog.Debug("Branch does not match opened pull request, deleting", "branch_name", branch.Name)
		_, err := h.client.DeleteProjectBranch(h.projectId, branch.ID)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to delete project branch: %v", err))
		}
		prunedBranches = append(prunedBranches, branch.Name)
	}

	slog.Debug(fmt.Sprintf("Pruned %d branches", len(prunedBranches)))
	return prunedBranches, nil
}

// previewPatternToRegex converts a preview branch pattern like "preview-${{ PR_NUMBER }}"
// into a compiled regex like ^preview-.*$
func previewPatternToRegex(previewPattern string) (*regexp.Regexp, error) {
	replaceRegex := regexp.MustCompile(`\${{\s*\w+\s*}}`)
	patternRegexString := replaceRegex.ReplaceAllString(previewPattern, ".*")
	patternRegexString = "^" + patternRegexString + "$"

	patternRegex, err := regexp.Compile(patternRegexString)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to compile regex for preview pattern substitution: %v", err))
	}
	return patternRegex, nil
}

// filterBranchesByParentAndPattern filters branches that belong to the given parent
// and match the given pattern regex.
func filterBranchesByParentAndPattern(branches []neon.Branch, parentBranchId string, patternRegex *regexp.Regexp) []neon.Branch {
	filtered := make([]neon.Branch, 0)
	for _, branch := range branches {
		if branch.ParentID == nil || *branch.ParentID != parentBranchId {
			continue
		}
		if patternRegex.MatchString(branch.Name) {
			filtered = append(filtered, branch)
		}
	}
	return filtered
}

// selectBranchesForPruning returns branches that do not match any open pull request.
func selectBranchesForPruning(branches []neon.Branch, previewPattern string, openPRNumbers []string) []neon.Branch {
	result := make([]neon.Branch, 0)
	for _, branch := range branches {
		matched := false
		for _, prNumber := range openPRNumbers {
			prBranchName := config.ReplaceVariables(previewPattern, map[string]string{"PR_NUMBER": prNumber})
			if branch.Name == prBranchName {
				matched = true
				break
			}
		}
		if !matched {
			result = append(result, branch)
		}
	}
	return result
}

func (h *NeonBranchHandler) GetWarnings() []string {
	return h.warnings
}
