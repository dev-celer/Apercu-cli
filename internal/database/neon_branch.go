package database

import (
	"apercu-cli/config"
	"apercu-cli/helper"
	neonHelper "apercu-cli/helper/neon"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"regexp"

	neon "github.com/kislerdm/neon-sdk-go"
)

type NeonBranchHandler struct {
	projectId        string
	apiKey           string
	parentBranch     string
	parentBranchId   string
	branchingType    config.DatabaseNeonBranchingType
	client           *neon.Client
	previewBranch    string
	connectionFields helper.ConnectionFields
	warnings         []string
}

func NewNeonBranchHandler(projectId string, apiKey string, parentBranch string, previewBranch string, branchingType config.DatabaseNeonBranchingType) (*NeonBranchHandler, error) {
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
		branchingType: branchingType,
	}, nil
}

func (h *NeonBranchHandler) Apply() error {
	_, _ = fmt.Fprintln(log.Writer(), "Branching from parent branch", h.parentBranch+"...")

	// Find parent branch id from name
	if h.parentBranchId == "" {
		parentBranch, err := neonHelper.GetBranchByName(h.client, h.projectId, h.parentBranch)
		if err != nil {
			return err
		}
		if parentBranch == nil {
			return errors.New(fmt.Sprintf("Failed to find parent branch with name: %v", h.parentBranch))
		}
		h.parentBranchId = parentBranch.ID
	}

	// Check if preview branch exists
	previewBranch, err := neonHelper.GetBranchByName(h.client, h.projectId, h.previewBranch)
	if err != nil {
		return err
	}
	if previewBranch != nil {
		// Preview branch already exist
		slog.Debug("Preview branch already exist, nothing to do")
		return nil
	}

	slog.Debug("Preview branch does not exist, creating new one", "branching_type", h.branchingType)

	var initSource string
	switch h.branchingType {
	case config.DatabaseNeonBranchingTypeParentData:
		initSource = "parent-data"
	case config.DatabaseNeonBranchingTypeSchemaOnly:
		initSource = "schema-only"
	}

	// Create preview branch
	resp, err := h.client.CreateProjectBranch(h.projectId, &neon.CreateProjectBranchReqObj{
		BranchCreateRequest: neon.BranchCreateRequest{
			Branch: &neon.BranchCreateRequestBranch{
				Name:       &h.previewBranch,
				ParentID:   &h.parentBranchId,
				InitSource: &initSource,
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
		connection, err := neonHelper.ExtractConnectionFieldsFromUrl((*resp.ConnectionURIs)[0].ConnectionURI)
		if err != nil {
			return err
		}

		h.connectionFields = connection
		slog.Debug("Preview branch database_url found", "database_url", h.connectionFields.Url)
	} else {
		slog.Debug("Preview branch database_url not found")
	}

	// Wait for the branch to finish resetting before proceeding
	if err := neonHelper.WaitForBranchToBeReady(h.client, h.projectId, resp.Branch.ID); err != nil {
		return err
	}

	if err := neonHelper.ResetRolePassword(h.client, h.projectId, h.previewBranch); err != nil {
		return err
	}
	h.connectionFields = helper.ConnectionFields{}
	return nil
}

func (h *NeonBranchHandler) Cleanup() error {
	_, _ = fmt.Fprintln(log.Writer(), "Cleaning up preview branch", h.previewBranch+"...")

	// Find branch id by name
	previewBranch, err := neonHelper.GetBranchByName(h.client, h.projectId, h.previewBranch)
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

func (h *NeonBranchHandler) Reset() error {
	_, _ = fmt.Fprintln(log.Writer(), "Resetting preview branch", h.previewBranch, "to it's parent state...")

	// Find branch id by name
	previewBranch, err := neonHelper.GetBranchByName(h.client, h.projectId, h.previewBranch)
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
	if err := neonHelper.WaitForBranchToBeReady(h.client, h.projectId, previewBranch.ID); err != nil {
		return err
	}

	if err := neonHelper.ResetRolePassword(h.client, h.projectId, h.previewBranch); err != nil {
		return err
	}
	h.connectionFields = helper.ConnectionFields{}
	return nil
}

func (h *NeonBranchHandler) GetParentConnectionFields() (helper.ConnectionFields, error) {
	return neonHelper.GetConnectionFieldsFromBranch(h.client, h.projectId, h.parentBranch)
}

func (h *NeonBranchHandler) GetPreviewConnectionFields() (helper.ConnectionFields, error) {
	if h.connectionFields.Url == "" {
		conn, err := neonHelper.GetConnectionFieldsFromBranch(h.client, h.projectId, h.previewBranch)
		if err != nil {
			return helper.ConnectionFields{}, err
		}
		h.connectionFields = conn
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
		parentBranch, err := neonHelper.GetBranchByName(h.client, h.projectId, h.parentBranch)
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
