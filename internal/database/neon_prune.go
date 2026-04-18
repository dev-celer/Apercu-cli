package database

import (
	"apercu-cli/config"
	neonHelper "apercu-cli/helper/neon"
	"errors"
	"fmt"
	"log/slog"
	"regexp"

	neon "github.com/kislerdm/neon-sdk-go"
)

type NeonPruneHandler struct {
	client        *neon.Client
	projectId     string
	apiKey        string
	parentBranch  string
	branchPattern string
	warnings      []string
}

func NewNeonPruneHandler(projectId string, apiKey string, parentBranch string, branchPattern string) (*NeonPruneHandler, error) {
	client, err := neon.NewClient(neon.Config{Key: apiKey})
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to connect to Neon API: %v", err))
	}

	return new(NeonPruneHandler{
		client:        client,
		projectId:     projectId,
		apiKey:        apiKey,
		parentBranch:  parentBranch,
		branchPattern: branchPattern,
		warnings:      make([]string, 0),
	}), nil
}

func (h *NeonPruneHandler) GetWarnings() []string {
	return h.warnings
}

func (h *NeonPruneHandler) Prune(openedPullRequestNumber []string) ([]string, error) {
	// Find parent branch id from name
	parentBranch, err := neonHelper.GetBranchByName(h.client, h.projectId, h.parentBranch)
	if err != nil {
		return nil, err
	}
	if parentBranch == nil {
		return nil, errors.New(fmt.Sprintf("Failed to find parent branch with name: %v", h.parentBranch))
	}

	previewBranches, err := h.getAllPreviewBranches(h.branchPattern, parentBranch.ID)
	if err != nil {
		return nil, err
	}

	branchesToPrune := selectBranchesForPruning(previewBranches, h.branchPattern, openedPullRequestNumber)

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

func (h *NeonPruneHandler) getAllPreviewBranches(previewPattern string, parentBranchId string) ([]neon.Branch, error) {
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

	filteredBranches := filterBranchesByParentAndPattern(branches, parentBranchId, patternRegex)

	return filteredBranches, nil
}
