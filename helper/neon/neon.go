package neon

import (
	"errors"
	"fmt"
	"log/slog"

	neon "github.com/kislerdm/neon-sdk-go"
)

func GetBranchByName(client *neon.Client, projectId string, branchName string) (*neon.Branch, error) {
	slog.Debug("Getting branch by name", "branch_name", branchName, "project_id", projectId)
	var branches []neon.Branch
	var cursor *string
	for {
		resp, err := client.ListProjectBranches(projectId, &branchName, nil, cursor, nil, nil)
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
