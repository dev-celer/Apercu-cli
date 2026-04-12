package neon

import (
	"apercu-cli/helper"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"time"

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

func ResetRolePassword(client *neon.Client, projectId string, branchName string) error {
	// Get preview branch
	branch, err := GetBranchByName(client, projectId, branchName)
	if err != nil {
		return err
	}
	if branch == nil {
		return errors.New(fmt.Sprintf("Failed to find preview branch with name: %v", branchName))
	}

	// Get role
	resp, err := client.ListProjectBranchRoles(projectId, branch.ID)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to list project branch roles: %v", err))
	}
	if len(resp.Roles) == 0 {
		return errors.New(fmt.Sprintf("No role found in branch: %v", branchName))
	}

	// Reset role password
	slog.Debug("Resetting role password", "role_name", resp.Roles[0].Name)
	_, err = client.ResetProjectBranchRolePassword(projectId, branch.ID, resp.Roles[0].Name)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to reset project branch role password: %v", err))
	}

	return nil
}

func GetConnectionFieldsFromBranch(client *neon.Client, projectId string, branchName string) (helper.ConnectionFields, error) {
	slog.Debug("Connection fields not found, retrieving from database")
	branch, err := GetBranchByName(client, projectId, branchName)
	if err != nil {
		return helper.ConnectionFields{}, err
	}
	if branch == nil {
		return helper.ConnectionFields{}, errors.New(fmt.Sprintf("Failed to find branch with name: %v", branchName))
	}

	database, err := client.ListProjectBranchDatabases(projectId, branch.ID)
	if err != nil {
		return helper.ConnectionFields{}, errors.New(fmt.Sprintf("Failed to list project branch databases: %v", err))
	}
	if len(database.Databases) == 0 {
		return helper.ConnectionFields{}, errors.New(fmt.Sprintf("No database found in branch: %v", branchName))
	}
	slog.Debug("Found database with name", "database_name", database.Databases[0].Name)

	roles, err := client.ListProjectBranchRoles(projectId, branch.ID)
	if err != nil {
		return helper.ConnectionFields{}, errors.New(fmt.Sprintf("Failed to list project branch roles: %v", err))
	}
	if len(roles.Roles) == 0 {
		return helper.ConnectionFields{}, errors.New(fmt.Sprintf("No role found in branch: %v", branchName))
	}
	slog.Debug("Found role with name", "role_name", roles.Roles[0].Name)

	resp, err := client.GetConnectionURI(projectId, &branch.ID, nil, database.Databases[0].Name, roles.Roles[0].Name, nil)
	if err != nil {
		return helper.ConnectionFields{}, errors.New(fmt.Sprintf("Failed to get branch connection uri: %v", err))
	}
	slog.Debug("Database url found")

	// Extract values from database url
	return ExtractConnectionFieldsFromUrl(resp.URI)
}

func ExtractConnectionFieldsFromUrl(databaseUrl string) (helper.ConnectionFields, error) {
	reg := regexp.MustCompile(`postgresql:\/\/(.+?):(.+?)@(.+?)[\/:](\d*)\/?(.+?)\?`)
	matches := reg.FindStringSubmatch(databaseUrl)

	portStr := matches[4]
	if portStr == "" {
		portStr = "5432"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return helper.ConnectionFields{}, errors.New(fmt.Sprintf("Failed to parse port from database url: %v", err))
	}

	return helper.ConnectionFields{
		Host:     matches[3],
		Port:     port,
		User:     matches[1],
		Password: matches[2],
		Database: matches[5],
		Url:      databaseUrl,
	}, nil
}

func WaitForBranchToBeReady(client *neon.Client, projectId, branchId string) error {
	slog.Debug("Waiting for branch to be ready", "branch_id", branchId)
	timeout := time.After(2 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeout:
			return errors.New("timed out waiting for branch to be ready")
		case <-ticker.C:
			resp, err := client.GetProjectBranch(projectId, branchId)
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
