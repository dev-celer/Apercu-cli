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
	"strconv"
	"time"

	neon "github.com/kislerdm/neon-sdk-go"
)

type NeonHandler struct {
	client        *neon.Client
	projectId     string
	apiKey        string
	branch        string
	parentBranch  *string
	branchingType *config.DatabaseNeonBranchingType
	warnings      []string
}

func NewNeonHandler(projectId string, apiKey string, parentBranch *string, branch string, branchingType *config.DatabaseNeonBranchingType) (*NeonHandler, error) {
	client, err := neon.NewClient(neon.Config{Key: apiKey})
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to connect to Neon API: %v", err))
	}

	return &NeonHandler{
		client:        client,
		projectId:     projectId,
		apiKey:        apiKey,
		parentBranch:  parentBranch,
		branch:        branch,
		warnings:      make([]string, 0),
		branchingType: branchingType,
	}, nil
}

func (h *NeonHandler) resetRolePassword() error {
	// Get preview branch
	branch, err := neonHelper.GetBranchByName(h.client, h.projectId, h.branch)
	if err != nil {
		return err
	}
	if branch == nil {
		return errors.New(fmt.Sprintf("Failed to find preview branch with name: %v", h.branch))
	}

	// Get role
	resp, err := h.client.ListProjectBranchRoles(h.projectId, branch.ID)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to list project branch roles: %v", err))
	}
	if len(resp.Roles) == 0 {
		return errors.New(fmt.Sprintf("No role found in branch: %v", h.branch))
	}

	// Reset role password
	slog.Debug("Resetting role password", "role_name", resp.Roles[0].Name)
	_, err = h.client.ResetProjectBranchRolePassword(h.projectId, branch.ID, resp.Roles[0].Name)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to reset project branch role password: %v", err))
	}

	return nil
}

func (h *NeonHandler) GetConnectionFields() (helper.ConnectionFields, error) {
	slog.Debug("Connection fields not found, retrieving from database")
	branch, err := neonHelper.GetBranchByName(h.client, h.projectId, h.branch)
	if err != nil {
		return helper.ConnectionFields{}, err
	}
	if branch == nil {
		return helper.ConnectionFields{}, errors.New(fmt.Sprintf("Failed to find branch with name: %v", h.branch))
	}

	database, err := h.client.ListProjectBranchDatabases(h.projectId, branch.ID)
	if err != nil {
		return helper.ConnectionFields{}, errors.New(fmt.Sprintf("Failed to list project branch databases: %v", err))
	}
	if len(database.Databases) == 0 {
		return helper.ConnectionFields{}, errors.New(fmt.Sprintf("No database found in branch: %v", h.branch))
	}
	slog.Debug("Found database with name", "database_name", database.Databases[0].Name)

	roles, err := h.client.ListProjectBranchRoles(h.projectId, branch.ID)
	if err != nil {
		return helper.ConnectionFields{}, errors.New(fmt.Sprintf("Failed to list project branch roles: %v", err))
	}
	if len(roles.Roles) == 0 {
		return helper.ConnectionFields{}, errors.New(fmt.Sprintf("No role found in branch: %v", h.branch))
	}
	slog.Debug("Found role with name", "role_name", roles.Roles[0].Name)

	resp, err := h.client.GetConnectionURI(h.projectId, &branch.ID, nil, database.Databases[0].Name, roles.Roles[0].Name, nil)
	if err != nil {
		return helper.ConnectionFields{}, errors.New(fmt.Sprintf("Failed to get branch connection uri: %v", err))
	}
	slog.Debug("Database url found")

	// Extract values from database url
	return extractConnectionFieldsFromUrl(resp.URI)
}

var reg = regexp.MustCompile(`postgresql:\/\/(.+?):(.+?)@(.+?)[\/:](\d*)\/?(.+?)?(?:\?|$)`)

func extractConnectionFieldsFromUrl(databaseUrl string) (helper.ConnectionFields, error) {
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

func (h *NeonHandler) waitForOperationToComplete(operationId string) error {
	slog.Debug("Waiting for operation to complete", "operation_id", operationId)
	timeout := time.After(2 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeout:
			return errors.New("timed out waiting for operation to complete")
		case <-ticker.C:
			resp, err := h.client.GetProjectOperation(h.projectId, operationId)
			if err != nil {
				return errors.New(fmt.Sprintf("Failed to get operation state: %v", err))
			}
			slog.Debug("Operation state", "state", resp.Operation.Status)
			if resp.Operation.Status == neon.OperationStatusFinished {
				return nil
			}
		}
	}
}

func (h *NeonHandler) GetWarnings() []string {
	return h.warnings
}

func (h *NeonHandler) Exists() (bool, error) {
	branch, err := neonHelper.GetBranchByName(h.client, h.projectId, h.branch)
	if err != nil {
		return false, err
	}
	return branch != nil, nil
}

func (h *NeonHandler) Create() error {
	if h.parentBranch == nil {
		return errors.New("parent branch is required")
	}
	if h.branchingType == nil {
		return errors.New("branching type is required")
	}

	_, _ = fmt.Fprintln(log.Writer(), "Branching from parent branch", *h.parentBranch+"...")

	// Find parent branch id from name
	parentBranch, err := neonHelper.GetBranchByName(h.client, h.projectId, *h.parentBranch)
	if err != nil {
		return err
	}
	if parentBranch == nil {
		return errors.New(fmt.Sprintf("Failed to find parent branch with name: %v", h.parentBranch))
	}

	var initSource string
	switch *h.branchingType {
	case config.DatabaseNeonBranchingTypeParentData:
		initSource = "parent-data"
	case config.DatabaseNeonBranchingTypeSchemaOnly:
		initSource = "schema-only"
	}

	// Create preview branch
	resp, err := h.client.CreateProjectBranch(h.projectId, &neon.CreateProjectBranchReqObj{
		BranchCreateRequest: neon.BranchCreateRequest{
			Branch: &neon.BranchCreateRequestBranch{
				Name:       &h.branch,
				ParentID:   &parentBranch.ID,
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
	slog.Debug("Branch created, id", "branch_id", resp.Branch.ID)

	// Wait for operation to complete before proceeding
	for _, op := range resp.Operations {
		if err := h.waitForOperationToComplete(op.ID); err != nil {
			return err
		}
	}

	if err := h.resetRolePassword(); err != nil {
		return err
	}
	return nil
}

func (h *NeonHandler) Delete() error {
	_, _ = fmt.Fprintln(log.Writer(), "Deleting branch", h.branch+"...")

	// Find branch id by name
	branch, err := neonHelper.GetBranchByName(h.client, h.projectId, h.branch)
	if err != nil {
		return err
	}
	if branch == nil {
		slog.Debug("Branch not found, nothing to cleanup")
		return nil
	}

	// Delete branch
	resp, err := h.client.DeleteProjectBranch(h.projectId, branch.ID)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to delete branch: %v", err))
	}

	for _, op := range resp.Operations {
		if err := h.waitForOperationToComplete(op.ID); err != nil {
			return err
		}
	}

	return nil
}

func (h *NeonHandler) Reset() error {
	// Find branch id by name
	branch, err := neonHelper.GetBranchByName(h.client, h.projectId, h.branch)
	if err != nil {
		return err
	}
	if branch == nil {
		slog.Debug("Branch not found, creating")
		return h.Create()
	}

	if branch.ParentID == nil {
		_, _ = fmt.Fprintln(log.Writer(), "Branch already at root, nothing to reset")
		return nil
	}

	_, _ = fmt.Fprintln(log.Writer(), "Resetting branch", h.branch, "to it's parent state...")

	// Reset branch to parent state
	resp, err := h.client.RestoreProjectBranch(h.projectId, branch.ID, neon.BranchRestoreRequest{
		SourceBranchID: *branch.ParentID,
	})
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to restore branch: %v", err))
	}

	// Wait for the branch to finish resetting before proceeding
	for _, op := range resp.Operations {
		if err := h.waitForOperationToComplete(op.ID); err != nil {
			return err
		}
	}

	if err := h.resetRolePassword(); err != nil {
		return err
	}

	return nil
}
