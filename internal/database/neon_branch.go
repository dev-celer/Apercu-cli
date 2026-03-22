package database

import (
	"errors"
	"fmt"

	neon "github.com/kislerdm/neon-sdk-go"
)

type NeonBranchHandler struct {
	projectId     string
	apiKey        string
	parentBranch  string
	client        *neon.Client
	previewBranch string
	databaseUrl   string
}

func NewNeonBranchHandler(projectId string, apiKey string, parentBranch string) (*NeonBranchHandler, error) {
	client, err := neon.NewClient(neon.Config{Key: apiKey})
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to connect to Neon API: %v", err))
	}

	return &NeonBranchHandler{
		projectId:    projectId,
		apiKey:       apiKey,
		parentBranch: parentBranch,
		client:       client,
	}, nil
}
