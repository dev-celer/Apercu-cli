package repository

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/google/go-github/github"
	"golang.org/x/oauth2"
)

type GithubHandler struct {
	ctx        context.Context
	client     *github.Client
	owner      string
	repository string
}

func NewGithubHandler(ctx context.Context, token string, owner string, repository string) *GithubHandler {
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
	tc := oauth2.NewClient(ctx, ts)
	client := github.NewClient(tc)

	return &GithubHandler{
		ctx:        ctx,
		client:     client,
		owner:      owner,
		repository: repository,
	}
}

func (h *GithubHandler) GetOpenedPullRequestsNumber() ([]string, error) {
	slog.Debug("Getting opened pull requests")
	pullRequests, _, err := h.client.PullRequests.List(h.ctx, h.owner, h.repository, &github.PullRequestListOptions{
		State: "open",
	})
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to list pull requests from github: %v", err))
	}
	slog.Debug(fmt.Sprintf("Found %d opened pull requests", len(pullRequests)))

	pullRequestsNumber := make([]string, len(pullRequests))
	for i, pr := range pullRequests {
		pullRequestsNumber[i] = strconv.Itoa(pr.GetNumber())
	}
	return pullRequestsNumber, nil
}
