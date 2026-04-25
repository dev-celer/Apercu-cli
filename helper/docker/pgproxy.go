package docker

import (
	"apercu-cli/helper"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
)

const PGPROXY_IMAGE = "smontard/apercu-pgproxy:dev"

func StartPgProxy(cli *client.Client, networkName string, conn *helper.ConnectionFields) (string, error) {
	ctx := context.Background()

	// Pull docker image
	slog.Debug("Pulling docker image", "image", PGPROXY_IMAGE)
	readCloser, err := cli.ImagePull(ctx, PGPROXY_IMAGE, image.PullOptions{})
	if err != nil {
		return "", errors.New(fmt.Sprintf("Failed to pull docker image: %v", err))
	}
	defer func() { _ = readCloser.Close() }()

	var pullBuffer bytes.Buffer
	_, err = io.Copy(&pullBuffer, readCloser)
	if err != nil {
		slog.Error("Failed to read docker pull logs", "error", err)
	}
	slog.Debug("Docker image pulled", "output", pullBuffer.String())

	// Create container config
	env := []string{
		fmt.Sprintf("DATABASE_HOST=%s", conn.Host),
		fmt.Sprintf("DATABASE_PORT=%d", conn.Port),
	}

	containerConfig := container.Config{
		Image: PGPROXY_IMAGE,
		Env:   env,
	}
	hostConfig := container.HostConfig{
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
	}
	networkConfig := network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			networkName: {
				Aliases: []string{"apercu-pgproxy"},
			},
		},
	}

	// Create container
	resp, err := cli.ContainerCreate(
		ctx,
		&containerConfig,
		&hostConfig,
		&networkConfig,
		nil,
		"",
	)
	if err != nil {
		return "", errors.New(fmt.Sprintf("Failed to create docker container: %v", err))
	}
	slog.Debug("Docker container for pg proxy created", "container_id", resp.ID)

	// Start container
	slog.Debug("Starting docker container for pg proxy")
	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", errors.New(fmt.Sprintf("Failed to start docker container: %v", err))
	}
	return resp.ID, nil
}
