package docker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
)

func CreateNetwork(cli *client.Client) (string, error) {
	ctx := context.Background()

	buf := make([]byte, 6)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("Failed to generate network name: %v", err)
	}
	name := fmt.Sprintf("apercu-%s", hex.EncodeToString(buf))

	slog.Debug("Creating docker network", "name", name)
	if _, err := cli.NetworkCreate(ctx, name, network.CreateOptions{}); err != nil {
		return "", fmt.Errorf("Failed to create docker network: %v", err)
	}

	return name, nil
}

func CleanupContainer(cli *client.Client, name string) error {
	ctx := context.Background()

	info, err := cli.ContainerInspect(ctx, name)
	if err != nil {
		return fmt.Errorf("Failed to inspect docker container %s: %v", name, err)
	}

	if info.State != nil && info.State.Running {
		slog.Debug("Stopping docker container", "name", name)
		if err := cli.ContainerStop(ctx, name, container.StopOptions{}); err != nil {
			return fmt.Errorf("Failed to stop docker container %s: %v", name, err)
		}
	}

	slog.Debug("Removing docker container", "name", name)
	if err := cli.ContainerRemove(ctx, name, container.RemoveOptions{}); err != nil {
		return fmt.Errorf("Failed to remove docker container %s: %v", name, err)
	}

	return nil
}

func CleanupNetwork(cli *client.Client, name string) error {
	ctx := context.Background()

	slog.Debug("Removing docker network", "name", name)
	if err := cli.NetworkRemove(ctx, name); err != nil {
		return fmt.Errorf("Failed to remove docker network %s: %v", name, err)
	}

	return nil
}
