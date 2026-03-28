package migration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

type DockerHandler struct {
	image     string
	command   []string
	env       map[string]string
	startTime *time.Time
	endTime   *time.Time
	output    string
}

func NewDockerHandler(image string, command []string, env map[string]string) *DockerHandler {
	return &DockerHandler{
		image:   image,
		command: command,
		env:     env,
	}
}

func (h DockerHandler) GetCount() (int, error) {
	//TODO implement me
	panic("implement me")
}

func (h DockerHandler) Apply(ctx context.Context) error {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to connect to Docker: %v", err))
	}
	defer func() { _ = cli.Close() }()

	// Pull docker image
	slog.Debug("Pulling docker image", "image", h.image)
	readCloser, err := cli.ImagePull(ctx, h.image, image.PullOptions{})
	defer func() { _ = readCloser.Close() }()
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to pull docker image: %v", err))
	}

	// Create container config
	env := make([]string, len(h.env))
	for k, v := range h.env {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	containerConfig := container.Config{
		Image:      h.image,
		Entrypoint: h.command,
		WorkingDir: "/data",
		Env:        env,
	}
	cwd, _ := os.Getwd()
	hostConfig := container.HostConfig{
		Binds:         []string{cwd + ":/data"},
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
	}

	// Initialize start time
	slog.Debug("Starting docker container")
	h.startTime = new(time.Now())
	// Create container
	resp, err := cli.ContainerCreate(
		ctx,
		&containerConfig,
		&hostConfig,
		nil,
		nil,
		"",
	)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to create docker container: %v", err))
	}
	slog.Debug("Docker container created", "container_id", resp.ID)

	// Read logs until completion
	slog.Debug("Reading docker container logs")
	logs, err := cli.ContainerLogs(ctx, resp.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true, Follow: true})
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to read docker container logs: %v", err))
	}
	defer func() { _ = logs.Close() }()

	var buffer bytes.Buffer
	go func() {
		_, err := stdcopy.StdCopy(&buffer, &buffer, logs)
		if err != nil {
			slog.Error("Failed to read docker container logs", "error", err)
		}
	}()

	// Wait for container to finish
	statusCh, errCh := cli.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		return errors.New(fmt.Sprintf("Failed to wait for docker container: %v", err))
	case status := <-statusCh:
		if status.StatusCode != 0 {
			return errors.New(fmt.Sprintf("Docker container exited with status code: %d", status.StatusCode))
		}
	}

	// Set end time
	h.endTime = new(time.Now())
	slog.Debug("Docker container finished")

	// Cleanup container
	slog.Debug("Cleanup docker container")
	if err := cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true}); err != nil {
		slog.Error("Failed to cleanup docker container", "error", err)
	}

	return nil
}

func (h DockerHandler) GetDuration() *time.Duration {
	if h.startTime == nil || h.endTime == nil {
		return nil
	}

	return new(h.endTime.Sub(*h.startTime))
}

func (h DockerHandler) GetOutput() string {
	return h.output
}
