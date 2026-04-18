package anonymization

import (
	"apercu-cli/helper"
	"apercu-cli/output"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

const GREENMASK_IMAGE = "greenmask/greenmask:v0.2.18"

type GreenmaskHandler struct {
	sourceConnection  helper.ConnectionFields
	storageConnection helper.ConnectionFields
	output            *output.OutputDatabaseAnonymization
	env               map[string]string
	configPath        string
}

func NewGreenmaskHandler(sourceConnection helper.ConnectionFields, storageConnection helper.ConnectionFields, env map[string]string, configPath string) *GreenmaskHandler {
	return &GreenmaskHandler{
		sourceConnection:  sourceConnection,
		storageConnection: storageConnection,
		output:            output.NewAnonymizationOutput(),
		env:               env,
		configPath:        configPath,
	}
}

func (h *GreenmaskHandler) GetOutput() *output.OutputDatabaseAnonymization {
	return h.output
}

func (h *GreenmaskHandler) Anonymize(ctx context.Context) error {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to connect to Docker: %v", err))
	}
	defer func() { _ = cli.Close() }()

	// Pull docker image
	slog.Debug("Pulling docker image", "image", GREENMASK_IMAGE)
	readCloser, err := cli.ImagePull(ctx, GREENMASK_IMAGE, image.PullOptions{})
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to pull docker image: %v", err))
	}
	defer func() { _ = readCloser.Close() }()

	var pullBuffer bytes.Buffer
	_, err = io.Copy(&pullBuffer, readCloser)
	if err != nil {
		slog.Error("Failed to read docker pull logs", "error", err)
	}
	slog.Debug("Docker image pulled", "output", pullBuffer.String())

	// Create GreenMask container config
	env := make([]string, 0)
	for k, v := range h.env {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	cwd, _ := os.Getwd()
	configPath := filepath.Join(cwd, h.configPath)
	configDir := filepath.Dir(configPath)
	configPathInContainer := filepath.Join("/tmp/greenmask", filepath.Base(h.configPath))

	cmd := fmt.Sprintf(
		"PGPASSWORD=%s greenmask --config %s dump -h %s -p %d -d %s -U %s && PGPASSWORD=%s greenmask --config %s restore latest -c -h %s -p %d -d %s -U %s",
		h.sourceConnection.Password, configPathInContainer, h.sourceConnection.Host, h.sourceConnection.Port, h.sourceConnection.Database, h.sourceConnection.User,
		h.storageConnection.Password, configPathInContainer, h.storageConnection.Host, h.storageConnection.Port, h.storageConnection.Database, h.storageConnection.User,
	)

	entrypoint := []string{"sh", "-c", cmd}

	containerConfig := container.Config{
		Image:      GREENMASK_IMAGE,
		Entrypoint: entrypoint,
		Env:        env,
	}

	hostConfig := container.HostConfig{
		Binds:         []string{configDir + ":/tmp/greenmask"},
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
	}

	// Run GreenMask container
	_, _ = fmt.Fprintln(log.Writer(), "Running GreenMask...")

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

	slog.Debug("Starting docker container")
	startTime := time.Now()
	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return errors.New(fmt.Sprintf("Failed to start docker container: %v", err))
	}

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
			h.output.Logs = new(buffer.String())
			return errors.New(fmt.Sprintf("Docker container exited with status code: %d", status.StatusCode))
		}
	}

	endTime := time.Now()
	h.output.Duration = endTime.Sub(startTime).String()
	slog.Debug("Docker container finished")
	h.output.Logs = new(buffer.String())

	// Cleanup container
	slog.Debug("Cleanup docker container")
	if err := cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true}); err != nil {
		slog.Error("Failed to cleanup docker container", "error", err)
	}

	return nil
}
