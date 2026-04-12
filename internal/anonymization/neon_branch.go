package anonymization

import (
	neonHelper "apercu-cli/helper/neon"
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
	neon "github.com/kislerdm/neon-sdk-go"
)

type NeonBranchAnonymizerHandler struct {
	parentClient     *neon.Client
	parentProjectId  string
	parentApiKey     string
	parentBranch     string
	storageClient    *neon.Client
	storageProjectId string
	storageApiKey    string
	storageBranch    string
	output           *output.OutputDatabaseAnonymization
	env              map[string]string
	configPath       string
}

func NewNeonBranchAnonymizerHandler(parentApiKey string, parentProjectId string, parentBranch string, storageBranch string, storageApiKey *string, storageProjectId string, env map[string]string, configPath string) (*NeonBranchAnonymizerHandler, error) {
	parentClient, err := neon.NewClient(neon.Config{Key: parentApiKey})
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to connect to Neon API: %v", err))
	}

	var storageClient *neon.Client
	if storageApiKey != nil {
		storageClient, err = neon.NewClient(neon.Config{Key: *storageApiKey})
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to connect to Neon API: %v", err))
		}
	} else {
		storageClient = parentClient
	}

	return &NeonBranchAnonymizerHandler{
		parentClient:     parentClient,
		parentProjectId:  parentProjectId,
		parentBranch:     parentBranch,
		storageClient:    storageClient,
		storageProjectId: storageProjectId,
		storageBranch:    storageBranch,
		output:           output.NewAnonymizationOutput(),
		env:              env,
		configPath:       configPath,
	}, nil
}

func (h *NeonBranchAnonymizerHandler) GetOutput() *output.OutputDatabaseAnonymization {
	return h.output
}

func (h *NeonBranchAnonymizerHandler) createStorageBranch() (*neon.Branch, error) {
	_, _ = fmt.Fprintln(log.Writer(), "Creating storage branch for anonymized data", h.storageBranch+"...")
	resp, err := h.storageClient.CreateProjectBranch(h.storageProjectId, &neon.CreateProjectBranchReqObj{
		BranchCreateRequest: neon.BranchCreateRequest{
			Branch: &neon.BranchCreateRequestBranch{
				Name: &h.storageBranch,
			},
			Endpoints: &[]neon.BranchCreateRequestEndpointOptions{
				{Type: neon.EndpointTypeReadWrite},
			},
		},
	})

	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to create storage branch: %v", err))
	}
	slog.Debug("Storage branch created", "branch_id", resp.Branch.ID, "branch_name", resp.Branch.Name)

	err = neonHelper.WaitForBranchToBeReady(h.storageClient, h.storageProjectId, resp.Branch.ID)
	if err != nil {
		return nil, err
	}

	return &resp.Branch, nil
}

func (h *NeonBranchAnonymizerHandler) Anonymize(ctx context.Context) error {
	// Validate storage branch exist
	storageBranch, err := neonHelper.GetBranchByName(h.storageClient, h.storageProjectId, h.storageBranch)
	if err != nil {
		return err
	}
	if storageBranch == nil {
		// Create target branch if missing
		storageBranch, err = h.createStorageBranch()
		if err != nil {
			return err
		}
	}

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

	// Get credentials for storage and parent branches
	storageCred, err := neonHelper.GetConnectionFieldsFromBranch(h.storageClient, h.storageProjectId, h.storageBranch)
	if err != nil {
		return err
	}
	parentCred, err := neonHelper.GetConnectionFieldsFromBranch(h.parentClient, h.parentProjectId, h.parentBranch)
	if err != nil {
		return err
	}

	// Create GreenMask container config
	env := make([]string, len(h.env))
	for k, v := range h.env {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	cwd, _ := os.Getwd()
	configPath := filepath.Join(cwd, h.configPath)
	configDir := filepath.Dir(configPath)
	configPathInContainer := filepath.Join("/tmp/greenmask", filepath.Join(filepath.Base(configDir), filepath.Base(configPath)))

	cmd := fmt.Sprintf(
		"PGPASSWORD=%s greenmask --config %s dump -h %s -p %d -d %s -U %s && PGPASSWORD=%s greenmask --config %s restore latest -h %s -p %d -d %s -U %s",
		parentCred.Password, configPathInContainer, parentCred.Host, parentCred.Port, parentCred.Database, parentCred.User,
		storageCred.Password, configPathInContainer, storageCred.Host, storageCred.Port, storageCred.Database, storageCred.User,
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
