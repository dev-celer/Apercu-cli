package migration

import (
	"apercu-cli/output"
	"bytes"
	"context"
	"database/sql"
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
	_ "github.com/lib/pq"
)

type DockerHandler struct {
	image       string
	command     []string
	env         map[string]string
	workDir     string
	localFolder string
	databaseUrl string
	output      *output.OutputDatabaseMigration
}

func NewDockerHandler(image string, command []string, env map[string]string, workDir string, localFolder string, databaseUrl string) *DockerHandler {
	return &DockerHandler{
		image:       image,
		command:     command,
		env:         env,
		workDir:     workDir,
		localFolder: localFolder,
		databaseUrl: databaseUrl,
		output:      output.NewMigrationOutput(),
	}
}

var (
	ErrMigrationTableNotFound = errors.New("migration table not found")
)

func (h *DockerHandler) getMigrationTableName(db *sql.DB) (string, error) {
	slog.Debug("Getting migration table name")
	res, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_name IN " +
		"('flyway_schema_history', 'schema_migrations', '_prisma_migrations', 'django_migrations', 'alembic_version', 'knex_migrations', 'typeorm_migrations', 'sequelize_meta')")
	if err != nil {
		return "", errors.New(fmt.Sprintf("Failed to query database: %v", err))
	}
	defer func() { _ = res.Close() }()

	var tableName string
	if res.Next() {
		if err := res.Scan(&tableName); err != nil {
			return "", errors.New(fmt.Sprintf("Failed to get database result: %v", err))
		}
		slog.Debug("Migration table name found", "table_name", tableName)
	} else {
		slog.Debug("Migration table name not found")
	}
	return tableName, nil
}

func (h *DockerHandler) getCount() (int, error) {
	slog.Debug("Connect to database")
	db, err := sql.Open("postgres", h.databaseUrl)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Failed to connect to database: %v", err))
	}
	defer func() { _ = db.Close() }()

	tableName, err := h.getMigrationTableName(db)
	if err != nil {
		return 0, err
	}

	if tableName == "" {
		return 0, ErrMigrationTableNotFound
	}

	slog.Debug("Query number of rows in migration table")
	res, err := db.Query("SELECT COUNT(*) FROM " + tableName)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Failed to query database: %v", err))
	}
	defer func() { _ = res.Close() }()

	var count int
	if res.Next() {
		if err := res.Scan(&count); err != nil {
			return 0, errors.New(fmt.Sprintf("Failed to get database result: %v", err))
		}
	}
	slog.Debug("Number of rows in migration table", "count", count)

	return count, nil
}

func (h *DockerHandler) Apply(ctx context.Context) error {
	_, _ = fmt.Fprintln(log.Writer(), "Applying migrations...")

	// Get the current migration count
	initCount, initCountErr := h.getCount()
	if initCountErr != nil && !errors.Is(initCountErr, ErrMigrationTableNotFound) {
		return initCountErr
	}

	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to connect to Docker: %v", err))
	}
	defer func() { _ = cli.Close() }()

	// Pull docker image
	slog.Debug("Pulling docker image", "image", h.image)
	readCloser, err := cli.ImagePull(ctx, h.image, image.PullOptions{})
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

	// Create container config
	env := make([]string, 0)
	for k, v := range h.env {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	containerConfig := container.Config{
		Image:      h.image,
		Cmd:        h.command,
		WorkingDir: h.workDir,
		Env:        env,
	}
	cwd, _ := os.Getwd()
	path := filepath.Join(cwd, h.localFolder)
	hostConfig := container.HostConfig{
		Binds:         []string{path + ":" + h.workDir},
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
	}

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

	// Initialize start time
	slog.Debug("Starting docker container")
	startTime := time.Now()
	// Start container
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

	// Set end time
	endTime := time.Now()
	h.output.Duration = endTime.Sub(startTime).String()
	slog.Debug("Docker container finished")
	h.output.Logs = new(buffer.String())

	// Cleanup container
	slog.Debug("Cleanup docker container")
	if err := cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true}); err != nil {
		slog.Error("Failed to cleanup docker container", "error", err)
	}

	// Get the new migration count
	finalCount, finalCountErr := h.getCount()
	if finalCountErr != nil {
		if errors.Is(finalCountErr, ErrMigrationTableNotFound) {
			h.output.Warnings = append(h.output.Warnings, "Migration table not found, cannot determine migration count")
			_, _ = fmt.Fprintln(log.Writer(), "WARNING: migration table not found, cannot determine migration count")
		} else {
			return finalCountErr
		}
	} else {
		if initCountErr != nil {
			initCount = 0
		}
		h.output.Count = finalCount - initCount
	}

	return nil
}

func (h *DockerHandler) GetOutput() *output.OutputDatabaseMigration {
	return h.output
}
