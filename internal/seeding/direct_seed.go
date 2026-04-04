package seeding

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"time"

	_ "github.com/lib/pq"
)

type DirectSeed struct {
	db            *sql.DB
	state         *config.DatabaseState
	errCount      int
	seedFilesPath []string
	startTime     *time.Time
	endTime       *time.Time
	output        string
}

func NewDirectSeed(conn database.ConnectionFields, seedPath []string, state *config.DatabaseState) (*DirectSeed, error) {
	db, err := sql.Open("postgres", conn.Url)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to connect to database: %v", err))
	}

	seedFiles := make([]string, 0)
	for _, path := range seedPath {
		slog.Debug("Searching for seed files", "path", path)
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Println("WARNING: Seed path not found:", path)
				continue
			}
			return nil, errors.New(fmt.Sprintf("Failed to get seed path: %v", err))
		}

		if info.IsDir() {
			slog.Debug("Path is a directory, searching for sql files inside")
			if err := filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !d.IsDir() && filepath.Ext(p) == ".sql" {
					if slices.Contains(state.AppliedSeeds, p) {
						slog.Debug("Seed is already applied, skipping", "file", p)
						return nil
					}

					slog.Debug("Found sql file", "file", p)
					seedFiles = append(seedFiles, p)
				}
				return nil
			}); err != nil {
				return nil, errors.New(fmt.Sprintf("Failed to search for sql files inside seed path: %v", err))
			}
		} else {
			if slices.Contains(state.AppliedSeeds, path) {
				slog.Debug("Seed is already applied, skipping", "file", path)
				continue
			}

			slog.Debug("Path is a file, adding to seed files list", "file", path)
			seedFiles = append(seedFiles, path)
		}
	}

	return &DirectSeed{db: db, seedFilesPath: seedFiles, state: state}, nil
}

func (h *DirectSeed) Close() error {
	return h.db.Close()
}

func (h *DirectSeed) Apply() {
	_, _ = fmt.Fprintln(log.Writer(), "Seeding database...")

	// Set start time
	h.startTime = new(time.Time)

	for _, seedFile := range h.seedFilesPath {
		slog.Debug("Seeding file", "file", seedFile)

		h.output += fmt.Sprintf("Seeding file %s\n", seedFile)
		// Read seed file
		content, err := os.ReadFile(seedFile)
		if err != nil {
			h.errCount++
			_, _ = fmt.Fprintln(os.Stderr, "Failed to read seed file:", seedFile)
			continue
		}
		// Execute seed file
		res, err := h.db.Exec(string(content))
		if err != nil {
			h.errCount++
			_, _ = fmt.Fprintln(os.Stderr, "Failed to execute seed file:", seedFile+"\n"+err.Error())
			h.output += "----------\n"
			continue
		}

		affectedRows, err := res.RowsAffected()
		if err == nil {
			h.output += fmt.Sprintf("Affected rows: %d\n", affectedRows)
		}
		duration := time.Now().Sub(*h.startTime)

		h.output += fmt.Sprintf("Seeding completed in %s\n", duration.String())
		h.output += "----------\n"

		// Append to state
		h.state.AppliedSeeds = append(h.state.AppliedSeeds, seedFile)
	}

	// Set end time
	h.endTime = new(time.Time)
}

func (h *DirectSeed) GetAppliedCount() int {
	return len(h.seedFilesPath) - h.errCount
}

func (h *DirectSeed) GetFailedCount() int {
	return h.errCount
}

func (h *DirectSeed) GetDuration() *time.Duration {
	if h.startTime == nil || h.endTime == nil {
		return nil
	}

	return new(h.endTime.Sub(*h.startTime))
}

func (h *DirectSeed) GetOutput() string {
	return h.output
}
