package seeding

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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

// Returns true if the content of the seed file matches the hash
func compareSeedContentFromHash(hash string, filePath string) (bool, error) {
	f, err := os.Open(filePath)
	if err != nil {
		_, _ = fmt.Fprintln(log.Writer(), "WARNING: Failed to open seed file:", filePath)
		return false, errors.New(fmt.Sprintf("Failed to read seed file: %v", err))
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		_, _ = fmt.Fprintln(log.Writer(), "WARNING: Failed to read seed file:", filePath)
		return false, errors.New(fmt.Sprintf("Failed to read seed file: %v", err))
	}

	contentHash := hex.EncodeToString(h.Sum(nil))
	return contentHash == hash, nil
}

func isSeedAlreadyApplied(filePath string, state []config.SeedState) (bool, error) {
	seedIndex := slices.IndexFunc(state, func(seed config.SeedState) bool {
		return seed.Name == filePath
	})
	if seedIndex == -1 {
		return false, nil
	}

	slog.Debug("Seed file has already been applied, checking content", "file", filePath)
	return compareSeedContentFromHash(state[seedIndex].Hash, filePath)
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
				_, _ = fmt.Fprintln(log.Writer(), "WARNING: Seed path not found:", path)
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
					applied, err := isSeedAlreadyApplied(p, state.AppliedSeeds)
					if err != nil {
						return nil
					}
					if applied {
						slog.Debug("Seed file has already been applied, skipping", "file", p)
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
			applied, err := isSeedAlreadyApplied(path, state.AppliedSeeds)
			if err != nil {
				continue
			}
			if applied {
				slog.Debug("Seed file has already been applied, skipping", "file", path)
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
	h.startTime = new(time.Now())

	for _, seedFile := range h.seedFilesPath {
		slog.Debug("Seeding file", "file", seedFile)

		startTime := time.Now()
		h.output += fmt.Sprintf("Seeding file %s\n", seedFile)
		// Read seed file
		content, err := os.ReadFile(seedFile)
		if err != nil {
			h.errCount++
			h.output += fmt.Sprintf("Failed to read seed file: %s\n", seedFile)
			h.output += "----------\n"
			continue
		}
		// Execute seed file
		res, err := h.db.Exec(string(content))
		if err != nil {
			h.errCount++
			h.output += fmt.Sprintf("Failed to execute seed file: %s\n", seedFile)
			h.output += "----------\n"
			continue
		}

		affectedRows, err := res.RowsAffected()
		if err == nil {
			h.output += fmt.Sprintf("Affected rows: %d\n", affectedRows)
		}
		duration := time.Now().Sub(startTime)

		h.output += fmt.Sprintf("Seeding completed in %s\n", duration.String())
		h.output += "----------\n"

		// Append to state
		hasher := md5.New()
		_, _ = hasher.Write(content)
		hash := hasher.Sum(nil)
		hashStr := hex.EncodeToString(hash)

		h.state.AppliedSeeds = append(h.state.AppliedSeeds, config.SeedState{
			Name: seedFile,
			Hash: hashStr,
		})
	}

	// Set end time
	h.endTime = new(time.Now())
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
