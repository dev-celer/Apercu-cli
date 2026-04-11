package seeding

import (
	"apercu-cli/config"
	"apercu-cli/internal/database"
	"apercu-cli/output"
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
	seedFilesPath []string
	output        *output.OutputDatabaseSeeding
}

// Returns true if the content of the seed file matches the hash
func compareSeedContentFromHash(hash string, filePath string, output *output.OutputDatabaseSeeding) (bool, error) {
	f, err := os.Open(filePath)
	if err != nil {
		output.Warnings = append(output.Warnings, fmt.Sprintf("Failed to open seed file: %s", filePath))
		_, _ = fmt.Fprintln(log.Writer(), "WARNING: Failed to open seed file:", filePath)
		return false, errors.New(fmt.Sprintf("Failed to read seed file: %v", err))
	}
	defer f.Close()

	hasher := md5.New()
	if _, err := io.Copy(hasher, f); err != nil {
		output.Warnings = append(output.Warnings, fmt.Sprintf("Failed to open seed file: %s", filePath))
		_, _ = fmt.Fprintln(log.Writer(), "WARNING: Failed to read seed file:", filePath)
		return false, errors.New(fmt.Sprintf("Failed to read seed file: %v", err))
	}

	contentHash := hex.EncodeToString(hasher.Sum(nil))
	return contentHash == hash, nil
}

func shouldSeedBeApplied(filePath string, seedOn config.DatabaseSeedType, state []config.SeedState, output *output.OutputDatabaseSeeding) (bool, error) {
	if seedOn == config.DatabaseSeedTypeAlways {
		slog.Debug("Seed on always, applying seed file", "file", filePath)
		return true, nil
	}

	seedIndex := slices.IndexFunc(state, func(seed config.SeedState) bool {
		return seed.Name == filePath
	})
	if seedIndex == -1 {
		slog.Debug("Seed file has not been applied yet, applying seed file", "file", filePath)
		return true, nil
	}

	slog.Debug("Seed file has already been applied, checking content", "file", filePath)
	seedUnchanged, err := compareSeedContentFromHash(state[seedIndex].Hash, filePath, output)
	if err != nil {
		return false, err
	}

	if !seedUnchanged && seedOn == config.DatabaseSeedTypeUpdate {
		slog.Debug("Seed file has been updated, applying seed file", "file", filePath)
		return true, nil
	}
	return false, nil
}

func NewDirectSeed(conn database.ConnectionFields, seedFiles []config.DatabaseSeed, state *config.DatabaseState) (*DirectSeed, error) {
	outputData := output.NewSeedingOutput()
	db, err := sql.Open("postgres", conn.Url)
	if err != nil {
		outputData.Errors = append(outputData.Errors, err.Error())
		return nil, errors.New(fmt.Sprintf("Failed to connect to database: %v", err))
	}

	seedFilesToApply := make([]string, 0)
	for _, seedFile := range seedFiles {
		slog.Debug("Searching for seed files", "path", seedFile.Path)
		info, err := os.Stat(seedFile.Path)
		if err != nil {
			if os.IsNotExist(err) {
				outputData.Warnings = append(outputData.Warnings, fmt.Sprintf("Seed path not found: %s", seedFile.Path))
				_, _ = fmt.Fprintln(log.Writer(), "WARNING: Seed path not found:", seedFile.Path)
				continue
			}
			outputData.Errors = append(outputData.Errors, fmt.Sprintf("Failed to get seed path: %v", err))
			return nil, errors.New(fmt.Sprintf("Failed to get seed path: %v", err))
		}

		if info.IsDir() {
			slog.Debug("Path is a directory, searching for sql files inside")
			if err := filepath.WalkDir(seedFile.Path, func(p string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !d.IsDir() && filepath.Ext(p) == ".sql" {
					slog.Debug("Found sql file", "file", p)
					needApply, err := shouldSeedBeApplied(p, seedFile.SeedOn, state.AppliedSeeds, outputData)
					if err != nil {
						return nil
					}
					if !needApply {
						return nil
					}

					seedFilesToApply = append(seedFilesToApply, p)
				}
				return nil
			}); err != nil {
				outputData.Errors = append(outputData.Errors, fmt.Sprintf("Failed to search for sql files inside seed path: %v", err))
				return nil, errors.New(fmt.Sprintf("Failed to search for sql files inside seed path: %v", err))
			}
		} else {
			slog.Debug("Path is a file", "file", seedFile.Path)
			needApply, err := shouldSeedBeApplied(seedFile.Path, seedFile.SeedOn, state.AppliedSeeds, outputData)
			if err != nil {
				continue
			}
			if !needApply {
				continue
			}
			seedFilesToApply = append(seedFilesToApply, seedFile.Path)
		}
	}

	return &DirectSeed{db: db, seedFilesPath: seedFilesToApply, state: state, output: outputData}, nil
}

func (h *DirectSeed) Close() error {
	return h.db.Close()
}

func (h *DirectSeed) Apply() {
	_, _ = fmt.Fprintln(log.Writer(), "Seeding database...")

	// Set start time
	startTime := time.Now()

	for _, seedFile := range h.seedFilesPath {
		slog.Debug("Seeding file", "file", seedFile)

		startTime := time.Now()
		if h.output.Logs == nil {
			h.output.Logs = new(string)
		}
		*h.output.Logs += fmt.Sprintf("Seeding file %s\n", seedFile)
		// Read seed file
		content, err := os.ReadFile(seedFile)
		if err != nil {
			h.output.FailedCount++
			*h.output.Logs += fmt.Sprintf("Failed to read seed file: %s\n", seedFile)
			*h.output.Logs += "----------\n"
			continue
		}
		// Execute seed file
		res, err := h.db.Exec(string(content))
		if err != nil {
			h.output.FailedCount++
			*h.output.Logs += fmt.Sprintf("Failed to execute seed file: %s\n", seedFile)
			*h.output.Logs += fmt.Sprintf("Error: %s\n", err.Error())
			*h.output.Logs += "----------\n"
			continue
		}

		affectedRows, err := res.RowsAffected()
		if err == nil {
			*h.output.Logs += fmt.Sprintf("Affected rows: %d\n", affectedRows)
		}
		duration := time.Now().Sub(startTime)

		*h.output.Logs += fmt.Sprintf("Seeding completed in %s\n", duration.String())
		*h.output.Logs += "----------\n"
		h.output.SuccessCount++

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
	h.output.Duration = time.Now().Sub(startTime).String()
}

func (h *DirectSeed) GetOutput() *output.OutputDatabaseSeeding {
	return h.output
}
