package seeding

import (
	"apercu-cli/config"
	"apercu-cli/helper"
	"apercu-cli/helper/warning"
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
	seedFiles     []config.DatabaseSeed
	output        *output.OutputDatabaseSeeding
	warnings      []warning.Warning
}

// Returns true if the content of the seed file matches the hash
func (h *DirectSeed) compareSeedContentFromHash(hash string, filePath string) (bool, error) {
	f, err := os.Open(filePath)
	if err != nil {
		w := warning.NewSeedingError(warning.CodeFailedToOpenSeedFile, filePath)
		if w != nil {
			h.warnings = append(h.warnings, w)
			warning.PrintWarning(w)
			return false, errors.New(w.GetWarningText())
		}
	}
	defer f.Close()

	hasher := md5.New()
	if _, err := io.Copy(hasher, f); err != nil {
		w := warning.NewSeedingError(warning.CodeFailedToOpenSeedFile, filePath)
		if w != nil {
			h.warnings = append(h.warnings, w)
			warning.PrintWarning(w)
			return false, errors.New(w.GetWarningText())
		}
	}

	contentHash := hex.EncodeToString(hasher.Sum(nil))
	return contentHash == hash, nil
}

func (h *DirectSeed) shouldSeedBeApplied(filePath string, seedOn config.DatabaseSeedType, state []config.SeedState) (bool, error) {
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
	seedUnchanged, err := h.compareSeedContentFromHash(state[seedIndex].Hash, filePath)
	if err != nil {
		return false, err
	}

	if !seedUnchanged && seedOn == config.DatabaseSeedTypeUpdate {
		slog.Debug("Seed file has been updated, applying seed file", "file", filePath)
		return true, nil
	}
	return false, nil
}

func (h *DirectSeed) getSeedFilesToApply() {
	h.seedFilesPath = make([]string, 0)
	for _, seedFile := range h.seedFiles {
		slog.Debug("Searching for seed files", "path", seedFile.Path)
		info, err := os.Stat(seedFile.Path)
		if err != nil {
			var w warning.Warning
			if os.IsNotExist(err) {
				w = warning.NewSeedingError(warning.CodeSeedFileNotFound, seedFile.Path)
			} else {
				w = warning.NewSeedingError(warning.CodeFailedToOpenSeedFile, seedFile.Path)
			}
			if w != nil {
				h.warnings = append(h.warnings, w)
				warning.PrintWarning(w)
			}
			continue
		}

		if info.IsDir() {
			slog.Debug("Path is a directory, searching for sql files inside")
			if err := filepath.WalkDir(seedFile.Path, func(p string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !d.IsDir() && filepath.Ext(p) == ".sql" {
					slog.Debug("Found sql file", "file", p)
					needApply, err := h.shouldSeedBeApplied(p, seedFile.SeedOn, h.state.AppliedSeeds)
					if err != nil {
						return nil
					}
					if !needApply {
						return nil
					}

					h.seedFilesPath = append(h.seedFilesPath, p)
				}
				return nil
			}); err != nil {
				if w := warning.NewSeedingError(warning.CodeFailedToOpenSeedFile, seedFile.Path); w != nil {
					h.warnings = append(h.warnings, w)
					warning.PrintWarning(w)
					continue
				}
			}
		} else {
			slog.Debug("Path is a file", "file", seedFile.Path)
			needApply, err := h.shouldSeedBeApplied(seedFile.Path, seedFile.SeedOn, h.state.AppliedSeeds)
			if err != nil {
				continue
			}
			if !needApply {
				continue
			}
			h.seedFilesPath = append(h.seedFilesPath, seedFile.Path)
		}
	}
}

func NewDirectSeed(conn helper.ConnectionFields, seedFiles []config.DatabaseSeed, state *config.DatabaseState) (*DirectSeed, error) {
	outputData := output.NewSeedingOutput()
	db, err := sql.Open("postgres", conn.Url)
	if err != nil {
		outputData.Errors = append(outputData.Errors, err.Error())
		return nil, errors.New(fmt.Sprintf("Failed to connect to database: %v", err))
	}

	return &DirectSeed{db: db, seedFiles: seedFiles, state: state, output: outputData, warnings: make([]warning.Warning, 0)}, nil
}

func (h *DirectSeed) Close() error {
	return h.db.Close()
}

func (h *DirectSeed) Apply() {
	_, _ = fmt.Fprintln(log.Writer(), "Seeding database...")

	h.getSeedFilesToApply()

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
			*h.output.Logs += "----------"
			continue
		}
		// Execute seed file
		res, err := h.db.Exec(string(content))
		if err != nil {
			h.output.FailedCount++
			*h.output.Logs += fmt.Sprintf("Failed to execute seed file: %s\n", seedFile)
			*h.output.Logs += fmt.Sprintf("Error: %s\n", err.Error())
			*h.output.Logs += "----------"
			continue
		}

		affectedRows, err := res.RowsAffected()
		if err == nil {
			*h.output.Logs += fmt.Sprintf("Affected rows: %d\n", affectedRows)
		}
		duration := time.Now().Sub(startTime)

		*h.output.Logs += fmt.Sprintf("Seeding completed in %s\n", duration.String())
		*h.output.Logs += "----------"
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

func (h *DirectSeed) GetWarnings() []warning.Warning { return h.warnings }
