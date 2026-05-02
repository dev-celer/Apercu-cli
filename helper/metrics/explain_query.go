package metrics

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

func extractQueriesFromFile(path string) ([]string, error) {
	slog.Debug("Extracting queries from file", "file_path", path)

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	queries := strings.Split(string(content), ";")

	result := make([]string, 0)
	for _, query := range queries {
		if query == "" {
			continue
		}
		result = append(result, strings.TrimSpace(query))
	}

	slog.Debug("Extracted queries from file", "file_path", path, "queries_found", len(result))
	return result, nil
}

type ExtractQueriesOutput struct {
	Queries  map[string][]string
	Warnings []string
}

func ExtractAllQueriesToExplain(paths []string) (*ExtractQueriesOutput, error) {
	outputData := &ExtractQueriesOutput{
		Queries:  make(map[string][]string),
		Warnings: make([]string, 0),
	}

	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				outputData.Warnings = append(outputData.Warnings, fmt.Sprintf("Explain query path not found: %s", path))
				_, _ = fmt.Fprintln(log.Writer(), "WARNING: Explain query path not found:", path)
				continue
			}
			return nil, errors.New(fmt.Sprintf("Failed to get explain query file path %s: %v", path, err))
		}

		if info.IsDir() {
			slog.Debug("Explain query path is a directory, finding all .sql files", "path", path)
			if err := filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}

				if !d.IsDir() && filepath.Ext(p) == ".sql" {
					slog.Debug("Found explain query file", "file", p)
					queries, err := extractQueriesFromFile(path)
					if err != nil {
						outputData.Warnings = append(outputData.Warnings, fmt.Sprintf("Failed to get queries from file %s: %v", path, err))
						_, _ = fmt.Fprintf(log.Writer(), "Failed to get queries from file %s: %v\n", path, err)
						return nil
					}
					if len(queries) == 0 {
						outputData.Warnings = append(outputData.Warnings, fmt.Sprintf("No queries found in file %s", path))
						_, _ = fmt.Fprintf(log.Writer(), "No queries found in file %s\n", path)
						return nil
					}

					outputData.Queries[path] = queries
				}

				return nil
			}); err != nil {
				outputData.Warnings = append(outputData.Warnings, fmt.Sprintf("Failed to walk through explain query directory %s: %v", path, err))
				_, _ = fmt.Fprintf(log.Writer(), "Failed to walk through explain query directory %s: %v\n", path, err)
				continue
			}
		} else {
			slog.Debug("Explain query file found", "file", path)
			queries, err := extractQueriesFromFile(path)
			if err != nil {
				outputData.Warnings = append(outputData.Warnings, fmt.Sprintf("Failed to get queries from file %s: %v", path, err))
				_, _ = fmt.Fprintf(log.Writer(), "Failed to get queries from file %s: %v\n", path, err)
				continue
			}
			if len(queries) == 0 {
				outputData.Warnings = append(outputData.Warnings, fmt.Sprintf("No queries found in file %s", path))
				_, _ = fmt.Fprintf(log.Writer(), "No queries found in file %s\n", path)
				continue
			}

			outputData.Queries[path] = queries
		}
	}

	return outputData, nil
}

func ExplainQuery(db *sql.DB, query string) (*ExplainResult, error) {
	slog.Debug("Explaining query", "query", query)

	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var raw []byte
	stmt := "EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) " + query
	if err := tx.QueryRow(stmt).Scan(&raw); err != nil {
		return nil, fmt.Errorf("explain query failed: %w", err)
	}

	out, err := ParseExplainJSON(raw)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("explain returned no results")
	}

	return &out[0], nil
}
