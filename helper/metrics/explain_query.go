package metrics

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

func ExtractQueriesFromFile(path string) ([]string, error) {
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
