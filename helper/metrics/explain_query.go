package metrics

import (
	"fmt"
	"os"
	"strings"
)

func ExtractQueriesFromFile(path string) ([]string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	queries := strings.Split(string(content), ";")

	result := make([]string, len(queries))
	for i, query := range queries {
		result[i] = strings.TrimSpace(query)
	}

	return result, nil
}
