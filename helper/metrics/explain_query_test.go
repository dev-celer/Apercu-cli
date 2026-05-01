package metrics

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func writeQueryFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queries.sql")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return path
}

func TestExtractQueriesFromFile(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    []string
	}{
		{
			name:    "single query without semicolon",
			content: "SELECT 1",
			want:    []string{"SELECT 1"},
		},
		{
			name:    "single query with trailing semicolon",
			content: "SELECT 1;",
			want:    []string{"SELECT 1"},
		},
		{
			name:    "multiple queries",
			content: "SELECT 1; SELECT 2; SELECT 3;",
			want:    []string{"SELECT 1", "SELECT 2", "SELECT 3"},
		},
		{
			name:    "surrounding whitespace is trimmed",
			content: "  SELECT 1  ;\n\tSELECT 2\n",
			want:    []string{"SELECT 1", "SELECT 2"},
		},
		{
			name:    "multiline query is preserved",
			content: "SELECT *\nFROM users\nWHERE id = 1;\nSELECT 2;",
			want:    []string{"SELECT *\nFROM users\nWHERE id = 1", "SELECT 2"},
		},
		{
			name:    "empty file",
			content: "",
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeQueryFile(t, tt.content)
			got, err := ExtractQueriesFromFile(path)
			if err != nil {
				t.Fatalf("ExtractQueriesFromFile() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExtractQueriesFromFile() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestExtractQueriesFromFile_MissingFile(t *testing.T) {
	_, err := ExtractQueriesFromFile(filepath.Join(t.TempDir(), "does-not-exist.sql"))
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}
