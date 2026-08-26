package pg_catalog

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// gzipMagic is the two-byte header every gzip stream starts with.
var gzipMagic = []byte{0x1f, 0x8b}

// WriteJSON dumps the snapshot as a fixture. Every phase built on top of the
// catalog tests against these files, so the derived lookups and the rules need
// no database of their own.
//
// If the path end with .gz the fixture will be compressed using gzip.
func (s *Snapshot) WriteJSON(path string) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("Failed to encode snapshot: %v", err)
	}
	data = append(data, '\n')

	if strings.HasSuffix(path, ".gz") {
		buffer := &bytes.Buffer{}
		writer := gzip.NewWriter(buffer)
		if _, err := writer.Write(data); err != nil {
			return fmt.Errorf("Failed to compress snapshot: %v", err)
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("Failed to compress snapshot: %v", err)
		}
		data = buffer.Bytes()
	}

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("Failed to write snapshot to %s: %v", path, err)
	}
	return nil
}

// LoadJSON reads a snapshot fixture written by WriteJSON, compressed or not.
func LoadJSON(path string) (*Snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to read snapshot from %s: %v", path, err)
	}

	// Sniff the content rather than trusting the extension, so a fixture stays readable after a rename.
	if bytes.HasPrefix(data, gzipMagic) {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("Failed to decompress snapshot from %s: %v", path, err)
		}
		defer func() { _ = reader.Close() }()
		if data, err = io.ReadAll(reader); err != nil {
			return nil, fmt.Errorf("Failed to decompress snapshot from %s: %v", path, err)
		}
	}

	snapshot := &Snapshot{}
	if err := json.Unmarshal(data, snapshot); err != nil {
		return nil, fmt.Errorf("Failed to decode snapshot from %s: %v", path, err)
	}
	return snapshot, nil
}
