package greenmask

import (
	"fmt"
	"os"
	"slices"

	"go.yaml.in/yaml/v3"
)

/*
Partial GreenMask config parser
Used to know which schema->table->rows are anonymized by a configuration
*/

type Config struct {
	Dump Dump `yaml:"dump"`
}

type Dump struct {
	Transformations []Transformation `yaml:"transformation"`
}

type Transformation struct {
	Schema       string        `yaml:"schema"`
	Name         string        `yaml:"name"`
	Transformers []Transformer `yaml:"transformers"`
}

type Transformer struct {
	Name   string            `yaml:"name"`
	Params TransformerParams `yaml:"params"`
}

type TransformerParams struct {
	Column  string         `yaml:"column,omitempty"`
	Columns []ColumnConfig `yaml:"columns,omitempty"`
}

type ColumnConfig struct {
	Name string `yaml:"name"`
}

type ModifiedTable struct {
	Schema  string
	Table   string
	Columns []string
}

func ParseConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read GreenMask config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse GreenMask config: %w", err)
	}
	return &cfg, nil
}

func (c *Config) ModifiedTables() []ModifiedTable {
	out := make([]ModifiedTable, 0, len(c.Dump.Transformations))
	for _, t := range c.Dump.Transformations {
		seen := make(map[string]struct{})
		columns := make([]string, 0)
		for _, tr := range t.Transformers {
			if tr.Params.Column != "" {
				if _, ok := seen[tr.Params.Column]; !ok {
					seen[tr.Params.Column] = struct{}{}
					columns = append(columns, tr.Params.Column)
				}
			}
			for _, col := range tr.Params.Columns {
				if col.Name == "" {
					continue
				}
				if _, ok := seen[col.Name]; !ok {
					seen[col.Name] = struct{}{}
					columns = append(columns, col.Name)
				}
			}
		}
		out = append(out, ModifiedTable{
			Schema:  t.Schema,
			Table:   t.Name,
			Columns: columns,
		})
	}
	return out
}

func IsRowModified(modifiedTables []ModifiedTable, schema, table, column string) bool {
	for _, t := range modifiedTables {
		if t.Schema != schema || t.Table != table {
			continue
		}
		if slices.Contains(t.Columns, column) {
			return true
		}
		return false
	}
	return false
}
