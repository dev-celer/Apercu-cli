package metrics

import (
	_ "github.com/lib/pq"
)

type Schema struct {
	Tables []Table `json:"tables"`
}

func NewSchema() *Schema {
	return &Schema{
		Tables: make([]Table, 0),
	}
}

type Table struct {
	Name        string
	Columns     []Column     `yaml:"columns,omitempty" json:"columns,omitempty"`
	Indexes     []Index      `yaml:"indexes,omitempty" json:"indexes,omitempty"`
	Constraints []Constraint `yaml:"constraints,omitempty" json:"constraints,omitempty"`
}

func NewTable(name string) *Table {
	return &Table{
		Name:        name,
		Columns:     make([]Column, 0),
		Indexes:     make([]Index, 0),
		Constraints: make([]Constraint, 0),
	}
}

type Column struct {
	Name     string `yaml:"name" json:"name"`
	DataType string `yaml:"data_type" json:"data_type"`
	Nullable bool   `yaml:"nullable" json:"nullable"`
}

func NewColumn(name string, dataType string, nullable bool) *Column {
	return &Column{
		Name:     name,
		DataType: dataType,
		Nullable: nullable,
	}
}

// text renders a column as "name<TAB>type[ NOT NULL]".
func (c Column) text() string {
	t := c.Name + "\t" + c.DataType
	if !c.Nullable {
		t += " NOT NULL"
	}
	return t
}

type Index struct {
	Name       string `yaml:"name" json:"name"`
	Definition string `yaml:"definition" json:"definition"`
	Unique     bool   `yaml:"unique" json:"unique"`
}

func NewIndex(name string, definition string, unique bool) *Index {
	return &Index{
		Name:       name,
		Definition: definition,
		Unique:     unique,
	}
}

type Constraint struct {
	Name       string `yaml:"name" json:"name"`
	Type       string `yaml:"type" json:"type"`
	Definition string `yaml:"definition" json:"definition"`
}

func NewConstraint(name string, constraintType string, definition string) *Constraint {
	return &Constraint{
		Name:       name,
		Type:       constraintType,
		Definition: definition,
	}
}

type SchemaDiff struct {
	DeletedTables []Table     `json:"deleted_tables" yaml:"deleted_tables"`
	CreatedTables []Table     `json:"created_tables" yaml:"created_tables"`
	UpdatedTables []TableDiff `json:"updated_tables" yaml:"updated_tables"`
}

func NewSchemaDiff() SchemaDiff {
	return SchemaDiff{
		DeletedTables: make([]Table, 0),
		CreatedTables: make([]Table, 0),
		UpdatedTables: make([]TableDiff, 0),
	}
}

func (d *SchemaDiff) HasChanges() bool {
	return len(d.DeletedTables) > 0 || len(d.CreatedTables) > 0 || len(d.UpdatedTables) > 0
}

type TableDiff struct {
	Name             string   `json:"name" yaml:"name"`
	UnchangedColumns []Column `json:"unchanged_columns" yaml:"unchanged_columns"`
	DeletedColumns   []Column `json:"deleted_columns" yaml:"deleted_columns"`
	CreatedColumns   []Column `json:"created_columns" yaml:"created_columns"`
	UpdatedColumns   []struct {
		Old Column `json:"old" yaml:"old"`
		New Column `json:"new" yaml:"new"`
	} `json:"updated_columns" yaml:"updated_columns"`
	DeletedIndexes []Index `json:"deleted_indexes" yaml:"deleted_indexes"`
	CreatedIndexes []Index `json:"created_indexes" yaml:"created_indexes"`
	UpdatedIndexes []struct {
		Old Index `json:"old" yaml:"old"`
		New Index `json:"new" yaml:"new"`
	} `json:"updated_indexes" yaml:"updated_indexes"`
	DeletedConstraints []Constraint `json:"deleted_constraints" yaml:"deleted_constraints"`
	CreatedConstraints []Constraint `json:"created_constraints" yaml:"created_constraints"`
	UpdatedConstraints []struct {
		Old Constraint `json:"old" yaml:"old"`
		New Constraint `json:"new" yaml:"new"`
	} `json:"updated_constraints" yaml:"updated_constraints"`
}

func NewTableDiff(name string) TableDiff {
	return TableDiff{
		Name:           name,
		DeletedColumns: make([]Column, 0),
		CreatedColumns: make([]Column, 0),
		UpdatedColumns: make([]struct {
			Old Column `json:"old" yaml:"old"`
			New Column `json:"new" yaml:"new"`
		}, 0),
		DeletedIndexes: make([]Index, 0),
		CreatedIndexes: make([]Index, 0),
		UpdatedIndexes: make([]struct {
			Old Index `json:"old" yaml:"old"`
			New Index `json:"new" yaml:"new"`
		}, 0),
		DeletedConstraints: make([]Constraint, 0),
		CreatedConstraints: make([]Constraint, 0),
		UpdatedConstraints: make([]struct {
			Old Constraint `json:"old" yaml:"old"`
			New Constraint `json:"new" yaml:"new"`
		}, 0),
	}
}

func (d *TableDiff) HasChanges() bool {
	return len(d.DeletedColumns) > 0 || len(d.CreatedColumns) > 0 || len(d.UpdatedColumns) > 0 ||
		len(d.DeletedIndexes) > 0 || len(d.CreatedIndexes) > 0 || len(d.UpdatedIndexes) > 0 ||
		len(d.DeletedConstraints) > 0 || len(d.CreatedConstraints) > 0 || len(d.UpdatedConstraints) > 0
}

// text renders an index using its full definition.
func (i Index) text() string {
	return i.Definition
}

// text renders a constraint as "CONSTRAINT name definition".
func (c Constraint) text() string {
	return "CONSTRAINT " + c.Name + " " + c.Definition
}

func (t *Table) GenerateText(prefix string) string {
	var text string

	text += prefix + t.Name + " (\n"

	for _, column := range t.Columns {
		text += prefix + "\t" + column.text() + "\n"
	}

	for _, constraint := range t.Constraints {
		text += prefix + "\t" + constraint.text() + "\n"
	}

	for _, index := range t.Indexes {
		text += prefix + "\t" + index.text() + "\n"
	}

	text += prefix + ")\n"

	return text
}

func (t *TableDiff) GenerateText() string {
	var text string

	text += t.Name + " (\n"

	// Print unchanged columns
	for _, column := range t.UnchangedColumns {
		text += "\t" + column.text() + "\n"
	}

	// Print deleted columns
	for _, column := range t.DeletedColumns {
		text += "- \t" + column.text() + "\n"
	}

	// Print Updated columns
	for _, column := range t.UpdatedColumns {
		text += "- \t" + column.Old.text() + "\n"
		text += "+ \t" + column.New.text() + "\n"
	}

	// Print created columns
	for _, column := range t.CreatedColumns {
		text += "+ \t" + column.text() + "\n"
	}

	// Print constraint changes
	for _, constraint := range t.DeletedConstraints {
		text += "- \t" + constraint.text() + "\n"
	}
	for _, constraint := range t.UpdatedConstraints {
		text += "- \t" + constraint.Old.text() + "\n"
		text += "+ \t" + constraint.New.text() + "\n"
	}
	for _, constraint := range t.CreatedConstraints {
		text += "+ \t" + constraint.text() + "\n"
	}

	// Print index changes
	for _, index := range t.DeletedIndexes {
		text += "- \t" + index.text() + "\n"
	}
	for _, index := range t.UpdatedIndexes {
		text += "- \t" + index.Old.text() + "\n"
		text += "+ \t" + index.New.text() + "\n"
	}
	for _, index := range t.CreatedIndexes {
		text += "+ \t" + index.text() + "\n"
	}

	text += ")\n"

	return text
}

func (d *SchemaDiff) GenerateText() string {
	var text string

	// Print deleted tables
	for _, tableDiff := range d.DeletedTables {
		text += tableDiff.GenerateText("- ") + "\n"
	}

	// Print updated tables
	for _, tableDiff := range d.UpdatedTables {
		text += tableDiff.GenerateText() + "\n"
	}

	// Print created tables
	for _, tableDiff := range d.CreatedTables {
		text += tableDiff.GenerateText("+ ") + "\n"
	}

	return text
}

func GetSchemasDiffText(schemasDiff map[string]*SchemaDiff) *string {
	var text string

	for schemaName, diff := range schemasDiff {
		if diff != nil {
			text += "\n" + schemaName + ":\n" + diff.GenerateText() + "\n"
		}
	}
	if text == "" {
		return nil
	}
	return &text
}
