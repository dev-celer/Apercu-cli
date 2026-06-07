package metrics

import (
	_ "github.com/lib/pq"
)

type Schema struct {
	Tables []Table
}

func NewSchema() *Schema {
	return &Schema{
		Tables: make([]Table, 0),
	}
}

type Table struct {
	Name        string
	Columns     []Column
	Indexes     []Index
	Constraints []Constraint
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
	Name     string
	DataType string
	Nullable bool
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
	Name       string
	Definition string
	Unique     bool
}

func NewIndex(name string, definition string, unique bool) *Index {
	return &Index{
		Name:       name,
		Definition: definition,
		Unique:     unique,
	}
}

type Constraint struct {
	Name       string
	Type       string
	Definition string
}

func NewConstraint(name string, constraintType string, definition string) *Constraint {
	return &Constraint{
		Name:       name,
		Type:       constraintType,
		Definition: definition,
	}
}

type SchemaDiff struct {
	DeletedTables []Table
	CreatedTables []Table
	UpdatedTables []TableDiff
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
	Name             string
	UnchangedColumns []Column
	DeletedColumns   []Column
	CreatedColumns   []Column
	UpdatedColumns   []struct {
		Old Column
		New Column
	}
	DeletedIndexes []Index
	CreatedIndexes []Index
	UpdatedIndexes []struct {
		Old Index
		New Index
	}
	DeletedConstraints []Constraint
	CreatedConstraints []Constraint
	UpdatedConstraints []struct {
		Old Constraint
		New Constraint
	}
}

func NewTableDiff(name string) TableDiff {
	return TableDiff{
		Name:           name,
		DeletedColumns: make([]Column, 0),
		CreatedColumns: make([]Column, 0),
		UpdatedColumns: make([]struct {
			Old Column
			New Column
		}, 0),
		DeletedIndexes: make([]Index, 0),
		CreatedIndexes: make([]Index, 0),
		UpdatedIndexes: make([]struct {
			Old Index
			New Index
		}, 0),
		DeletedConstraints: make([]Constraint, 0),
		CreatedConstraints: make([]Constraint, 0),
		UpdatedConstraints: make([]struct {
			Old Constraint
			New Constraint
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
