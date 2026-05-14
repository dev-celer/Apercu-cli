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
	Name    string
	Columns []Column
}

func NewTable(name string) *Table {
	return &Table{
		Name:    name,
		Columns: make([]Column, 0),
	}
}

type Column struct {
	Name     string
	DataType string
}

func NewColumn(name string, dataType string) *Column {
	return &Column{
		Name:     name,
		DataType: dataType,
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
	}
}

func (d *TableDiff) HasChanges() bool {
	return len(d.DeletedColumns) > 0 || len(d.CreatedColumns) > 0 || len(d.UpdatedColumns) > 0
}

func (t *Table) GenerateText(prefix string) string {
	var text string

	text += prefix + t.Name + " (\n"

	for _, column := range t.Columns {
		text += prefix + "\t" + column.Name + "\t" + column.DataType + "\n"
	}

	text += prefix + ")\n"

	return text
}

func (t *TableDiff) GenerateText() string {
	var text string

	text += t.Name + " (\n"

	// Print unchanged columns
	for _, column := range t.UnchangedColumns {
		text += "\t" + column.Name + "\t" + column.DataType + "\n"
	}

	// Print deleted columns
	for _, column := range t.DeletedColumns {
		text += "- \t" + column.Name + "\t" + column.DataType + "\n"
	}

	// Print Updated columns
	for _, column := range t.UpdatedColumns {
		text += "- \t" + column.Old.Name + "\t" + column.Old.DataType + "\n"
		text += "+ \t" + column.New.Name + "\t" + column.New.DataType + "\n"
	}

	// Print created columns
	for _, column := range t.CreatedColumns {
		text += "+ \t" + column.Name + "\t" + column.DataType + "\n"
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
