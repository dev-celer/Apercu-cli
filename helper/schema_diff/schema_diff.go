package schema_diff

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"

	_ "github.com/lib/pq"
)

type Schema struct {
	Tables []Table
}

func newSchema() *Schema {
	return &Schema{
		Tables: make([]Table, 0),
	}
}

type Table struct {
	Name    string
	Columns []Column
}

func newTable(name string) *Table {
	return &Table{
		Name:    name,
		Columns: make([]Column, 0),
	}
}

type Column struct {
	Name     string
	DataType string
}

func newColumn(name string, dataType string) *Column {
	return &Column{
		Name:     name,
		DataType: dataType,
	}
}

func convertRawColumnToSchemaStructs(columns []rawColumn) map[string]Schema {
	schemas := make(map[string]Schema)
	for _, column := range columns {
		// Get or create schema
		s, exist := schemas[column.TableSchema]
		if !exist {
			s = *newSchema()
		}

		// Get or create table
		tableIndex := slices.IndexFunc(s.Tables, func(t Table) bool { return t.Name == column.TableName })
		if tableIndex == -1 {
			s.Tables = append(s.Tables, *newTable(column.TableName))
			tableIndex = len(s.Tables) - 1
		}

		// Add column to table
		s.Tables[tableIndex].Columns = append(s.Tables[tableIndex].Columns, *newColumn(column.ColumnName, column.DataType))

		// Store updated schema
		schemas[column.TableSchema] = s
	}

	return schemas
}

type rawColumn struct {
	TableSchema string
	TableName   string
	ColumnName  string
	DataType    string
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

func hasColumnChanged(oldColumn, newColumn Column) bool {
	return oldColumn.DataType != newColumn.DataType || oldColumn.Name != newColumn.Name
}

func getTableDiff(oldTable, newTable Table) *TableDiff {
	diff := NewTableDiff(oldTable.Name)

	// Check updated of created columns
	for _, newTableColumn := range newTable.Columns {
		oldIndex := slices.IndexFunc(oldTable.Columns, func(c Column) bool { return c.Name == newTableColumn.Name })
		if oldIndex == -1 {
			diff.CreatedColumns = append(diff.CreatedColumns, newTableColumn)
			continue
		}

		// Check if column has been updated
		oldColumn := oldTable.Columns[oldIndex]
		if hasColumnChanged(oldColumn, newTableColumn) {
			diff.UpdatedColumns = append(diff.UpdatedColumns, struct{ Old, New Column }{oldColumn, newTableColumn})
		} else {
			diff.UnchangedColumns = append(diff.UnchangedColumns, oldColumn)
		}
		oldTable.Columns = slices.Delete(oldTable.Columns, oldIndex, oldIndex+1)
	}

	// Check deleted columns
	diff.DeletedColumns = append(diff.DeletedColumns, oldTable.Columns...)

	if diff.HasChanges() {
		return &diff
	}
	return nil
}

func GetSchemaDiff(oldSchema, newSchema *Schema) *SchemaDiff {
	diff := NewSchemaDiff()

	// If old schema is nil, all tables are new
	if oldSchema == nil {
		diff.CreatedTables = newSchema.Tables
		return &diff
	}

	// If new schema is nil, all tables are deleted
	if newSchema == nil {
		diff.DeletedTables = oldSchema.Tables
		return &diff
	}

	// Check updated of created tables
	for _, newTable := range newSchema.Tables {
		oldIndex := slices.IndexFunc(oldSchema.Tables, func(t Table) bool { return t.Name == newTable.Name })
		if oldIndex == -1 {
			diff.CreatedTables = append(diff.CreatedTables, newTable)
			continue
		}

		// Check if table has been updated
		if updatedTable := getTableDiff(oldSchema.Tables[oldIndex], newTable); updatedTable != nil {
			diff.UpdatedTables = append(diff.UpdatedTables, *updatedTable)
		}
		oldSchema.Tables = slices.Delete(oldSchema.Tables, oldIndex, oldIndex+1)
	}

	// Check deleted tables
	diff.DeletedTables = append(diff.DeletedTables, oldSchema.Tables...)

	if diff.HasChanges() {
		return &diff
	}
	return nil
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

func getColumns(databaseUrl string) ([]rawColumn, error) {
	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to connect to database: %v", err))
	}
	defer func() { _ = db.Close() }()

	rows, err := db.Query("SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns WHERE table_schema NOT IN ('information_schema', 'pg_catalog')")
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to query database for schema: %v", err))
	}
	defer func() { _ = rows.Close() }()

	var columns []rawColumn
	for rows.Next() {
		var c rawColumn
		if err := rows.Scan(&c.TableSchema, &c.TableName, &c.ColumnName, &c.DataType); err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to scan schema: %v", err))
		}
		columns = append(columns, c)
	}

	return columns, nil
}

func GetSchema(databaseUrl string) (map[string]Schema, error) {
	columns, err := getColumns(databaseUrl)
	if err != nil {
		return nil, err
	}

	convertedSchemas := convertRawColumnToSchemaStructs(columns)
	return convertedSchemas, nil
}

func GetSchemaDiffText(oldSchema, newSchema map[string]Schema) *string {
	var text string
	// Handle deleted and updated schema
	for schemaName, schema := range oldSchema {
		diff := GetSchemaDiff(&schema, new(newSchema[schemaName]))
		if diff != nil {
			text += "\n" + schemaName + ":\n" + diff.GenerateText() + "\n"
		}
		delete(newSchema, schemaName)
	}

	// Handle created schema
	for schemaName, schema := range newSchema {
		diff := GetSchemaDiff(nil, &schema)
		if diff != nil {
			text += "\n" + schemaName + ":\n" + diff.GenerateText() + "\n"
		}
	}

	if text == "" {
		return nil
	}
	return &text
}
