package schema_diff

import (
	"database/sql"
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

func getColumns(databaseUrl string) ([]rawColumn, error) {
	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	rows, err := db.Query("SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns WHERE table_schema NOT IN ('information_schema', 'pg_catalog')")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var columns []rawColumn
	for rows.Next() {
		var c rawColumn
		if err := rows.Scan(&c.TableSchema, &c.TableName, &c.ColumnName, &c.DataType); err != nil {
			return nil, err
		}
		columns = append(columns, c)
	}

	return columns, nil
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
	OldTable       *Table
	NewTable       *Table
	DeletedColumns []Column
	CreatedColumns []Column
	UpdatedColumns []struct {
		Old Column
		New Column
	}
}

func NewTableDiff() TableDiff {
	return TableDiff{
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
	diff := NewTableDiff()

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

func getSchemaDiff(oldSchema, newSchema *Schema) *SchemaDiff {
	diff := NewSchemaDiff()

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
