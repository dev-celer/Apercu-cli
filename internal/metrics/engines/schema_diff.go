package engines

import (
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"apercu-cli/output"
	"database/sql"
	"errors"
	"fmt"
	"slices"
)

type SchemaDiffEngine struct {
	db                        *sql.DB
	beforeSchema, afterSchema map[string]metricshelper.Schema
}

func NewSchemaDiffEngine(db *sql.DB) *SchemaDiffEngine {
	return &SchemaDiffEngine{
		db: db,
	}
}

func (e *SchemaDiffEngine) CollectPreMigrationMetrics() error {
	schema, err := e.getSchema()
	if err != nil {
		return err
	}
	e.beforeSchema = schema
	return nil
}

func (e *SchemaDiffEngine) SendPgProxyLogs(s string) {}

func (e *SchemaDiffEngine) CollectPostMigrationMetrics() error {
	schema, err := e.getSchema()
	if err != nil {
		return err
	}
	e.afterSchema = schema
	return nil
}

func (e *SchemaDiffEngine) StoreMetricsToOutput(metrics *output.OutputDatabaseMetrics) error {
	diff := getSchemasDiff(e.beforeSchema, e.afterSchema)
	metrics.SchemaDiff = diff
	return nil
}

func (e *SchemaDiffEngine) GetWarnings() []warning.Warning {
	return nil
}

func convertRawColumnToSchemaStructs(columns []rawColumn) map[string]metricshelper.Schema {
	schemas := make(map[string]metricshelper.Schema)
	for _, column := range columns {
		// Get or create schema
		s, exist := schemas[column.TableSchema]
		if !exist {
			s = *metricshelper.NewSchema()
		}

		// Get or create table
		tableIndex := slices.IndexFunc(s.Tables, func(t metricshelper.Table) bool { return t.Name == column.TableName })
		if tableIndex == -1 {
			s.Tables = append(s.Tables, *metricshelper.NewTable(column.TableName))
			tableIndex = len(s.Tables) - 1
		}

		// Add column to table
		s.Tables[tableIndex].Columns = append(s.Tables[tableIndex].Columns, *metricshelper.NewColumn(column.ColumnName, column.DataType))

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

func (e *SchemaDiffEngine) getSchema() (map[string]metricshelper.Schema, error) {
	columns, err := e.getColumns()
	if err != nil {
		return nil, err
	}

	convertedSchemas := convertRawColumnToSchemaStructs(columns)
	return convertedSchemas, nil
}

func (e *SchemaDiffEngine) getColumns() ([]rawColumn, error) {
	rows, err := e.db.Query("SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns WHERE table_schema NOT IN ('information_schema', 'pg_catalog')")
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

func getSchemaDiff(oldSchema, newSchema *metricshelper.Schema) *metricshelper.SchemaDiff {
	diff := metricshelper.NewSchemaDiff()

	// If old and new are nil, nothing to diff
	if oldSchema == nil && newSchema == nil {
		return &diff
	}

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
		oldIndex := slices.IndexFunc(oldSchema.Tables, func(t metricshelper.Table) bool { return t.Name == newTable.Name })
		if oldIndex == -1 {
			diff.CreatedTables = append(diff.CreatedTables, newTable)
			continue
		}

		// Check if table has been updated
		if updatedTable := getTableDiff(oldSchema.Tables[oldIndex], newTable); updatedTable != nil {
			diff.UpdatedTables = append(diff.UpdatedTables, *updatedTable)
		}
	}

	// Check deleted tables
	for _, oldTable := range oldSchema.Tables {
		idx := slices.IndexFunc(newSchema.Tables, func(t metricshelper.Table) bool { return t.Name == oldTable.Name })
		if idx == -1 {
			diff.DeletedTables = append(diff.DeletedTables, oldTable)
		}
	}

	if diff.HasChanges() {
		return &diff
	}
	return nil
}

func getSchemasDiff(oldSchema, newSchema map[string]metricshelper.Schema) map[string]*metricshelper.SchemaDiff {
	diffs := make(map[string]*metricshelper.SchemaDiff)

	// Handle deleted and updated schema
	for schemaName, schema := range oldSchema {
		nSchema, ok := newSchema[schemaName]
		var diff *metricshelper.SchemaDiff
		if !ok {
			diff = getSchemaDiff(&schema, nil)
		} else {
			diff = getSchemaDiff(&schema, &nSchema)
		}

		if diff != nil {
			diffs[schemaName] = diff
		}
		delete(newSchema, schemaName)
	}

	// Handle created schema
	for schemaName, schema := range newSchema {
		diff := getSchemaDiff(nil, &schema)
		if diff != nil {
			diffs[schemaName] = diff
		}
	}

	return diffs
}

func hasColumnChanged(oldColumn, newColumn metricshelper.Column) bool {
	return oldColumn.DataType != newColumn.DataType || oldColumn.Name != newColumn.Name
}

func getTableDiff(oldTable, newTable metricshelper.Table) *metricshelper.TableDiff {
	diff := metricshelper.NewTableDiff(oldTable.Name)

	// Check updated of created columns
	for _, newTableColumn := range newTable.Columns {
		oldIndex := slices.IndexFunc(oldTable.Columns, func(c metricshelper.Column) bool { return c.Name == newTableColumn.Name })
		if oldIndex == -1 {
			diff.CreatedColumns = append(diff.CreatedColumns, newTableColumn)
			continue
		}

		// Check if column has been updated
		oldColumn := oldTable.Columns[oldIndex]
		if hasColumnChanged(oldColumn, newTableColumn) {
			diff.UpdatedColumns = append(diff.UpdatedColumns, struct{ Old, New metricshelper.Column }{oldColumn, newTableColumn})
		} else {
			diff.UnchangedColumns = append(diff.UnchangedColumns, oldColumn)
		}
	}

	// Check deleted columns
	for _, oldTableColumn := range oldTable.Columns {
		idx := slices.IndexFunc(newTable.Columns, func(c metricshelper.Column) bool { return c.Name == oldTableColumn.Name })
		if idx == -1 {
			diff.DeletedColumns = append(diff.DeletedColumns, oldTableColumn)
		}
	}

	if diff.HasChanges() {
		return &diff
	}
	return nil
}
