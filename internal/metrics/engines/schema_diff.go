package engines

import (
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/output"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
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
	slog.Debug("Start to collect pre-migration schema")
	schema, err := e.getSchema()
	if err != nil {
		return err
	}
	e.beforeSchema = schema
	return nil
}

func (e *SchemaDiffEngine) SendPgProxyLogs(s string) {}

func (e *SchemaDiffEngine) CollectPostMigrationMetrics() error {
	slog.Debug("Start to collect post-migration schema")
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
		s.Tables[tableIndex].Columns = append(s.Tables[tableIndex].Columns, *metricshelper.NewColumn(column.ColumnName, column.DataType, column.IsNullable == "YES"))

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
	IsNullable  string
}

type rawIndex struct {
	SchemaName string
	TableName  string
	IndexName  string
	IndexDef   string
	IsUnique   bool
}

type rawConstraint struct {
	SchemaName     string
	TableName      string
	ConstraintName string
	ConstraintType string
	Definition     string
}

func (e *SchemaDiffEngine) getSchema() (map[string]metricshelper.Schema, error) {
	columns, err := e.getColumns()
	if err != nil {
		return nil, err
	}

	schemas := convertRawColumnToSchemaStructs(columns)

	indexes, err := e.getIndexes()
	if err != nil {
		return nil, err
	}
	attachIndexes(schemas, indexes)

	constraints, err := e.getConstraints()
	if err != nil {
		return nil, err
	}
	attachConstraints(schemas, constraints)

	return schemas, nil
}

func (e *SchemaDiffEngine) getColumns() ([]rawColumn, error) {
	rows, err := e.db.Query("SELECT table_schema, table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema NOT IN ('information_schema', 'pg_catalog')")
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to query database for schema: %v", err))
	}
	defer func() { _ = rows.Close() }()

	var columns []rawColumn
	for rows.Next() {
		var c rawColumn
		if err := rows.Scan(&c.TableSchema, &c.TableName, &c.ColumnName, &c.DataType, &c.IsNullable); err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to scan schema: %v", err))
		}
		columns = append(columns, c)
	}

	return columns, nil
}

// getIndexes returns every index that does NOT back a constraint. Indexes
// implicitly created for primary keys, unique and exclusion constraints are
// reported through getConstraints instead, so they are filtered out here to
// avoid duplicate entries in the diff.
func (e *SchemaDiffEngine) getIndexes() ([]rawIndex, error) {
	const query = `
SELECT n.nspname AS schema_name,
       t.relname AS table_name,
       ic.relname AS index_name,
       pg_get_indexdef(i.indexrelid) AS index_def,
       i.indisunique AS is_unique
FROM pg_index i
JOIN pg_class ic ON ic.oid = i.indexrelid
JOIN pg_class t ON t.oid = i.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
  AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid)`

	rows, err := e.db.Query(query)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to query database for indexes: %v", err))
	}
	defer func() { _ = rows.Close() }()

	var indexes []rawIndex
	for rows.Next() {
		var i rawIndex
		if err := rows.Scan(&i.SchemaName, &i.TableName, &i.IndexName, &i.IndexDef, &i.IsUnique); err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to scan indexes: %v", err))
		}
		indexes = append(indexes, i)
	}

	return indexes, nil
}

func (e *SchemaDiffEngine) getConstraints() ([]rawConstraint, error) {
	const query = `
SELECT n.nspname AS schema_name,
       t.relname AS table_name,
       c.conname AS constraint_name,
       c.contype AS constraint_type,
       pg_get_constraintdef(c.oid) AS definition
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
  AND c.conrelid <> 0`

	rows, err := e.db.Query(query)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to query database for constraints: %v", err))
	}
	defer func() { _ = rows.Close() }()

	var constraints []rawConstraint
	for rows.Next() {
		var c rawConstraint
		var contype string
		if err := rows.Scan(&c.SchemaName, &c.TableName, &c.ConstraintName, &contype, &c.Definition); err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to scan constraints: %v", err))
		}
		c.ConstraintType = constraintTypeLabel(contype)
		constraints = append(constraints, c)
	}

	return constraints, nil
}

// constraintTypeLabel maps a pg_constraint.contype char to a readable label.
func constraintTypeLabel(contype string) string {
	switch contype {
	case "p":
		return "PRIMARY KEY"
	case "f":
		return "FOREIGN KEY"
	case "u":
		return "UNIQUE"
	case "c":
		return "CHECK"
	case "x":
		return "EXCLUSION"
	case "t":
		return "TRIGGER"
	default:
		return contype
	}
}

// attachIndexes appends each index to its owning table. Indexes whose
// schema/table is not present in the column-derived map are skipped.
func attachIndexes(schemas map[string]metricshelper.Schema, indexes []rawIndex) {
	for _, idx := range indexes {
		s, ok := schemas[idx.SchemaName]
		if !ok {
			continue
		}
		tableIndex := slices.IndexFunc(s.Tables, func(t metricshelper.Table) bool { return t.Name == idx.TableName })
		if tableIndex == -1 {
			continue
		}
		s.Tables[tableIndex].Indexes = append(s.Tables[tableIndex].Indexes, *metricshelper.NewIndex(idx.IndexName, idx.IndexDef, idx.IsUnique))
		schemas[idx.SchemaName] = s
	}
}

// attachConstraints appends each constraint to its owning table. Constraints
// whose schema/table is not present in the column-derived map are skipped.
func attachConstraints(schemas map[string]metricshelper.Schema, constraints []rawConstraint) {
	for _, con := range constraints {
		s, ok := schemas[con.SchemaName]
		if !ok {
			continue
		}
		tableIndex := slices.IndexFunc(s.Tables, func(t metricshelper.Table) bool { return t.Name == con.TableName })
		if tableIndex == -1 {
			continue
		}
		s.Tables[tableIndex].Constraints = append(s.Tables[tableIndex].Constraints, *metricshelper.NewConstraint(con.ConstraintName, con.ConstraintType, con.Definition))
		schemas[con.SchemaName] = s
	}
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
	return oldColumn.DataType != newColumn.DataType ||
		oldColumn.Name != newColumn.Name ||
		oldColumn.Nullable != newColumn.Nullable
}

func hasIndexChanged(oldIndex, newIndex metricshelper.Index) bool {
	return oldIndex.Definition != newIndex.Definition || oldIndex.Unique != newIndex.Unique
}

func hasConstraintChanged(oldConstraint, newConstraint metricshelper.Constraint) bool {
	return oldConstraint.Definition != newConstraint.Definition || oldConstraint.Type != newConstraint.Type
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

	// Check updated or created constraints
	for _, newConstraint := range newTable.Constraints {
		oldIndex := slices.IndexFunc(oldTable.Constraints, func(c metricshelper.Constraint) bool { return c.Name == newConstraint.Name })
		if oldIndex == -1 {
			diff.CreatedConstraints = append(diff.CreatedConstraints, newConstraint)
			continue
		}
		oldConstraint := oldTable.Constraints[oldIndex]
		if hasConstraintChanged(oldConstraint, newConstraint) {
			diff.UpdatedConstraints = append(diff.UpdatedConstraints, struct{ Old, New metricshelper.Constraint }{oldConstraint, newConstraint})
		}
	}

	// Check deleted constraints
	for _, oldConstraint := range oldTable.Constraints {
		idx := slices.IndexFunc(newTable.Constraints, func(c metricshelper.Constraint) bool { return c.Name == oldConstraint.Name })
		if idx == -1 {
			diff.DeletedConstraints = append(diff.DeletedConstraints, oldConstraint)
		}
	}

	// Check updated or created indexes
	for _, newIndex := range newTable.Indexes {
		oldIndex := slices.IndexFunc(oldTable.Indexes, func(i metricshelper.Index) bool { return i.Name == newIndex.Name })
		if oldIndex == -1 {
			diff.CreatedIndexes = append(diff.CreatedIndexes, newIndex)
			continue
		}
		oldIdx := oldTable.Indexes[oldIndex]
		if hasIndexChanged(oldIdx, newIndex) {
			diff.UpdatedIndexes = append(diff.UpdatedIndexes, struct{ Old, New metricshelper.Index }{oldIdx, newIndex})
		}
	}

	// Check deleted indexes
	for _, oldIndex := range oldTable.Indexes {
		idx := slices.IndexFunc(newTable.Indexes, func(i metricshelper.Index) bool { return i.Name == oldIndex.Name })
		if idx == -1 {
			diff.DeletedIndexes = append(diff.DeletedIndexes, oldIndex)
		}
	}

	if diff.HasChanges() {
		return &diff
	}
	return nil
}
