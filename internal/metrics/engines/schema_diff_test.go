package engines

import (
	metricshelper "apercu-cli/helper/metrics"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertRawColumnToSchemaStructs_Empty(t *testing.T) {
	t.Parallel()
	schemas := convertRawColumnToSchemaStructs([]rawColumn{})
	assert.Empty(t, schemas)
}

func TestConvertRawColumnToSchemaStructs_SingleTable(t *testing.T) {
	t.Parallel()
	raw := []rawColumn{
		{TableSchema: "public", TableName: "users", ColumnName: "id", DataType: "integer"},
		{TableSchema: "public", TableName: "users", ColumnName: "email", DataType: "text"},
	}

	schemas := convertRawColumnToSchemaStructs(raw)

	require.Len(t, schemas, 1)
	require.Contains(t, schemas, "public")
	require.Len(t, schemas["public"].Tables, 1)
	assert.Equal(t, "users", schemas["public"].Tables[0].Name)
	assert.Equal(t,
		[]metricshelper.Column{
			{Name: "id", DataType: "integer"},
			{Name: "email", DataType: "text"},
		},
		schemas["public"].Tables[0].Columns,
	)
}

func TestConvertRawColumnToSchemaStructs_MultipleTables(t *testing.T) {
	t.Parallel()
	raw := []rawColumn{
		{TableSchema: "public", TableName: "users", ColumnName: "id", DataType: "integer"},
		{TableSchema: "public", TableName: "orders", ColumnName: "id", DataType: "bigint"},
		{TableSchema: "public", TableName: "orders", ColumnName: "user_id", DataType: "integer"},
	}

	schemas := convertRawColumnToSchemaStructs(raw)

	require.Len(t, schemas, 1)
	require.Len(t, schemas["public"].Tables, 2)

	tablesByName := map[string]metricshelper.Table{}
	for _, tbl := range schemas["public"].Tables {
		tablesByName[tbl.Name] = tbl
	}
	assert.Len(t, tablesByName["users"].Columns, 1)
	assert.Len(t, tablesByName["orders"].Columns, 2)
}

func TestConvertRawColumnToSchemaStructs_MultipleSchemas(t *testing.T) {
	t.Parallel()
	raw := []rawColumn{
		{TableSchema: "public", TableName: "users", ColumnName: "id", DataType: "integer"},
		{TableSchema: "auth", TableName: "sessions", ColumnName: "token", DataType: "text"},
	}

	schemas := convertRawColumnToSchemaStructs(raw)

	require.Len(t, schemas, 2)
	require.Contains(t, schemas, "public")
	require.Contains(t, schemas, "auth")
	assert.Equal(t, "users", schemas["public"].Tables[0].Name)
	assert.Equal(t, "sessions", schemas["auth"].Tables[0].Name)
}

func TestHasColumnChanged(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		old  metricshelper.Column
		new  metricshelper.Column
		want bool
	}{
		{"identical", metricshelper.Column{Name: "id", DataType: "integer"}, metricshelper.Column{Name: "id", DataType: "integer"}, false},
		{"different type", metricshelper.Column{Name: "id", DataType: "integer"}, metricshelper.Column{Name: "id", DataType: "bigint"}, true},
		{"different name", metricshelper.Column{Name: "id", DataType: "integer"}, metricshelper.Column{Name: "user_id", DataType: "integer"}, true},
		{"both different", metricshelper.Column{Name: "id", DataType: "integer"}, metricshelper.Column{Name: "user_id", DataType: "bigint"}, true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, hasColumnChanged(tt.old, tt.new))
		})
	}
}

func TestGetTableDiff_NoChanges(t *testing.T) {
	t.Parallel()
	old := metricshelper.Table{Name: "users", Columns: []metricshelper.Column{
		{Name: "id", DataType: "integer"},
		{Name: "email", DataType: "text"},
	}}
	// new is a distinct but structurally identical copy
	newT := metricshelper.Table{Name: "users", Columns: []metricshelper.Column{
		{Name: "id", DataType: "integer"},
		{Name: "email", DataType: "text"},
	}}

	diff := getTableDiff(old, newT)
	assert.Nil(t, diff, "identical tables should produce no diff")
}

func TestGetTableDiff_UpdatedColumn(t *testing.T) {
	t.Parallel()
	old := metricshelper.Table{Name: "users", Columns: []metricshelper.Column{
		{Name: "id", DataType: "integer"},
		{Name: "email", DataType: "text"},
	}}
	newT := metricshelper.Table{Name: "users", Columns: []metricshelper.Column{
		{Name: "id", DataType: "bigint"},
		{Name: "email", DataType: "text"},
	}}

	diff := getTableDiff(old, newT)

	require.NotNil(t, diff)
	assert.Empty(t, diff.CreatedColumns)
	assert.Empty(t, diff.DeletedColumns)
	require.Len(t, diff.UpdatedColumns, 1)
	assert.Equal(t, metricshelper.Column{Name: "id", DataType: "integer"}, diff.UpdatedColumns[0].Old)
	assert.Equal(t, metricshelper.Column{Name: "id", DataType: "bigint"}, diff.UpdatedColumns[0].New)
	assert.Equal(t, []metricshelper.Column{{Name: "email", DataType: "text"}}, diff.UnchangedColumns)
}

func TestGetTableDiff_CreatedColumn(t *testing.T) {
	t.Parallel()
	old := metricshelper.Table{Name: "users", Columns: []metricshelper.Column{
		{Name: "id", DataType: "integer"},
	}}
	newT := metricshelper.Table{Name: "users", Columns: []metricshelper.Column{
		{Name: "id", DataType: "integer"},
		{Name: "email", DataType: "text"},
	}}

	diff := getTableDiff(old, newT)

	require.NotNil(t, diff)
	require.Len(t, diff.CreatedColumns, 1)
	assert.Equal(t, metricshelper.Column{Name: "email", DataType: "text"}, diff.CreatedColumns[0])
	assert.Empty(t, diff.UpdatedColumns)
	assert.Empty(t, diff.DeletedColumns)
	assert.Equal(t, []metricshelper.Column{{Name: "id", DataType: "integer"}}, diff.UnchangedColumns)
}

func TestGetTableDiff_DeletedColumn(t *testing.T) {
	t.Parallel()
	old := metricshelper.Table{Name: "users", Columns: []metricshelper.Column{
		{Name: "id", DataType: "integer"},
		{Name: "email", DataType: "text"},
	}}
	newT := metricshelper.Table{Name: "users", Columns: []metricshelper.Column{
		{Name: "id", DataType: "integer"},
	}}

	diff := getTableDiff(old, newT)

	require.NotNil(t, diff)
	require.Len(t, diff.DeletedColumns, 1)
	assert.Equal(t, metricshelper.Column{Name: "email", DataType: "text"}, diff.DeletedColumns[0])
	assert.Empty(t, diff.CreatedColumns)
	assert.Empty(t, diff.UpdatedColumns)
	assert.Equal(t, []metricshelper.Column{{Name: "id", DataType: "integer"}}, diff.UnchangedColumns)
}

func TestGetSchemaDiff_NoChanges(t *testing.T) {
	t.Parallel()
	tables := []metricshelper.Table{
		{Name: "users", Columns: []metricshelper.Column{{Name: "id", DataType: "integer"}}},
	}
	oldS := &metricshelper.Schema{Tables: tables}
	newS := &metricshelper.Schema{Tables: []metricshelper.Table{
		{Name: "users", Columns: []metricshelper.Column{{Name: "id", DataType: "integer"}}},
	}}

	diff := getSchemaDiff(oldS, newS)
	assert.Nil(t, diff)
}

func TestGetSchemaDiff_CreatedTable(t *testing.T) {
	t.Parallel()
	oldS := &metricshelper.Schema{Tables: []metricshelper.Table{
		{Name: "users", Columns: []metricshelper.Column{{Name: "id", DataType: "integer"}}},
	}}
	newS := &metricshelper.Schema{Tables: []metricshelper.Table{
		{Name: "users", Columns: []metricshelper.Column{{Name: "id", DataType: "integer"}}},
		{Name: "orders", Columns: []metricshelper.Column{{Name: "id", DataType: "bigint"}}},
	}}

	diff := getSchemaDiff(oldS, newS)

	require.NotNil(t, diff)
	require.Len(t, diff.CreatedTables, 1)
	assert.Equal(t, "orders", diff.CreatedTables[0].Name)
}

func TestGetSchemaDiff_DeletedTable(t *testing.T) {
	t.Parallel()
	oldS := &metricshelper.Schema{Tables: []metricshelper.Table{
		{Name: "users", Columns: []metricshelper.Column{{Name: "id", DataType: "integer"}}},
		{Name: "orders", Columns: []metricshelper.Column{{Name: "id", DataType: "bigint"}}},
	}}
	newS := &metricshelper.Schema{Tables: []metricshelper.Table{
		{Name: "users", Columns: []metricshelper.Column{{Name: "id", DataType: "integer"}}},
	}}

	diff := getSchemaDiff(oldS, newS)

	require.NotNil(t, diff)
	require.Len(t, diff.DeletedTables, 1)
	assert.Equal(t, "orders", diff.DeletedTables[0].Name)
}

func TestGetSchemaDiff_UpdatedTable(t *testing.T) {
	t.Parallel()
	oldS := &metricshelper.Schema{Tables: []metricshelper.Table{
		{Name: "users", Columns: []metricshelper.Column{{Name: "id", DataType: "integer"}}},
	}}
	newS := &metricshelper.Schema{Tables: []metricshelper.Table{
		{Name: "users", Columns: []metricshelper.Column{{Name: "id", DataType: "bigint"}}},
	}}

	diff := getSchemaDiff(oldS, newS)

	require.NotNil(t, diff)
	require.Len(t, diff.UpdatedTables, 1)
	require.Len(t, diff.UpdatedTables[0].UpdatedColumns, 1)
	assert.Equal(t, "integer", diff.UpdatedTables[0].UpdatedColumns[0].Old.DataType)
	assert.Equal(t, "bigint", diff.UpdatedTables[0].UpdatedColumns[0].New.DataType)
}

func TestGetSchemaDiff_NilOldSchema(t *testing.T) {
	t.Parallel()
	newS := &metricshelper.Schema{Tables: []metricshelper.Table{
		{Name: "users", Columns: []metricshelper.Column{{Name: "id", DataType: "integer"}}},
		{Name: "orders", Columns: []metricshelper.Column{{Name: "id", DataType: "bigint"}}},
	}}

	diff := getSchemaDiff(nil, newS)

	require.NotNil(t, diff)
	assert.Equal(t, newS.Tables, diff.CreatedTables)
	assert.Empty(t, diff.DeletedTables)
	assert.Empty(t, diff.UpdatedTables)
}

func TestGetSchemaDiff_NilNewSchema(t *testing.T) {
	t.Parallel()
	oldS := &metricshelper.Schema{Tables: []metricshelper.Table{
		{Name: "users", Columns: []metricshelper.Column{{Name: "id", DataType: "integer"}}},
		{Name: "orders", Columns: []metricshelper.Column{{Name: "id", DataType: "bigint"}}},
	}}

	diff := getSchemaDiff(oldS, nil)

	require.NotNil(t, diff)
	assert.Equal(t, oldS.Tables, diff.DeletedTables)
	assert.Empty(t, diff.CreatedTables)
	assert.Empty(t, diff.UpdatedTables)
}
