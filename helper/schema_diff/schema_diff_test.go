package schema_diff

import (
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
		[]Column{
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

	tablesByName := map[string]Table{}
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
		old  Column
		new  Column
		want bool
	}{
		{"identical", Column{Name: "id", DataType: "integer"}, Column{Name: "id", DataType: "integer"}, false},
		{"different type", Column{Name: "id", DataType: "integer"}, Column{Name: "id", DataType: "bigint"}, true},
		{"different name", Column{Name: "id", DataType: "integer"}, Column{Name: "user_id", DataType: "integer"}, true},
		{"both different", Column{Name: "id", DataType: "integer"}, Column{Name: "user_id", DataType: "bigint"}, true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, hasColumnChanged(tt.old, tt.new))
		})
	}
}

func TestNewSchemaDiff(t *testing.T) {
	t.Parallel()
	d := NewSchemaDiff()
	assert.NotNil(t, d.DeletedTables)
	assert.NotNil(t, d.CreatedTables)
	assert.NotNil(t, d.UpdatedTables)
	assert.Empty(t, d.DeletedTables)
	assert.Empty(t, d.CreatedTables)
	assert.Empty(t, d.UpdatedTables)
	assert.False(t, d.HasChanges())
}

func TestSchemaDiff_HasChanges(t *testing.T) {
	t.Parallel()
	t.Run("empty has no changes", func(t *testing.T) {
		t.Parallel()
		d := NewSchemaDiff()
		assert.False(t, d.HasChanges())
	})
	t.Run("deleted table", func(t *testing.T) {
		t.Parallel()
		d := NewSchemaDiff()
		d.DeletedTables = append(d.DeletedTables, Table{Name: "users"})
		assert.True(t, d.HasChanges())
	})
	t.Run("created table", func(t *testing.T) {
		t.Parallel()
		d := NewSchemaDiff()
		d.CreatedTables = append(d.CreatedTables, Table{Name: "users"})
		assert.True(t, d.HasChanges())
	})
	t.Run("updated table", func(t *testing.T) {
		t.Parallel()
		d := NewSchemaDiff()
		d.UpdatedTables = append(d.UpdatedTables, NewTableDiff())
		assert.True(t, d.HasChanges())
	})
}

func TestNewTableDiff(t *testing.T) {
	t.Parallel()
	d := NewTableDiff()
	assert.NotNil(t, d.DeletedColumns)
	assert.NotNil(t, d.CreatedColumns)
	assert.NotNil(t, d.UpdatedColumns)
	assert.Empty(t, d.DeletedColumns)
	assert.Empty(t, d.CreatedColumns)
	assert.Empty(t, d.UpdatedColumns)
	assert.False(t, d.HasChanges())
}

func TestTableDiff_HasChanges(t *testing.T) {
	t.Parallel()
	t.Run("empty has no changes", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff()
		assert.False(t, d.HasChanges())
	})
	t.Run("deleted column", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff()
		d.DeletedColumns = append(d.DeletedColumns, Column{Name: "id"})
		assert.True(t, d.HasChanges())
	})
	t.Run("created column", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff()
		d.CreatedColumns = append(d.CreatedColumns, Column{Name: "id"})
		assert.True(t, d.HasChanges())
	})
	t.Run("updated column", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff()
		d.UpdatedColumns = append(d.UpdatedColumns, struct {
			Old Column
			New Column
		}{
			Old: Column{Name: "id", DataType: "integer"},
			New: Column{Name: "id", DataType: "bigint"},
		})
		assert.True(t, d.HasChanges())
	})
}

func TestGetTableDiff_NoChanges(t *testing.T) {
	t.Parallel()
	old := Table{Name: "users", Columns: []Column{
		{Name: "id", DataType: "integer"},
		{Name: "email", DataType: "text"},
	}}
	// new is a distinct but structurally identical copy
	newT := Table{Name: "users", Columns: []Column{
		{Name: "id", DataType: "integer"},
		{Name: "email", DataType: "text"},
	}}

	diff := getTableDiff(old, newT)
	assert.Nil(t, diff, "identical tables should produce no diff")
}

func TestGetTableDiff_UpdatedColumn(t *testing.T) {
	t.Parallel()
	old := Table{Name: "users", Columns: []Column{
		{Name: "id", DataType: "integer"},
		{Name: "email", DataType: "text"},
	}}
	newT := Table{Name: "users", Columns: []Column{
		{Name: "id", DataType: "bigint"},
		{Name: "email", DataType: "text"},
	}}

	diff := getTableDiff(old, newT)

	require.NotNil(t, diff)
	assert.Empty(t, diff.CreatedColumns)
	require.Len(t, diff.UpdatedColumns, 1)
	assert.Equal(t, Column{Name: "id", DataType: "integer"}, diff.UpdatedColumns[0].Old)
	assert.Equal(t, Column{Name: "id", DataType: "bigint"}, diff.UpdatedColumns[0].New)
}

func TestGetTableDiff_CreatedColumn(t *testing.T) {
	t.Parallel()
	old := Table{Name: "users", Columns: []Column{
		{Name: "id", DataType: "integer"},
	}}
	newT := Table{Name: "users", Columns: []Column{
		{Name: "id", DataType: "integer"},
		{Name: "email", DataType: "text"},
	}}

	diff := getTableDiff(old, newT)

	require.NotNil(t, diff)
	require.Len(t, diff.CreatedColumns, 1)
	assert.Equal(t, Column{Name: "email", DataType: "text"}, diff.CreatedColumns[0])
	assert.Empty(t, diff.UpdatedColumns)
	assert.Empty(t, diff.DeletedColumns)
}

func TestGetTableDiff_DeletedColumn(t *testing.T) {
	t.Parallel()
	old := Table{Name: "users", Columns: []Column{
		{Name: "id", DataType: "integer"},
		{Name: "email", DataType: "text"},
	}}
	newT := Table{Name: "users", Columns: []Column{
		{Name: "id", DataType: "integer"},
	}}

	diff := getTableDiff(old, newT)

	require.NotNil(t, diff)
	require.Len(t, diff.DeletedColumns, 1)
	assert.Equal(t, Column{Name: "email", DataType: "text"}, diff.DeletedColumns[0])
	assert.Empty(t, diff.CreatedColumns)
	assert.Empty(t, diff.UpdatedColumns)
}

func TestGetSchemaDiff_NoChanges(t *testing.T) {
	t.Parallel()
	tables := []Table{
		{Name: "users", Columns: []Column{{Name: "id", DataType: "integer"}}},
	}
	oldS := &Schema{Tables: tables}
	newS := &Schema{Tables: []Table{
		{Name: "users", Columns: []Column{{Name: "id", DataType: "integer"}}},
	}}

	diff := getSchemaDiff(oldS, newS)
	assert.Nil(t, diff)
}

func TestGetSchemaDiff_CreatedTable(t *testing.T) {
	t.Parallel()
	oldS := &Schema{Tables: []Table{
		{Name: "users", Columns: []Column{{Name: "id", DataType: "integer"}}},
	}}
	newS := &Schema{Tables: []Table{
		{Name: "users", Columns: []Column{{Name: "id", DataType: "integer"}}},
		{Name: "orders", Columns: []Column{{Name: "id", DataType: "bigint"}}},
	}}

	diff := getSchemaDiff(oldS, newS)

	require.NotNil(t, diff)
	require.Len(t, diff.CreatedTables, 1)
	assert.Equal(t, "orders", diff.CreatedTables[0].Name)
}

func TestGetSchemaDiff_DeletedTable(t *testing.T) {
	t.Parallel()
	oldS := &Schema{Tables: []Table{
		{Name: "users", Columns: []Column{{Name: "id", DataType: "integer"}}},
		{Name: "orders", Columns: []Column{{Name: "id", DataType: "bigint"}}},
	}}
	newS := &Schema{Tables: []Table{
		{Name: "users", Columns: []Column{{Name: "id", DataType: "integer"}}},
	}}

	diff := getSchemaDiff(oldS, newS)

	require.NotNil(t, diff)
	require.Len(t, diff.DeletedTables, 1)
	assert.Equal(t, "orders", diff.DeletedTables[0].Name)
}

func TestGetSchemaDiff_UpdatedTable(t *testing.T) {
	t.Parallel()
	oldS := &Schema{Tables: []Table{
		{Name: "users", Columns: []Column{{Name: "id", DataType: "integer"}}},
	}}
	newS := &Schema{Tables: []Table{
		{Name: "users", Columns: []Column{{Name: "id", DataType: "bigint"}}},
	}}

	diff := getSchemaDiff(oldS, newS)

	require.NotNil(t, diff)
	require.Len(t, diff.UpdatedTables, 1)
	require.Len(t, diff.UpdatedTables[0].UpdatedColumns, 1)
	assert.Equal(t, "integer", diff.UpdatedTables[0].UpdatedColumns[0].Old.DataType)
	assert.Equal(t, "bigint", diff.UpdatedTables[0].UpdatedColumns[0].New.DataType)
}
