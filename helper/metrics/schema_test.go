package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
		d.UpdatedTables = append(d.UpdatedTables, NewTableDiff("test_table"))
		assert.True(t, d.HasChanges())
	})
}

func TestTableDiff_HasChanges(t *testing.T) {
	t.Parallel()
	t.Run("empty has no changes", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff("")
		assert.False(t, d.HasChanges())
	})
	t.Run("deleted column", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff("test_table")
		d.DeletedColumns = append(d.DeletedColumns, Column{Name: "id"})
		assert.True(t, d.HasChanges())
	})
	t.Run("created column", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff("test_table")
		d.CreatedColumns = append(d.CreatedColumns, Column{Name: "id"})
		assert.True(t, d.HasChanges())
	})
	t.Run("updated column", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff("test_table")
		d.UpdatedColumns = append(d.UpdatedColumns, struct {
			Old Column
			New Column
		}{
			Old: Column{Name: "id", DataType: "integer"},
			New: Column{Name: "id", DataType: "bigint"},
		})
		assert.True(t, d.HasChanges())
	})
	t.Run("unchanged columns alone are not a change", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff("test_table")
		d.UnchangedColumns = append(d.UnchangedColumns, Column{Name: "id", DataType: "integer"})
		assert.False(t, d.HasChanges())
	})
	t.Run("created index", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff("test_table")
		d.CreatedIndexes = append(d.CreatedIndexes, Index{Name: "idx"})
		assert.True(t, d.HasChanges())
	})
	t.Run("deleted index", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff("test_table")
		d.DeletedIndexes = append(d.DeletedIndexes, Index{Name: "idx"})
		assert.True(t, d.HasChanges())
	})
	t.Run("updated index", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff("test_table")
		d.UpdatedIndexes = append(d.UpdatedIndexes, struct {
			Old Index
			New Index
		}{Old: Index{Name: "idx", Definition: "a"}, New: Index{Name: "idx", Definition: "b"}})
		assert.True(t, d.HasChanges())
	})
	t.Run("created constraint", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff("test_table")
		d.CreatedConstraints = append(d.CreatedConstraints, Constraint{Name: "c"})
		assert.True(t, d.HasChanges())
	})
	t.Run("deleted constraint", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff("test_table")
		d.DeletedConstraints = append(d.DeletedConstraints, Constraint{Name: "c"})
		assert.True(t, d.HasChanges())
	})
	t.Run("updated constraint", func(t *testing.T) {
		t.Parallel()
		d := NewTableDiff("test_table")
		d.UpdatedConstraints = append(d.UpdatedConstraints, struct {
			Old Constraint
			New Constraint
		}{Old: Constraint{Name: "c", Definition: "a"}, New: Constraint{Name: "c", Definition: "b"}})
		assert.True(t, d.HasChanges())
	})
}

func TestColumnText_Nullability(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "id\tinteger NOT NULL", Column{Name: "id", DataType: "integer", Nullable: false}.text())
	assert.Equal(t, "email\ttext", Column{Name: "email", DataType: "text", Nullable: true}.text())
}
