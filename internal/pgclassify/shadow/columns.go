package shadow

import (
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgparse/ir"
)

// addColumn records a column a CREATE TABLE or an ADD COLUMN defines. A table that already has a column of that
// name, a child merging it with the one it inherits or a partition giving its parent's options of its own, keeps
// the type it has and takes the NOT NULL the definition adds.
func (m *Checks) addColumn(s Scope, table pgcatalog.RelationInfo, def *ir.ColumnDef) {
	if def == nil || def.Name == "" {
		return
	}
	key := table.Key()
	notNull := declaresNotNull(def)
	if state, exists := s.State.ColumnState(key, def.Name); exists {
		if notNull {
			state.NotNull = &notNull
			m.recorder.SetColumn(key, def.Name, state)
		}
		return
	}
	typeID, typeMod := columnType(s, def.Type)
	m.recorder.SetColumn(key, def.Name, pgcatalog.ColumnState{
		TypeID: typeID, TypeMod: typeMod, NotNull: &notNull, Generated: attGenerated(def.Generated),
	})
}

// declaresNotNull reports whether a column definition leaves the column NOT NULL: an inline NOT NULL or PRIMARY KEY,
// an identity, a serial.
func declaresNotNull(def *ir.ColumnDef) bool {
	for _, constraint := range def.Constraints {
		if constraint.Type == ir.ConstraintNotNull || constraint.Type == ir.ConstraintPrimaryKey {
			return true
		}
	}
	return def.Generated.IsIdentity() || isSerial(def.Type)
}

// attGenerated is pg_attribute.attgenerated for how a column definition produces its value.
func attGenerated(generated ir.Generated) string {
	switch generated {
	case ir.GeneratedStored:
		return "s"
	case ir.GeneratedVirtual:
		return "v"
	}
	return ""
}

// columnType is the type a column definition names, as pg_attribute stores it. TypeID 0 for a type the snapshot does
// not know, or whose modifier cannot be packed.
func columnType(s Scope, ref ir.TypeRef) (snapshot.OID, int32) {
	typ, ok := s.TypeOf(ref)
	if !ok {
		return 0, -1
	}
	mod, ok := s.State.EncodeTypmod(typ.OID, ref.Typmods)
	if !ok {
		return 0, -1
	}
	return typ.OID, mod
}

// copyColumns gives a new table the columns of another: its parent's for PARTITION OF and INHERITS, a LIKE source's.
// A column the table already has is merged, not copied. generated keeps how a generated column is produced, which a
// LIKE does only under INCLUDING GENERATED.
func (m *Checks) copyColumns(s Scope, source, table pgcatalog.RelationInfo, generated bool) {
	if !source.Exists() {
		return
	}
	key := table.Key()
	for _, name := range s.State.ColumnNames(source.Key()) {
		if _, exists := s.State.ColumnState(key, name); exists {
			continue
		}
		column, ok := s.State.Column(source.Key(), name)
		if !ok {
			continue
		}
		notNull := column.NotNull
		state := pgcatalog.ColumnState{TypeID: column.TypeID, TypeMod: column.TypeMod, NotNull: &notNull}
		if generated {
			state.Generated = column.Generated
		}
		m.recorder.SetColumn(key, name, state)
	}
}

// inheritColumns gives a new child its parent's columns. From 18 on, a NOT NULL the parent holds as NO INHERIT stays
// on the parent.
func (m *Checks) inheritColumns(s Scope, parent, child pgcatalog.RelationInfo) {
	m.copyColumns(s, parent, child, true)
	if !s.AtLeast18() || !parent.Exists() {
		return
	}
	key := child.Key()
	for _, constraint := range s.State.Constraints(parent.Key()) {
		if constraint.Kind != "n" || !constraint.NoInherit {
			continue
		}
		for _, column := range constraint.Columns {
			if state, ok := s.State.ColumnState(key, column); ok && state.SnapshotNum == 0 {
				notNull := false
				state.NotNull = &notNull
				m.recorder.SetColumn(key, column, state)
			}
		}
	}
}

// retype records the type an ALTER COLUMN TYPE gives a column, on every table the clause reaches.
func (m *Checks) retype(s Scope, table pgcatalog.RelationInfo, def *ir.ColumnDef) {
	if def == nil {
		return
	}
	typeID, typeMod := columnType(s, def.Type)
	for _, reached := range s.reached(table) {
		state, ok := s.State.ColumnState(reached.Key(), def.Name)
		if !ok {
			continue
		}
		state.TypeID, state.TypeMod = typeID, typeMod
		m.recorder.SetColumn(reached.Key(), def.Name, state)
	}
}

// dropExpression records a generated column turned into a plain one, on every table the clause reaches.
func (m *Checks) dropExpression(s Scope, table pgcatalog.RelationInfo, column string) {
	for _, reached := range s.reached(table) {
		state, ok := s.State.ColumnState(reached.Key(), column)
		if !ok {
			continue
		}
		state.Generated = ""
		m.recorder.SetColumn(reached.Key(), column, state)
	}
}
