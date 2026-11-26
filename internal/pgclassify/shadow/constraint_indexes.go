package shadow

import (
	"slices"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgparse/ir"
)

// The index behind a PRIMARY KEY, UNIQUE or EXCLUDE is a relation of its own, under the constraint's name: the server
// builds, renames and drops the two together.

// declareConstraintIndex records the index a new PRIMARY KEY, UNIQUE or EXCLUDE clause builds, or the one it takes with USING
// INDEX, which goes by the constraint's name from then on.
func (m *Checks) declareConstraintIndex(s Scope, table pgcatalog.RelationInfo, def *ir.ConstraintDef, name string, columns []string) {
	if def.UsingIndex != "" {
		if index, ok := s.State.IndexByName(table.Key(), def.UsingIndex); ok && name != def.UsingIndex {
			m.recorder.Rename(index, helper.FullRelationName{Schema: table.Name.Schema, Table: name})
		}
		return
	}
	m.buildIndex(s, table, name, constraintIndex(def, columns))
}

// buildIndex records the index of a table's constraint, unless the table already has one under its name.
func (m *Checks) buildIndex(s Scope, table pgcatalog.RelationInfo, name string, shape pgcatalog.IndexState) {
	if _, ok := s.State.IndexByName(table.Key(), name); ok {
		return
	}
	declareIndexOn(m.recorder, table, helper.FullRelationName{Schema: table.Name.Schema, Table: name}, shape)
}

// indexShape is the index behind a table's constraint, which its copies are built like.
func (m *Checks) indexShape(s Scope, table pgcatalog.RelationInfo, constraint held) pgcatalog.IndexState {
	if !constraint.BackedByIndex() {
		return pgcatalog.IndexState{}
	}
	index, ok := indexBehind(s, table.Key(), constraint)
	if !ok {
		return pgcatalog.IndexState{Columns: constraint.Columns, Keys: constraint.Columns, IndexNameSources: constraint.Columns}
	}
	shape, _ := s.State.IndexState(index)
	return shape
}

// indexBehind is the index a table's PRIMARY KEY, UNIQUE or EXCLUDE is enforced by, which goes by its name.
func indexBehind(s Scope, table pgcatalog.RelationKey, constraint held) (pgcatalog.RelationKey, bool) {
	if !constraint.BackedByIndex() {
		return pgcatalog.RelationKey{}, false
	}
	return s.State.IndexByName(table, constraint.Name)
}

// constraintIndex is the index a PRIMARY KEY, UNIQUE or EXCLUDE clause builds over its columns.
func constraintIndex(def *ir.ConstraintDef, columns []string) pgcatalog.IndexState {
	exclusion := def.Type == ir.ConstraintExclusion
	shape := pgcatalog.IndexState{
		Keys:       slices.Clone(columns),
		Including:  slices.Clone(def.Including),
		Method:     "btree",
		Unique:     !exclusion,
		Comparable: !exclusion && !def.WithoutOverlaps,
	}
	sources := columns
	if exclusion {
		// Its elements lend their names in the order written, expressions included; the access method is not kept.
		sources, shape.Method = def.KeyNames, ""
	} else if def.WithoutOverlaps {
		shape.Method = "gist"
	}
	shape.IndexNameSources = ir.DistinctNames(append(slices.Clone(sources), def.Including...))
	shape.Columns = ir.Subcommand{Index: &ir.IndexDef{Keys: columns, Exprs: def.KeyExprs, Including: def.Including}}.IndexColumns()
	return shape
}

// linkIndexes ties the index behind each table's copy of a constraint to the one behind its partition parent's,
// which is how the copies form an index tree. names is the name of the constraint each table of the tree ended up
// with, the root's included.
func (m *Checks) linkIndexes(s Scope, targets []pgcatalog.RelationInfo, names map[pgcatalog.RelationKey]string) {
	for _, target := range targets {
		name, ok := names[target.Key()]
		if !ok {
			continue
		}
		index, ok := s.State.IndexByName(target.Key(), name)
		if !ok {
			continue
		}
		for _, link := range s.State.Parents(target.Key()) {
			parentName, ok := names[link.Parent]
			if !ok || !link.ParentIsPartitioned {
				continue
			}
			// Link replaces an edge already drawn, so a copy adopted with its index in the tree stays linked once.
			if parent, ok := s.State.IndexByName(link.Parent, parentName); ok {
				m.recorder.Link(pgcatalog.Link{Parent: parent, Child: index, ParentIsPartitioned: true})
			}
		}
	}
}

// deleteConstraint forgets a constraint the migration drops, with the index behind it.
func (m *Checks) deleteConstraint(s Scope, key pgcatalog.RelationKey, constraint held) {
	if index, ok := indexBehind(s, key, constraint); ok {
		m.recorder.Drop(index)
	}
	m.recorder.DeleteConstraint(key, constraint.Name)
}
