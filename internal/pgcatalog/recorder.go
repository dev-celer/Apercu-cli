package pgcatalog

import (
	"slices"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgparse/ir"
)

// Recorder is the only way to write to the shadow overlay. Whoever builds it holds the State it writes to; whoever
// is handed only the State can read, never write. The rules get the State, the shadow package the Recorder.
//
// The Recorder job is to keep the values in the Overlay consistent when a change occur to it.
type Recorder struct {
	state *State
}

func NewRecorder(catalog *Catalog) *Recorder {
	return &Recorder{state: &State{catalog: catalog, overlay: newShadowOverlay(), checks: map[snapshot.OID]*ir.Expr{}}}
}

func (r *Recorder) State() *State { return r.state }

// Declare records a relation the migration creates and returns its key.
// If a relation with the same name and kind already exist, it returns the key to that relation.
func (r *Recorder) Declare(name helper.FullRelationName, kind string) RelationKey {
	if info, ok := r.state.find(name); ok && info.Relation.Kind == kind {
		return info.Key()
	}
	return r.state.overlay.create(name, kind)
}

// Drop forgets a relation the migration drops, every partition and inheritance edge it had, what the overlay held
// about it, and the foreign keys pointing at it, which go with it under CASCADE.
func (r *Recorder) Drop(key RelationKey) {
	for _, link := range r.state.Children(key) {
		r.Unlink(key, link.Child)
	}
	for _, link := range r.state.Parents(key) {
		r.Unlink(link.Parent, key)
	}
	r.state.overlay.drop(key)
	for _, reference := range r.state.ReferencesTo(key) {
		r.DeleteConstraint(reference.Table, reference.Constraint.Name)
	}
}

// Rename moves a relation the migration renames or moves to another schema to its new name. Its key stays the same.
func (r *Recorder) Rename(key RelationKey, to helper.FullRelationName) {
	r.state.overlay.rename(key, to)
}

// Link records a parent-child link the migration draws: ATTACH, PARTITION OF, INHERIT.
func (r *Recorder) Link(link Link) {
	r.state.overlay.link(link, r.snapshotEdge(link.Parent, link.Child))
}

// Unlink records a parent-child link the migration breaks: DETACH, NO INHERIT.
func (r *Recorder) Unlink(parent, child RelationKey) {
	r.state.overlay.unlink(LinkKey{Parent: parent, Child: child}, r.snapshotEdge(parent, child))
}

func (r *Recorder) snapshotEdge(parent, child RelationKey) bool {
	return slices.ContainsFunc(r.state.catalog.relations.children[parent], func(link Link) bool { return link.Child == child })
}

// MarkBlind records that DDL this file cannot read ran.
func (r *Recorder) MarkBlind() {
	r.state.overlay.Blind = true
}

// SetColumn records the state of a column under the name it goes by now.
func (r *Recorder) SetColumn(key RelationKey, name string, state ColumnState) {
	r.state.overlay.setColumn(key, name, state)
}

// DeleteColumn records that no column of the table goes by this name any more: dropped, or renamed away.
func (r *Recorder) DeleteColumn(key RelationKey, name string) {
	r.state.overlay.deleteColumn(key, name)
}

// SetConstraint records the state of a constraint under the name it goes by now.
func (r *Recorder) SetConstraint(key RelationKey, name string, state ConstraintState) {
	r.state.overlay.setConstraint(key, name, state)
}

// DeleteConstraint records that no constraint of the table goes by this name any more: dropped, or renamed away.
func (r *Recorder) DeleteConstraint(key RelationKey, name string) {
	r.state.overlay.deleteConstraint(key, name)
}

// RenameConstraint moves a constraint of one table to its new name. The copies cloned from it under a name of their
// own, on its partitions and, for a FOREIGN KEY, on the table itself, are rewritten to name it under the new one.
func (r *Recorder) RenameConstraint(key RelationKey, from, to string) {
	constraint, ok := r.state.ConstraintByName(key, from)
	if !ok {
		return
	}
	overlay := r.state.overlay
	for _, holder := range append([]RelationKey{key}, r.state.Descendants(key)...) {
		for name, state := range overlay.Constraints[holder] {
			if !state.gone && state.ParentName == from {
				state.ParentName = to
				overlay.Constraints[holder][name] = state
			}
		}
	}

	// The new name is the user's, no longer one the server derived.
	constraint.GeneratedPrefix = ""
	overlay.setConstraint(key, to, constraint.ConstraintState)
	overlay.deleteConstraint(key, from)
}

// RenameColumn moves a column of one table to its new name. The overlay's constraints are rewritten to read the new
// name, the table's own and the FOREIGN KEYs pointing at it; the snapshot's are read under it by State.
func (r *Recorder) RenameColumn(key RelationKey, from, to string) {
	overlay := r.state.overlay
	names := map[string]string{from: to}
	for owner, constraints := range overlay.Constraints {
		for name, state := range constraints {
			if state.gone {
				continue
			}
			if owner == key {
				state.Columns = renameColumns(state.Columns, from, to)
				if state.Expr != nil {
					state.Expr = renameColumnsInExpression(state.Expr, names)
				}
			}
			if state.References == key {
				state.ReferencedColumns = renameColumns(state.ReferencedColumns, from, to)
			}
			constraints[name] = state
		}
	}

	column, _ := r.state.ColumnState(key, from)
	overlay.setColumn(key, to, column)
	overlay.deleteColumn(key, from)
}

// renameColumns copies a column list with one column renamed.
func renameColumns(columns []string, from, to string) []string {
	i := slices.Index(columns, from)
	if i < 0 {
		return columns
	}
	columns = slices.Clone(columns)
	columns[i] = to
	return columns
}

// renameColumnsInExpression copies an expression with its column references renamed, in one pass so that two columns can swap
// names.
func renameColumnsInExpression(expr *ir.Expr, names map[string]string) *ir.Expr {
	return mapColumns(expr, func(column string) string {
		if name, renamed := names[column]; renamed {
			return name
		}
		return column
	})
}
