package pgcatalog

import (
	"cmp"
	"slices"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgparse/ir"
)

// shadowOverlay records what the migration changed in the database, on top of the read-only Catalog.
type shadowOverlay struct {
	// Relations
	CreatedRelations map[CreatedKey]CreatedRelation
	DroppedRelations map[snapshot.OID]struct{}
	// RelationNames is the name a snapshot relation goes by once the migration renamed it or moved it to another schema.
	RelationNames map[snapshot.OID]helper.FullRelationName

	// Partition / inheritance graph
	// AddedLinks are the edges, keyed by the parent.
	AddedLinks   map[RelationKey][]Link
	RemovedLinks map[LinkKey]struct{}

	// Per-table state, keyed by the current name of the column or constraint.
	Columns     map[RelationKey]map[string]ColumnState
	Constraints map[RelationKey]map[string]ConstraintState

	Indexes        map[RelationKey]IndexState
	Triggers       map[RelationKey]map[string]TriggerState
	OwnedSequences map[RelationKey]SequenceOwner

	// Blind says DDL this file cannot read ran (DO, CALL, CREATE EXTENSION), so an object can hold a name nothing recorded.
	Blind bool
	// Unresolved holds the names a lookup found neither in the overlay nor in the catalog.
	Unresolved map[helper.FullRelationName]struct{}

	// createdKey is the last CreatedKey handed out. Keys are never reused, a dropped relation's included.
	createdKey CreatedKey
}

// CreatedRelation is a relation the migration created.
type CreatedRelation struct {
	Name helper.FullRelationName
	// Kind is pg_class.relkind.
	Kind string
}

// ColumnState is a column the migration touched.
type ColumnState struct {
	// SnapshotNum is the attnum of the snapshot column going by this name, 0 for a column the migration added.
	SnapshotNum int16
	// gone marks a name no column goes by any more: dropped, or renamed away, the new name having its own entry.
	gone bool
	// TypeID and TypeMod are the column's type now, TypeID 0 for one the snapshot does not know. An entry always
	// carries them, a snapshot column's starting from State.ColumnState.
	TypeID  snapshot.OID
	TypeMod int32
	// if NotNull is nil, the lookup should be done in the catalog
	NotNull *bool
	// Generated is pg_attribute.attgenerated: '' none, 's' stored, 'v' virtual.
	Generated string
}

// ConstraintState is a constraint the migration touched.
type ConstraintState struct {
	// SnapshotOID is original OID in the catalog, 0 for one the migration created.
	SnapshotOID snapshot.OID
	// gone marks a name no constraint goes by any more: dropped, or renamed away, the new name having its own entry.
	gone bool
	// Kind is pg_constraint.contype: c f n p u x.
	Kind string
	// Columns are the columns of the table it is built on, under their current names.
	Columns []string
	// References and ReferencedColumns are the other side of a FOREIGN KEY.
	References        RelationKey
	ReferencedColumns []string
	// ParentName is the constraint this partition copy was cloned from, when the copy has a name of its own.
	ParentName                     string
	Validated, Enforced, NoInherit bool
	// Expr is a CHECK's expression, its columns under their current names and never qualified.
	Expr *ir.Expr
	// GeneratedPrefix is the name the server derives for an unnamed CHECK before any numeric suffix, which is what a
	// drop under Blind can match.
	GeneratedPrefix string
}

// BackedByIndex reports whether the constraint is enforced by an index of its own.
func (c ConstraintState) BackedByIndex() bool {
	return c.Kind == "p" || c.Kind == "u" || c.Kind == "x"
}

// IndexState is an index the migration created, keyed by its own relation key.
type IndexState struct {
	Table RelationKey
	// Columns are the columns it depends on, by their current names so ALTER TYPE and DROP COLUMN can match: its key
	// and INCLUDE columns, and those its expressions and predicate read.
	Columns []string
	// Keys are its key elements in order, the column each names under its current name, empty for an expression, and
	// Including its INCLUDE columns.
	Keys, Including []string
	// IndexNameSources are the names of the index's attributes used to generate a name, column or expressions.
	IndexNameSources []string
	Method           string
	Unique           bool
	// Comparable says Keys, Including, Method and Unique are the whole definition: no expression, no predicate. Only
	// such an index is matched against another, the way ATTACH looks for one a partition already has.
	Comparable bool
}

// NewIndexState is the index a CREATE INDEX statement builds on a table. The statement holds its index clause.
func NewIndexState(table RelationKey, statement ir.Statement) IndexState {
	sub := statement.Subcommands[0]
	method := sub.Value
	if method == "" {
		method = "btree"
	}
	return IndexState{
		Table:            table,
		Columns:          sub.IndexColumns(),
		Keys:             sub.Index.Keys,
		Including:        sub.Index.Including,
		IndexNameSources: sub.Index.ColumnNames(),
		Method:           method,
		Unique:           statement.Flags.Unique,
		Comparable:       !slices.Contains(sub.Index.Keys, "") && sub.Expr == nil,
	}
}

// Matches reports whether two indexes have the same definition, as far as both are Comparable.
func (i IndexState) Matches(other IndexState) bool {
	return i.Comparable && other.Comparable && i.Method == other.Method && i.Unique == other.Unique &&
		slices.Equal(i.Keys, other.Keys) && slices.Equal(i.Including, other.Including)
}

// TriggerState is a trigger the migration created or dropped.
type TriggerState struct{}

// SequenceOwner is the column a sequence the migration touched belongs to.
type SequenceOwner struct{}

func newShadowOverlay() *shadowOverlay {
	return &shadowOverlay{
		CreatedRelations: map[CreatedKey]CreatedRelation{},
		DroppedRelations: map[snapshot.OID]struct{}{},
		RelationNames:    map[snapshot.OID]helper.FullRelationName{},
		AddedLinks:       map[RelationKey][]Link{},
		RemovedLinks:     map[LinkKey]struct{}{},
		Columns:          map[RelationKey]map[string]ColumnState{},
		Constraints:      map[RelationKey]map[string]ConstraintState{},
		Indexes:          map[RelationKey]IndexState{},
		Triggers:         map[RelationKey]map[string]TriggerState{},
		OwnedSequences:   map[RelationKey]SequenceOwner{},
		Unresolved:       map[helper.FullRelationName]struct{}{},
	}
}

// createdNamed is the relation the migration created with that name.
func (o *shadowOverlay) createdNamed(name helper.FullRelationName) (CreatedKey, bool) {
	for key, rel := range o.CreatedRelations {
		if rel.Name == name {
			return key, true
		}
	}
	return 0, false
}

// renamedTo is the snapshot relation the migration renamed to that name.
func (o *shadowOverlay) renamedTo(name helper.FullRelationName) (snapshot.OID, bool) {
	for oid, current := range o.RelationNames {
		if _, dropped := o.DroppedRelations[oid]; current == name && !dropped {
			return oid, true
		}
	}
	return 0, false
}

// addedParents lists the edges the migration drew to a child, ordered by parent.
func (o *shadowOverlay) addedParents(child RelationKey) []Link {
	var out []Link
	for _, links := range o.AddedLinks {
		for _, link := range links {
			if link.Child == child {
				out = append(out, link)
			}
		}
	}
	slices.SortFunc(out, func(a, b Link) int { return compareKeys(a.Parent, b.Parent) })
	return out
}

// compareKeys orders relation keys, so that an answer read off a map does not depend on map order.
func compareKeys(a, b RelationKey) int {
	if a.oid != b.oid {
		return cmp.Compare(a.oid, b.oid)
	}
	return cmp.Compare(a.created, b.created)
}

// create records a relation the migration creates and hands out its key.
func (o *shadowOverlay) create(name helper.FullRelationName, kind string) RelationKey {
	o.createdKey++
	o.CreatedRelations[o.createdKey] = CreatedRelation{Name: name, Kind: kind}
	delete(o.Unresolved, name)
	return KeyFromCreatedKey(o.createdKey)
}

// drop forgets a relation the migration drops, with what it held.
func (o *shadowOverlay) drop(key RelationKey) {
	if oid, ok := key.OID(); ok {
		o.DroppedRelations[oid] = struct{}{}
	} else if created, ok := key.Created(); ok {
		delete(o.CreatedRelations, created)
	}
	delete(o.Columns, key)
	delete(o.Constraints, key)
	delete(o.Indexes, key)
}

// rename moves a relation to its new name.
func (o *shadowOverlay) rename(key RelationKey, to helper.FullRelationName) {
	delete(o.Unresolved, to)
	if oid, ok := key.OID(); ok {
		o.RelationNames[oid] = to
		return
	}
	created, ok := key.Created()
	if !ok {
		return
	}
	rel, ok := o.CreatedRelations[created]
	if !ok {
		return
	}
	rel.Name = to
	o.CreatedRelations[created] = rel
}

// link records an edge the migration draws. snapshotEdge says the snapshot already holds an edge between the two,
// which the new one replaces.
func (o *shadowOverlay) link(link Link, snapshotEdge bool) {
	o.unlink(link.Key(), snapshotEdge)
	o.AddedLinks[link.Parent] = append(o.AddedLinks[link.Parent], link)
}

// unlink records an edge the migration breaks.
func (o *shadowOverlay) unlink(edge LinkKey, snapshotEdge bool) {
	if snapshotEdge {
		o.RemovedLinks[edge] = struct{}{}
	}
	o.AddedLinks[edge.Parent] = slices.DeleteFunc(o.AddedLinks[edge.Parent], func(link Link) bool {
		return link.Key() == edge
	})
}

// setColumn records a change to a column.
func (o *shadowOverlay) setColumn(key RelationKey, name string, state ColumnState) {
	if o.Columns[key] == nil {
		o.Columns[key] = map[string]ColumnState{}
	}
	o.Columns[key][name] = state.clone()
}

// setConstraint records a change to a constraint.
func (o *shadowOverlay) setConstraint(key RelationKey, name string, state ConstraintState) {
	if o.Constraints[key] == nil {
		o.Constraints[key] = map[string]ConstraintState{}
	}
	o.Constraints[key][name] = state.clone()
}

// setIndex records the table and columns of an index the migration creates.
func (o *shadowOverlay) setIndex(key RelationKey, state IndexState) {
	o.Indexes[key] = state.clone()
}

func (o *shadowOverlay) deleteColumn(key RelationKey, name string) {
	o.setColumn(key, name, ColumnState{gone: true})
}

func (o *shadowOverlay) deleteConstraint(key RelationKey, name string) {
	o.setConstraint(key, name, ConstraintState{gone: true})
}

func (c ColumnState) clone() ColumnState {
	if c.NotNull != nil {
		notNull := *c.NotNull
		c.NotNull = &notNull
	}
	return c
}

func (c ConstraintState) clone() ConstraintState {
	c.Columns = slices.Clone(c.Columns)
	c.ReferencedColumns = slices.Clone(c.ReferencedColumns)
	return c
}

func (i IndexState) clone() IndexState {
	i.Columns = slices.Clone(i.Columns)
	i.Keys = slices.Clone(i.Keys)
	i.Including = slices.Clone(i.Including)
	i.IndexNameSources = slices.Clone(i.IndexNameSources)
	return i
}
