package pgcatalog

import (
	"apercu-cli/helper"
	"fmt"
	"strings"

	"apercu-cli/internal/pgcatalog/snapshot"
)

// CreatedKey is a synthetic incremental OID.
type CreatedKey uint32

// RelationKey point to a relation in either the catalog or the shadow overlay.
type RelationKey struct {
	oid     snapshot.OID
	created CreatedKey
}

// KeyFromOID is the key of a relation the pre-migration snapshot holds.
func KeyFromOID(oid snapshot.OID) RelationKey { return RelationKey{oid: oid} }

// KeyFromCreatedKey is the key of a relation the migration created. Only the overlay hands them out.
func KeyFromCreatedKey(key CreatedKey) RelationKey { return RelationKey{created: key} }

func (k RelationKey) OID() (snapshot.OID, bool) { return k.oid, k.oid != 0 }

func (k RelationKey) Created() (CreatedKey, bool) { return k.created, k.created != 0 }

func (k RelationKey) String() string {
	if k.oid != 0 {
		return fmt.Sprintf("oid %d", k.oid)
	}
	return fmt.Sprintf("created #%d", k.created)
}

// RelationInfo if a wrapper for Relation that include lookup and size information.
type RelationInfo struct {
	// Name is the qualified name that was used to find this relation.
	Name     helper.FullRelationName
	Relation snapshot.Relation
	Origin   Origin
	// PreviewBytes represent the relation size in bytes, or, for partitioned parent the sum of it children size.
	// This is taken on the preview database.
	PreviewBytes int64
	// ProdBytes represent the relation size in bytes, or, for partitioned parent the sum of it children size.
	// This is taken on the production database, -1 if unavailable.
	ProdBytes int64

	key RelationKey
}

// Exists reports whether the relation is found from the snapshot or the shadow overlay.
func (r RelationInfo) Exists() bool { return r.Origin != OriginUnknown }

func (r RelationInfo) Key() RelationKey { return r.key }

// describe is a debugging aid.
func (r RelationInfo) String() string {
	if !r.Exists() {
		return fmt.Sprintf("%s (unknown)", r.Name)
	}
	size := fmt.Sprintf("%d bytes preview", r.PreviewBytes)
	if r.ProdBytes >= 0 {
		size += fmt.Sprintf(", %d bytes prod", r.ProdBytes)
	}
	return fmt.Sprintf("%s %s/%s %s", r.Name, r.Relation.Kind, strings.ToLower(r.Origin.String()), size)
}

// LinkKey is one parent-child edge.
type LinkKey struct {
	Parent RelationKey
	Child  RelationKey
}

// Link is one parent-child edge of the partition and inheritance graph.
type Link struct {
	Parent RelationKey
	Child  RelationKey
	// DetachPending marks an interrupted DETACH CONCURRENTLY.
	DetachPending      bool
	IsDefaultPartition bool
	// ParentIsPartitioned separates declarative partitioning, index trees included, from classic INHERITS.
	ParentIsPartitioned bool
}

func (l Link) Key() LinkKey { return LinkKey{Parent: l.Parent, Child: l.Child} }

// linkOf is a snapshot edge as a node of the graph.
func linkOf(edge snapshot.InheritEdge) Link {
	return Link{
		Parent:              KeyFromOID(edge.Parent),
		Child:               KeyFromOID(edge.Child),
		DetachPending:       edge.DetachPending,
		IsDefaultPartition:  edge.IsDefaultPartition,
		ParentIsPartitioned: edge.ParentIsPartitioned,
	}
}

// columnKey identifies one column by its relation and its name.
type columnKey struct {
	relID snapshot.OID
	name  string
}

// relationIndex holds the lookups that are keyed on a relation.
type relationIndex struct {
	indexes        map[snapshot.OID]snapshot.Index        // All Index, keyed by index OID
	indexesOfTable map[snapshot.OID][]snapshot.Index      // All Index for a table, keyed by table OID
	constraints    map[snapshot.OID][]snapshot.Constraint // All Constraint for a relation, keyed by relation OID
	fkFrom         map[snapshot.OID][]snapshot.Constraint // All foreign key Constraint starting from this table, keyed by table OID
	fkTo           map[snapshot.OID][]snapshot.Constraint // All foreign key Constraint targeting this table, keyed by table OID
	sequences      map[snapshot.OID]snapshot.Sequence     // All Sequence, keyed by sequence OID
	sequencesOwned map[snapshot.OID][]snapshot.Sequence   // All Sequence a table owns, keyed by the owning table OID
	children       map[RelationKey][]Link                 // All Link to the children of a relation, keyed by the parent
	parents        map[RelationKey][]Link                 // All Link to the parents of a relation, keyed by the child
	columns        map[snapshot.OID][]snapshot.Column     // All Column for a table, keyed by table OID
	columnByName   map[columnKey]snapshot.Column          // All Column keyed by columnKey (table OID + column name)
	defaults       map[columnKey]snapshot.ColumnDefault   // All Column default keyed by columnKey (table OID + column name)
	viewDeps       map[snapshot.OID][]snapshot.ViewDep    // All ViewDep, keyed by the referenced relation OID
	viewSources    map[snapshot.OID][]snapshot.ViewDep    // All ViewDep, keyed by the dependent relation OID
	collations     map[snapshot.OID]snapshot.Collation    // All Collation, keyed by Collation OID
	triggers       map[snapshot.OID][]snapshot.Trigger    // All user Trigger of a relation, keyed by relation OID
}

func (r *relationIndex) build(snap *snapshot.Snapshot) {
	r.indexes = make(map[snapshot.OID]snapshot.Index, len(snap.Indexes))
	r.indexesOfTable = map[snapshot.OID][]snapshot.Index{}
	r.constraints = map[snapshot.OID][]snapshot.Constraint{}
	r.fkFrom = map[snapshot.OID][]snapshot.Constraint{}
	r.fkTo = map[snapshot.OID][]snapshot.Constraint{}
	r.sequences = make(map[snapshot.OID]snapshot.Sequence, len(snap.Sequences))
	r.sequencesOwned = map[snapshot.OID][]snapshot.Sequence{}
	r.children = map[RelationKey][]Link{}
	r.parents = map[RelationKey][]Link{}
	r.columns = map[snapshot.OID][]snapshot.Column{}
	r.columnByName = make(map[columnKey]snapshot.Column, len(snap.Columns))
	r.defaults = make(map[columnKey]snapshot.ColumnDefault, len(snap.Defaults))
	r.viewDeps = map[snapshot.OID][]snapshot.ViewDep{}
	r.viewSources = map[snapshot.OID][]snapshot.ViewDep{}
	r.collations = make(map[snapshot.OID]snapshot.Collation, len(snap.Collations))
	r.triggers = map[snapshot.OID][]snapshot.Trigger{}

	for _, index := range snap.Indexes {
		r.indexes[index.IndexRelID] = index
		r.indexesOfTable[index.RelID] = append(r.indexesOfTable[index.RelID], index)
	}
	for _, constraint := range snap.Constraints {
		if constraint.RelID != 0 {
			r.constraints[constraint.RelID] = append(r.constraints[constraint.RelID], constraint)
		}
		if constraint.Type == "f" {
			r.fkFrom[constraint.RelID] = append(r.fkFrom[constraint.RelID], constraint)
			r.fkTo[constraint.ForeignRelID] = append(r.fkTo[constraint.ForeignRelID], constraint)
		}
	}
	for _, sequence := range snap.Sequences {
		r.sequences[sequence.SeqRelID] = sequence
		if sequence.OwnerTable != 0 {
			r.sequencesOwned[sequence.OwnerTable] = append(r.sequencesOwned[sequence.OwnerTable], sequence)
		}
	}
	for _, edge := range snap.Inherits {
		link := linkOf(edge)
		r.children[link.Parent] = append(r.children[link.Parent], link)
		r.parents[link.Child] = append(r.parents[link.Child], link)
	}
	for _, column := range snap.Columns {
		r.columns[column.RelID] = append(r.columns[column.RelID], column)
		r.columnByName[columnKey{column.RelID, column.Name}] = column
	}
	for _, def := range snap.Defaults {
		if column, ok := r.columnAt(def.RelID, def.Num); ok {
			r.defaults[columnKey{def.RelID, column.Name}] = def
		}
	}
	for _, dep := range snap.ViewDeps {
		r.viewDeps[dep.ReferencedRelID] = append(r.viewDeps[dep.ReferencedRelID], dep)
		r.viewSources[dep.DependentRelID] = append(r.viewSources[dep.DependentRelID], dep)
	}
	for _, collation := range snap.Collations {
		r.collations[collation.OID] = collation
	}
	for _, trigger := range snap.Triggers {
		r.triggers[trigger.RelID] = append(r.triggers[trigger.RelID], trigger)
	}
}

func (r *relationIndex) columnAt(relID snapshot.OID, num int16) (snapshot.Column, bool) {
	for _, column := range r.columns[relID] {
		if column.Num == num {
			return column, true
		}
	}
	return snapshot.Column{}, false
}

// partitionDescendants return all the partition child of a partitioned table.
func (r *relationIndex) partitionDescendants(parent RelationKey) []RelationKey {
	return descend(parent, func(key RelationKey) []Link { return r.children[key] }, func(link Link) bool { return link.ParentIsPartitioned })
}

// descend walks a tree with the children function provided, filter and return the list of relation encountered.
func descend(parent RelationKey, children func(RelationKey) []Link, keep func(Link) bool) []RelationKey {
	var out []RelationKey
	seen := map[RelationKey]bool{parent: true}
	queue := []RelationKey{parent}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, link := range children(current) {
			if !keep(link) || seen[link.Child] {
				continue
			}
			seen[link.Child] = true
			out = append(out, link.Child)
			queue = append(queue, link.Child)
		}
	}
	return out
}

// index returns one index by its own relid.
func (c *Catalog) index(indexRelID snapshot.OID) (snapshot.Index, bool) {
	index, ok := c.relations.indexes[indexRelID]
	return index, ok
}

// indexesOf lists the indexes on a table.
func (c *Catalog) indexesOf(relID snapshot.OID) []snapshot.Index {
	return c.relations.indexesOfTable[relID]
}

// constraintsOf lists every constraint on a table.
func (c *Catalog) constraintsOf(relID snapshot.OID) []snapshot.Constraint {
	return c.relations.constraints[relID]
}

// constraintByOID finds one constraint of a table by its OID.
func (c *Catalog) constraintByOID(relID, oid snapshot.OID) (snapshot.Constraint, bool) {
	for _, constraint := range c.relations.constraints[relID] {
		if constraint.OID == oid {
			return constraint, true
		}
	}
	return snapshot.Constraint{}, false
}

// constraintByName finds one named constraint on a table.
func (c *Catalog) constraintByName(relID snapshot.OID, name string) (snapshot.Constraint, bool) {
	for _, constraint := range c.relations.constraints[relID] {
		if constraint.Name == name {
			return constraint, true
		}
	}
	return snapshot.Constraint{}, false
}

// ColumnInfo groups all information about a column
type ColumnInfo struct {
	snapshot.Column
	Default    snapshot.ColumnDefault
	HasDefault bool
	Collation  snapshot.Collation
}

// column resolves one column by table OID and column name, as the snapshot names it.
func (c *Catalog) column(relID snapshot.OID, name string) (ColumnInfo, bool) {
	column, ok := c.relations.columnByName[columnKey{relID, name}]
	if !ok {
		return ColumnInfo{}, false
	}
	info := ColumnInfo{Column: column}
	info.Default, info.HasDefault = c.relations.defaults[columnKey{relID, name}]
	info.Collation = c.relations.collations[column.Collation]
	return info, true
}

// Collation resolves a collation from it's OID.
func (c *Catalog) Collation(oid snapshot.OID) (snapshot.Collation, bool) {
	collation, ok := c.relations.collations[oid]
	return collation, ok
}

// NotNullProof is why a column is known never to be null.
type NotNullProof uint8

const (
	// NotNullUnproven means a scan must happen to prove it is not null.
	NotNullUnproven NotNullProof = iota
	// NotNullAttribute the column is already NOT NULL.
	NotNullAttribute
	// NotNullConstraint a not null constraint exist PG18+.
	NotNullConstraint
	// NotNullCheck is a validated CHECK that already proves the column is never null.
	NotNullCheck
)

func (p NotNullProof) String() string {
	switch p {
	case NotNullAttribute:
		return "ATTNOTNULL"
	case NotNullConstraint:
		return "NOT_NULL_CONSTRAINT"
	case NotNullCheck:
		return "CHECK_CONSTRAINT"
	default:
		return "UNPROVEN"
	}
}

// Proven reports whether the column is provably never null.
func (p NotNullProof) Proven() bool { return p != NotNullUnproven }
