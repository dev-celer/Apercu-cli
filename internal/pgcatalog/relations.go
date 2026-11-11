package pgcatalog

import (
	"fmt"
	"regexp"
	"slices"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog/snapshot"
)

// RelationKey is a node of the partition and inheritance graph: a relation a snapshot holds, by
// OID, or one the migration declared, by the qualified name it was declared under. Exactly one of
// the two is set, so a declared relation never carries an OID it could be mistaken for.
type RelationKey struct {
	oid      snapshot.OID
	declared helper.FullRelationName
}

// SnapshotKey is the key of a relation the pre-migration snapshot holds.
func SnapshotKey(oid snapshot.OID) RelationKey { return RelationKey{oid: oid} }

// DeclaredKey is the key of a relation the migration declared, by its qualified name.
func DeclaredKey(name helper.FullRelationName) RelationKey { return RelationKey{declared: name} }

// OID is the snapshot OID behind the key, false for a declared relation.
func (k RelationKey) OID() (snapshot.OID, bool) { return k.oid, k.oid != 0 }

func (k RelationKey) String() string {
	if k.oid != 0 {
		return fmt.Sprintf("oid %d", k.oid)
	}
	return k.declared.String()
}

// Link is one parent-child edge of the partition and inheritance graph, between relations the
// snapshot holds or relations the migration declared.
type Link struct {
	Parent RelationKey
	Child  RelationKey
	// DetachPending marks an interrupted DETACH CONCURRENTLY.
	DetachPending      bool
	IsDefaultPartition bool
	// ParentIsPartitioned separates declarative partitioning, index trees included, from classic INHERITS.
	ParentIsPartitioned bool
}

// linkOf is a snapshot edge as a node of the graph.
func linkOf(edge snapshot.InheritEdge) Link {
	return Link{
		Parent:              SnapshotKey(edge.Parent),
		Child:               SnapshotKey(edge.Child),
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

// relationIndex holds the lookups that are keyed on a relation
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

// partitionDescendants return all the partition child of a partitioned table
func (r *relationIndex) partitionDescendants(parent RelationKey) []RelationKey {
	return r.descend(parent, func(link Link) bool { return link.ParentIsPartitioned })
}

func (r *relationIndex) descend(parent RelationKey, keep func(Link) bool) []RelationKey {
	var out []RelationKey
	seen := map[RelationKey]bool{parent: true}
	queue := []RelationKey{parent}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, link := range r.children[current] {
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

// Index returns one index by its own relid.
func (c *Catalog) Index(indexRelID snapshot.OID) (snapshot.Index, bool) {
	index, ok := c.relations.indexes[indexRelID]
	return index, ok
}

// TableOfIndex return the table an index is defined on.
func (c *Catalog) TableOfIndex(indexRelID snapshot.OID) (snapshot.Relation, bool) {
	index, ok := c.relations.indexes[indexRelID]
	if !ok {
		return snapshot.Relation{}, false
	}
	return c.ByOID(index.RelID)
}

// IndexesOf lists the indexes on a table.
func (c *Catalog) IndexesOf(relID snapshot.OID) []snapshot.Index {
	return c.relations.indexesOfTable[relID]
}

// InvalidIndexes lists the indexes on a table left behind by a failed CREATE INDEX CONCURRENTLY.
func (c *Catalog) InvalidIndexes(relID snapshot.OID) []snapshot.Index {
	var invalid []snapshot.Index
	for _, index := range c.relations.indexesOfTable[relID] {
		if !index.IsValid || !index.IsReady {
			invalid = append(invalid, index)
		}
	}
	return invalid
}

// ConstraintsInSchema lists the constraints a schema holds at this point of the migration, those of its relations and
// those of its domains: the namespace the server keeps generated constraint names unique in.
func (c *Catalog) ConstraintsInSchema(schema string) []snapshot.Constraint {
	var constraints []snapshot.Constraint
	for _, constraint := range c.pre.Constraints {
		if constraint.RelID == 0 {
			if domain, ok := c.Type(constraint.TypeID); ok && domain.Namespace == schema {
				constraints = append(constraints, constraint)
			}
			continue
		}
		if _, dropped := c.droppedOIDs[constraint.RelID]; dropped || c.nameOf(constraint.RelID).Schema != schema {
			continue
		}
		constraints = append(constraints, constraint)
	}
	return constraints
}

// ConstraintsOf lists every constraint on a table.
func (c *Catalog) ConstraintsOf(relID snapshot.OID) []snapshot.Constraint {
	return c.relations.constraints[relID]
}

// ConstraintByName finds one named constraint on a table.
func (c *Catalog) ConstraintByName(relID snapshot.OID, name string) (snapshot.Constraint, bool) {
	for _, constraint := range c.relations.constraints[relID] {
		if constraint.Name == name {
			return constraint, true
		}
	}
	return snapshot.Constraint{}, false
}

// ForeignKeysFrom lists the foreign keys starting from this table.
func (c *Catalog) ForeignKeysFrom(relID snapshot.OID) []snapshot.Constraint {
	return c.relations.fkFrom[relID]
}

// ForeignKeysTo lists the foreign keys targeting this table.
func (c *Catalog) ForeignKeysTo(relID snapshot.OID) []snapshot.Constraint {
	return c.relations.fkTo[relID]
}

// SequenceOwner return the table and column a sequence belongs to, if it isn't a standalone sequence
// return value (Table OID, Column Num, isLinkedToATable)
func (c *Catalog) SequenceOwner(seqRelID snapshot.OID) (snapshot.OID, int16, bool) {
	sequence, ok := c.relations.sequences[seqRelID]
	if !ok || sequence.OwnerTable == 0 {
		return 0, 0, false
	}
	return sequence.OwnerTable, sequence.OwnerAttNum, true
}

// SequencesOwnedBy lists the sequences a table's identity and serial columns draw from.
func (c *Catalog) SequencesOwnedBy(relID snapshot.OID) []snapshot.Sequence {
	return c.relations.sequencesOwned[relID]
}

// Children lists the direct children of a relation.
func (c *Catalog) Children(key RelationKey) []Link {
	return c.relations.children[key]
}

// Parents lists the direct parents of a relation.
func (c *Catalog) Parents(key RelationKey) []Link {
	return c.relations.parents[key]
}

// Descendants return all children recursively from a relation.
func (c *Catalog) Descendants(key RelationKey) []RelationKey {
	return c.relations.descend(key, func(Link) bool { return true })
}

// PartitionDescendants return all the partition child of a partitioned table
func (c *Catalog) PartitionDescendants(key RelationKey) []RelationKey {
	return c.relations.partitionDescendants(key)
}

// DefaultPartition return the default partition of a partitioned table, or false if no default partition exist
func (c *Catalog) DefaultPartition(key RelationKey) (RelationKey, bool) {
	for _, link := range c.relations.children[key] {
		if link.IsDefaultPartition {
			return link.Child, true
		}
	}
	return RelationKey{}, false
}

// TriggerByName resolves a user trigger by its table and name.
func (c *Catalog) TriggerByName(relID snapshot.OID, name string) (snapshot.Trigger, bool) {
	for _, trigger := range c.relations.triggers[relID] {
		if trigger.Name == name {
			return trigger, true
		}
	}
	return snapshot.Trigger{}, false
}

// TriggersOf lists the user triggers of a relation.
func (c *Catalog) TriggersOf(relID snapshot.OID) []snapshot.Trigger {
	return c.relations.triggers[relID]
}

// PendingDetach lists the children of a partitioned table left pending by an interrupted DETACH CONCURRENTLY.
func (c *Catalog) PendingDetach(key RelationKey) []RelationKey {
	var pending []RelationKey
	for _, link := range c.relations.children[key] {
		if link.DetachPending {
			pending = append(pending, link.Child)
		}
	}
	return pending
}

// ColumnInfo groups all information about a column
type ColumnInfo struct {
	snapshot.Column
	Default    snapshot.ColumnDefault
	HasDefault bool
	Collation  snapshot.Collation
}

// Column resolves one column by table OID and Column name.
func (c *Catalog) Column(relID snapshot.OID, name string) (ColumnInfo, bool) {
	column, ok := c.relations.columnByName[columnKey{relID, name}]
	if !ok {
		return ColumnInfo{}, false
	}
	info := ColumnInfo{Column: column}
	info.Default, info.HasDefault = c.relations.defaults[columnKey{relID, name}]
	info.Collation = c.relations.collations[column.Collation]
	return info, true
}

// Columns lists a relation's columns.
func (c *Catalog) Columns(relID snapshot.OID) []snapshot.Column {
	return c.relations.columns[relID]
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

// NotNullProof return the NotNullProof for this column
func (c *Catalog) NotNullProof(relID snapshot.OID, name string) NotNullProof {
	column, ok := c.relations.columnByName[columnKey{relID, name}]
	if !ok {
		return NotNullUnproven
	}

	for _, constraint := range c.relations.constraints[relID] {
		if constraint.Type != "n" || !constraintCovers(constraint, column.Num) {
			continue
		}
		if constraint.Validated && constraint.Enforced {
			return NotNullConstraint
		}
		// The column carries an unproven NOT NULL: attnotnull says nothing here.
		return NotNullUnproven
	}

	if column.NotNull {
		return NotNullAttribute
	}

	for _, constraint := range c.relations.constraints[relID] {
		if constraint.Type != "c" || !constraint.Validated || !constraint.Enforced {
			continue
		}
		if checkProvesNotNull(constraint.Def, name) {
			return NotNullCheck
		}
	}
	return NotNullUnproven
}

// constraintCovers reports whether a constraint's key includes the column.
func constraintCovers(constraint snapshot.Constraint, num int16) bool {
	return slices.Contains(constraint.Key, num)
}

// checkProvesNotNull reports whether a CHECK definition rules out NULL for the column on its own.
func checkProvesNotNull(def, column string) bool {
	if def == "" {
		return false
	}
	name := regexp.QuoteMeta(column)
	quoted := `(?:` + name + `|"` + name + `")`
	patterns := []string{
		`(?i)(?:^|[^\w"])` + quoted + `\s+is\s+not\s+null`,
		`(?i)not\s*\(\s*` + quoted + `\s+is\s+null\s*\)`,
	}
	for _, pattern := range patterns {
		if regexp.MustCompile(pattern).MatchString(def) {
			return true
		}
	}
	return false
}

// ViewDependents lists the views and matviews built on a relation.
func (c *Catalog) ViewDependents(relID snapshot.OID) []snapshot.ViewDep {
	return c.relations.viewDeps[relID]
}

// ViewSources lists the relations a view or a materialized view reads.
func (c *Catalog) ViewSources(relID snapshot.OID) []snapshot.ViewDep {
	return c.relations.viewSources[relID]
}

// ViewDependentsOfColumn lists the views and matviews built on a relation that read one particular column.
func (c *Catalog) ViewDependentsOfColumn(relID snapshot.OID, num int16) []snapshot.ViewDep {
	var deps []snapshot.ViewDep
	for _, dep := range c.relations.viewDeps[relID] {
		if dep.ReferencedAttNum == num {
			deps = append(deps, dep)
		}
	}
	return deps
}
