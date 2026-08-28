package pg_catalog

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// columnKey identifies one column by its relation and its name.
type columnKey struct {
	relID OID
	name  string
}

// relationIndex holds the lookups that are keyed on a relation
type relationIndex struct {
	indexes        map[OID]Index               // All Index, keyed by index OID
	indexesOfTable map[OID][]Index             // All Index for a table, keyed by table OID
	constraints    map[OID][]Constraint        // All Constraint for a relation, keyed by relation OID
	fkFrom         map[OID][]Constraint        // All foreign key Constraint starting from this table, keyed by table OID
	fkTo           map[OID][]Constraint        // All foreign key Constraint targeting this table, keyed by table OID
	sequences      map[OID]Sequence            // All Sequence, keyed by sequence OID
	children       map[OID][]InheritEdge       // All InheritEdge children for a relation, keyed by parent relation OID
	parents        map[OID][]InheritEdge       // All InheritEdge parents for a relation, keyed by children relation OID
	columns        map[OID][]Column            // All Column for a table, keyed by table OID
	columnByName   map[columnKey]Column        // All Column keyed by columnKey (table OID + column name)
	defaults       map[columnKey]ColumnDefault // All Column default keyed by columnKey (table OID + column name)
	viewDeps       map[OID][]ViewDep           // All ViewDep, keyed by the referenced relation OID
	collations     map[OID]Collation           // All Collation, keyed by Collation OID
}

func (r *relationIndex) build(snapshot *Snapshot) {
	r.indexes = make(map[OID]Index, len(snapshot.Indexes))
	r.indexesOfTable = map[OID][]Index{}
	r.constraints = map[OID][]Constraint{}
	r.fkFrom = map[OID][]Constraint{}
	r.fkTo = map[OID][]Constraint{}
	r.sequences = make(map[OID]Sequence, len(snapshot.Sequences))
	r.children = map[OID][]InheritEdge{}
	r.parents = map[OID][]InheritEdge{}
	r.columns = map[OID][]Column{}
	r.columnByName = make(map[columnKey]Column, len(snapshot.Columns))
	r.defaults = make(map[columnKey]ColumnDefault, len(snapshot.Defaults))
	r.viewDeps = map[OID][]ViewDep{}
	r.collations = make(map[OID]Collation, len(snapshot.Collations))

	for _, index := range snapshot.Indexes {
		r.indexes[index.IndexRelID] = index
		r.indexesOfTable[index.RelID] = append(r.indexesOfTable[index.RelID], index)
	}
	for _, constraint := range snapshot.Constraints {
		if constraint.RelID != 0 {
			r.constraints[constraint.RelID] = append(r.constraints[constraint.RelID], constraint)
		}
		if constraint.Type == "f" {
			r.fkFrom[constraint.RelID] = append(r.fkFrom[constraint.RelID], constraint)
			r.fkTo[constraint.ForeignRelID] = append(r.fkTo[constraint.ForeignRelID], constraint)
		}
	}
	for _, sequence := range snapshot.Sequences {
		r.sequences[sequence.SeqRelID] = sequence
	}
	for _, edge := range snapshot.Inherits {
		r.children[edge.Parent] = append(r.children[edge.Parent], edge)
		r.parents[edge.Child] = append(r.parents[edge.Child], edge)
	}
	for _, column := range snapshot.Columns {
		r.columns[column.RelID] = append(r.columns[column.RelID], column)
		r.columnByName[columnKey{column.RelID, column.Name}] = column
	}
	for _, def := range snapshot.Defaults {
		if column, ok := r.columnAt(def.RelID, def.Num); ok {
			r.defaults[columnKey{def.RelID, column.Name}] = def
		}
	}
	for _, dep := range snapshot.ViewDeps {
		r.viewDeps[dep.ReferencedRelID] = append(r.viewDeps[dep.ReferencedRelID], dep)
	}
	for _, collation := range snapshot.Collations {
		r.collations[collation.OID] = collation
	}
}

func (r *relationIndex) columnAt(relID OID, num int16) (Column, bool) {
	for _, column := range r.columns[relID] {
		if column.Num == num {
			return column, true
		}
	}
	return Column{}, false
}

// partitionDescendants return all the partition child of a partitioned table
func (r *relationIndex) partitionDescendants(parent OID) []OID {
	return r.descend(parent, func(edge InheritEdge) bool { return edge.ParentIsPartitioned })
}

func (r *relationIndex) descend(parent OID, keep func(InheritEdge) bool) []OID {
	var out []OID
	seen := map[OID]bool{parent: true}
	queue := []OID{parent}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, edge := range r.children[current] {
			if !keep(edge) || seen[edge.Child] {
				continue
			}
			seen[edge.Child] = true
			out = append(out, edge.Child)
			queue = append(queue, edge.Child)
		}
	}
	return out
}

// Index returns one index by its own relid.
func (c *Catalog) Index(indexRelID OID) (Index, bool) {
	index, ok := c.relations.indexes[indexRelID]
	return index, ok
}

// TableOfIndex return the table an index is defined on.
func (c *Catalog) TableOfIndex(indexRelID OID) (Relation, bool) {
	index, ok := c.relations.indexes[indexRelID]
	if !ok {
		return Relation{}, false
	}
	return c.ByOID(index.RelID)
}

// IndexesOf lists the indexes on a table.
func (c *Catalog) IndexesOf(relID OID) []Index {
	return c.relations.indexesOfTable[relID]
}

// InvalidIndexes lists the indexes on a table left behind by a failed CREATE INDEX CONCURRENTLY.
func (c *Catalog) InvalidIndexes(relID OID) []Index {
	var invalid []Index
	for _, index := range c.relations.indexesOfTable[relID] {
		if !index.IsValid || !index.IsReady {
			invalid = append(invalid, index)
		}
	}
	return invalid
}

// ConstraintsOf lists every constraint on a table.
func (c *Catalog) ConstraintsOf(relID OID) []Constraint {
	return c.relations.constraints[relID]
}

// ConstraintByName finds one named constraint on a table.
func (c *Catalog) ConstraintByName(relID OID, name string) (Constraint, bool) {
	for _, constraint := range c.relations.constraints[relID] {
		if constraint.Name == name {
			return constraint, true
		}
	}
	return Constraint{}, false
}

// ForeignKeysFrom lists the foreign keys starting from this table.
func (c *Catalog) ForeignKeysFrom(relID OID) []Constraint {
	return c.relations.fkFrom[relID]
}

// ForeignKeysTo lists the foreign keys targeting this table.
func (c *Catalog) ForeignKeysTo(relID OID) []Constraint {
	return c.relations.fkTo[relID]
}

// SequenceOwner return the table and column a sequence belongs to, if it isn't a standalone sequence
// return value (Table OID, Column Num, isLinkedToATable)
func (c *Catalog) SequenceOwner(seqRelID OID) (OID, int16, bool) {
	sequence, ok := c.relations.sequences[seqRelID]
	if !ok || sequence.OwnerTable == 0 {
		return 0, 0, false
	}
	return sequence.OwnerTable, sequence.OwnerAttNum, true
}

// Children lists the direct children of a relation.
func (c *Catalog) Children(relID OID) []InheritEdge {
	return c.relations.children[relID]
}

// Parents lists the direct parents of a relation.
func (c *Catalog) Parents(relID OID) []InheritEdge {
	return c.relations.parents[relID]
}

// Descendants return all children recursively from a relation.
func (c *Catalog) Descendants(relID OID) []OID {
	return c.relations.descend(relID, func(InheritEdge) bool { return true })
}

// PartitionDescendants return all the partition child of a partitioned table
func (c *Catalog) PartitionDescendants(relID OID) []OID {
	return c.relations.partitionDescendants(relID)
}

// DefaultPartition return the default partition of a partitioned table, or false if no default partition exist
func (c *Catalog) DefaultPartition(relID OID) (OID, bool) {
	for _, edge := range c.relations.children[relID] {
		if edge.IsDefaultPartition {
			return edge.Child, true
		}
	}
	return 0, false
}

// PendingDetach lists the children of a partitioned table left pending by an interrupted DETACH CONCURRENTLY.
func (c *Catalog) PendingDetach(relID OID) []OID {
	var pending []OID
	for _, edge := range c.relations.children[relID] {
		if edge.DetachPending {
			pending = append(pending, edge.Child)
		}
	}
	return pending
}

// ColumnInfo groups all information about a column
type ColumnInfo struct {
	Column
	Default    ColumnDefault
	HasDefault bool
	Collation  Collation
}

// Column resolves one column by table OID and Column name.
func (c *Catalog) Column(relID OID, name string) (ColumnInfo, bool) {
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
func (c *Catalog) Columns(relID OID) []Column {
	return c.relations.columns[relID]
}

// Collation resolves a collation from it's OID.
func (c *Catalog) Collation(oid OID) (Collation, bool) {
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
func (c *Catalog) NotNullProof(relID OID, name string) NotNullProof {
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
func constraintCovers(constraint Constraint, num int16) bool {
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
func (c *Catalog) ViewDependents(relID OID) []ViewDep {
	return c.relations.viewDeps[relID]
}

// ViewDependentsOfColumn lists the views and matviews built on a relation that read one particular column.
func (c *Catalog) ViewDependentsOfColumn(relID OID, num int16) []ViewDep {
	var deps []ViewDep
	for _, dep := range c.relations.viewDeps[relID] {
		if dep.ReferencedAttNum == num {
			deps = append(deps, dep)
		}
	}
	return deps
}

// describe is a debugging aid, not an output format.
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
