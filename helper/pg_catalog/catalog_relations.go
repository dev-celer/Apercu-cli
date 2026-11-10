package pg_catalog

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"apercu-cli/helper"
)

// RelationKey is a node of the partition and inheritance graph: a relation a snapshot holds, by
// OID, or one the migration declared, by the qualified name it was declared under. Exactly one of
// the two is set, so a declared relation never carries an OID it could be mistaken for.
type RelationKey struct {
	oid      OID
	declared helper.FullRelationName
}

// SnapshotKey is the key of a relation the pre-migration snapshot holds.
func SnapshotKey(oid OID) RelationKey { return RelationKey{oid: oid} }

// DeclaredKey is the key of a relation the migration declared, by its qualified name.
func DeclaredKey(name helper.FullRelationName) RelationKey { return RelationKey{declared: name} }

// OID is the snapshot OID behind the key, false for a declared relation.
func (k RelationKey) OID() (OID, bool) { return k.oid, k.oid != 0 }

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
func linkOf(edge InheritEdge) Link {
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
	sequencesOwned map[OID][]Sequence          // All Sequence a table owns, keyed by the owning table OID
	children       map[RelationKey][]Link      // All Link to the children of a relation, keyed by the parent
	parents        map[RelationKey][]Link      // All Link to the parents of a relation, keyed by the child
	columns        map[OID][]Column            // All Column for a table, keyed by table OID
	columnByName   map[columnKey]Column        // All Column keyed by columnKey (table OID + column name)
	defaults       map[columnKey]ColumnDefault // All Column default keyed by columnKey (table OID + column name)
	viewDeps       map[OID][]ViewDep           // All ViewDep, keyed by the referenced relation OID
	viewSources    map[OID][]ViewDep           // All ViewDep, keyed by the dependent relation OID
	collations     map[OID]Collation           // All Collation, keyed by Collation OID
	triggers       map[OID][]Trigger           // All user Trigger of a relation, keyed by relation OID
}

func (r *relationIndex) build(snapshot *Snapshot) {
	r.indexes = make(map[OID]Index, len(snapshot.Indexes))
	r.indexesOfTable = map[OID][]Index{}
	r.constraints = map[OID][]Constraint{}
	r.fkFrom = map[OID][]Constraint{}
	r.fkTo = map[OID][]Constraint{}
	r.sequences = make(map[OID]Sequence, len(snapshot.Sequences))
	r.sequencesOwned = map[OID][]Sequence{}
	r.children = map[RelationKey][]Link{}
	r.parents = map[RelationKey][]Link{}
	r.columns = map[OID][]Column{}
	r.columnByName = make(map[columnKey]Column, len(snapshot.Columns))
	r.defaults = make(map[columnKey]ColumnDefault, len(snapshot.Defaults))
	r.viewDeps = map[OID][]ViewDep{}
	r.viewSources = map[OID][]ViewDep{}
	r.collations = make(map[OID]Collation, len(snapshot.Collations))
	r.triggers = map[OID][]Trigger{}

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
		if sequence.OwnerTable != 0 {
			r.sequencesOwned[sequence.OwnerTable] = append(r.sequencesOwned[sequence.OwnerTable], sequence)
		}
	}
	for _, edge := range snapshot.Inherits {
		link := linkOf(edge)
		r.children[link.Parent] = append(r.children[link.Parent], link)
		r.parents[link.Child] = append(r.parents[link.Child], link)
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
		r.viewSources[dep.DependentRelID] = append(r.viewSources[dep.DependentRelID], dep)
	}
	for _, collation := range snapshot.Collations {
		r.collations[collation.OID] = collation
	}
	for _, trigger := range snapshot.Triggers {
		r.triggers[trigger.RelID] = append(r.triggers[trigger.RelID], trigger)
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

// ConstraintsInSchema lists the constraints a schema holds at this point of the migration, those of its relations and
// those of its domains: the namespace the server keeps generated constraint names unique in.
func (c *Catalog) ConstraintsInSchema(schema string) []Constraint {
	var constraints []Constraint
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

// Holds reports whether a relation of that exact name exists at this point of the migration, without counting it as
// a name nothing explained. What only the post-migration snapshot knows has not been created yet.
func (c *Catalog) Holds(name helper.FullRelationName) bool {
	if _, renamed := c.renamed[name]; renamed {
		return true
	}
	if _, ok := c.byName[name]; ok && !c.isRetired(name) {
		return true
	}
	if _, onlyPost := c.postOnly[name]; onlyPost {
		return false
	}
	_, created := c.createdByMigration[name]
	return created
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

// SequencesOwnedBy lists the sequences a table's identity and serial columns draw from.
func (c *Catalog) SequencesOwnedBy(relID OID) []Sequence {
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

// DeclareLink records a parent-child link the migration draws: ATTACH, PARTITION OF, INHERIT.
func (c *Catalog) DeclareLink(link Link) {
	c.RemoveLink(link.Parent, link.Child)
	c.relations.children[link.Parent] = append(c.relations.children[link.Parent], link)
	c.relations.parents[link.Child] = append(c.relations.parents[link.Child], link)
}

// RemoveLink forgets a parent-child link the migration breaks: DETACH, NO INHERIT.
func (c *Catalog) RemoveLink(parent, child RelationKey) {
	c.relations.removeLink(parent, child)
}

func (r *relationIndex) removeLink(parent, child RelationKey) {
	r.children[parent] = slices.DeleteFunc(r.children[parent], func(link Link) bool { return link.Child == child })
	r.parents[child] = slices.DeleteFunc(r.parents[child], func(link Link) bool { return link.Parent == parent })
}

// forgetEdges removes every edge a relation has, to its parents and to its children.
func (r *relationIndex) forgetEdges(key RelationKey) {
	for _, link := range slices.Clone(r.children[key]) {
		r.removeLink(key, link.Child)
	}
	for _, link := range slices.Clone(r.parents[key]) {
		r.removeLink(link.Parent, key)
	}
	delete(r.children, key)
	delete(r.parents, key)
}

// rekeyEdges moves every edge of a relation to the key it goes by after a rename.
func (r *relationIndex) rekeyEdges(from, to RelationKey) {
	children, parents := slices.Clone(r.children[from]), slices.Clone(r.parents[from])
	r.forgetEdges(from)
	for _, link := range children {
		link.Parent = to
		r.children[to] = append(r.children[to], link)
		r.parents[link.Child] = append(r.parents[link.Child], link)
	}
	for _, link := range parents {
		link.Child = to
		r.children[link.Parent] = append(r.children[link.Parent], link)
		r.parents[to] = append(r.parents[to], link)
	}
}

// TriggerByName resolves a user trigger by its table and name.
func (c *Catalog) TriggerByName(relID OID, name string) (Trigger, bool) {
	for _, trigger := range c.relations.triggers[relID] {
		if trigger.Name == name {
			return trigger, true
		}
	}
	return Trigger{}, false
}

// TriggersOf lists the user triggers of a relation.
func (c *Catalog) TriggersOf(relID OID) []Trigger {
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

// PartitionKeyColumns splits a partitioned table's PartitionKey, as pg_get_partkeydef spells it, into its
// strategy and the columns it partitions on. A key element that is an expression has no column and is left out.
func (r Relation) PartitionKeyColumns() (string, []string) {
	open, closing := strings.Index(r.PartitionKey, "("), strings.LastIndex(r.PartitionKey, ")")
	if open < 0 || closing < open {
		return "", nil
	}
	strategy := strings.TrimSpace(r.PartitionKey[:open])

	var columns []string
	for _, element := range splitTopLevel(r.PartitionKey[open+1 : closing]) {
		if column, ok := leadingIdentifier(strings.TrimSpace(element)); ok {
			columns = append(columns, column)
		}
	}
	return strategy, columns
}

// splitTopLevel splits a list on the commas that sit outside parentheses and double quotes.
func splitTopLevel(list string) []string {
	var parts []string
	depth, quoted, start := 0, false, 0
	for i, char := range list {
		switch {
		case char == '"':
			quoted = !quoted
		case quoted:
		case char == '(':
			depth++
		case char == ')':
			depth--
		case char == ',' && depth == 0:
			parts = append(parts, list[start:i])
			start = i + 1
		}
	}
	return append(parts, list[start:])
}

// leadingIdentifier is the column a partition key element names, when it names one: a bare or quoted
// identifier, optionally followed by a COLLATE clause or an operator class.
func leadingIdentifier(element string) (string, bool) {
	var name, rest string
	if strings.HasPrefix(element, `"`) {
		end := 1
		for end < len(element) {
			if element[end] == '"' {
				if end+1 < len(element) && element[end+1] == '"' {
					end += 2
					continue
				}
				break
			}
			end++
		}
		if end >= len(element) {
			return "", false
		}
		name, rest = strings.ReplaceAll(element[1:end], `""`, `"`), element[end+1:]
	} else {
		match := identifierPattern.FindString(element)
		if match == "" {
			return "", false
		}
		name, rest = match, element[len(match):]
	}
	if rest != "" && !strings.HasPrefix(rest, " ") {
		// A call or an operator: the element is an expression.
		return "", false
	}
	return name, true
}

var identifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_$]*`)

// ViewDependents lists the views and matviews built on a relation.
func (c *Catalog) ViewDependents(relID OID) []ViewDep {
	return c.relations.viewDeps[relID]
}

// ViewSources lists the relations a view or a materialized view reads.
func (c *Catalog) ViewSources(relID OID) []ViewDep {
	return c.relations.viewSources[relID]
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
