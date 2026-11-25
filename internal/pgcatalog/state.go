package pgcatalog

import (
	"slices"
	"sort"
	"strings"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse"
	"apercu-cli/internal/pgparse/ir"
)

// State is the database at this point of the migration: the read-only Catalog with the shadowOverlay on top.
// It is the entrypoint for rules rules. It only reads: the writes go through the Recorder it was built with.
type State struct {
	catalog *Catalog
	overlay *shadowOverlay

	// checks caches the snapshot CHECKs parsed back into the IR, keyed on the constraint OID.
	checks map[snapshot.OID]*ir.Expr
}

// Resolve turns a name from a statement into a relation.
//
// An unqualified name is looked up through the search path.
// searchPath is optional, if provided it will override the catalog current recorded search path
func (s *State) Resolve(name helper.FullRelationName, searchPath []string) RelationInfo {
	if name.Schema != "" {
		return s.lookup(name)
	}

	if searchPath == nil {
		searchPath = s.catalog.SearchPath()
	}
	for _, schema := range searchPath {
		if info, ok := s.find(helper.FullRelationName{Schema: schema, Table: name.Table}); ok {
			return info
		}
	}

	// Nothing on the path holds it. Report it against the first schema on the path, which is where the statement would have created or expected it.
	unqualified := name
	if len(searchPath) > 0 {
		unqualified.Schema = searchPath[0]
	}
	return s.unresolved(unqualified)
}

// lookup finds a relation from its qualified name, and counts it as unresolved when nothing holds it.
func (s *State) lookup(name helper.FullRelationName) RelationInfo {
	if info, ok := s.find(name); ok {
		return info
	}
	return s.unresolved(name)
}

// unresolved counts a name nothing holds as unresolved and returns the relation it would be.
func (s *State) unresolved(name helper.FullRelationName) RelationInfo {
	s.overlay.Unresolved[name] = struct{}{}
	return RelationInfo{Name: name, ProdBytes: -1}
}

// find lookup qualified relation name in the catalog and the shadow overlay to return it's current state.
func (s *State) find(name helper.FullRelationName) (RelationInfo, bool) {
	if created, ok := s.overlay.createdNamed(name); ok {
		return s.created(created), true
	}
	if oid, ok := s.overlay.renamedTo(name); ok {
		return s.catalog.existing(name, s.catalog.byOID[oid]), true
	}
	rel, ok := s.catalog.byName[name]
	if !ok {
		return RelationInfo{}, false
	}
	if _, dropped := s.overlay.DroppedRelations[rel.OID]; dropped {
		return RelationInfo{}, false
	}
	if current, renamed := s.overlay.RelationNames[rel.OID]; renamed && current != name {
		return RelationInfo{}, false
	}
	return s.catalog.existing(name, rel), true
}

func (s *State) created(key CreatedKey) RelationInfo {
	rel := s.overlay.CreatedRelations[key]
	return RelationInfo{
		Name:      rel.Name,
		Relation:  snapshot.Relation{Namespace: rel.Name.Schema, Name: rel.Name.Table, Kind: rel.Kind},
		Origin:    OriginCreated,
		ProdBytes: -1,
		key:       KeyFromCreatedKey(key),
	}
}

// Holds reports whether a relation of that exact name exists at this point of the migration.
func (s *State) Holds(name helper.FullRelationName) bool {
	_, ok := s.find(name)
	return ok
}

// CreatedByMigration reports whether the relation going by this name was created by the migration.
func (s *State) CreatedByMigration(name helper.FullRelationName) bool {
	_, created := s.overlay.createdNamed(name)
	return created
}

// RelationByKey resolve a relation from it's OID or CreatedKey, under the name it goes by now.
func (s *State) RelationByKey(key RelationKey) (RelationInfo, bool) {
	if oid, ok := key.OID(); ok {
		rel, found := s.catalog.byOID[oid]
		if !found {
			return RelationInfo{}, false
		}
		if _, dropped := s.overlay.DroppedRelations[oid]; dropped {
			return RelationInfo{}, false
		}
		return s.catalog.existing(s.nameOf(oid), rel), true
	}
	if created, ok := key.Created(); ok {
		if _, found := s.overlay.CreatedRelations[created]; found {
			return s.created(created), true
		}
	}
	return RelationInfo{}, false
}

// nameOf is the name a snapshot relation goes by at this point of the migration.
func (s *State) nameOf(oid snapshot.OID) helper.FullRelationName {
	if name, ok := s.overlay.RelationNames[oid]; ok {
		return name
	}
	return s.catalog.byOID[oid].RelationName()
}

// ByOID is the snapshot row of a relation the catalog handed out an OID for, whatever the migration did to it.
func (s *State) ByOID(oid snapshot.OID) (snapshot.Relation, bool) {
	rel, ok := s.catalog.byOID[oid]
	return rel, ok
}

// RelationsInSchema lists the snapshot relations of a schema that are still there, filtered by RelationKind.
// An empty schema name answers with every relation.
func (s *State) RelationsInSchema(schema string, kinds []pgcontract.RelationKind) []snapshot.Relation {
	var relations []snapshot.Relation
	for _, rel := range s.catalog.pre.Relations {
		if _, dropped := s.overlay.DroppedRelations[rel.OID]; dropped {
			continue
		}
		if schema != "" && s.nameOf(rel.OID).Schema != schema {
			continue
		}
		if !slices.Contains(kinds, pgcontract.RelationKindFromRelkind(rel.Kind)) {
			continue
		}
		relations = append(relations, rel)
	}
	return relations
}

// RelationsInTablespace lists the snapshot relations in a tablespace filtered by RelationKind and, optionally, owner.
// exact is false when the name cannot be resolved: the answer is then every relation that is not in the default one.
func (s *State) RelationsInTablespace(name, owner string, kinds []pgcontract.RelationKind) ([]snapshot.Relation, bool) {
	return s.catalog.relationsInTablespace(name, owner, kinds)
}

// ExtStatTable is the table an extended statistics object is built on.
func (s *State) ExtStatTable(schema, name string, searchPath []string) (snapshot.Relation, bool) {
	return s.catalog.extStatTable(schema, name, searchPath)
}

// Unresolved lists the distinct names nothing explained.
func (s *State) Unresolved() []helper.FullRelationName {
	names := make([]helper.FullRelationName, 0, len(s.overlay.Unresolved))
	for name := range s.overlay.Unresolved {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		if names[i].Schema != names[j].Schema {
			return names[i].Schema < names[j].Schema
		}
		return names[i].Table < names[j].Table
	})
	return names
}

// Blind reports whether DDL this file cannot read ran (DO, CALL, CREATE EXTENSION), so an object can hold a name
// nothing recorded.
func (s *State) Blind() bool { return s.overlay.Blind }

// Children lists the direct children of a relation.
func (s *State) Children(key RelationKey) []Link {
	return s.edges(s.catalog.relations.children[key], s.overlay.AddedLinks[key])
}

// Parents lists the direct parents of a relation.
func (s *State) Parents(key RelationKey) []Link {
	return s.edges(s.catalog.relations.parents[key], s.overlay.addedParents(key))
}

// edges is the snapshot's edges the migration did not break, then the ones it drew.
func (s *State) edges(snapshotEdges, added []Link) []Link {
	var out []Link
	for _, link := range snapshotEdges {
		if _, removed := s.overlay.RemovedLinks[link.Key()]; !removed {
			out = append(out, link)
		}
	}
	return append(out, added...)
}

// Descendants return all children recursively from a relation.
func (s *State) Descendants(key RelationKey) []RelationKey {
	return descend(key, s.Children, func(Link) bool { return true })
}

// PartitionDescendants return all the partition child of a partitioned table
func (s *State) PartitionDescendants(key RelationKey) []RelationKey {
	return descend(key, s.Children, func(link Link) bool { return link.ParentIsPartitioned })
}

// DefaultPartition return the default partition of a partitioned table, or false if no default partition exist
func (s *State) DefaultPartition(key RelationKey) (RelationKey, bool) {
	for _, link := range s.Children(key) {
		if link.IsDefaultPartition {
			return link.Child, true
		}
	}
	return RelationKey{}, false
}

// PendingDetach lists the children of a partitioned table left pending by an interrupted DETACH CONCURRENTLY.
func (s *State) PendingDetach(key RelationKey) []RelationKey {
	var pending []RelationKey
	for _, link := range s.Children(key) {
		if link.DetachPending {
			pending = append(pending, link.Child)
		}
	}
	return pending
}

// Column resolves one column of a table by the name it goes by now.
func (s *State) Column(key RelationKey, name string) (ColumnInfo, bool) {
	state, touched := s.overlay.Columns[key][name]
	if touched && !state.gone && state.SnapshotNum == 0 {
		return ColumnInfo{Column: snapshot.Column{
			Name:          name,
			TypeID:        state.TypeID,
			TypeMod:       state.TypeMod,
			FormattedType: s.catalog.formatType(state.TypeID, state.TypeMod),
			NotNull:       state.NotNull != nil && *state.NotNull,
			Generated:     state.Generated,
		}}, true
	}
	oid, snapshotName, ok := s.snapshotColumn(key, name)
	if !ok {
		return ColumnInfo{}, false
	}
	info, ok := s.catalog.column(oid, snapshotName)
	if !ok {
		return ColumnInfo{}, false
	}
	info.Name = name
	if touched {
		if state.TypeID != info.TypeID || state.TypeMod != info.TypeMod {
			info.TypeID, info.TypeMod = state.TypeID, state.TypeMod
			info.FormattedType = s.catalog.formatType(state.TypeID, state.TypeMod)
		}
		info.Generated = state.Generated
		if state.NotNull != nil {
			info.NotNull = *state.NotNull
		}
	}
	return info, true
}

// ColumnNames lists the columns a table has now: the snapshot's still there in attnum order, then the migration's by
// name.
func (s *State) ColumnNames(key RelationKey) []string {
	var out []string
	for _, column := range s.Columns(key) {
		out = append(out, column.Name)
	}
	var added []string
	for name, state := range s.overlay.Columns[key] {
		if !state.gone && state.SnapshotNum == 0 {
			added = append(added, name)
		}
	}
	slices.Sort(added)
	return append(out, added...)
}

// snapshotColumn is the snapshot column going by a name now: its table's OID and the name the snapshot gives it.
// false for a column the migration added or dropped.
func (s *State) snapshotColumn(key RelationKey, name string) (snapshot.OID, string, bool) {
	oid, inSnapshot := key.OID()
	if !inSnapshot {
		return 0, "", false
	}
	state, ok := s.overlay.Columns[key][name]
	if !ok {
		return oid, name, true
	}
	if state.gone || state.SnapshotNum == 0 {
		return 0, "", false
	}
	column, ok := s.catalog.relations.columnAt(oid, state.SnapshotNum)
	return oid, column.Name, ok
}

// ColumnState is the state of the column going by a name now: the overlay's, or the snapshot column's when the
// overlay holds nothing about it.
func (s *State) ColumnState(key RelationKey, name string) (ColumnState, bool) {
	if state, ok := s.overlay.Columns[key][name]; ok {
		if state.gone {
			return ColumnState{}, false
		}
		return state.clone(), true
	}
	oid, ok := key.OID()
	if !ok {
		return ColumnState{}, false
	}
	column, ok := s.catalog.relations.columnByName[columnKey{oid, name}]
	if !ok {
		return ColumnState{}, false
	}
	return ColumnState{SnapshotNum: column.Num, TypeID: column.TypeID, TypeMod: column.TypeMod, Generated: column.Generated}, true
}

// Columns lists the snapshot columns of a table that are still there, under the names they go by now.
func (s *State) Columns(key RelationKey) []snapshot.Column {
	oid, ok := key.OID()
	if !ok {
		return nil
	}
	var out []snapshot.Column
	for _, column := range s.catalog.relations.columns[oid] {
		if name, ok := s.currentColumn(key, column.Num); ok {
			column.Name = name
			out = append(out, column)
		}
	}
	return out
}

// currentColumn is the name the snapshot column of that attnum goes by now, false once it was dropped.
func (s *State) currentColumn(key RelationKey, num int16) (string, bool) {
	for name, state := range s.overlay.Columns[key] {
		if !state.gone && state.SnapshotNum == num {
			return name, true
		}
	}
	oid, _ := key.OID()
	column, ok := s.catalog.relations.columnAt(oid, num)
	if !ok {
		return "", false
	}
	// The overlay holds that name for another column, or for none: this one was renamed away or dropped.
	if _, taken := s.overlay.Columns[key][column.Name]; taken {
		return "", false
	}
	return column.Name, true
}

// CurrentColumn is the name a snapshot column, named as the snapshot names it, goes by now. A column that is gone
// keeps its snapshot name.
func (s *State) CurrentColumn(key RelationKey, snapshotName string) string {
	oid, ok := key.OID()
	if !ok {
		return snapshotName
	}
	column, ok := s.catalog.relations.columnByName[columnKey{oid, snapshotName}]
	if !ok {
		return snapshotName
	}
	if current, ok := s.currentColumn(key, column.Num); ok {
		return current
	}
	return snapshotName
}

// renamedColumns maps the snapshot name of every column the migration renamed to the name it goes by now.
func (s *State) renamedColumns(key RelationKey) map[string]string {
	oid, ok := key.OID()
	if !ok {
		return nil
	}
	names := map[string]string{}
	for name, state := range s.overlay.Columns[key] {
		if state.gone || state.SnapshotNum == 0 {
			continue
		}
		if column, ok := s.catalog.relations.columnAt(oid, state.SnapshotNum); ok && column.Name != name {
			names[column.Name] = name
		}
	}
	return names
}

// NotNullProof is why the column going by that name now is known never to be null. It reads the constraints the
// table holds now, the snapshot's the migration left standing and the migration's own.
func (s *State) NotNullProof(key RelationKey, name string) NotNullProof {
	state, touched := s.overlay.Columns[key][name]
	if touched && state.gone {
		return NotNullUnproven
	}
	if touched && state.NotNull != nil && *state.NotNull {
		return NotNullAttribute
	}

	constraints := s.Constraints(key)
	for _, constraint := range constraints {
		if constraint.Kind != "n" || !slices.Contains(constraint.Columns, name) {
			continue
		}
		if constraint.Validated && constraint.Enforced {
			return NotNullConstraint
		}
		// The column carries an unproven NOT NULL (PG 18): SET NOT NULL validates it, whatever else proves it.
		return NotNullUnproven
	}

	// attnotnull, unless the migration dropped it since.
	if !touched || state.NotNull == nil {
		if oid, snapshotName, ok := s.snapshotColumn(key, name); ok {
			if column, ok := s.catalog.relations.columnByName[columnKey{oid, snapshotName}]; ok && column.NotNull {
				return NotNullAttribute
			}
		}
	}

	for _, constraint := range constraints {
		if constraint.Kind == "c" && constraint.Validated && constraint.Enforced && provesNotNull(constraint.Expr, name) {
			return NotNullCheck
		}
	}
	return NotNullUnproven
}

// provesNotNull reports whether a CHECK rules out NULL for the column on its own: it is, or ANDs in, one of the two
// forms PostgreSQL itself recognises, col IS NOT NULL and NOT (col IS NULL). Under an OR it proves nothing.
func provesNotNull(expr *ir.Expr, column string) bool {
	if expr == nil {
		return false
	}
	isColumn := func(arg []ir.Expr) bool {
		return len(arg) == 1 && arg[0].Kind == ir.ExprColumnRef && arg[0].Literal == column
	}
	switch {
	case expr.Kind == ir.ExprBoolean && slices.Equal(expr.Name, []string{"AND"}):
		for i := range expr.Args {
			if provesNotNull(&expr.Args[i], column) {
				return true
			}
		}
	case expr.Kind == ir.ExprNullTest && slices.Equal(expr.Name, []string{"IS NOT NULL"}):
		return isColumn(expr.Args)
	case expr.Kind == ir.ExprBoolean && slices.Equal(expr.Name, []string{"NOT"}) && len(expr.Args) == 1:
		test := expr.Args[0]
		return test.Kind == ir.ExprNullTest && slices.Equal(test.Name, []string{"IS NULL"}) && isColumn(test.Args)
	}
	return false
}

// ConstraintInfo is one constraint a table holds at this point of the migration.
type ConstraintInfo struct {
	// Name is the name it goes by now.
	Name string
	ConstraintState
	// Snapshot is the snapshot row it comes from, zero for one the migration created.
	Snapshot snapshot.Constraint
}

// ConstraintByName finds the constraint of a table going by that name now.
func (s *State) ConstraintByName(key RelationKey, name string) (ConstraintInfo, bool) {
	if state, ok := s.overlay.Constraints[key][name]; ok {
		if state.gone {
			return ConstraintInfo{}, false
		}
		return s.fromOverlay(key, name, state), true
	}
	oid, ok := key.OID()
	if !ok {
		return ConstraintInfo{}, false
	}
	constraint, ok := s.catalog.constraintByName(oid, name)
	if !ok {
		return ConstraintInfo{}, false
	}
	return s.fromSnapshot(key, constraint), true
}

// Constraints lists the constraints a table holds at this point of the migration: the snapshot's the overlay holds
// nothing about, in snapshot order, then the overlay's by name.
func (s *State) Constraints(key RelationKey) []ConstraintInfo {
	var out []ConstraintInfo
	if oid, ok := key.OID(); ok {
		for _, constraint := range s.catalog.constraintsOf(oid) {
			if _, held := s.overlay.Constraints[key][constraint.Name]; !held {
				out = append(out, s.fromSnapshot(key, constraint))
			}
		}
	}
	held := s.overlay.Constraints[key]
	names := make([]string, 0, len(held))
	for name, state := range held {
		if !state.gone {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	for _, name := range names {
		out = append(out, s.fromOverlay(key, name, held[name]))
	}
	return out
}

// Reference is a FOREIGN KEY, and the table that holds it.
type Reference struct {
	Table      RelationKey
	Constraint ConstraintInfo
}

// ReferencesTo lists the foreign keys pointing at a table at this point of the migration.
func (s *State) ReferencesTo(key RelationKey) []Reference {
	var out []Reference
	for table, held := range s.overlay.Constraints {
		for name, state := range held {
			if !state.gone && state.Kind == "f" && state.References == key {
				out = append(out, Reference{Table: table, Constraint: s.fromOverlay(table, name, state)})
			}
		}
	}
	oid, ok := key.OID()
	if !ok {
		return out
	}
	for _, constraint := range s.catalog.relations.fkTo[oid] {
		table := KeyFromOID(constraint.RelID)
		if _, dropped := s.overlay.DroppedRelations[constraint.RelID]; dropped {
			continue
		}
		if _, held := s.overlay.Constraints[table][constraint.Name]; held {
			continue
		}
		out = append(out, Reference{Table: table, Constraint: s.fromSnapshot(table, constraint)})
	}
	return out
}

// ConstraintsHoldingIndexName lists the constraints the migration built whose index goes by a name, ordered by table.
// Such an index is not a relation of the overlay: only its constraint holds the name.
func (s *State) ConstraintsHoldingIndexName(name string) []Reference {
	var out []Reference
	for table, held := range s.overlay.Constraints {
		if state, ok := held[name]; ok && !state.gone && state.BackedByIndex() {
			out = append(out, Reference{Table: table, Constraint: s.fromOverlay(table, name, state)})
		}
	}
	slices.SortFunc(out, func(a, b Reference) int { return compareKeys(a.Table, b.Table) })
	return out
}

// ConstraintNamesInSchema lists the constraint names a schema holds at this point of the migration, those of its
// relations and those of its domains: the namespace the server keeps generated constraint names unique in.
func (s *State) ConstraintNamesInSchema(schema string) map[string]bool {
	names := map[string]bool{}
	for _, constraint := range s.catalog.pre.Constraints {
		if constraint.RelID == 0 {
			if domain, ok := s.catalog.Type(constraint.TypeID); ok && domain.Namespace == schema {
				names[constraint.Name] = true
			}
			continue
		}
		key := KeyFromOID(constraint.RelID)
		if state, held := s.overlay.Constraints[key][constraint.Name]; held && state.gone {
			continue
		}
		if _, dropped := s.overlay.DroppedRelations[constraint.RelID]; !dropped && s.nameOf(constraint.RelID).Schema == schema {
			names[constraint.Name] = true
		}
	}
	for key, held := range s.overlay.Constraints {
		info, ok := s.RelationByKey(key)
		if !ok || info.Name.Schema != schema {
			continue
		}
		for name, state := range held {
			if !state.gone {
				names[name] = true
			}
		}
	}
	return names
}

func (s *State) fromOverlay(key RelationKey, name string, state ConstraintState) ConstraintInfo {
	info := ConstraintInfo{Name: name, ConstraintState: state.clone()}
	if oid, ok := key.OID(); ok && state.SnapshotOID != 0 {
		info.Snapshot, _ = s.catalog.constraintByOID(oid, state.SnapshotOID)
	}
	return info
}

// fromSnapshot reads a snapshot constraint the overlay holds nothing about, its columns under the names they go by now.
func (s *State) fromSnapshot(key RelationKey, constraint snapshot.Constraint) ConstraintInfo {
	state := ConstraintState{
		SnapshotOID: constraint.OID,
		Kind:        constraint.Type,
		Columns:     s.columnNames(key, constraint.Key),
		Validated:   constraint.Validated,
		Enforced:    constraint.Enforced,
		NoInherit:   constraint.NoInherit,
	}
	if constraint.Type == "f" {
		state.References = KeyFromOID(constraint.ForeignRelID)
		state.ReferencedColumns = s.columnNames(state.References, constraint.ForeignKey)
	}
	if constraint.Type == "c" {
		if expr := s.parsedCheck(constraint); expr != nil {
			state.Expr = renameColumnsInExpression(expr, s.renamedColumns(key))
		}
	}
	return ConstraintInfo{Name: constraint.Name, ConstraintState: state, Snapshot: constraint}
}

// columnNames is the current name of each snapshot column of a key, the dropped ones left out.
func (s *State) columnNames(key RelationKey, nums []int16) []string {
	var names []string
	for _, num := range nums {
		if name, ok := s.currentColumn(key, num); ok {
			names = append(names, name)
		}
	}
	return names
}

// parsedCheck reads a snapshot CHECK's definition back into the IR, through the statement that would add it.
func (s *State) parsedCheck(constraint snapshot.Constraint) *ir.Expr {
	if expr, ok := s.checks[constraint.OID]; ok {
		return expr
	}
	var expr *ir.Expr
	statement := pgparse.ParseOne("ALTER TABLE t ADD " + constraint.Def)
	if statement.Parsed() && len(statement.Subcommands) > 0 && statement.Subcommands[0].Constraint != nil {
		expr = Unqualified(statement.Subcommands[0].Constraint.Expr)
	}
	s.checks[constraint.OID] = expr
	return expr
}

// Unqualified copies an expression with its column references stripped of the table they are qualified with. A
// CHECK can only read its own table, so the column name alone says which column it is, and it stays right whatever
// the table is renamed to.
func Unqualified(expr *ir.Expr) *ir.Expr {
	return mapColumns(expr, func(column string) string { return column })
}

func mapColumns(expr *ir.Expr, rename func(string) string) *ir.Expr {
	if expr == nil {
		return nil
	}
	out := *expr
	if out.Kind == ir.ExprColumnRef {
		parts := strings.Split(out.Literal, ".")
		out.Literal = rename(parts[len(parts)-1])
	}
	if out.Args != nil {
		out.Args = make([]ir.Expr, len(expr.Args))
		for i := range expr.Args {
			out.Args[i] = *mapColumns(&expr.Args[i], rename)
		}
	}
	return &out
}

// Indexes, sequences, triggers and views are not followed by the overlay yet: the snapshot answers.

// Index returns one index by its own relid.
func (s *State) Index(indexRelID snapshot.OID) (snapshot.Index, bool) {
	return s.catalog.index(indexRelID)
}

// TableOfIndex return the table an index is defined on.
func (s *State) TableOfIndex(indexRelID snapshot.OID) (snapshot.Relation, bool) {
	index, ok := s.catalog.index(indexRelID)
	if !ok {
		return snapshot.Relation{}, false
	}
	return s.ByOID(index.RelID)
}

// IndexesOf lists the indexes on a table.
func (s *State) IndexesOf(relID snapshot.OID) []snapshot.Index {
	return s.catalog.indexesOf(relID)
}

// InvalidIndexes lists the indexes on a table left behind by a failed CREATE INDEX CONCURRENTLY.
func (s *State) InvalidIndexes(relID snapshot.OID) []snapshot.Index {
	var invalid []snapshot.Index
	for _, index := range s.catalog.indexesOf(relID) {
		if !index.IsValid || !index.IsReady {
			invalid = append(invalid, index)
		}
	}
	return invalid
}

// ForeignKeysFrom lists the snapshot foreign keys starting from this table.
func (s *State) ForeignKeysFrom(relID snapshot.OID) []snapshot.Constraint {
	return s.catalog.relations.fkFrom[relID]
}

// ForeignKeysTo lists the snapshot foreign keys targeting this table.
func (s *State) ForeignKeysTo(relID snapshot.OID) []snapshot.Constraint {
	return s.catalog.relations.fkTo[relID]
}

// SequenceOwner return the table and column a sequence belongs to, if it isn't a standalone sequence
// return value (Table OID, Column Num, isLinkedToATable)
func (s *State) SequenceOwner(seqRelID snapshot.OID) (snapshot.OID, int16, bool) {
	sequence, ok := s.catalog.relations.sequences[seqRelID]
	if !ok || sequence.OwnerTable == 0 {
		return 0, 0, false
	}
	return sequence.OwnerTable, sequence.OwnerAttNum, true
}

// SequencesOwnedBy lists the sequences a table's identity and serial columns draw from.
func (s *State) SequencesOwnedBy(relID snapshot.OID) []snapshot.Sequence {
	return s.catalog.relations.sequencesOwned[relID]
}

// TriggerByName resolves a user trigger by its table and name.
func (s *State) TriggerByName(relID snapshot.OID, name string) (snapshot.Trigger, bool) {
	for _, trigger := range s.catalog.relations.triggers[relID] {
		if trigger.Name == name {
			return trigger, true
		}
	}
	return snapshot.Trigger{}, false
}

// TriggersOf lists the user triggers of a relation.
func (s *State) TriggersOf(relID snapshot.OID) []snapshot.Trigger {
	return s.catalog.relations.triggers[relID]
}

// ViewDependents lists the views and matviews built on a relation.
func (s *State) ViewDependents(relID snapshot.OID) []snapshot.ViewDep {
	return s.catalog.relations.viewDeps[relID]
}

// ViewSources lists the relations a view or a materialized view reads.
func (s *State) ViewSources(relID snapshot.OID) []snapshot.ViewDep {
	return s.catalog.relations.viewSources[relID]
}

// ViewDependentsOfColumn lists the views and matviews built on a relation that read one particular column.
func (s *State) ViewDependentsOfColumn(relID snapshot.OID, num int16) []snapshot.ViewDep {
	var deps []snapshot.ViewDep
	for _, dep := range s.catalog.relations.viewDeps[relID] {
		if dep.ReferencedAttNum == num {
			deps = append(deps, dep)
		}
	}
	return deps
}

// Database-wide definitions the migration does not change as far as the overlay knows: the catalog answers.

func (s *State) Version() pgcontract.Version           { return s.catalog.Version() }
func (s *State) VersionRange() pgcontract.VersionRange { return s.catalog.VersionRange() }
func (s *State) PreviewVersion() pgcontract.Version    { return s.catalog.PreviewVersion() }
func (s *State) SearchPath() []string                  { return s.catalog.SearchPath() }
func (s *State) Setting(name string) (string, bool)    { return s.catalog.Setting(name) }
func (s *State) Heat(name helper.FullRelationName) (snapshot.TableStat, bool) {
	return s.catalog.Heat(name)
}
func (s *State) HasProdActivity() bool { return s.catalog.HasProdActivity() }
func (s *State) Extension(name string) (snapshot.Extension, bool) {
	return s.catalog.Extension(name)
}
func (s *State) Type(oid snapshot.OID) (snapshot.Type, bool) { return s.catalog.Type(oid) }
func (s *State) Proc(oid snapshot.OID) (snapshot.Proc, bool) { return s.catalog.Proc(oid) }
func (s *State) TypeByName(schema, name string, searchPath []string) (snapshot.Type, bool) {
	return s.catalog.TypeByName(schema, name, searchPath)
}
func (s *State) ArrayOf(element snapshot.OID) (snapshot.Type, bool) {
	return s.catalog.ArrayOf(element)
}
func (s *State) ProcsByName(schema, name string, args int, searchPath []string) []snapshot.Proc {
	return s.catalog.ProcsByName(schema, name, args, searchPath)
}
func (s *State) OperatorsByName(schema, name string, searchPath []string) []snapshot.Operator {
	return s.catalog.OperatorsByName(schema, name, searchPath)
}
func (s *State) CastsTo(target snapshot.OID) []snapshot.Cast { return s.catalog.CastsTo(target) }
func (s *State) PostDefault(relation helper.FullRelationName, column string) (snapshot.ColumnDefault, bool) {
	return s.catalog.PostDefault(relation, column)
}
func (s *State) ProcVolatility(oid snapshot.OID) Volatility { return s.catalog.ProcVolatility(oid) }
func (s *State) OperatorVolatility(oid snapshot.OID) Volatility {
	return s.catalog.OperatorVolatility(oid)
}
func (s *State) TypeInputVolatility(typeOID snapshot.OID) Volatility {
	return s.catalog.TypeInputVolatility(typeOID)
}
func (s *State) CastVolatility(source, target snapshot.OID) Volatility {
	return s.catalog.CastVolatility(source, target)
}
func (s *State) DomainHasConstraints(typeOID snapshot.OID) bool {
	return s.catalog.DomainHasConstraints(typeOID)
}
func (s *State) BinaryCoercible(source, target snapshot.OID) bool {
	return s.catalog.BinaryCoercible(source, target)
}
func (s *State) TypeChangeRequiresRewrite(source snapshot.OID, sourceMod int32, target snapshot.OID, targetMod int32) bool {
	return s.catalog.TypeChangeRequiresRewrite(source, sourceMod, target, targetMod)
}
func (s *State) EncodeTypmod(typeOID snapshot.OID, mods []string) (int32, bool) {
	return s.catalog.EncodeTypmod(typeOID, mods)
}
func (s *State) TypeDependents(typeOID snapshot.OID) ([]ColumnRef, []snapshot.OID) {
	return s.catalog.TypeDependents(typeOID)
}
