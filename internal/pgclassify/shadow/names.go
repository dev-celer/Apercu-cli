package shadow

import (
	"slices"
	"strconv"
	"unicode/utf8"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgclassify/stmt"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// blindCommands run DDL the statement does not spell, so a constraint can take a name nothing here sees.
var blindCommands = map[string]bool{"DO": true, "CALL": true, "CREATE EXTENSION": true}

// heldConstraint is one constraint name a table holds.
type heldConstraint struct {
	name string
	// kind is pg_constraint.contype: c f n p u x.
	kind string
	// columns are the columns of the table it is built on: the server drops it with any of them.
	columns []string
	// references and referenced are the other side of a FOREIGN KEY.
	references pgcatalog.RelationKey
	referenced []string
	noInherit  bool
	// of is the parent constraint this partition's copy was generated for, when the copy has a name of its own.
	of string
}

func (h heldConstraint) backedByIndex() bool {
	return h.kind == "p" || h.kind == "u" || h.kind == "x"
}

func (m *Checks) hold(key pgcatalog.RelationKey, held heldConstraint) {
	if m.names[key] == nil {
		m.names[key] = map[string]heldConstraint{}
	}
	m.names[key][held.name] = held
}

// recordCreate claims the names of every constraint a CREATE TABLE brings with it.
func (m *Checks) recordCreate(s Scope) {
	if len(s.Statement.Relations) == 0 || s.Relation().Exists() || s.Statement.Flags.Persistence == ir.PersistenceTemporary {
		return
	}
	table := s.Resolve(s.Statement.Relations[0])
	if !table.Exists() {
		return
	}
	for _, sub := range s.Statement.Subcommands {
		switch sub.Kind {
		case ir.SubAddColumn:
			m.claimColumn(s, table, sub.Column)
		case ir.SubAddConstraint:
			m.claim(s, table, sub.Constraint, "")
		case ir.SubLike:
			if len(sub.Relations) > 0 {
				m.like(s, table, s.Resolve(sub.Relations[0]), sub.Flags)
			}
		case ir.SubAttachPartition, ir.SubAddInherit:
			if len(sub.Relations) > 0 {
				m.inherit(s, s.Resolve(sub.Relations[0]), table, sub.Kind == ir.SubAttachPartition)
			}
		}
	}
}

// claimColumn claims the names of a column definition's inline constraints.
func (m *Checks) claimColumn(s Scope, table pgcatalog.RelationInfo, column *ir.ColumnDef) {
	if column == nil {
		return
	}
	for i := range column.Constraints {
		m.claim(s, table, &column.Constraints[i], column.Name)
	}
	if column.Generated == ir.GeneratedIdentityAlways || column.Generated == ir.GeneratedIdentityByDefault || isSerial(column.Type) {
		m.claimNotNull(s, table, column.Name, "", false)
	}
}

func isSerial(t ir.TypeRef) bool {
	switch t.Base() {
	case "serial", "serial2", "serial4", "serial8", "smallserial", "bigserial":
		return true
	}
	return false
}

// claim records the name of a constraint the statement adds and returns it: the one written, or the one the server
// generates. column is the column an inline constraint sits on, empty for a table constraint.
func (m *Checks) claim(s Scope, table pgcatalog.RelationInfo, def *ir.ConstraintDef, column string) string {
	if def == nil || m.merged[def] {
		return ""
	}
	columns := def.Columns
	if len(columns) == 0 && column != "" {
		columns = []string{column}
	}

	held := heldConstraint{name: def.Name, columns: columns, noInherit: def.NoInherit}
	var label, addition string
	switch def.Type {
	case ir.ConstraintCheck:
		held.kind, label = "c", "check"
		held.columns = stmt.ColumnsOf(def.Expr)
		if len(held.columns) == 1 {
			addition = held.columns[0]
		}
	case ir.ConstraintForeignKey:
		held.kind, label = "f", "fkey"
		addition = nameAddition(columns)
		target := s.Resolve(def.References)
		held.references, held.referenced = target.Key(), def.ReferencedColumns
		if len(held.referenced) == 0 {
			held.referenced = m.primaryKeyOf(s, target)
		}
	case ir.ConstraintPrimaryKey:
		held.kind, label = "p", "pkey"
	case ir.ConstraintUnique:
		held.kind, label = "u", "key"
		addition = nameAddition(append(slices.Clone(columns), def.Including...))
	case ir.ConstraintExclusion:
		held.kind, label = "x", "excl"
		addition = nameAddition(distinctColumnNames(def.KeyNames))
	case ir.ConstraintNotNull:
		name := ""
		for _, target := range columns {
			name = m.claimNotNull(s, table, target, def.Name, def.NoInherit)
		}
		return name
	default:
		return ""
	}

	if held.name == "" {
		held.name = m.mergedNames[def]
	}
	if held.name == "" {
		held.name = def.UsingIndex
	}
	if held.name == "" {
		held.name = m.choose(s, table.Name, addition, label, held.backedByIndex())
	}
	m.hold(table.Key(), held)
	m.clone(s, table, held, addition, label)
	if held.kind == "f" {
		m.cloneReferenced(s, table, held, addition)
	}

	if held.kind == "p" {
		// From 18 on a primary key's columns carry a NOT NULL constraint of their own.
		for _, target := range columns {
			m.claimNotNull(s, table, target, "", false)
		}
	}
	return held.name
}

// mergeRedundantIndexes finds the PRIMARY KEY and UNIQUE clauses of one statement the server folds into an identical
// one, the primary key first, which also hands the kept clause a name it lacks.
func (m *Checks) mergeRedundantIndexes(subs []ir.Subcommand) {
	type clause struct {
		def     *ir.ConstraintDef
		columns []string
	}
	var keys, uniques []clause
	add := func(def *ir.ConstraintDef, column string) {
		columns := def.Columns
		if len(columns) == 0 && column != "" {
			columns = []string{column}
		}
		switch {
		case def.UsingIndex != "":
		case def.Type == ir.ConstraintPrimaryKey:
			keys = append(keys, clause{def, columns})
		case def.Type == ir.ConstraintUnique:
			uniques = append(uniques, clause{def, columns})
		}
	}
	for _, sub := range subs {
		if sub.Constraint != nil {
			add(sub.Constraint, "")
		}
		if sub.Column != nil {
			for i := range sub.Column.Constraints {
				add(&sub.Column.Constraints[i], sub.Column.Name)
			}
		}
	}

	m.merged, m.mergedNames = map[*ir.ConstraintDef]bool{}, map[*ir.ConstraintDef]string{}
	var kept []clause
	for _, candidate := range append(keys, uniques...) {
		prior := slices.IndexFunc(kept, func(k clause) bool {
			return slices.Equal(k.columns, candidate.columns) && slices.Equal(k.def.Including, candidate.def.Including) &&
				k.def.Deferrable == candidate.def.Deferrable && k.def.InitiallyDef == candidate.def.InitiallyDef
		})
		if prior < 0 {
			kept = append(kept, candidate)
			continue
		}
		m.merged[candidate.def] = true
		if kept[prior].def.Name == "" && m.mergedNames[kept[prior].def] == "" {
			m.mergedNames[kept[prior].def] = candidate.def.Name
		}
	}
}

// claimNotNull records the NOT NULL constraint PG 18 keeps in pg_constraint, unless the column already carries one.
func (m *Checks) claimNotNull(s Scope, table pgcatalog.RelationInfo, column, name string, noInherit bool) string {
	if !s.AtLeast18() || m.holdsNotNull(s, table, column) {
		return ""
	}
	if name == "" {
		name = m.choose(s, table.Name, column, "not_null", false)
	}
	held := heldConstraint{name: name, kind: "n", columns: []string{column}, noInherit: noInherit}
	m.hold(table.Key(), held)
	m.clone(s, table, held, column, "not_null")
	return name
}

func (m *Checks) holdsNotNull(s Scope, table pgcatalog.RelationInfo, column string) bool {
	for _, held := range m.heldOn(s, table) {
		if held.kind == "n" && slices.Contains(held.columns, column) {
			return true
		}
	}
	return false
}

// clone gives the tables below one the copies of a constraint it just received. CHECK and NOT NULL go to every child
// under the parent's name, the others to partitions only: a FOREIGN KEY under the same name, and an index-backed
// constraint under one generated from the partition's own name, like the index behind it.
func (m *Checks) clone(s Scope, table pgcatalog.RelationInfo, held heldConstraint, addition, label string) {
	var below []pgcatalog.RelationKey
	switch held.kind {
	case "c", "n":
		if held.noInherit || s.Only() {
			return
		}
		below = s.Catalog.Descendants(table.Key())
	default:
		below = s.Catalog.PartitionDescendants(table.Key())
	}
	for _, key := range below {
		child, ok := s.Catalog.RelationByKey(key)
		if !ok {
			continue
		}
		m.copyTo(s, child, held, addition, label)
	}
}

// cloneReferenced records the extra row the server keeps on the referencing table for each partition a FOREIGN KEY
// points at. Each has a name of its own, and PG 18 changed how it is built.
func (m *Checks) cloneReferenced(s Scope, table pgcatalog.RelationInfo, held heldConstraint, addition string) {
	for _, key := range s.Catalog.PartitionDescendants(held.references) {
		copied := held
		copied.of, copied.references = held.name, key
		if s.AtLeast18() {
			copied.name = m.numbered(s, table.Name.Schema, held.name)
		} else {
			copied.name = m.choose(s, table.Name, addition, "fkey", false)
		}
		m.hold(table.Key(), copied)
	}
}

// numbered is <name>_<n> with the first number no constraint of the schema holds.
func (m *Checks) numbered(s Scope, schema, name string) string {
	taken := m.NamesInSchema(s, schema)
	for pass := 1; ; pass++ {
		if candidate := objectName(name, "", strconv.Itoa(pass)); !taken[candidate] {
			return candidate
		}
	}
}

// copyTo gives one table its copy of a parent's constraint.
func (m *Checks) copyTo(s Scope, child pgcatalog.RelationInfo, held heldConstraint, addition, label string) {
	if held.kind == "n" && m.holdsNotNull(s, child, held.columns[0]) {
		return
	}
	copied := held
	if held.backedByIndex() {
		copied.of = held.name
		copied.name = m.choose(s, child.Name, addition, label, true)
	}
	m.hold(child.Key(), copied)
}

// inherit gives a new child the copies of what its parent holds, for CREATE TABLE … PARTITION OF, INHERITS and ATTACH.
func (m *Checks) inherit(s Scope, parent, child pgcatalog.RelationInfo, partition bool) {
	if !parent.Exists() || !child.Exists() {
		return
	}
	targets := []pgcatalog.RelationInfo{child}
	for _, key := range s.Catalog.PartitionDescendants(child.Key()) {
		if info, ok := s.Catalog.RelationByKey(key); ok {
			targets = append(targets, info)
		}
	}
	for _, held := range m.heldOn(s, parent) {
		inherited := held.kind == "c" || held.kind == "n"
		if held.of != "" || (inherited && held.noInherit) || (!inherited && !partition) {
			continue
		}
		label, addition := nameParts(held)
		for _, target := range targets {
			if !m.holdsEquivalent(s, target, held) {
				m.copyTo(s, target, held, addition, label)
			}
		}
	}
}

// holdsEquivalent reports whether a table already has what a parent's constraint would give it, which is what an
// ATTACH adopts instead of creating another.
func (m *Checks) holdsEquivalent(s Scope, table pgcatalog.RelationInfo, parent heldConstraint) bool {
	// Before 17 the server matched on the index alone, so a UNIQUE could stand in for a PRIMARY KEY and the other way round.
	matches := func(kind string) bool {
		if kind == parent.kind {
			return true
		}
		keys := func(k string) bool { return k == "p" || k == "u" }
		return s.Version != pgcontract.VersionUnknown && s.Version < pgcontract.Version17 && keys(kind) && keys(parent.kind)
	}
	for _, held := range m.heldOn(s, table) {
		if !matches(held.kind) {
			continue
		}
		if held.name == parent.name || (held.kind != "c" && slices.Equal(held.columns, parent.columns)) {
			return true
		}
	}
	return false
}

// like copies what CREATE TABLE … LIKE takes from its source: CHECKs keep their names, index-backed constraints get
// new ones from the new table, and PG 18 carries the NOT NULL constraints whatever the options.
func (m *Checks) like(s Scope, table, source pgcatalog.RelationInfo, flags ir.Flags) {
	if !source.Exists() {
		return
	}
	for _, held := range m.heldOn(s, source) {
		label, addition := nameParts(held)
		switch {
		case held.kind == "c" && flags.LikeConstraints:
			held.references = pgcatalog.RelationKey{}
			m.hold(table.Key(), held)
		case held.backedByIndex() && flags.LikeIndexes:
			held.name = m.choose(s, table.Name, addition, label, true)
			m.hold(table.Key(), held)
		case held.kind == "n":
			m.hold(table.Key(), held)
		}
	}
}

// nameParts is what the server builds the generated name of a constraint of this shape from.
func nameParts(held heldConstraint) (label, addition string) {
	switch held.kind {
	case "p":
		return "pkey", ""
	case "u":
		return "key", nameAddition(held.columns)
	case "x":
		return "excl", nameAddition(held.columns)
	case "f":
		return "fkey", nameAddition(held.columns)
	case "n":
		return "not_null", nameAddition(held.columns)
	}
	return "check", ""
}

// heldOn lists the constraints a table holds at this point of the migration.
func (m *Checks) heldOn(s Scope, table pgcatalog.RelationInfo) []heldConstraint {
	key := table.Key()
	var out []heldConstraint
	for _, held := range m.names[key] {
		out = append(out, held)
	}
	for _, constraint := range s.Catalog.ConstraintsOf(table.Relation.OID) {
		if _, replaced := m.names[key][constraint.Name]; !replaced && !m.dropped[key][constraint.Name] {
			out = append(out, snapshotHeld(s, table, constraint))
		}
	}
	return out
}

func snapshotHeld(s Scope, table pgcatalog.RelationInfo, constraint snapshot.Constraint) heldConstraint {
	held := heldConstraint{name: constraint.Name, kind: constraint.Type, noInherit: constraint.NoInherit}
	for _, column := range s.Catalog.Columns(table.Relation.OID) {
		if slices.Contains(constraint.Key, column.Num) {
			held.columns = append(held.columns, column.Name)
		}
	}
	if constraint.Type == "f" {
		held.references = pgcatalog.SnapshotKey(constraint.ForeignRelID)
		for _, column := range s.Catalog.Columns(constraint.ForeignRelID) {
			if slices.Contains(constraint.ForeignKey, column.Num) {
				held.referenced = append(held.referenced, column.Name)
			}
		}
	}
	return held
}

func (m *Checks) primaryKeyOf(s Scope, table pgcatalog.RelationInfo) []string {
	for _, held := range m.heldOn(s, table) {
		if held.kind == "p" {
			return held.columns
		}
	}
	return nil
}

// choose is the server's ChooseConstraintName, or ChooseRelationName for a constraint backed by an index, whose name
// also has to be free among the schema's relations.
func (m *Checks) choose(s Scope, table helper.FullRelationName, addition, label string, relation bool) string {
	taken := m.NamesInSchema(s, table.Schema)
	free := func(name string) bool {
		if taken[name] {
			return false
		}
		return !relation || !s.Catalog.Holds(helper.FullRelationName{Schema: table.Schema, Table: name})
	}

	name := objectName(table.Table, addition, label)
	for pass := 1; !free(name); pass++ {
		name = objectName(table.Table, addition, label+strconv.Itoa(pass))
	}
	return name
}

// nameAddition is the server's ChooseIndexNameAddition: the columns joined by underscores, stopping once the name is
// certain to be cut anyway.
func nameAddition(columns []string) string {
	const maxName = 64
	out := ""
	for _, column := range columns {
		if out != "" {
			out += "_"
		}
		out += column
		if len(out) >= maxName {
			break
		}
	}
	return out
}

// distinctColumnNames is the end of ChooseIndexColumnNames: a repeated column name gets a number.
func distinctColumnNames(names []string) []string {
	out := make([]string, 0, len(names))
	for _, name := range names {
		candidate := name
		for pass := 1; slices.Contains(out, candidate); pass++ {
			candidate = name + strconv.Itoa(pass)
		}
		out = append(out, candidate)
	}
	return out
}

// NamesInSchema lists the constraint names a schema holds at this point of the migration: the snapshot's that were
// not dropped or renamed away, and the ones the migration created.
func (m *Checks) NamesInSchema(s Scope, schema string) map[string]bool {
	names := map[string]bool{}
	for _, constraint := range s.Catalog.ConstraintsInSchema(schema) {
		if !m.dropped[pgcatalog.SnapshotKey(constraint.RelID)][constraint.Name] {
			names[constraint.Name] = true
		}
	}
	for key, held := range m.names {
		if info, ok := s.Catalog.RelationByKey(key); ok && info.Name.Schema == schema {
			for name := range held {
				names[name] = true
			}
		}
	}
	return names
}

// dropClones frees the names of the copies of a dropped constraint that do not share its name: an index-backed one's
// on the partitions, and the rows a FOREIGN KEY keeps per referenced partition.
func (m *Checks) dropClones(s Scope, table pgcatalog.RelationInfo, name string) {
	parents := map[snapshot.OID]bool{}
	if constraint, ok := s.Catalog.ConstraintByName(table.Relation.OID, name); ok {
		parents[constraint.OID] = true
	}
	for _, child := range s.reached(table) {
		key := child.Key()
		for held, copied := range m.names[key] {
			if copied.of == name {
				delete(m.names[key], held)
			}
		}
		for _, constraint := range s.Catalog.ConstraintsOf(child.Relation.OID) {
			if parents[constraint.ParentID] {
				parents[constraint.OID] = true
				m.markDropped(key, constraint.Name)
			}
		}
	}
}

// renameIndex follows a rename of an index that backs a constraint: the constraint takes the index's new name.
func (m *Checks) renameIndex(s Scope, to string) {
	if len(s.Statement.Relations) == 0 {
		return
	}
	index := s.Relation()
	if index.Origin == pgcatalog.OriginExisting {
		table, ok := s.Catalog.TableOfIndex(index.Relation.OID)
		if !ok {
			return
		}
		info, ok := s.Catalog.RelationByKey(pgcatalog.SnapshotKey(table.OID))
		if !ok {
			return
		}
		for _, constraint := range s.Catalog.ConstraintsOf(table.OID) {
			if constraint.IndexID == index.Relation.OID && !m.dropped[info.Key()][constraint.Name] {
				m.renameConstraint(s, info, constraint.Name, to)
			}
		}
		return
	}

	// An index the migration's own constraint built is not a relation of the shadow catalog, its name is only held here.
	named := s.Statement.Relations[0].Name
	for key, held := range m.names {
		found, ok := held[named.Table]
		if !ok || !found.backedByIndex() {
			continue
		}
		info, ok := s.Catalog.RelationByKey(key)
		if !ok || (named.Schema != "" && named.Schema != info.Name.Schema) || (named.Schema == "" && !slices.Contains(s.Context.SearchPath, info.Name.Schema)) {
			continue
		}
		m.renameConstraint(s, info, named.Table, to)
		return
	}
}

// objectName is the server's makeObjectName: name1_name2_label, the longer of name1 and name2 shortened one byte
// at a time until the whole fits in an identifier.
func objectName(name1, name2, label string) string {
	const maxIdentifier = 63
	overhead := len(label) + 1
	if name2 != "" {
		overhead++
	}
	n1, n2 := len(name1), len(name2)
	for n1+n2 > maxIdentifier-overhead {
		if n1 > n2 {
			n1--
		} else {
			n2--
		}
	}
	out := clip(name1, n1)
	if name2 != "" {
		out += "_" + clip(name2, n2)
	}
	return out + "_" + label
}

// clip cuts a name to at most n bytes without splitting a character, as pg_mbcliplen does.
func clip(name string, n int) string {
	for n > 0 && n < len(name) && !utf8.RuneStart(name[n]) {
		n--
	}
	return name[:n]
}
