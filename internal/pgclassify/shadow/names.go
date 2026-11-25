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

// held is one constraint a table holds, under the name it goes by.
type held = pgcatalog.ConstraintInfo

func (m *Checks) hold(key pgcatalog.RelationKey, constraint held) {
	m.recorder.SetConstraint(key, constraint.Name, constraint.ConstraintState)
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
		m.claimNotNull(s, table, column.Name, "", false, true, true)
	}
}

func isSerial(t ir.TypeRef) bool {
	switch t.Base() {
	case "serial", "serial2", "serial4", "serial8", "smallserial", "bigserial":
		return true
	}
	return false
}

// claim records a constraint the statement adds and returns its name: the one written, or the one the server
// generates. column is the column an inline constraint sits on, empty for a table constraint.
func (m *Checks) claim(s Scope, table pgcatalog.RelationInfo, def *ir.ConstraintDef, column string) string {
	if def == nil || m.merged[def] {
		return ""
	}
	columns := def.Columns
	if len(columns) == 0 && column != "" {
		columns = []string{column}
	}

	added := held{Name: def.Name, ConstraintState: pgcatalog.ConstraintState{
		Columns:   columns,
		NoInherit: def.NoInherit,
		Validated: !def.NotValid,
		Enforced:  !def.NotEnforced,
	}}
	var label, addition string
	switch def.Type {
	case ir.ConstraintCheck:
		added.Kind, label = "c", "check"
		added.Expr = pgcatalog.Unqualified(def.Expr)
		added.Columns = nil
		if added.Expr != nil {
			added.Columns = stmt.ColumnsOf(added.Expr)
		}
		if len(added.Columns) == 1 {
			addition = added.Columns[0]
		}
	case ir.ConstraintForeignKey:
		added.Kind, label = "f", "fkey"
		addition = nameAddition(columns)
		target := s.Resolve(def.References)
		added.References, added.ReferencedColumns = target.Key(), def.ReferencedColumns
		if len(added.ReferencedColumns) == 0 {
			added.ReferencedColumns = m.primaryKeyOf(s, target)
		}
	case ir.ConstraintPrimaryKey:
		added.Kind, label = "p", "pkey"
	case ir.ConstraintUnique:
		added.Kind, label = "u", "key"
		addition = nameAddition(append(slices.Clone(columns), def.Including...))
	case ir.ConstraintExclusion:
		added.Kind, label = "x", "excl"
		addition = nameAddition(distinctColumnNames(def.KeyNames))
	case ir.ConstraintNotNull:
		name := ""
		for _, target := range columns {
			name = m.claimNotNull(s, table, target, def.Name, def.NoInherit, !def.NotValid, !def.NotEnforced)
		}
		return name
	default:
		return ""
	}

	if added.Name == "" {
		added.Name = m.mergedNames[def]
	}
	if added.Name == "" {
		added.Name = def.UsingIndex
	}
	if added.Name == "" {
		added.Name = m.choose(s, table.Name, addition, label, added.BackedByIndex())
		if added.Kind == "c" {
			added.GeneratedPrefix = objectName(table.Name.Table, addition, "check")
		}
	}
	m.hold(table.Key(), added)
	m.clone(s, table, added, addition, label)
	if added.Kind == "f" {
		m.cloneReferenced(s, table, added, addition)
	}

	if added.Kind == "p" {
		// From 18 on a primary key's columns carry a NOT NULL constraint of their own.
		for _, target := range columns {
			m.claimNotNull(s, table, target, "", false, true, true)
		}
	}
	return added.Name
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
// Only ADD CONSTRAINT … NOT NULL can leave it NOT VALID or NOT ENFORCED.
func (m *Checks) claimNotNull(s Scope, table pgcatalog.RelationInfo, column, name string, noInherit, validated, enforced bool) string {
	if !s.AtLeast18() || m.holdsNotNull(s, table, column) {
		return ""
	}
	if name == "" {
		name = m.choose(s, table.Name, column, "not_null", false)
	}
	added := held{Name: name, ConstraintState: pgcatalog.ConstraintState{
		Kind: "n", Columns: []string{column}, NoInherit: noInherit, Validated: validated, Enforced: enforced,
	}}
	m.hold(table.Key(), added)
	m.clone(s, table, added, column, "not_null")
	return name
}

func (m *Checks) holdsNotNull(s Scope, table pgcatalog.RelationInfo, column string) bool {
	for _, constraint := range s.State.Constraints(table.Key()) {
		if constraint.Kind == "n" && slices.Contains(constraint.Columns, column) {
			return true
		}
	}
	return false
}

// clone gives the tables below one the copies of a constraint it just received. CHECK and NOT NULL go to every child
// under the parent's name, the others to partitions only: a FOREIGN KEY under the same name, and an index-backed
// constraint under one generated from the partition's own name, like the index behind it.
func (m *Checks) clone(s Scope, table pgcatalog.RelationInfo, parent held, addition, label string) {
	var below []pgcatalog.RelationKey
	switch parent.Kind {
	case "c", "n":
		if parent.NoInherit || s.Only() {
			return
		}
		below = s.State.Descendants(table.Key())
	default:
		below = s.State.PartitionDescendants(table.Key())
	}
	for _, key := range below {
		child, ok := s.State.RelationByKey(key)
		if !ok {
			continue
		}
		m.copyTo(s, child, parent, addition, label)
	}
}

// cloneReferenced records the extra row the server keeps on the referencing table for each partition a FOREIGN KEY
// points at. Each has a name of its own, and PG 18 changed how it is built.
func (m *Checks) cloneReferenced(s Scope, table pgcatalog.RelationInfo, parent held, addition string) {
	for _, key := range s.State.PartitionDescendants(parent.References) {
		copied := parent
		copied.ParentName, copied.References = parent.Name, key
		if s.AtLeast18() {
			copied.Name = m.numbered(s, table.Name.Schema, parent.Name)
		} else {
			copied.Name = m.choose(s, table.Name, addition, "fkey", false)
		}
		m.hold(table.Key(), copied)
	}
}

// numbered is <name>_<n> with the first number no constraint of the schema holds.
func (m *Checks) numbered(s Scope, schema, name string) string {
	taken := s.State.ConstraintNamesInSchema(schema)
	for pass := 1; ; pass++ {
		if candidate := objectName(name, "", strconv.Itoa(pass)); !taken[candidate] {
			return candidate
		}
	}
}

// copyTo gives one table its copy of a parent's constraint. The copy is the migration's, whatever the parent's is.
func (m *Checks) copyTo(s Scope, child pgcatalog.RelationInfo, parent held, addition, label string) {
	if parent.Kind == "n" && m.holdsNotNull(s, child, parent.Columns[0]) {
		return
	}
	copied := parent
	copied.SnapshotOID, copied.Snapshot = 0, snapshot.Constraint{}
	if parent.BackedByIndex() {
		copied.ParentName = parent.Name
		copied.Name = m.choose(s, child.Name, addition, label, true)
	}
	m.hold(child.Key(), copied)
}

// inherit gives a new child the copies of what its parent holds, for CREATE TABLE … PARTITION OF, INHERITS and ATTACH.
func (m *Checks) inherit(s Scope, parent, child pgcatalog.RelationInfo, partition bool) {
	if !parent.Exists() || !child.Exists() {
		return
	}
	targets := []pgcatalog.RelationInfo{child}
	for _, key := range s.State.PartitionDescendants(child.Key()) {
		if info, ok := s.State.RelationByKey(key); ok {
			targets = append(targets, info)
		}
	}
	for _, constraint := range s.State.Constraints(parent.Key()) {
		inherited := constraint.Kind == "c" || constraint.Kind == "n"
		if constraint.ParentName != "" || (inherited && constraint.NoInherit) || (!inherited && !partition) {
			continue
		}
		label, addition := nameParts(constraint)
		for _, target := range targets {
			if !m.holdsEquivalent(s, target, constraint) {
				m.copyTo(s, target, constraint, addition, label)
			}
		}
	}
}

// holdsEquivalent reports whether a table already has what a parent's constraint would give it, which is what an
// ATTACH adopts instead of creating another.
func (m *Checks) holdsEquivalent(s Scope, table pgcatalog.RelationInfo, parent held) bool {
	// Before 17 the server matched on the index alone, so a UNIQUE could stand in for a PRIMARY KEY and the other way round.
	matches := func(kind string) bool {
		if kind == parent.Kind {
			return true
		}
		keys := func(k string) bool { return k == "p" || k == "u" }
		return s.Version != pgcontract.VersionUnknown && s.Version < pgcontract.Version17 && keys(kind) && keys(parent.Kind)
	}
	for _, constraint := range s.State.Constraints(table.Key()) {
		if !matches(constraint.Kind) {
			continue
		}
		if constraint.Name == parent.Name || (constraint.Kind != "c" && slices.Equal(constraint.Columns, parent.Columns)) {
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
	for _, constraint := range s.State.Constraints(source.Key()) {
		label, addition := nameParts(constraint)
		constraint.SnapshotOID, constraint.Snapshot = 0, snapshot.Constraint{}
		switch {
		case constraint.Kind == "c" && flags.LikeConstraints:
			constraint.References = pgcatalog.RelationKey{}
			m.hold(table.Key(), constraint)
		case constraint.BackedByIndex() && flags.LikeIndexes:
			constraint.Name = m.choose(s, table.Name, addition, label, true)
			m.hold(table.Key(), constraint)
		case constraint.Kind == "n":
			m.hold(table.Key(), constraint)
		}
	}
}

// nameParts is what the server builds the generated name of a constraint of this shape from.
func nameParts(constraint held) (label, addition string) {
	switch constraint.Kind {
	case "p":
		return "pkey", ""
	case "u":
		return "key", nameAddition(constraint.Columns)
	case "x":
		return "excl", nameAddition(constraint.Columns)
	case "f":
		return "fkey", nameAddition(constraint.Columns)
	case "n":
		return "not_null", nameAddition(constraint.Columns)
	}
	return "check", ""
}

func (m *Checks) primaryKeyOf(s Scope, table pgcatalog.RelationInfo) []string {
	for _, constraint := range s.State.Constraints(table.Key()) {
		if constraint.Kind == "p" {
			return constraint.Columns
		}
	}
	return nil
}

// choose is the server's ChooseConstraintName, or ChooseRelationName for a constraint backed by an index, whose name
// also has to be free among the schema's relations.
func (m *Checks) choose(s Scope, table helper.FullRelationName, addition, label string, relation bool) string {
	taken := s.State.ConstraintNamesInSchema(table.Schema)
	free := func(name string) bool {
		if taken[name] {
			return false
		}
		return !relation || !s.State.Holds(helper.FullRelationName{Schema: table.Schema, Table: name})
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

// dropClones frees the names of the copies of a dropped constraint that do not share its name: an index-backed one's
// on the partitions, and the rows a FOREIGN KEY keeps per referenced partition.
func (m *Checks) dropClones(s Scope, table pgcatalog.RelationInfo, name string) {
	parents := map[snapshot.OID]bool{}
	if constraint, ok := s.State.ConstraintByName(table.Key(), name); ok && constraint.Snapshot.OID != 0 {
		parents[constraint.Snapshot.OID] = true
	}
	for _, child := range s.reached(table) {
		key := child.Key()
		for _, constraint := range s.State.Constraints(key) {
			switch {
			case constraint.ParentName == name:
				m.recorder.DeleteConstraint(key, constraint.Name)
			case constraint.Snapshot.OID != 0 && parents[constraint.Snapshot.ParentID]:
				parents[constraint.Snapshot.OID] = true
				m.recorder.DeleteConstraint(key, constraint.Name)
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
		table, ok := s.State.TableOfIndex(index.Relation.OID)
		if !ok {
			return
		}
		info, ok := s.State.RelationByKey(pgcatalog.KeyFromOID(table.OID))
		if !ok {
			return
		}
		for _, constraint := range s.State.Constraints(info.Key()) {
			if constraint.Snapshot.OID != 0 && constraint.Snapshot.IndexID == index.Relation.OID {
				m.renameConstraint(s, info, constraint.Name, to)
			}
		}
		return
	}

	// An index the migration's own constraint built is not a relation of the shadow overlay, its name is only held
	// by the constraint.
	named := s.Statement.Relations[0].Name
	for _, holder := range s.State.ConstraintsHoldingIndexName(named.Table) {
		info, ok := s.State.RelationByKey(holder.Table)
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
