package pg_classify

import (
	"slices"
	"strings"
	"unicode/utf8"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// check is one CHECK constraint on a table, read from the snapshot or from the statement that added it.
type check struct {
	name string
	// generated is the name the server derives for an unnamed CHECK before any numeric suffix, empty for a named one.
	generated string
	expr      *pg_parse.Expr
	validated bool
}

// migrationChecks records a migration action on check constraints
type migrationChecks struct {
	added   map[pg_catalog.RelationKey]map[string]check
	dropped map[pg_catalog.RelationKey]map[string]bool
	notNull map[pg_catalog.RelationKey]map[string]bool
	// names holds every constraint the migration created, of any type, under the name it has on the server.
	names map[pg_catalog.RelationKey]map[string]heldConstraint
	// columnOrigins maps a column name the migration changed to the snapshot column going by it.
	columnOrigins map[pg_catalog.RelationKey]map[string]string
	// merged and mergedNames are, for the statement being recorded, what mergeRedundantIndexes found.
	merged      map[*pg_parse.ConstraintDef]bool
	mergedNames map[*pg_parse.ConstraintDef]string
	// blind says DDL this file cannot read has run, so a constraint can hold a name nothing recorded.
	blind bool
}

func newMigrationChecks() *migrationChecks {
	return &migrationChecks{
		added:   map[pg_catalog.RelationKey]map[string]check{},
		dropped: map[pg_catalog.RelationKey]map[string]bool{},
		notNull: map[pg_catalog.RelationKey]map[string]bool{},
		names:   map[pg_catalog.RelationKey]map[string]heldConstraint{},

		columnOrigins: map[pg_catalog.RelationKey]map[string]string{},
	}
}

// record applies a statement the server ran to the constraints of the table it alters.
func (m *migrationChecks) record(s scope) {
	if blindCommands[string(s.statement.Command)] {
		m.blind = true
	}
	m.mergeRedundantIndexes(s.statement.Subcommands)
	switch s.statement.Command {
	case "CREATE TABLE":
		m.recordCreate(s)
		return
	case "ALTER INDEX RENAME":
		if len(s.statement.Subcommands) == 1 {
			m.renameIndex(s, s.statement.Subcommands[0].NewName)
		}
		return
	case "ALTER TABLE", "ALTER TABLE RENAME":
	default:
		return
	}
	table := s.relation()
	if len(s.statement.Subcommands) == 1 && s.statement.Subcommands[0].Kind == pg_parse.SubRenameRelation &&
		(!table.Exists() || pg_contract.RelationKindFromRelkind(table.Relation.Kind).IsIndex()) {
		m.renameIndex(s, s.statement.Subcommands[0].NewName)
		return
	}
	if !table.Exists() {
		return
	}
	key := table.Key()

	for _, sub := range s.statement.Subcommands {
		switch sub.Kind {
		case pg_parse.SubAddConstraint:
			m.addConstraint(s, table, sub.Constraint, m.claim(s, table, sub.Constraint, ""))
		case pg_parse.SubAddColumn:
			m.claimColumn(s, table, sub.Column)
		case pg_parse.SubAttachPartition:
			if len(sub.Relations) > 0 {
				m.inherit(s, table, s.resolve(sub.Relations[0]), true)
			}
		case pg_parse.SubValidateConstraint:
			if added, ok := m.added[key][sub.Name]; ok {
				added.validated = true
				m.added[key][sub.Name] = added
			} else if snapshot, ok := snapshotCheck(s, table, sub.Name); ok {
				snapshot.validated = true
				m.put(key, snapshot)
			}
		case pg_parse.SubDropConstraint:
			m.dropClones(s, table, sub.Name)
			for _, reached := range s.reached(table) {
				m.dropConstraint(s, reached, sub.Name)
			}
		case pg_parse.SubRenameConstraint:
			// Only a CHECK has copies on the children under the same name for the rename to reach.
			targets := []pg_catalog.RelationInfo{table}
			if m.isCheck(s, table, sub.Name) {
				targets = s.reached(table)
			}
			for _, reached := range targets {
				m.renameConstraint(s, reached, sub.Name, sub.NewName)
			}
		case pg_parse.SubDropColumn:
			for _, reached := range s.reached(table) {
				m.dropColumn(s, reached, sub.Name)
			}
		case pg_parse.SubRenameColumn:
			for _, reached := range s.reached(table) {
				m.renameColumn(s, reached, sub.Name, sub.NewName)
			}
		case pg_parse.SubSetNotNull:
			m.markNotNull(key, sub.Name, true)
			m.claimNotNull(s, table, sub.Name, "", false)
		case pg_parse.SubDropNotNull:
			m.markNotNull(key, sub.Name, false)
			for _, reached := range s.reached(table) {
				m.freeNotNull(s, reached, sub.Name)
			}
		}
	}
}

// addConstraint records what a new constraint proves. name is the one it holds on the server.
func (m *migrationChecks) addConstraint(s scope, table pg_catalog.RelationInfo, constraint *pg_parse.ConstraintDef, name string) {
	if constraint == nil || constraint.NotEnforced {
		return
	}
	key := table.Key()
	switch constraint.Type {
	case pg_parse.ConstraintCheck:
		added := check{name: name, expr: constraint.Expr, validated: !constraint.NotValid}
		if constraint.Name == "" {
			column := ""
			if columns := columnsOf(constraint.Expr); len(columns) == 1 {
				column = columns[0]
			}
			added.generated = objectName(table.Name.Table, column, "check")
		}
		m.put(key, added)
	case pg_parse.ConstraintNotNull, pg_parse.ConstraintPrimaryKey:
		if constraint.NotValid {
			return
		}
		for _, column := range constraint.Columns {
			m.markNotNull(key, column, true)
		}
	}
}

// dropConstraint forgets a constraint the migration drops by name.
func (m *migrationChecks) dropConstraint(s scope, table pg_catalog.RelationInfo, name string) {
	key := table.Key()
	_, known := m.added[key][name]
	if _, held := m.names[key][name]; held {
		known = true
	}
	delete(m.added[key], name)
	delete(m.names[key], name)
	if constraint, ok := s.catalog.ConstraintByName(table.Relation.OID, name); ok {
		known = true
		if constraint.Type == "n" {
			// A PG 18 NOT NULL constraint: the column goes back to nullable.
			for _, column := range s.catalog.Columns(table.Relation.OID) {
				if slices.Contains(constraint.Key, column.Num) {
					m.markNotNull(key, m.currentColumn(key, column.Name), false)
				}
			}
		}
	}
	if !known && m.blind {
		// DDL this file cannot read can have taken a name in the schema and moved the server's suffix: forget every
		// unnamed CHECK the dropped name could be.
		for added, c := range m.added[key] {
			if c.generated != "" && strings.HasPrefix(name, c.generated) {
				delete(m.added[key], added)
			}
		}
	}
	m.markDropped(key, name)
}

// renameConstraint moves a constraint to its new name, keeping what is known of it.
func (m *migrationChecks) renameConstraint(s scope, table pg_catalog.RelationInfo, from, to string) {
	key := table.Key()
	if held, ok := m.names[key][from]; ok {
		delete(m.names[key], from)
		held.name = to
		m.hold(key, held)
	} else if constraint, ok := s.catalog.ConstraintByName(table.Relation.OID, from); ok && !m.dropped[key][from] {
		held := snapshotHeld(s, table, constraint)
		held.name = to
		m.hold(key, held)
	} else {
		return
	}
	if added, ok := m.added[key][from]; ok {
		delete(m.added[key], from)
		added.name, added.generated = to, ""
		m.put(key, added)
	} else if snapshot, ok := snapshotCheck(s, table, from); ok && !m.dropped[key][from] {
		snapshot.name = to
		m.put(key, snapshot)
	}
	m.markDropped(key, from)
}

func (m *migrationChecks) isCheck(s scope, table pg_catalog.RelationInfo, name string) bool {
	for _, held := range m.heldOn(s, table) {
		if held.name == name {
			return held.kind == "c"
		}
	}
	return false
}

// snapshotColumn get the original name of the column in the snapshot, false when no snapshot column goes by this name anymore.
func (m *migrationChecks) snapshotColumn(key pg_catalog.RelationKey, column string) (string, bool) {
	origin, renamed := m.columnOrigins[key][column]
	if !renamed {
		return column, true
	}
	return origin, origin != ""
}

func (m *migrationChecks) originOf(key pg_catalog.RelationKey) map[string]string {
	if m.columnOrigins[key] == nil {
		m.columnOrigins[key] = map[string]string{}
	}
	return m.columnOrigins[key]
}

// currentNames maps the snapshot columns the migration renamed to the names they go by at this point.
func (m *migrationChecks) currentNames(key pg_catalog.RelationKey) map[string]string {
	names := map[string]string{}
	for current, origin := range m.columnOrigins[key] {
		if origin != "" {
			names[origin] = current
		}
	}
	return names
}

// currentColumn is the name a snapshot column goes by at this point of the migration.
func (m *migrationChecks) currentColumn(key pg_catalog.RelationKey, column string) string {
	if current, renamed := m.currentNames(key)[column]; renamed {
		return current
	}
	return column
}

// dropColumn follows a column the migration drops: the server drops every constraint built on it with it, and, under
// CASCADE, the foreign keys that point at it.
func (m *migrationChecks) dropColumn(s scope, table pg_catalog.RelationInfo, column string) {
	key := table.Key()
	for name, added := range m.added[key] {
		if added.expr == nil || mentions(added.expr, column) {
			delete(m.added[key], name)
		}
	}
	m.markNotNull(key, column, false)
	for name, held := range m.names[key] {
		if slices.Contains(held.columns, column) {
			delete(m.names[key], name)
		}
	}
	for other, held := range m.names {
		for name, constraint := range held {
			if constraint.references == key && slices.Contains(constraint.referenced, column) {
				delete(m.names[other], name)
			}
		}
	}

	origin, inSnapshot := m.snapshotColumn(key, column)
	m.originOf(key)[column] = ""
	if !inSnapshot {
		return
	}
	for _, constraint := range s.catalog.ConstraintsOf(table.Relation.OID) {
		if builtOn(s, table, constraint, origin) {
			m.markDropped(key, constraint.Name)
		}
	}
	info, ok := s.catalog.Column(table.Relation.OID, origin)
	if !ok {
		return
	}
	for _, constraint := range s.catalog.ForeignKeysTo(table.Relation.OID) {
		if slices.Contains(constraint.ForeignKey, info.Num) {
			m.markDropped(pg_catalog.SnapshotKey(constraint.RelID), constraint.Name)
		}
	}
}

// builtOn reports whether a snapshot constraint depends on a column, named as the snapshot names it.
func builtOn(s scope, table pg_catalog.RelationInfo, constraint pg_catalog.Constraint, column string) bool {
	if constraint.Type == "c" {
		parsed, ok := parseCheck(constraint)
		return ok && mentions(parsed.expr, column)
	}
	info, ok := s.catalog.Column(table.Relation.OID, column)
	return ok && slices.Contains(constraint.Key, info.Num)
}

// renameColumn follows a column the migration renames: its constraints stay, under the same names, and now read the new one.
func (m *migrationChecks) renameColumn(s scope, table pg_catalog.RelationInfo, from, to string) {
	key := table.Key()
	for name, added := range m.added[key] {
		if added.expr != nil {
			added.expr = readingColumns(added.expr, map[string]string{from: to})
			m.added[key][name] = added
		}
	}
	if notNull, known := m.notNull[key][from]; known {
		m.notNull[key][to] = notNull
		delete(m.notNull[key], from)
	} else {
		delete(m.notNull[key], to)
	}

	origin, inSnapshot := m.snapshotColumn(key, from)
	if !inSnapshot {
		origin = ""
	}
	origins := m.originOf(key)
	origins[to] = origin
	origins[from] = ""

	for name, held := range m.names[key] {
		if i := slices.Index(held.columns, from); i >= 0 {
			held.columns = slices.Clone(held.columns)
			held.columns[i] = to
			m.names[key][name] = held
		}
	}
	for other, named := range m.names {
		for name, held := range named {
			if i := slices.Index(held.referenced, from); i >= 0 && held.references == key {
				held.referenced = slices.Clone(held.referenced)
				held.referenced[i] = to
				m.names[other][name] = held
			}
		}
	}
}

// freeNotNull releases the name of the NOT NULL constraint a DROP NOT NULL removes on PG 18.
func (m *migrationChecks) freeNotNull(s scope, table pg_catalog.RelationInfo, column string) {
	key := table.Key()
	for name, held := range m.names[key] {
		if held.kind == "n" && slices.Contains(held.columns, column) {
			delete(m.names[key], name)
		}
	}
	origin, inSnapshot := m.snapshotColumn(key, column)
	if !inSnapshot {
		return
	}
	for _, constraint := range s.catalog.ConstraintsOf(table.Relation.OID) {
		if constraint.Type == "n" && builtOn(s, table, constraint, origin) {
			m.markDropped(key, constraint.Name)
		}
	}
}

// forget follows the relations a statement dropped or renamed: a dropped table's constraints go with it,
// a renamed one's follow it to its new key.
func (m *migrationChecks) forget(catalog *pg_catalog.Catalog, moves []moved) {
	for _, move := range moves {
		moveKey(m.added, move)
		moveKey(m.dropped, move)
		moveKey(m.notNull, move)
		moveKey(m.names, move)
		moveKey(m.columnOrigins, move)
		m.followReferences(catalog, move)
	}
}

func moveKey[V any](tables map[pg_catalog.RelationKey]map[string]V, move moved) {
	named, ok := tables[move.from]
	delete(tables, move.from)
	if ok && move.to != (pg_catalog.RelationKey{}) {
		tables[move.to] = named
	}
}

// followReferences updates the foreign keys pointing at a relation that moved. Dropping it took them with it, under CASCADE.
func (m *migrationChecks) followReferences(catalog *pg_catalog.Catalog, move moved) {
	dropped := move.to == (pg_catalog.RelationKey{})
	for other, named := range m.names {
		for name, held := range named {
			if held.references != move.from {
				continue
			}
			if dropped {
				delete(m.names[other], name)
				continue
			}
			held.references = move.to
			m.names[other][name] = held
		}
	}
	if oid, ok := move.from.OID(); ok && dropped {
		for _, constraint := range catalog.ForeignKeysTo(oid) {
			m.markDropped(pg_catalog.SnapshotKey(constraint.RelID), constraint.Name)
		}
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

func (m *migrationChecks) markDropped(key pg_catalog.RelationKey, name string) {
	if m.dropped[key] == nil {
		m.dropped[key] = map[string]bool{}
	}
	m.dropped[key][name] = true
}

func (m *migrationChecks) put(key pg_catalog.RelationKey, added check) {
	if m.added[key] == nil {
		m.added[key] = map[string]check{}
	}
	m.added[key][added.name] = added
}

func (m *migrationChecks) markNotNull(key pg_catalog.RelationKey, column string, notNull bool) {
	if m.notNull[key] == nil {
		m.notNull[key] = map[string]bool{}
	}
	m.notNull[key][column] = notNull
}

// reached is the table a constraint or column clause names, and every table below it the clause recurses to,
// unless ONLY stops it.
func (s scope) reached(table pg_catalog.RelationInfo) []pg_catalog.RelationInfo {
	out := []pg_catalog.RelationInfo{table}
	if s.only() {
		return out
	}
	for _, key := range s.catalog.Descendants(table.Key()) {
		if info, ok := s.catalog.RelationByKey(key); ok {
			out = append(out, info)
		}
	}
	return out
}

// checksOn lists the validated CHECKs a table carries at this point of the migration.
func (s scope) checksOn(table pg_catalog.RelationInfo) []check {
	key := table.Key()
	renamed := s.checks.currentNames(key)
	var out []check
	for _, constraint := range s.catalog.ConstraintsOf(table.Relation.OID) {
		if constraint.Type != "c" || !constraint.Validated || !constraint.Enforced {
			continue
		}
		if s.checks.dropped[key][constraint.Name] {
			continue
		}
		if _, replaced := s.checks.added[key][constraint.Name]; replaced {
			continue
		}
		if parsed, ok := parseCheck(constraint); ok {
			parsed.expr = readingColumns(parsed.expr, renamed)
			out = append(out, parsed)
		}
	}
	for _, added := range s.checks.added[key] {
		if added.validated && added.expr != nil {
			out = append(out, added)
		}
	}
	return out
}

// checkNamesOn lists the names of every CHECK a table carries at this point of the migration, validated or not.
func (s scope) checkNamesOn(table pg_catalog.RelationInfo) map[string]bool {
	key := table.Key()
	names := map[string]bool{}
	for _, constraint := range s.catalog.ConstraintsOf(table.Relation.OID) {
		if constraint.Type == "c" && !s.checks.dropped[key][constraint.Name] {
			names[constraint.Name] = true
		}
	}
	for name := range s.checks.added[key] {
		names[name] = true
	}
	return names
}

// neverNull reports whether a column is known to hold no NULL at this point of the migration.
func (s scope) neverNull(table pg_catalog.RelationInfo, column string) bool {
	if notNull, ok := s.checks.notNull[table.Key()][column]; ok {
		return notNull
	}
	origin, inSnapshot := s.checks.snapshotColumn(table.Key(), column)
	if !inSnapshot {
		return false
	}
	proof := s.catalog.NotNullProof(table.Relation.OID, origin)
	return proof == pg_catalog.NotNullAttribute || proof == pg_catalog.NotNullConstraint
}

// snapshotCheck is the named CHECK the snapshot holds on a table, whatever its validation state, reading the columns
// under their current names.
func snapshotCheck(s scope, table pg_catalog.RelationInfo, name string) (check, bool) {
	constraint, ok := s.catalog.ConstraintByName(table.Relation.OID, name)
	if !ok || constraint.Type != "c" || !constraint.Enforced {
		return check{}, false
	}
	parsed, ok := parseCheck(constraint)
	if ok {
		parsed.expr = readingColumns(parsed.expr, s.checks.currentNames(table.Key()))
	}
	return parsed, ok
}

// readingColumns copies an expression with its column references renamed, in one pass so that two columns can swap names.
func readingColumns(expr *pg_parse.Expr, names map[string]string) *pg_parse.Expr {
	out := *expr
	if out.Kind == pg_parse.ExprColumnRef {
		parts := strings.Split(out.Literal, ".")
		if name, renamed := names[parts[len(parts)-1]]; renamed {
			parts[len(parts)-1] = name
			out.Literal = strings.Join(parts, ".")
		}
	}
	if out.Args != nil {
		out.Args = make([]pg_parse.Expr, len(expr.Args))
		for i := range expr.Args {
			out.Args[i] = *readingColumns(&expr.Args[i], names)
		}
	}
	return &out
}

// parseCheck reads a snapshot CHECK's definition back into the IR, through the statement that would add it.
func parseCheck(constraint pg_catalog.Constraint) (check, bool) {
	statement := pg_parse.ParseOne("ALTER TABLE t ADD " + constraint.Def)
	if !statement.Parsed() || len(statement.Subcommands) == 0 || statement.Subcommands[0].Constraint == nil {
		return check{}, false
	}
	expr := statement.Subcommands[0].Constraint.Expr
	if expr == nil {
		return check{}, false
	}
	return check{name: constraint.Name, expr: expr, validated: constraint.Validated}, true
}
