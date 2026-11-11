package shadow

import (
	"slices"
	"strings"

	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgclassify/stmt"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse"
	"apercu-cli/internal/pgparse/ir"
)

// Check is one CHECK constraint on a table, read from the snapshot or from the statement that added it.
type Check struct {
	Name string
	// generated is the name the server derives for an unnamed CHECK before any numeric suffix, empty for a named one.
	generated string
	Expr      *ir.Expr
	validated bool
}

// Checks records a migration action on check constraints
type Checks struct {
	added   map[pgcatalog.RelationKey]map[string]Check
	dropped map[pgcatalog.RelationKey]map[string]bool
	notNull map[pgcatalog.RelationKey]map[string]bool
	// names holds every constraint the migration created, of any type, under the name it has on the server.
	names map[pgcatalog.RelationKey]map[string]heldConstraint
	// columnOrigins maps a column name the migration changed to the snapshot column going by it.
	columnOrigins map[pgcatalog.RelationKey]map[string]string
	// merged and mergedNames are, for the statement being recorded, what mergeRedundantIndexes found.
	merged      map[*ir.ConstraintDef]bool
	mergedNames map[*ir.ConstraintDef]string
	// blind says DDL this file cannot read has run, so a constraint can hold a name nothing recorded.
	Blind bool
}

func NewChecks() *Checks {
	return &Checks{
		added:   map[pgcatalog.RelationKey]map[string]Check{},
		dropped: map[pgcatalog.RelationKey]map[string]bool{},
		notNull: map[pgcatalog.RelationKey]map[string]bool{},
		names:   map[pgcatalog.RelationKey]map[string]heldConstraint{},

		columnOrigins: map[pgcatalog.RelationKey]map[string]string{},
	}
}

// Record applies a statement the server ran to the constraints of the table it alters.
func (m *Checks) Record(s Scope) {
	if blindCommands[string(s.Statement.Command)] {
		m.Blind = true
	}
	m.mergeRedundantIndexes(s.Statement.Subcommands)
	switch s.Statement.Command {
	case "CREATE TABLE":
		m.recordCreate(s)
		return
	case "ALTER INDEX RENAME":
		if len(s.Statement.Subcommands) == 1 {
			m.renameIndex(s, s.Statement.Subcommands[0].NewName)
		}
		return
	case "ALTER TABLE", "ALTER TABLE RENAME":
	default:
		return
	}
	table := s.Relation()
	if len(s.Statement.Subcommands) == 1 && s.Statement.Subcommands[0].Kind == ir.SubRenameRelation &&
		(!table.Exists() || pgcontract.RelationKindFromRelkind(table.Relation.Kind).IsIndex()) {
		m.renameIndex(s, s.Statement.Subcommands[0].NewName)
		return
	}
	if !table.Exists() {
		return
	}
	key := table.Key()

	for _, sub := range s.Statement.Subcommands {
		switch sub.Kind {
		case ir.SubAddConstraint:
			m.addConstraint(s, table, sub.Constraint, m.claim(s, table, sub.Constraint, ""))
		case ir.SubAddColumn:
			m.claimColumn(s, table, sub.Column)
		case ir.SubAttachPartition:
			if len(sub.Relations) > 0 {
				m.inherit(s, table, s.Resolve(sub.Relations[0]), true)
			}
		case ir.SubValidateConstraint:
			if added, ok := m.added[key][sub.Name]; ok {
				added.validated = true
				m.added[key][sub.Name] = added
			} else if snapshot, ok := snapshotCheck(s, table, sub.Name); ok {
				snapshot.validated = true
				m.put(key, snapshot)
			}
		case ir.SubDropConstraint:
			m.dropClones(s, table, sub.Name)
			for _, reached := range s.reached(table) {
				m.dropConstraint(s, reached, sub.Name)
			}
		case ir.SubRenameConstraint:
			// Only a CHECK has copies on the children under the same name for the rename to reach.
			targets := []pgcatalog.RelationInfo{table}
			if m.isCheck(s, table, sub.Name) {
				targets = s.reached(table)
			}
			for _, reached := range targets {
				m.renameConstraint(s, reached, sub.Name, sub.NewName)
			}
		case ir.SubDropColumn:
			for _, reached := range s.reached(table) {
				m.dropColumn(s, reached, sub.Name)
			}
		case ir.SubRenameColumn:
			for _, reached := range s.reached(table) {
				m.renameColumn(s, reached, sub.Name, sub.NewName)
			}
		case ir.SubSetNotNull:
			m.markNotNull(key, sub.Name, true)
			m.claimNotNull(s, table, sub.Name, "", false)
		case ir.SubDropNotNull:
			m.markNotNull(key, sub.Name, false)
			for _, reached := range s.reached(table) {
				m.freeNotNull(s, reached, sub.Name)
			}
		}
	}
}

// addConstraint records what a new constraint proves. name is the one it holds on the server.
func (m *Checks) addConstraint(s Scope, table pgcatalog.RelationInfo, constraint *ir.ConstraintDef, name string) {
	if constraint == nil || constraint.NotEnforced {
		return
	}
	key := table.Key()
	switch constraint.Type {
	case ir.ConstraintCheck:
		added := Check{Name: name, Expr: constraint.Expr, validated: !constraint.NotValid}
		if constraint.Name == "" {
			column := ""
			if columns := stmt.ColumnsOf(constraint.Expr); len(columns) == 1 {
				column = columns[0]
			}
			added.generated = objectName(table.Name.Table, column, "check")
		}
		m.put(key, added)
	case ir.ConstraintNotNull, ir.ConstraintPrimaryKey:
		if constraint.NotValid {
			return
		}
		for _, column := range constraint.Columns {
			m.markNotNull(key, column, true)
		}
	}
}

// dropConstraint forgets a constraint the migration drops by name.
func (m *Checks) dropConstraint(s Scope, table pgcatalog.RelationInfo, name string) {
	key := table.Key()
	_, known := m.added[key][name]
	if _, held := m.names[key][name]; held {
		known = true
	}
	delete(m.added[key], name)
	delete(m.names[key], name)
	if constraint, ok := s.Catalog.ConstraintByName(table.Relation.OID, name); ok {
		known = true
		if constraint.Type == "n" {
			// A PG 18 NOT NULL constraint: the column goes back to nullable.
			for _, column := range s.Catalog.Columns(table.Relation.OID) {
				if slices.Contains(constraint.Key, column.Num) {
					m.markNotNull(key, m.CurrentColumn(key, column.Name), false)
				}
			}
		}
	}
	if !known && m.Blind {
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
func (m *Checks) renameConstraint(s Scope, table pgcatalog.RelationInfo, from, to string) {
	key := table.Key()
	if held, ok := m.names[key][from]; ok {
		delete(m.names[key], from)
		held.name = to
		m.hold(key, held)
	} else if constraint, ok := s.Catalog.ConstraintByName(table.Relation.OID, from); ok && !m.dropped[key][from] {
		held := snapshotHeld(s, table, constraint)
		held.name = to
		m.hold(key, held)
	} else {
		return
	}
	if added, ok := m.added[key][from]; ok {
		delete(m.added[key], from)
		added.Name, added.generated = to, ""
		m.put(key, added)
	} else if snapshot, ok := snapshotCheck(s, table, from); ok && !m.dropped[key][from] {
		snapshot.Name = to
		m.put(key, snapshot)
	}
	m.markDropped(key, from)
}

func (m *Checks) isCheck(s Scope, table pgcatalog.RelationInfo, name string) bool {
	for _, held := range m.heldOn(s, table) {
		if held.name == name {
			return held.kind == "c"
		}
	}
	return false
}

// snapshotColumn get the original name of the column in the snapshot, false when no snapshot column goes by this name anymore.
func (m *Checks) snapshotColumn(key pgcatalog.RelationKey, column string) (string, bool) {
	origin, renamed := m.columnOrigins[key][column]
	if !renamed {
		return column, true
	}
	return origin, origin != ""
}

func (m *Checks) originOf(key pgcatalog.RelationKey) map[string]string {
	if m.columnOrigins[key] == nil {
		m.columnOrigins[key] = map[string]string{}
	}
	return m.columnOrigins[key]
}

// currentNames maps the snapshot columns the migration renamed to the names they go by at this point.
func (m *Checks) currentNames(key pgcatalog.RelationKey) map[string]string {
	names := map[string]string{}
	for current, origin := range m.columnOrigins[key] {
		if origin != "" {
			names[origin] = current
		}
	}
	return names
}

// CurrentColumn is the name a snapshot column goes by at this point of the migration.
func (m *Checks) CurrentColumn(key pgcatalog.RelationKey, column string) string {
	if current, renamed := m.currentNames(key)[column]; renamed {
		return current
	}
	return column
}

// dropColumn follows a column the migration drops: the server drops every constraint built on it with it, and, under
// CASCADE, the foreign keys that point at it.
func (m *Checks) dropColumn(s Scope, table pgcatalog.RelationInfo, column string) {
	key := table.Key()
	for name, added := range m.added[key] {
		if added.Expr == nil || stmt.Mentions(added.Expr, column) {
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
	for _, constraint := range s.Catalog.ConstraintsOf(table.Relation.OID) {
		if builtOn(s, table, constraint, origin) {
			m.markDropped(key, constraint.Name)
		}
	}
	info, ok := s.Catalog.Column(table.Relation.OID, origin)
	if !ok {
		return
	}
	for _, constraint := range s.Catalog.ForeignKeysTo(table.Relation.OID) {
		if slices.Contains(constraint.ForeignKey, info.Num) {
			m.markDropped(pgcatalog.SnapshotKey(constraint.RelID), constraint.Name)
		}
	}
}

// builtOn reports whether a snapshot constraint depends on a column, named as the snapshot names it.
func builtOn(s Scope, table pgcatalog.RelationInfo, constraint snapshot.Constraint, column string) bool {
	if constraint.Type == "c" {
		parsed, ok := parseCheck(constraint)
		return ok && stmt.Mentions(parsed.Expr, column)
	}
	info, ok := s.Catalog.Column(table.Relation.OID, column)
	return ok && slices.Contains(constraint.Key, info.Num)
}

// renameColumn follows a column the migration renames: its constraints stay, under the same names, and now read the new one.
func (m *Checks) renameColumn(s Scope, table pgcatalog.RelationInfo, from, to string) {
	key := table.Key()
	for name, added := range m.added[key] {
		if added.Expr != nil {
			added.Expr = readingColumns(added.Expr, map[string]string{from: to})
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
func (m *Checks) freeNotNull(s Scope, table pgcatalog.RelationInfo, column string) {
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
	for _, constraint := range s.Catalog.ConstraintsOf(table.Relation.OID) {
		if constraint.Type == "n" && builtOn(s, table, constraint, origin) {
			m.markDropped(key, constraint.Name)
		}
	}
}

// Forget follows the relations a statement dropped or renamed: a dropped table's constraints go with it,
// a renamed one's follow it to its new key.
func (m *Checks) Forget(catalog *pgcatalog.Catalog, moves []Moved) {
	for _, move := range moves {
		moveKey(m.added, move)
		moveKey(m.dropped, move)
		moveKey(m.notNull, move)
		moveKey(m.names, move)
		moveKey(m.columnOrigins, move)
		m.followReferences(catalog, move)
	}
}

func moveKey[V any](tables map[pgcatalog.RelationKey]map[string]V, move Moved) {
	named, ok := tables[move.from]
	delete(tables, move.from)
	if ok && move.to != (pgcatalog.RelationKey{}) {
		tables[move.to] = named
	}
}

// followReferences updates the foreign keys pointing at a relation that moved. Dropping it took them with it, under CASCADE.
func (m *Checks) followReferences(catalog *pgcatalog.Catalog, move Moved) {
	dropped := move.to == (pgcatalog.RelationKey{})
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
			m.markDropped(pgcatalog.SnapshotKey(constraint.RelID), constraint.Name)
		}
	}
}

func (m *Checks) markDropped(key pgcatalog.RelationKey, name string) {
	if m.dropped[key] == nil {
		m.dropped[key] = map[string]bool{}
	}
	m.dropped[key][name] = true
}

func (m *Checks) put(key pgcatalog.RelationKey, added Check) {
	if m.added[key] == nil {
		m.added[key] = map[string]Check{}
	}
	m.added[key][added.Name] = added
}

func (m *Checks) markNotNull(key pgcatalog.RelationKey, column string, notNull bool) {
	if m.notNull[key] == nil {
		m.notNull[key] = map[string]bool{}
	}
	m.notNull[key][column] = notNull
}

// reached is the table a constraint or column clause names, and every table below it the clause recurses to,
// unless ONLY stops it.
func (s Scope) reached(table pgcatalog.RelationInfo) []pgcatalog.RelationInfo {
	out := []pgcatalog.RelationInfo{table}
	if s.Only() {
		return out
	}
	for _, key := range s.Catalog.Descendants(table.Key()) {
		if info, ok := s.Catalog.RelationByKey(key); ok {
			out = append(out, info)
		}
	}
	return out
}

// ChecksOn lists the validated CHECKs a table carries at this point of the migration.
func (s Scope) ChecksOn(table pgcatalog.RelationInfo) []Check {
	key := table.Key()
	renamed := s.Checks.currentNames(key)
	var out []Check
	for _, constraint := range s.Catalog.ConstraintsOf(table.Relation.OID) {
		if constraint.Type != "c" || !constraint.Validated || !constraint.Enforced {
			continue
		}
		if s.Checks.dropped[key][constraint.Name] {
			continue
		}
		if _, replaced := s.Checks.added[key][constraint.Name]; replaced {
			continue
		}
		if parsed, ok := parseCheck(constraint); ok {
			parsed.Expr = readingColumns(parsed.Expr, renamed)
			out = append(out, parsed)
		}
	}
	for _, added := range s.Checks.added[key] {
		if added.validated && added.Expr != nil {
			out = append(out, added)
		}
	}
	return out
}

// CheckNamesOn lists the names of every CHECK a table carries at this point of the migration, validated or not.
func (s Scope) CheckNamesOn(table pgcatalog.RelationInfo) map[string]bool {
	key := table.Key()
	names := map[string]bool{}
	for _, constraint := range s.Catalog.ConstraintsOf(table.Relation.OID) {
		if constraint.Type == "c" && !s.Checks.dropped[key][constraint.Name] {
			names[constraint.Name] = true
		}
	}
	for name := range s.Checks.added[key] {
		names[name] = true
	}
	return names
}

// NeverNull reports whether a column is known to hold no NULL at this point of the migration.
func (s Scope) NeverNull(table pgcatalog.RelationInfo, column string) bool {
	if notNull, ok := s.Checks.notNull[table.Key()][column]; ok {
		return notNull
	}
	origin, inSnapshot := s.Checks.snapshotColumn(table.Key(), column)
	if !inSnapshot {
		return false
	}
	proof := s.Catalog.NotNullProof(table.Relation.OID, origin)
	return proof == pgcatalog.NotNullAttribute || proof == pgcatalog.NotNullConstraint
}

// snapshotCheck is the named CHECK the snapshot holds on a table, whatever its validation state, reading the columns
// under their current names.
func snapshotCheck(s Scope, table pgcatalog.RelationInfo, name string) (Check, bool) {
	constraint, ok := s.Catalog.ConstraintByName(table.Relation.OID, name)
	if !ok || constraint.Type != "c" || !constraint.Enforced {
		return Check{}, false
	}
	parsed, ok := parseCheck(constraint)
	if ok {
		parsed.Expr = readingColumns(parsed.Expr, s.Checks.currentNames(table.Key()))
	}
	return parsed, ok
}

// readingColumns copies an expression with its column references renamed, in one pass so that two columns can swap names.
func readingColumns(expr *ir.Expr, names map[string]string) *ir.Expr {
	out := *expr
	if out.Kind == ir.ExprColumnRef {
		parts := strings.Split(out.Literal, ".")
		if name, renamed := names[parts[len(parts)-1]]; renamed {
			parts[len(parts)-1] = name
			out.Literal = strings.Join(parts, ".")
		}
	}
	if out.Args != nil {
		out.Args = make([]ir.Expr, len(expr.Args))
		for i := range expr.Args {
			out.Args[i] = *readingColumns(&expr.Args[i], names)
		}
	}
	return &out
}

// parseCheck reads a snapshot CHECK's definition back into the IR, through the statement that would add it.
func parseCheck(constraint snapshot.Constraint) (Check, bool) {
	statement := pgparse.ParseOne("ALTER TABLE t ADD " + constraint.Def)
	if !statement.Parsed() || len(statement.Subcommands) == 0 || statement.Subcommands[0].Constraint == nil {
		return Check{}, false
	}
	expr := statement.Subcommands[0].Constraint.Expr
	if expr == nil {
		return Check{}, false
	}
	return Check{Name: constraint.Name, Expr: expr, validated: constraint.Validated}, true
}

// NotNull reports whether every column is already NOT NULL on the named table, a SET NOT NULL earlier in the
// migration included.
func (s Scope) NotNull(columns []string) bool {
	table := s.Relation()
	if !table.Exists() {
		return false
	}
	for _, name := range columns {
		if !s.NeverNull(table, name) {
			return false
		}
	}
	return true
}
