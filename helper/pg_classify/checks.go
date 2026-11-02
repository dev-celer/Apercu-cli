package pg_classify

import (
	"strconv"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_parse"
)

// check is one CHECK constraint on a table, read from the snapshot or from the statement that added it.
type check struct {
	name      string
	expr      *pg_parse.Expr
	validated bool
}

// migrationChecks records a migration action on check constraints
type migrationChecks struct {
	added   map[pg_catalog.RelationKey]map[string]check
	dropped map[pg_catalog.RelationKey]map[string]bool
	notNull map[pg_catalog.RelationKey]map[string]bool
}

func newMigrationChecks() *migrationChecks {
	return &migrationChecks{
		added:   map[pg_catalog.RelationKey]map[string]check{},
		dropped: map[pg_catalog.RelationKey]map[string]bool{},
		notNull: map[pg_catalog.RelationKey]map[string]bool{},
	}
}

// record applies a statement the server ran to the constraints of the table it alters.
func (m *migrationChecks) record(s scope) {
	if s.statement.Command != "ALTER TABLE" {
		return
	}
	table := s.relation()
	if !table.Exists() {
		return
	}
	key := table.Key()

	for _, sub := range s.statement.Subcommands {
		switch sub.Kind {
		case pg_parse.SubAddConstraint:
			m.addConstraint(key, sub.Constraint)
		case pg_parse.SubValidateConstraint:
			if added, ok := m.added[key][sub.Name]; ok {
				added.validated = true
				m.added[key][sub.Name] = added
			} else if snapshot, ok := snapshotCheck(s, table, sub.Name); ok {
				snapshot.validated = true
				m.put(key, snapshot)
			}
		case pg_parse.SubDropConstraint:
			delete(m.added[key], sub.Name)
			if m.dropped[key] == nil {
				m.dropped[key] = map[string]bool{}
			}
			m.dropped[key][sub.Name] = true
		case pg_parse.SubSetNotNull:
			m.markNotNull(key, sub.Name, true)
		case pg_parse.SubDropNotNull:
			m.markNotNull(key, sub.Name, false)
		}
	}
}

func (m *migrationChecks) addConstraint(key pg_catalog.RelationKey, constraint *pg_parse.ConstraintDef) {
	if constraint == nil || constraint.NotEnforced {
		return
	}
	switch constraint.Type {
	case pg_parse.ConstraintCheck:
		name := constraint.Name
		if name == "" {
			// The server names it; nothing can refer to it before it is read back, so any distinct key does.
			name = "#" + strconv.Itoa(len(m.added[key]))
		}
		m.put(key, check{name: name, expr: constraint.Expr, validated: !constraint.NotValid})
	case pg_parse.ConstraintNotNull, pg_parse.ConstraintPrimaryKey:
		if constraint.NotValid {
			return
		}
		for _, column := range constraint.Columns {
			m.markNotNull(key, column, true)
		}
	}
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

// checksOn lists the validated CHECKs a table carries at this point of the migration.
func (s scope) checksOn(table pg_catalog.RelationInfo) []check {
	key := table.Key()
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
	proof := s.catalog.NotNullProof(table.Relation.OID, column)
	return proof == pg_catalog.NotNullAttribute || proof == pg_catalog.NotNullConstraint
}

// snapshotCheck is the named CHECK the snapshot holds on a table, whatever its validation state.
func snapshotCheck(s scope, table pg_catalog.RelationInfo, name string) (check, bool) {
	constraint, ok := s.catalog.ConstraintByName(table.Relation.OID, name)
	if !ok || constraint.Type != "c" || !constraint.Enforced {
		return check{}, false
	}
	return parseCheck(constraint)
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
