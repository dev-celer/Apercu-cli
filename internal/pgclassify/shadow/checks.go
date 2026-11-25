package shadow

import (
	"slices"
	"strings"

	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/stmt"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// Check is one CHECK constraint a table carries, its columns under their current names.
type Check struct {
	Name string
	Expr *ir.Expr
}

// Checks records in the shadow overlay what each statement the server ran did to the columns and constraints of the
// tables it alters. It holds nothing between statements: the overlay does.
type Checks struct {
	// recorder writes to the State the scopes handed to Record read.
	recorder *pgcatalog.Recorder

	// merged and mergedNames are, for the statement being recorded, what mergeRedundantIndexes found.
	merged      map[*ir.ConstraintDef]bool
	mergedNames map[*ir.ConstraintDef]string
}

func NewChecks(recorder *pgcatalog.Recorder) *Checks {
	return &Checks{recorder: recorder}
}

// Record applies a statement the server ran to the constraints of the table it alters.
func (m *Checks) Record(s Scope) {
	if blindCommands[string(s.Statement.Command)] {
		m.recorder.MarkBlind()
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
			m.claim(s, table, sub.Constraint, "")
			m.addConstraint(s, table, sub.Constraint)
		case ir.SubAddColumn:
			for _, reached := range s.reached(table) {
				m.addColumn(s, reached, sub.Column)
			}
			m.claimColumn(s, table, sub.Column)
		case ir.SubAlterColumnType:
			m.retype(s, table, sub.Column)
		case ir.SubDropExpression:
			m.dropExpression(s, table, sub.Name)
		case ir.SubAttachPartition:
			if len(sub.Relations) > 0 {
				m.inherit(s, table, s.Resolve(sub.Relations[0]), true)
			}
		case ir.SubValidateConstraint:
			if constraint, ok := s.State.ConstraintByName(key, sub.Name); ok {
				constraint.Validated = true
				m.hold(key, constraint)
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
				m.recorder.RenameConstraint(reached.Key(), sub.Name, sub.NewName)
			}
		case ir.SubDropColumn:
			for _, reached := range s.reached(table) {
				m.dropColumn(s, reached, sub.Name)
			}
		case ir.SubRenameColumn:
			for _, reached := range s.reached(table) {
				// Its constraints stay, under the same names, and now read the new one.
				m.recorder.RenameColumn(reached.Key(), sub.Name, sub.NewName)
			}
		case ir.SubSetNotNull:
			m.markNotNull(s, key, sub.Name, true)
			m.claimNotNull(s, table, sub.Name, "", false, true, true)
		case ir.SubDropNotNull:
			m.markNotNull(s, key, sub.Name, false)
			for _, reached := range s.reached(table) {
				m.freeNotNull(s, reached, sub.Name)
			}
		}
	}
}

// addConstraint records what a new constraint proves about its columns. claim already recorded the constraint.
func (m *Checks) addConstraint(s Scope, table pgcatalog.RelationInfo, constraint *ir.ConstraintDef) {
	if constraint == nil || constraint.NotEnforced || constraint.NotValid {
		return
	}
	switch constraint.Type {
	case ir.ConstraintNotNull, ir.ConstraintPrimaryKey:
		for _, column := range constraint.Columns {
			m.markNotNull(s, table.Key(), column, true)
		}
	}
}

// dropConstraint forgets a constraint the migration drops by name.
func (m *Checks) dropConstraint(s Scope, table pgcatalog.RelationInfo, name string) {
	key := table.Key()
	constraint, known := s.State.ConstraintByName(key, name)
	if known && constraint.Kind == "n" {
		// A PG 18 NOT NULL constraint: the column goes back to nullable.
		for _, column := range constraint.Columns {
			m.markNotNull(s, key, column, false)
		}
	}
	if !known && s.State.Blind() {
		// DDL this file cannot read can have taken a name in the schema and moved the server's suffix: no unnamed
		// CHECK the dropped name could be proves anything any more, though its name stays taken.
		for _, held := range s.State.Constraints(key) {
			if held.GeneratedPrefix != "" && strings.HasPrefix(name, held.GeneratedPrefix) {
				held.Expr = nil
				m.hold(key, held)
			}
		}
	}
	m.recorder.DeleteConstraint(key, name)
}

func (m *Checks) isCheck(s Scope, table pgcatalog.RelationInfo, name string) bool {
	constraint, ok := s.State.ConstraintByName(table.Key(), name)
	return ok && constraint.Kind == "c"
}

// builtOn reports whether a constraint depends on a column: the server drops it with the column.
func builtOn(constraint held, column string) bool {
	if slices.Contains(constraint.Columns, column) {
		return true
	}
	return constraint.Kind == "c" && constraint.Expr != nil && stmt.Mentions(constraint.Expr, column)
}

// dropColumn follows a column the migration drops: the server drops every constraint and index built on it with it,
// and, under CASCADE, the foreign keys that point at it.
func (m *Checks) dropColumn(s Scope, table pgcatalog.RelationInfo, column string) {
	key := table.Key()
	for _, constraint := range s.State.Constraints(key) {
		if builtOn(constraint, column) {
			m.recorder.DeleteConstraint(key, constraint.Name)
		}
	}
	for _, reference := range s.State.ReferencesTo(key) {
		if slices.Contains(reference.Constraint.ReferencedColumns, column) {
			m.recorder.DeleteConstraint(reference.Table, reference.Constraint.Name)
		}
	}
	for _, index := range s.State.IndexesOn(key, column) {
		m.recorder.Drop(index.Key)
	}
	m.recorder.DeleteColumn(key, column)
}

// markNotNull records whether a column holds no NULL, under the name it goes by now.
func (m *Checks) markNotNull(s Scope, key pgcatalog.RelationKey, column string, notNull bool) {
	state, _ := s.State.ColumnState(key, column)
	state.NotNull = &notNull
	m.recorder.SetColumn(key, column, state)
}

// freeNotNull releases the name of the NOT NULL constraint a DROP NOT NULL removes on PG 18.
func (m *Checks) freeNotNull(s Scope, table pgcatalog.RelationInfo, column string) {
	key := table.Key()
	for _, constraint := range s.State.Constraints(key) {
		if constraint.Kind == "n" && slices.Contains(constraint.Columns, column) {
			m.recorder.DeleteConstraint(key, constraint.Name)
		}
	}
}

// reached is the table a constraint or column clause names, and every table below it the clause recurses to,
// unless ONLY stops it.
func (s Scope) reached(table pgcatalog.RelationInfo) []pgcatalog.RelationInfo {
	out := []pgcatalog.RelationInfo{table}
	if s.Only() {
		return out
	}
	for _, key := range s.State.Descendants(table.Key()) {
		if info, ok := s.State.RelationByKey(key); ok {
			out = append(out, info)
		}
	}
	return out
}

// ChecksOn lists the validated CHECKs a table carries at this point of the migration.
func (s Scope) ChecksOn(table pgcatalog.RelationInfo) []Check {
	var out []Check
	for _, constraint := range s.State.Constraints(table.Key()) {
		if constraint.Kind == "c" && constraint.Validated && constraint.Enforced && constraint.Expr != nil {
			out = append(out, Check{Name: constraint.Name, Expr: constraint.Expr})
		}
	}
	return out
}

// CheckNamesOn lists the names of every CHECK a table carries at this point of the migration, validated or not.
func (s Scope) CheckNamesOn(table pgcatalog.RelationInfo) map[string]bool {
	names := map[string]bool{}
	for _, constraint := range s.State.Constraints(table.Key()) {
		if constraint.Kind == "c" {
			names[constraint.Name] = true
		}
	}
	return names
}

// NeverNull reports whether a column is known to hold no NULL at this point of the migration.
func (s Scope) NeverNull(table pgcatalog.RelationInfo, column string) bool {
	proof := s.State.NotNullProof(table.Key(), column)
	return proof == pgcatalog.NotNullAttribute || proof == pgcatalog.NotNullConstraint
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
