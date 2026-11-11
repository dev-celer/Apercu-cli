package rules

import (
	"fmt"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// ruleAddEnumValue is R-TY-ADDVALUE.
func ruleAddEnumValue(s engine.Scope) engine.Effect {
	value, name := enumClause(s)
	e := engine.NewEffect(s, "R-TY-ADDVALUE",
		fmt.Sprintf("value %q is appended to enum %s; the type is locked, no table is, and the new value cannot be used until the transaction that added it has committed", value, name))
	// The type is not a relation, so there is nothing in the target vocabulary to carry it.
	return e.OnNone()
}

// ruleRenameEnumValue is R-TY-RENAMEVALUE.
func ruleRenameEnumValue(s engine.Scope) engine.Effect {
	value, name := enumClause(s)
	e := engine.NewEffect(s, "R-TY-RENAMEVALUE", fmt.Sprintf("value %q of enum %s is renamed; every row already holding it reads back under the new spelling", value, name))
	return e.OnNone()
}

// enumClause is the value and the type name an ALTER TYPE … VALUE carries.
func enumClause(s engine.Scope) (string, string) {
	if len(s.Statement.Subcommands) == 0 {
		return "", ""
	}
	sub := s.Statement.Subcommands[0]
	return sub.Value, sub.ObjectName()
}

// ruleAddAttribute is R-TY-ATTR for ADD ATTRIBUTE.
func ruleAddAttribute(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := newAttributeEffect(s, fmt.Sprintf("attribute %q is added to the composite type", sub.Name))
	return carriedToTypedTables(e, s, sub)
}

// ruleDropAttribute is R-TY-ATTR for DROP ATTRIBUTE.
func ruleDropAttribute(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := newAttributeEffect(s, fmt.Sprintf("attribute %q is dropped from the composite type", sub.Name))
	return carriedToTypedTables(e, s, sub)
}

// ruleAlterAttribute is R-TY-ATTR for ALTER ATTRIBUTE.
func ruleAlterAttribute(s engine.Scope, sub ir.Subcommand) engine.Effect {
	changed := ruleAlterColumnType(s, sub)
	e := newAttributeEffect(s, fmt.Sprintf("attribute %q changes type: %s", sub.Name, changed.Message))
	e.Op = changed.Op

	// An ordinary column of the type cannot follow the change, and there is no CASCADE for it.
	if columns, _ := typeDependents(s, s.Relation()); len(columns) > 0 {
		return e.Reject(fmt.Sprintf("the type is used in the column %s.%s, and an ordinary column of a composite type cannot follow an attribute type change", relationOf(s, columns[0]), columns[0].Name), pgcontract.AnyVersion)
	}
	return carriedToTypedTables(e, s, sub)
}

// newAttributeEffect is the shape all three attribute clauses share: the composite type is a
// relation of its own, and nothing hangs below it.
func newAttributeEffect(s engine.Scope, message string) engine.Effect {
	e := engine.NewEffect(s, "R-TY-ATTR", message)
	return e
}

// carriedToTypedTables is the tail all three share attribute classifier.
func carriedToTypedTables(e engine.Effect, s engine.Scope, sub ir.Subcommand) engine.Effect {
	_, typedTables := typeDependents(s, s.Relation())
	if len(typedTables) == 0 {
		return e
	}
	if !sub.Flags.Cascade {
		return e.Reject("a table is declared OF this type, so an attribute clause needs CASCADE to carry the change into it", pgcontract.AnyVersion)
	}

	for _, table := range typedTables {
		rel, ok := s.Catalog.ByOID(table)
		if !ok {
			continue
		}
		e.Touches = append(e.Touches, engine.Touch{
			Info: s.InfoOf(rel),
			Lock: pgcontract.LockAccessExclusive,
			Op:   e.Op,
			Role: pgcontract.TargetRoleImplicit,
		})
	}
	e.Message += fmt.Sprintf("; %d table(s) declared OF this type are locked and changed with it", len(typedTables))
	return e
}

// typeDependents return everything that as to be locked when a type change.
func typeDependents(s engine.Scope, info pgcatalog.RelationInfo) ([]pgcatalog.ColumnRef, []snapshot.OID) {
	if !info.Exists() {
		return nil, nil
	}
	composite, ok := s.Catalog.TypeByName(info.Name.Schema, info.Name.Table, s.Context.SearchPath)
	if !ok {
		return nil, nil
	}
	return s.Catalog.TypeDependents(composite.OID)
}

// relationOf names the table a column reference points at.
func relationOf(s engine.Scope, column pgcatalog.ColumnRef) string {
	if rel, ok := s.Catalog.ByOID(column.RelID); ok {
		return rel.RelationName().String()
	}
	return "an unknown relation"
}

// ruleDomainAddConstraint is R-TY-DOMAIN-ADD. The statement names no table: every table with a
// column of the domain is read to prove the constraint holds.
func ruleDomainAddConstraint(s engine.Scope, sub ir.Subcommand) engine.Effect {
	notValid := sub.Constraint != nil && sub.Constraint.NotValid

	e := engine.NewEffect(s, "R-TY-DOMAIN-ADD", fmt.Sprintf("a constraint is added to domain %s, and every table holding a column of it is read to prove it", sub.ObjectName()))
	if notValid {
		// NOT VALID reads no user relation, the change is limited to the type only.
		e.Op = pgcontract.OpKindMetadata
		e.Message = fmt.Sprintf("NOT VALID records the constraint on domain %s and enforces it for new values only; no existing row is read and no table holding a column of it is opened", sub.ObjectName())
		return e.OnNone()
	}

	e = e.OnNone()
	e.Touches = append(e.Touches, domainColumnTargets(s, sub.Object, e.Lock, e.Op)...)
	if len(e.Touches) == 0 {
		e.Message += "; no column in the snapshot uses it, so nothing is read"
	}
	return e
}

// domainColumnTargets is the tables holding a column of a type.
func domainColumnTargets(s engine.Scope, name helper.FullRelationName, lock pgcontract.Lock, op pgcontract.OpKind) []engine.Touch {
	domain, ok := s.Catalog.TypeByName(name.Schema, name.Table, s.Context.SearchPath)
	if !ok {
		return nil
	}
	columns, typedTables := s.Catalog.TypeDependents(domain.OID)

	var targets []engine.Touch
	for _, column := range columns {
		if rel, ok := s.Catalog.ByOID(column.RelID); ok {
			targets = append(targets, engine.Touch{Info: s.InfoOf(rel), Lock: lock, Op: op, Role: pgcontract.TargetRoleImplicit})
		}
	}
	for _, oid := range typedTables {
		if rel, ok := s.Catalog.ByOID(oid); ok {
			targets = append(targets, engine.Touch{Info: s.InfoOf(rel), Lock: lock, Op: op, Role: pgcontract.TargetRoleImplicit})
		}
	}
	return targets
}

// ruleDropType is R-TY-DROP, which covers DROP DOMAIN.
func ruleDropType(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-TY-DROP", "").OnNone()

	names := make([]string, 0, len(s.Statement.Subcommands))
	used := false
	for _, sub := range s.Statement.Subcommands {
		names = append(names, sub.ObjectName())
		targets := domainColumnTargets(s, sub.Object, pgcontract.LockAccessExclusive, pgcontract.OpKindMetadata)
		used = used || len(targets) > 0
		e.Touches = append(e.Touches, targets...)
	}
	subject := "the type"
	if len(names) > 0 {
		subject = "type " + names[0]
	}

	switch {
	case !used:
		e.Message = fmt.Sprintf("%s is removed from the catalog; nothing in the snapshot declares a column with it", subject)
	case s.Statement.Flags.Cascade:
		e.Message = fmt.Sprintf("CASCADE drops %s *and every column declared with it*, so the tables below lose data, not just a catalog row", subject)
	default:
		e.Message = fmt.Sprintf("%s is still used by the columns below", subject)
		return e.Reject(fmt.Sprintf("%s cannot be dropped while columns are declared with it; CASCADE would drop those columns", subject), pgcontract.AnyVersion)
	}
	return e
}
