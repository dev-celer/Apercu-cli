package pg_classify

import (
	"fmt"

	"apercu-cli/helper"
	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// ruleAddEnumValue is R-TY-ADDVALUE.
func ruleAddEnumValue(s scope) effect {
	value, name := enumClause(s)
	e := newEffect(s, "R-TY-ADDVALUE",
		fmt.Sprintf("value %q is appended to enum %s; the type is locked, no table is, and the new value cannot be used until the transaction that added it has committed", value, name))
	e.op = pg_contract.OpKindMetadata
	e.recursion = parentOnly
	// The type is not a relation, so there is nothing in the target vocabulary to carry it.
	return e.onNone()
}

// ruleRenameEnumValue is R-TY-RENAMEVALUE.
func ruleRenameEnumValue(s scope) effect {
	value, name := enumClause(s)
	e := newEffect(s, "R-TY-RENAMEVALUE", fmt.Sprintf("value %q of enum %s is renamed; every row already holding it reads back under the new spelling", value, name))
	e.recursion = parentOnly
	return e.onNone()
}

// enumClause is the value and the type name an ALTER TYPE … VALUE carries.
func enumClause(s scope) (string, string) {
	if len(s.statement.Subcommands) == 0 {
		return "", ""
	}
	sub := s.statement.Subcommands[0]
	return sub.Value, sub.ObjectName()
}

// typeClauseRules is the ALTER TYPE half of the clause registry.
var typeClauseRules = map[pg_parse.SubKind]func(scope, pg_parse.Subcommand) effect{
	pg_parse.SubAddColumn:       ruleAddAttribute,
	pg_parse.SubDropColumn:      ruleDropAttribute,
	pg_parse.SubAlterColumnType: ruleAlterAttribute,
}

// classifyAlterType is ALTER TYPE … ATTRIBUTE.
func classifyAlterType(s scope) ([]pg_contract.Finding, []pg_contract.Error) {
	return classifyClauses(s, typeClauseRules)
}

// ruleAddAttribute is R-TY-ATTR for ADD ATTRIBUTE.
func ruleAddAttribute(s scope, sub pg_parse.Subcommand) effect {
	e := newAttributeEffect(s, fmt.Sprintf("attribute %q is added to the composite type", sub.Name))
	return e.carriedToTypedTables(s, sub)
}

// ruleDropAttribute is R-TY-ATTR for DROP ATTRIBUTE.
func ruleDropAttribute(s scope, sub pg_parse.Subcommand) effect {
	e := newAttributeEffect(s, fmt.Sprintf("attribute %q is dropped from the composite type", sub.Name))
	return e.carriedToTypedTables(s, sub)
}

// ruleAlterAttribute is R-TY-ATTR for ALTER ATTRIBUTE.
func ruleAlterAttribute(s scope, sub pg_parse.Subcommand) effect {
	changed := ruleAlterColumnType(s, sub)
	e := newAttributeEffect(s, fmt.Sprintf("attribute %q changes type: %s", sub.Name, changed.message))
	e.op = changed.op

	// An ordinary column of the type cannot follow the change, and there is no CASCADE for it.
	if columns, _ := s.typeDependents(s.relation()); len(columns) > 0 {
		return e.reject(fmt.Sprintf("the type is used in the column %s.%s, and an ordinary column of a composite type cannot follow an attribute type change", s.relationOf(columns[0]), columns[0].Name), pg_contract.AnyVersion)
	}
	return e.carriedToTypedTables(s, sub)
}

// newAttributeEffect is the shape all three attribute clauses share: the composite type is a
// relation of its own, and nothing hangs below it.
func newAttributeEffect(s scope, message string) effect {
	e := newEffect(s, "R-TY-ATTR", message)
	e.recursion = parentOnly
	return e
}

// carriedToTypedTables is the tail all three share attribute classifier.
func (e effect) carriedToTypedTables(s scope, sub pg_parse.Subcommand) effect {
	_, typedTables := s.typeDependents(s.relation())
	if len(typedTables) == 0 {
		return e
	}
	if !sub.Flags.Cascade {
		return e.reject("a table is declared OF this type, so an attribute clause needs CASCADE to carry the change into it", pg_contract.AnyVersion)
	}

	for _, table := range typedTables {
		rel, ok := s.catalog.ByOID(table)
		if !ok {
			continue
		}
		e.extra = append(e.extra, pg_contract.Target{
			Relation: rel.Contract(),
			Lock:     pg_contract.LockAccessExclusive,
			OpKind:   e.op,
			Role:     pg_contract.TargetRoleImplicit,
		})
	}
	e.message += fmt.Sprintf("; %d table(s) declared OF this type are locked and changed with it", len(typedTables))
	return e
}

// typeDependents return everything that as to be locked when a type change.
func (s scope) typeDependents(info pg_catalog.RelationInfo) ([]pg_catalog.ColumnRef, []pg_catalog.OID) {
	if !info.Exists() {
		return nil, nil
	}
	composite, ok := s.catalog.TypeByName(info.Name.Schema, info.Name.Table, s.context.SearchPath)
	if !ok {
		return nil, nil
	}
	return s.catalog.TypeDependents(composite.OID)
}

// relationOf names the table a column reference points at.
func (s scope) relationOf(column pg_catalog.ColumnRef) string {
	if rel, ok := s.catalog.ByOID(column.RelID); ok {
		return rel.RelationName().String()
	}
	return "an unknown relation"
}

// domainClauseRules is the ALTER DOMAIN half of the clause registry.
var domainClauseRules = map[pg_parse.SubKind]func(scope, pg_parse.Subcommand) effect{
	pg_parse.SubAddConstraint: ruleDomainAddConstraint,
}

// classifyAlterDomain is ALTER DOMAIN.
func classifyAlterDomain(s scope) ([]pg_contract.Finding, []pg_contract.Error) {
	return classifyClauses(s, domainClauseRules)
}

// ruleDomainAddConstraint is R-TY-DOMAIN-ADD. The statement names no table: every table with a
// column of the domain is read to prove the constraint holds.
func ruleDomainAddConstraint(s scope, sub pg_parse.Subcommand) effect {
	notValid := sub.Constraint != nil && sub.Constraint.NotValid

	e := newEffect(s, "R-TY-DOMAIN-ADD", fmt.Sprintf("a constraint is added to domain %s, and every table holding a column of it is read to prove it", sub.ObjectName()))
	e.lock = pg_contract.LockShare
	e.op = pg_contract.OpKindScan
	e.recursion = parentOnly
	if notValid {
		// NOT VALID reads no user relation, the change is limited to the type only.
		e.op = pg_contract.OpKindMetadata
		e.message = fmt.Sprintf("NOT VALID records the constraint on domain %s and enforces it for new values only; no existing row is read and no table holding a column of it is opened", sub.ObjectName())
		return e.onNone()
	}

	e = e.onNone()
	e.extra = append(e.extra, s.domainColumnTargets(sub.Object, e.lock, e.op)...)
	if len(e.extra) == 0 {
		e.message += "; no column in the snapshot uses it, so nothing is read"
	}
	return e
}

// domainColumnTargets is the tables holding a column of a type.
func (s scope) domainColumnTargets(name helper.FullRelationName, lock pg_contract.Lock, op pg_contract.OpKind) []pg_contract.Target {
	domain, ok := s.catalog.TypeByName(name.Schema, name.Table, s.context.SearchPath)
	if !ok {
		return nil
	}
	columns, typedTables := s.catalog.TypeDependents(domain.OID)

	var targets []pg_contract.Target
	for _, column := range columns {
		if rel, ok := s.catalog.ByOID(column.RelID); ok {
			targets = append(targets, pg_contract.Target{
				Relation: rel.Contract(), Lock: lock, OpKind: op, Role: pg_contract.TargetRoleImplicit,
			})
		}
	}
	for _, oid := range typedTables {
		if rel, ok := s.catalog.ByOID(oid); ok {
			targets = append(targets, pg_contract.Target{
				Relation: rel.Contract(), Lock: lock, OpKind: op, Role: pg_contract.TargetRoleImplicit,
			})
		}
	}
	return targets
}

// ruleDropType is R-TY-DROP, which covers DROP DOMAIN.
func ruleDropType(s scope) effect {
	e := newEffect(s, "R-TY-DROP", "").onNone()
	e.op = pg_contract.OpKindMetadata
	e.recursion = parentOnly

	names := make([]string, 0, len(s.statement.Subcommands))
	used := false
	for _, sub := range s.statement.Subcommands {
		names = append(names, sub.ObjectName())
		targets := s.domainColumnTargets(sub.Object, pg_contract.LockAccessExclusive, pg_contract.OpKindMetadata)
		used = used || len(targets) > 0
		e.extra = append(e.extra, targets...)
	}
	subject := "the type"
	if len(names) > 0 {
		subject = "type " + names[0]
	}

	switch {
	case !used:
		e.message = fmt.Sprintf("%s is removed from the catalog; nothing in the snapshot declares a column with it", subject)
	case s.statement.Flags.Cascade:
		e.message = fmt.Sprintf("CASCADE drops %s *and every column declared with it*, so the tables below lose data, not just a catalog row", subject)
	default:
		e.message = fmt.Sprintf("%s is still used by the columns below", subject)
		return e.reject(fmt.Sprintf("%s cannot be dropped while columns are declared with it; CASCADE would drop those columns", subject), pg_contract.AnyVersion)
	}
	return e
}
