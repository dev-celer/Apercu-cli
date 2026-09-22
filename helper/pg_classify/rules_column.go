package pg_classify

import (
	"fmt"
	"slices"
	"strings"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// column resolves one column of the named table.
func (s scope) column(name string) (pg_catalog.ColumnInfo, bool) {
	if !s.relation().Exists() || name == "" {
		return pg_catalog.ColumnInfo{}, false
	}
	return s.catalog.Column(s.relation().Relation.OID, name)
}

// typeOf resolves a type as the statement spelled it, arrays included.
func (s scope) typeOf(ref pg_parse.TypeRef) (pg_catalog.Type, bool) {
	base, ok := s.catalog.TypeByName(ref.Schema(), ref.Base(), s.context.SearchPath)
	if !ok || ref.ArrayBounds == 0 {
		return base, ok
	}
	return s.catalog.ArrayOf(base.OID)
}

// ruleAddColumn is R-AT-ADDCOL.
func ruleAddColumn(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-ADDCOL", "")
	column := sub.Column
	if column == nil {
		e.op = pg_contract.OpKindRewrite
		e.message = "the column definition did not reach the IR, so the most expensive reading is assumed"
		return e
	}

	switch {
	case column.Generated == pg_parse.GeneratedVirtual:
		e.message = "a virtual generated column is computed on read, so no existing row is touched"
	case column.Generated == pg_parse.GeneratedStored:
		e.op = pg_contract.OpKindRewrite
		e.message = "a stored generated column has to be materialised for every existing row"
	case column.Generated.IsIdentity():
		e.op = pg_contract.OpKindRewrite
		e.message = "an identity column draws a sequence value for every existing row"
	case s.domainIsConstrained(column.Type):
		e.op = pg_contract.OpKindRewrite
		e.message = fmt.Sprintf("%s is a domain with constraints, which are checked against every row", column.Type)
	case s.defaultMayBeVolatile(column):
		e.op = pg_contract.OpKindRewrite
		e.message = "the DEFAULT can invoke a volatile function, so every existing row gets its own value written out"
	case buildsIndex(column.Constraints):
		e.op = pg_contract.OpKindScan
		e.message = "the inline UNIQUE or PRIMARY KEY builds an index over every existing row"
	default:
		e.message = "the column is added to the catalog only; existing rows keep their pages and read the default back"
	}
	return e
}

// domainIsConstrained: A domain with constraints turns an added column into a rewrite.
func (s scope) domainIsConstrained(ref pg_parse.TypeRef) bool {
	typ, ok := s.typeOf(ref)
	if !ok {
		return false
	}
	return s.catalog.DomainHasConstraints(typ.OID)
}

// defaultMayBeVolatile walks the DEFAULT expression. A volatile default is the one shape that
// costs a rewrite, because the server cannot store one value and hand it to every row.
func (s scope) defaultMayBeVolatile(column *pg_parse.ColumnDef) bool {
	expr := column.Default
	if expr == nil {
		for _, constraint := range column.Constraints {
			if constraint.Type == pg_parse.ConstraintDefault {
				expr = constraint.Expr
			}
		}
	}
	if expr == nil {
		return false
	}
	return s.volatilityOf(expr, column.Name).MayBeVolatile()
}

// buildsIndex reports whether an inline constraint list asks for an index over the table.
func buildsIndex(constraints []pg_parse.ConstraintDef) bool {
	for _, constraint := range constraints {
		if constraint.Type == pg_parse.ConstraintPrimaryKey || constraint.Type == pg_parse.ConstraintUnique {
			return true
		}
	}
	return false
}

// ruleDropColumn is R-AT-DROPCOL.
func ruleDropColumn(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-DROPCOL", "the column is marked dropped in the catalog; existing rows keep their pages until something else rewrites them")
	// ONLY stops the column being dropped from the children, not the children being locked: their
	// inheritance count still has to come down.
	e.childrenUnderOnly = true
	column, ok := s.column(sub.Name)
	if !ok {
		return e
	}
	dependents := s.catalog.ViewDependentsOfColumn(s.relation().Relation.OID, column.Num)
	if len(dependents) == 0 {
		return e
	}
	for _, dep := range dependents {
		rel, ok := s.catalog.ByOID(dep.DependentRelID)
		if !ok {
			continue
		}
		e.extra = append(e.extra, pg_contract.Target{
			Relation: rel.Contract(),
			Lock:     pg_contract.LockAccessExclusive,
			OpKind:   pg_contract.OpKindMetadata,
			Role:     pg_contract.TargetRoleImplicit,
		})
	}
	e.message += fmt.Sprintf("; %d view(s) read this column and are dropped with it under CASCADE, or refuse the statement without it", len(e.extra))
	return e
}

// ruleAlterColumnType is R-AT-TYPE.
func ruleAlterColumnType(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-TYPE", "")
	e.op = pg_contract.OpKindRewrite

	def := sub.Column
	if def == nil {
		e.message = "the new type did not reach the IR, so a rewrite is assumed"
		return e
	}

	old, hasOld := s.column(def.Name)
	target, hasTarget := s.typeOf(def.Type)
	oldType, _ := s.catalog.Type(old.TypeID)

	switch {
	case def.Using != nil && !def.Using.IsColumnRef(def.Name):
		e.message = "the USING expression computes a new value for every row"
	case !hasOld || !hasTarget:
		e.message = fmt.Sprintf("%q or %s is not in the snapshot, so a rewrite is assumed", def.Name, def.Type)
	case isTimestampSwap(oldType.Name, target.Name):
		if s.context.TimeZoneIsUTC() {
			e.op = pg_contract.OpKindMetadata
			e.message = "timestamp and timestamptz share a representation and TimeZone is UTC, so the conversion changes no byte"
		} else {
			e.message = fmt.Sprintf("converting between timestamp and timestamptz under TimeZone %q shifts every stored value", s.context.TimeZone)
		}
	default:
		mod, packable := s.catalog.EncodeTypmod(target.OID, def.Type.Typmods)
		switch {
		case !packable:
			e.message = fmt.Sprintf("the modifier of %s cannot be compared against the one the column carries, so a rewrite is assumed", def.Type)
		case s.catalog.TypeChangeRequiresRewrite(old.TypeID, old.TypeMod, target.OID, mod):
			e.message = fmt.Sprintf("%s to %s is not reachable without rebuilding every tuple", old.FormattedType, def.Type)
		default:
			e.op = pg_contract.OpKindMetadata
			e.message = fmt.Sprintf("%s to %s is a relabel: the stored bytes already fit", old.FormattedType, def.Type)
		}
	}

	// Check 2 — the indexes over the column are dropped and rebuilt whatever happens to the heap.
	for _, index := range s.indexesOn(old) {
		rel, ok := s.catalog.ByOID(index.IndexRelID)
		if !ok {
			continue
		}
		e.extra = append(e.extra, pg_contract.Target{
			Relation: rel.Contract(),
			Lock:     pg_contract.LockAccessExclusive,
			OpKind:   pg_contract.OpKindRewrite,
			Role:     pg_contract.TargetRoleResolved,
		})
	}
	if len(e.extra) > 0 {
		e.message += fmt.Sprintf("; %d index(es) over the column are rebuilt", len(e.extra))
	}
	return e
}

// indexesOn lists the indexes that carry a column.
func (s scope) indexesOn(column pg_catalog.ColumnInfo) []pg_catalog.Index {
	if !s.relation().Exists() || column.Num == 0 {
		return nil
	}
	var out []pg_catalog.Index
	for _, index := range s.catalog.IndexesOf(s.relation().Relation.OID) {
		if slices.Contains(index.Columns, column.Num) {
			out = append(out, index)
		}
	}
	return out
}

// isTimestampSwap reports the one type change whose cost is a session setting rather than a
// property of the types: timestamp and timestamptz are the same eight bytes.
func isTimestampSwap(from, to string) bool {
	pair := []string{from, to}
	return from != to &&
		slices.Contains(pair, "timestamp") && slices.Contains(pair, "timestamptz")
}

// ruleSetDefault is R-AT-SETDEF.
func ruleSetDefault(s scope, _ pg_parse.Subcommand) effect {
	return newEffect(s, "R-AT-SETDEF", "the default is recorded in the catalog and applies to later inserts only")
}

// ruleDropDefault is R-AT-DROPDEF.
func ruleDropDefault(s scope, _ pg_parse.Subcommand) effect {
	return newEffect(s, "R-AT-DROPDEF", "the default is removed from the catalog; no stored row changes")
}

// ruleSetNotNull is R-AT-SETNOTNULL.
func ruleSetNotNull(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-SETNOTNULL", "")
	if !s.relation().Exists() {
		e.op = pg_contract.OpKindScan
		e.message = "the table is not in the snapshot, so the full scan is assumed"
		return e
	}
	proof := s.catalog.NotNullProof(s.relation().Relation.OID, sub.Name)
	if proof.Proven() {
		e.message = fmt.Sprintf("%s already rules out NULL, so the server skips the verifying scan", proof)
		return e
	}
	e.op = pg_contract.OpKindScan
	e.message = "nothing standing proves the column is never null, so every row is read to verify it"
	return e
}

// ruleDropNotNull is R-AT-DROPNOTNULL.
func ruleDropNotNull(s scope, _ pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-DROPNOTNULL", "the constraint is dropped from the catalog; no row is read")
	// From 18 a NOT NULL is a constraint with an inheritance count, so the children are opened to
	// have theirs corrected even under ONLY. Up to 17 it is nothing but attnotnull on the parent's
	// own row, and ONLY really does stop there.
	e.childrenUnderOnly = s.atLeast18()
	return e
}

// ruleSetExpression is R-AT-SETEXPR ⟨VERSION >= 17⟩.
func ruleSetExpression(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-SETEXPR", "")
	column, ok := s.column(sub.Name)
	switch {
	case ok && column.Generated == "v":
		e.message = "a virtual generated column stores nothing, so a new expression is a catalog change"
	case ok:
		e.op = pg_contract.OpKindRewrite
		e.message = "a stored generated column holds the old expression's result in every row, so all of them are recomputed"
	default:
		e.op = pg_contract.OpKindRewrite
		e.message = "the column is not in the snapshot; a stored generated column is assumed, which recomputes every row"
	}
	return e
}

// ruleDropExpression is R-AT-DROPEXPR.
func ruleDropExpression(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-DROPEXPR", "the column keeps the values it has and stops being generated")
	if column, ok := s.column(sub.Name); ok && column.Generated == "v" {
		return e.reject("DROP EXPRESSION is not supported for virtual generated columns", pg_contract.AnyVersion)
	}
	return e
}

// ruleAddIdentity is R-AT-ADDIDENT.
func ruleAddIdentity(s scope, _ pg_parse.Subcommand) effect {
	return newEffect(s, "R-AT-ADDIDENT", "the column keeps its values and gains a sequence for later inserts")
}

// ruleSetIdentity is R-AT-SETIDENT.
func ruleSetIdentity(s scope, _ pg_parse.Subcommand) effect {
	return newEffect(s, "R-AT-SETIDENT", "the identity's sequence options change; no stored row is touched")
}

// ruleDropIdentity is R-AT-DROPIDENT.
func ruleDropIdentity(s scope, _ pg_parse.Subcommand) effect {
	return newEffect(s, "R-AT-DROPIDENT", "the column keeps its values and loses its sequence")
}

// ruleSetStatistics is R-AT-SETSTATS.
// more than the lock a vacuum takes.
func ruleSetStatistics(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-SETSTATS", fmt.Sprintf("the statistics target becomes %s, which the next ANALYZE reads", sub.Value))
	e.lock = pg_contract.LockShareUpdateExclusive
	return e
}

// ruleAttributeOptions is R-AT-ATTROPT.
func ruleAttributeOptions(s scope, sub pg_parse.Subcommand) effect {
	names := make([]string, 0, len(sub.Options))
	for _, option := range sub.Options {
		names = append(names, option.Name)
	}
	e := newEffect(s, "R-AT-ATTROPT", fmt.Sprintf("the planner estimate carried by %s is recorded on the column", strings.Join(names, ", ")))
	e.lock = pg_contract.LockShareUpdateExclusive
	e.recursion = parentOnly
	return e
}

// ruleSetStorage is R-AT-SETSTORAGE.
func ruleSetStorage(s scope, sub pg_parse.Subcommand) effect {
	return newEffect(s, "R-AT-SETSTORAGE", fmt.Sprintf("later writes store the column as %s; values already written keep their form", sub.Value))
}

// ruleSetCompression is R-AT-SETCOMPRESSION.
func ruleSetCompression(s scope, sub pg_parse.Subcommand) effect {
	return newEffect(s, "R-AT-SETCOMPRESSION", fmt.Sprintf("later writes compress the column with %s; values already written keep theirs", sub.Value))
}

// ruleRenameColumn is R-AT-RENAMECOL.
func ruleRenameColumn(s scope, sub pg_parse.Subcommand) effect {
	return newEffect(s, "R-AT-RENAMECOL", fmt.Sprintf("the column is renamed to %q in the catalog", sub.NewName))
}
