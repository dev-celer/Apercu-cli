package rules

import (
	"fmt"
	"slices"
	"strings"

	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgclassify/prove"
	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// typeOf resolves a type as the statement spelled it, arrays included.
func typeOf(s engine.Scope, ref ir.TypeRef) (snapshot.Type, bool) {
	base, ok := s.Catalog.TypeByName(ref.Schema(), ref.Base(), s.Context.SearchPath)
	if !ok || ref.ArrayBounds == 0 {
		return base, ok
	}
	return s.Catalog.ArrayOf(base.OID)
}

// ruleAddColumn is R-AT-ADDCOL.
func ruleAddColumn(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-ADDCOL", "")
	column := sub.Column
	if column == nil {
		e.Op = pgcontract.OpKindRewrite
		e.Message = "the column definition did not reach the IR, so the most expensive reading is assumed"
		return e
	}

	switch {
	case column.Generated == ir.GeneratedVirtual:
		e.Message = "a virtual generated column is computed on read, so no existing row is touched"
	case column.Generated == ir.GeneratedStored:
		e.Op = pgcontract.OpKindRewrite
		e.Message = "a stored generated column has to be materialised for every existing row"
	case column.Generated.IsIdentity():
		e.Op = pgcontract.OpKindRewrite
		e.Message = "an identity column draws a sequence value for every existing row"
	case domainIsConstrained(s, column.Type):
		e.Op = pgcontract.OpKindRewrite
		e.Message = fmt.Sprintf("%s is a domain with constraints, which are checked against every row", column.Type)
	case defaultMayBeVolatile(s, column):
		e.Op = pgcontract.OpKindRewrite
		e.Message = "the DEFAULT can invoke a volatile function, so every existing row gets its own value written out"
	case buildsIndex(column.Constraints):
		e.Op = pgcontract.OpKindScan
		e.Message = "the inline UNIQUE or PRIMARY KEY builds an index over every existing row"
	default:
		e.Message = "the column is added to the catalog only; existing rows keep their pages and read the default back"
	}
	return e
}

// domainIsConstrained: A domain with constraints turns an added column into a rewrite.
func domainIsConstrained(s engine.Scope, ref ir.TypeRef) bool {
	typ, ok := typeOf(s, ref)
	if !ok {
		return false
	}
	return s.Catalog.DomainHasConstraints(typ.OID)
}

// defaultMayBeVolatile walks the DEFAULT expression. A volatile default is the one shape that
// costs a rewrite, because the server cannot store one value and hand it to every row.
func defaultMayBeVolatile(s engine.Scope, column *ir.ColumnDef) bool {
	expr := column.Default
	if expr == nil {
		for _, constraint := range column.Constraints {
			if constraint.Type == ir.ConstraintDefault {
				expr = constraint.Expr
			}
		}
	}
	if expr == nil {
		return false
	}
	return prove.VolatilityOf(s.Scope, expr, column.Name).MayBeVolatile()
}

// buildsIndex reports whether an inline constraint list asks for an index over the table.
func buildsIndex(constraints []ir.ConstraintDef) bool {
	for _, constraint := range constraints {
		if constraint.Type == ir.ConstraintPrimaryKey || constraint.Type == ir.ConstraintUnique {
			return true
		}
	}
	return false
}

// ruleDropColumn is R-AT-DROPCOL.
func ruleDropColumn(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-DROPCOL", "the column is marked dropped in the catalog; existing rows keep their pages until something else rewrites them")
	column, ok := s.Column(sub.Name)
	if !ok {
		return e
	}
	dependents := s.Catalog.ViewDependentsOfColumn(s.Relation().Relation.OID, column.Num)
	if len(dependents) == 0 {
		return e
	}
	for _, dep := range dependents {
		rel, ok := s.Catalog.ByOID(dep.DependentRelID)
		if !ok {
			continue
		}
		e.Touches = append(e.Touches, engine.Touch{
			Info: s.InfoOf(rel),
			Lock: pgcontract.LockAccessExclusive,
			Op:   pgcontract.OpKindMetadata,
			Role: pgcontract.TargetRoleImplicit,
		})
	}
	e.Message += fmt.Sprintf("; %d view(s) read this column and are dropped with it under CASCADE, or refuse the statement without it", len(e.Touches))
	return e
}

// ruleAlterColumnType is R-AT-TYPE.
func ruleAlterColumnType(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-TYPE", "")

	def := sub.Column
	if def == nil {
		e.Message = "the new type did not reach the IR, so a rewrite is assumed"
		return e
	}

	old, hasOld := s.Column(def.Name)
	target, hasTarget := typeOf(s, def.Type)
	oldType, _ := s.Catalog.Type(old.TypeID)

	switch {
	case def.Using != nil && !def.Using.IsColumnRef(def.Name):
		e.Message = "the USING expression computes a new value for every row"
	case !hasOld || !hasTarget:
		e.Message = fmt.Sprintf("%q or %s is not in the snapshot, so a rewrite is assumed", def.Name, def.Type)
	case isTimestampSwap(oldType.Name, target.Name):
		if s.Context.TimeZoneIsUTC() {
			e.Op = pgcontract.OpKindMetadata
			e.Message = "timestamp and timestamptz share a representation and TimeZone is UTC, so the conversion changes no byte"
		} else {
			e.Message = fmt.Sprintf("converting between timestamp and timestamptz under TimeZone %q shifts every stored value", s.Context.TimeZone)
		}
	default:
		mod, packable := s.Catalog.EncodeTypmod(target.OID, def.Type.Typmods)
		switch {
		case !packable:
			e.Message = fmt.Sprintf("the modifier of %s cannot be compared against the one the column carries, so a rewrite is assumed", def.Type)
		case s.Catalog.TypeChangeRequiresRewrite(old.TypeID, old.TypeMod, target.OID, mod):
			e.Message = fmt.Sprintf("%s to %s is not reachable without rebuilding every tuple", old.FormattedType, def.Type)
		default:
			e.Op = pgcontract.OpKindMetadata
			e.Message = fmt.Sprintf("%s to %s is a relabel: the stored bytes already fit", old.FormattedType, def.Type)
		}
	}

	// Check 2 — the indexes over the column are dropped and rebuilt whatever happens to the heap.
	for _, index := range indexesOn(s, old) {
		rel, ok := s.Catalog.ByOID(index.IndexRelID)
		if !ok {
			continue
		}
		e.Touches = append(e.Touches, engine.Touch{
			Info: s.InfoOf(rel),
			Lock: pgcontract.LockAccessExclusive,
			Op:   pgcontract.OpKindRewrite,
			Role: pgcontract.TargetRoleResolved,
		})
	}
	if len(e.Touches) > 0 {
		e.Message += fmt.Sprintf("; %d index(es) over the column are rebuilt", len(e.Touches))
	}
	return e
}

// indexesOn lists the indexes that carry a column.
func indexesOn(s engine.Scope, column pgcatalog.ColumnInfo) []snapshot.Index {
	if !s.Relation().Exists() || column.Num == 0 {
		return nil
	}
	var out []snapshot.Index
	for _, index := range s.Catalog.IndexesOf(s.Relation().Relation.OID) {
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
func ruleSetDefault(s engine.Scope, _ ir.Subcommand) engine.Effect {
	return engine.NewEffect(s, "R-AT-SETDEF", "the default is recorded in the catalog and applies to later inserts only")
}

// ruleDropDefault is R-AT-DROPDEF.
func ruleDropDefault(s engine.Scope, _ ir.Subcommand) engine.Effect {
	return engine.NewEffect(s, "R-AT-DROPDEF", "the default is removed from the catalog; no stored row changes")
}

// ruleSetNotNull is R-AT-SETNOTNULL.
func ruleSetNotNull(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-SETNOTNULL", "")
	if !s.Relation().Exists() {
		e.Op = pgcontract.OpKindScan
		e.Message = "the table is not in the snapshot, so the full scan is assumed"
		return e
	}
	// Every table below is verified on its own: a CHECK of its own spares it the scan the parent needs.
	e.LeafOp = func(leaf pgcatalog.RelationInfo) pgcontract.OpKind {
		if s.Catalog.NotNullProof(leaf.Relation.OID, sub.Name).Proven() {
			return pgcontract.OpKindMetadata
		}
		return pgcontract.OpKindScan
	}
	proof := s.Catalog.NotNullProof(s.Relation().Relation.OID, sub.Name)
	if proof.Proven() {
		e.Message = fmt.Sprintf("%s already rules out NULL, so the server skips the verifying scan", proof)
		return e
	}
	e.Op = pgcontract.OpKindScan
	e.Message = "nothing standing proves the column is never null, so every row is read to verify it"
	return e
}

// ruleDropNotNull is R-AT-DROPNOTNULL.
func ruleDropNotNull(s engine.Scope, _ ir.Subcommand) engine.Effect {
	return engine.NewEffect(s, "R-AT-DROPNOTNULL", "the constraint is dropped from the catalog; no row is read")
}

// ruleSetExpression is R-AT-SETEXPR ⟨VERSION >= 17⟩.
func ruleSetExpression(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-SETEXPR", "")
	column, ok := s.Column(sub.Name)
	switch {
	case ok && column.Generated == "v":
		e.Message = "a virtual generated column stores nothing, so a new expression is a catalog change"
	case ok:
		e.Op = pgcontract.OpKindRewrite
		e.Message = "a stored generated column holds the old expression's result in every row, so all of them are recomputed"
	default:
		e.Op = pgcontract.OpKindRewrite
		e.Message = "the column is not in the snapshot; a stored generated column is assumed, which recomputes every row"
	}
	return e
}

// ruleDropExpression is R-AT-DROPEXPR.
func ruleDropExpression(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-DROPEXPR", "the column keeps the values it has and stops being generated")
	if column, ok := s.Column(sub.Name); ok && column.Generated == "v" {
		e.Op = pgcontract.OpKindRewrite
		e.Message = "a virtual generated column stores nothing, so keeping its values means writing them into every row"
	}
	return e
}

// ruleAddIdentity is R-AT-ADDIDENT.
func ruleAddIdentity(s engine.Scope, _ ir.Subcommand) engine.Effect {
	return engine.NewEffect(s, "R-AT-ADDIDENT", "the column keeps its values and gains a sequence for later inserts")
}

// ruleSetIdentity is R-AT-SETIDENT.
func ruleSetIdentity(s engine.Scope, _ ir.Subcommand) engine.Effect {
	return engine.NewEffect(s, "R-AT-SETIDENT", "the identity's sequence options change; no stored row is touched")
}

// ruleDropIdentity is R-AT-DROPIDENT.
func ruleDropIdentity(s engine.Scope, _ ir.Subcommand) engine.Effect {
	return engine.NewEffect(s, "R-AT-DROPIDENT", "the column keeps its values and loses its sequence")
}

// ruleSetStatistics is R-AT-SETSTATS.
// more than the lock a vacuum takes.
func ruleSetStatistics(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-SETSTATS", fmt.Sprintf("the statistics target becomes %s, which the next ANALYZE reads", sub.Value))
	return e
}

// ruleAttributeOptions is R-AT-ATTROPT.
func ruleAttributeOptions(s engine.Scope, sub ir.Subcommand) engine.Effect {
	names := make([]string, 0, len(sub.Options))
	for _, option := range sub.Options {
		names = append(names, option.Name)
	}
	e := engine.NewEffect(s, "R-AT-ATTROPT", fmt.Sprintf("the planner estimate carried by %s is recorded on the column", strings.Join(names, ", ")))
	return e
}

// ruleSetStorage is R-AT-SETSTORAGE.
func ruleSetStorage(s engine.Scope, sub ir.Subcommand) engine.Effect {
	return engine.NewEffect(s, "R-AT-SETSTORAGE", fmt.Sprintf("later writes store the column as %s; values already written keep their form", sub.Value))
}

// ruleSetCompression is R-AT-SETCOMPRESSION.
func ruleSetCompression(s engine.Scope, sub ir.Subcommand) engine.Effect {
	return engine.NewEffect(s, "R-AT-SETCOMPRESSION", fmt.Sprintf("later writes compress the column with %s; values already written keep theirs", sub.Value))
}

// ruleRenameColumn is R-AT-RENAMECOL.
func ruleRenameColumn(s engine.Scope, sub ir.Subcommand) engine.Effect {
	return engine.NewEffect(s, "R-AT-RENAMECOL", fmt.Sprintf("the column is renamed to %q in the catalog", sub.NewName))
}
