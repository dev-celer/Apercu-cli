package pgparse

import (
	"apercu-cli/internal/pgparse/ir"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// columnDef converts a column declaration to it's IR ColumnDef object.
func columnDef(node *pg_query.Node) *ir.ColumnDef {
	def, ok := node.GetNode().(*pg_query.Node_ColumnDef)
	if !ok {
		return nil
	}
	raw := def.ColumnDef

	column := &ir.ColumnDef{
		Name: raw.Colname,
		Type: typeRef(raw.TypeName),
	}
	if raw.CollClause != nil {
		column.Collation = nameParts(raw.CollClause.Collname)
	}
	column.Default = convertExpr(raw.RawDefault)

	switch raw.Identity {
	case "a":
		column.Generated = ir.GeneratedIdentityAlways
	case "d":
		column.Generated = ir.GeneratedIdentityByDefault
	}
	if raw.Generated == "s" {
		column.Generated = ir.GeneratedStored
	}

	for _, item := range raw.Constraints {
		constraint := constraintDef(item)
		if constraint == nil {
			continue
		}
		switch constraint.Type {
		case ir.ConstraintDefault:
			column.Default = constraint.Expr
		case ir.ConstraintGenerated:
			column.Generated = ir.GeneratedStored
			column.GeneratedExpr = constraint.Expr
		case ir.ConstraintIdentity:
			column.Generated = ir.GeneratedIdentityByDefault
			if generatedWhen(item) == "a" {
				column.Generated = ir.GeneratedIdentityAlways
			}
		}
		switch constraint.Type {
		case ir.ConstraintDefault, ir.ConstraintGenerated, ir.ConstraintIdentity:
			// Already represented by the column's own Generated and Default.
		default:
			column.Constraints = append(column.Constraints, *constraint)
		}
	}

	if raw.IsNotNull && !hasConstraint(column.Constraints, ir.ConstraintNotNull) {
		column.Constraints = append(column.Constraints, ir.ConstraintDef{Type: ir.ConstraintNotNull})
	}
	return column
}

func hasConstraint(list []ir.ConstraintDef, kind ir.ConstraintType) bool {
	for _, constraint := range list {
		if constraint.Type == kind {
			return true
		}
	}
	return false
}

// constraintDef converts a constraint declaration into it's IR ConstraintDef Object.
func constraintDef(node *pg_query.Node) *ir.ConstraintDef {
	wrapped, ok := node.GetNode().(*pg_query.Node_Constraint)
	if !ok {
		return nil
	}
	raw := wrapped.Constraint

	constraint := &ir.ConstraintDef{
		Name:         raw.Conname,
		Type:         constraintType(raw.Contype),
		UsingIndex:   raw.Indexname,
		NotValid:     raw.SkipValidation,
		NoInherit:    raw.IsNoInherit,
		Deferrable:   raw.Deferrable,
		InitiallyDef: raw.Initdeferred,
		Tablespace:   raw.Indexspace,
		Options:      options(raw.Options),
	}

	switch constraint.Type {
	case ir.ConstraintCheck, ir.ConstraintDefault, ir.ConstraintGenerated:
		constraint.Expr = convertExpr(raw.RawExpr)
	}

	constraint.Columns = nameParts(raw.Keys)
	constraint.Including = nameParts(raw.Including)
	if len(raw.FkAttrs) > 0 {
		constraint.Columns = nameParts(raw.FkAttrs)
	}
	if raw.Pktable != nil {
		constraint.References = relationRef(raw.Pktable)
		constraint.ReferencedColumns = nameParts(raw.PkAttrs)
	}
	if constraint.Type == ir.ConstraintExclusion {
		constraint.Columns, constraint.KeyExprs, constraint.KeyNames = exclusionElements(raw.Exclusions)
	}
	return constraint
}

// exclusionElements splits an EXCLUDE list into the elements that name a column and the ones that are expressions,
// and lists what the server calls each one: the column, the function an expression calls, or "expr".
func exclusionElements(exclusions []*pg_query.Node) ([]string, []*ir.Expr, []string) {
	var (
		columns []string
		exprs   []*ir.Expr
		names   []string
	)
	for _, item := range exclusions {
		list, ok := item.GetNode().(*pg_query.Node_List)
		if !ok || len(list.List.Items) == 0 {
			continue
		}
		wrapped, ok := list.List.Items[0].GetNode().(*pg_query.Node_IndexElem)
		if !ok {
			continue
		}
		elem := wrapped.IndexElem
		if elem.Name != "" {
			columns = append(columns, elem.Name)
			names = append(names, elem.Name)
			continue
		}
		name := "expr"
		if expr := convertExpr(elem.Expr); expr != nil {
			exprs = append(exprs, expr)
			if expr.Kind == ir.ExprFuncCall && len(expr.Name) > 0 {
				name = expr.Name[len(expr.Name)-1]
			}
		}
		names = append(names, name)
	}
	return columns, exprs, names
}

func constraintType(contype pg_query.ConstrType) ir.ConstraintType {
	switch contype {
	case pg_query.ConstrType_CONSTR_NULL:
		return ir.ConstraintNull
	case pg_query.ConstrType_CONSTR_NOTNULL:
		return ir.ConstraintNotNull
	case pg_query.ConstrType_CONSTR_DEFAULT:
		return ir.ConstraintDefault
	case pg_query.ConstrType_CONSTR_IDENTITY:
		return ir.ConstraintIdentity
	case pg_query.ConstrType_CONSTR_GENERATED:
		return ir.ConstraintGenerated
	case pg_query.ConstrType_CONSTR_CHECK:
		return ir.ConstraintCheck
	case pg_query.ConstrType_CONSTR_PRIMARY:
		return ir.ConstraintPrimaryKey
	case pg_query.ConstrType_CONSTR_UNIQUE:
		return ir.ConstraintUnique
	case pg_query.ConstrType_CONSTR_EXCLUSION:
		return ir.ConstraintExclusion
	case pg_query.ConstrType_CONSTR_FOREIGN:
		return ir.ConstraintForeignKey
	default:
		return ir.ConstraintUnknown
	}
}

// generatedWhen is the parser's ALWAYS / BY DEFAULT marker on an identity constraint: "a", "d", or
// empty when the node is not a constraint.
func generatedWhen(node *pg_query.Node) string {
	return node.GetConstraint().GetGeneratedWhen()
}
