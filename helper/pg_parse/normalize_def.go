package pg_parse

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// columnDef converts a column declaration to it's IR ColumnDef object.
func columnDef(node *pg_query.Node) *ColumnDef {
	def, ok := node.GetNode().(*pg_query.Node_ColumnDef)
	if !ok {
		return nil
	}
	raw := def.ColumnDef

	column := &ColumnDef{
		Name: raw.Colname,
		Type: typeRef(raw.TypeName),
		loc:  int(raw.Location),
	}
	if raw.CollClause != nil {
		column.Collation = nameParts(raw.CollClause.Collname)
	}
	column.Default = convertExpr(raw.RawDefault)

	switch raw.Identity {
	case "a":
		column.Generated = GeneratedIdentityAlways
	case "d":
		column.Generated = GeneratedIdentityByDefault
	}
	if raw.Generated == "s" {
		column.Generated = GeneratedStored
	}

	for _, item := range raw.Constraints {
		constraint := constraintDef(item)
		if constraint == nil {
			continue
		}
		switch constraint.Type {
		case ConstraintDefault:
			column.Default = constraint.Expr
		case ConstraintGenerated:
			column.Generated = GeneratedStored
			column.GeneratedExpr = constraint.Expr
		case ConstraintIdentity:
			column.Generated = GeneratedIdentityByDefault
			if constraint.generatedWhen == "a" {
				column.Generated = GeneratedIdentityAlways
			}
		}
		switch constraint.Type {
		case ConstraintDefault, ConstraintGenerated, ConstraintIdentity:
			// Already represented by the column's own Generated and Default.
		default:
			column.Constraints = append(column.Constraints, *constraint)
		}
	}

	if raw.IsNotNull && !hasConstraint(column.Constraints, ConstraintNotNull) {
		column.Constraints = append(column.Constraints, ConstraintDef{Type: ConstraintNotNull})
	}
	return column
}

func hasConstraint(list []ConstraintDef, kind ConstraintType) bool {
	for _, constraint := range list {
		if constraint.Type == kind {
			return true
		}
	}
	return false
}

// constraintDef converts a constraint declaration into it's IR ConstraintDef Object.
func constraintDef(node *pg_query.Node) *ConstraintDef {
	wrapped, ok := node.GetNode().(*pg_query.Node_Constraint)
	if !ok {
		return nil
	}
	raw := wrapped.Constraint

	constraint := &ConstraintDef{
		Name:          raw.Conname,
		Type:          constraintType(raw.Contype),
		UsingIndex:    raw.Indexname,
		NotValid:      raw.SkipValidation,
		NoInherit:     raw.IsNoInherit,
		Deferrable:    raw.Deferrable,
		InitiallyDef:  raw.Initdeferred,
		Tablespace:    raw.Indexspace,
		Options:       options(raw.Options),
		generatedWhen: raw.GeneratedWhen,
		loc:           int(raw.Location),
	}

	switch constraint.Type {
	case ConstraintCheck, ConstraintDefault, ConstraintGenerated:
		constraint.Expr = convertExpr(raw.RawExpr)
	}

	constraint.Columns = nameParts(raw.Keys)
	if len(raw.FkAttrs) > 0 {
		constraint.Columns = nameParts(raw.FkAttrs)
	}
	if raw.Pktable != nil {
		constraint.References = relationRef(raw.Pktable)
		constraint.ReferencedColumns = nameParts(raw.PkAttrs)
	}
	if constraint.Type == ConstraintExclusion {
		constraint.Columns, constraint.KeyExprs = exclusionElements(raw.Exclusions)
	}
	return constraint
}

// exclusionElements splits an EXCLUDE list into the elements that name a column and the ones that are expressions.
func exclusionElements(exclusions []*pg_query.Node) ([]string, []*Expr) {
	var (
		columns []string
		exprs   []*Expr
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
			continue
		}
		if expr := convertExpr(elem.Expr); expr != nil {
			exprs = append(exprs, expr)
		}
	}
	return columns, exprs
}

func constraintType(contype pg_query.ConstrType) ConstraintType {
	switch contype {
	case pg_query.ConstrType_CONSTR_NULL:
		return ConstraintNull
	case pg_query.ConstrType_CONSTR_NOTNULL:
		return ConstraintNotNull
	case pg_query.ConstrType_CONSTR_DEFAULT:
		return ConstraintDefault
	case pg_query.ConstrType_CONSTR_IDENTITY:
		return ConstraintIdentity
	case pg_query.ConstrType_CONSTR_GENERATED:
		return ConstraintGenerated
	case pg_query.ConstrType_CONSTR_CHECK:
		return ConstraintCheck
	case pg_query.ConstrType_CONSTR_PRIMARY:
		return ConstraintPrimaryKey
	case pg_query.ConstrType_CONSTR_UNIQUE:
		return ConstraintUnique
	case pg_query.ConstrType_CONSTR_EXCLUSION:
		return ConstraintExclusion
	case pg_query.ConstrType_CONSTR_FOREIGN:
		return ConstraintForeignKey
	default:
		return ConstraintUnknown
	}
}
