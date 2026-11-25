package pgparse

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// figureColname is the server's FigureIndexColname: the name an index expression lends to the index's generated name,
// empty when it lends none and the server falls back to "expr".
func figureColname(node *pg_query.Node) string {
	name, _ := figureColnameInternal(node)
	return name
}

// figureColnameInternal is FigureColnameInternal: a name, and how strongly the expression suggests it. 2 is a name
// the expression carries, 1 a fallback a wrapping cast or CASE may still replace, 0 none.
func figureColnameInternal(node *pg_query.Node) (string, int) {
	switch n := node.GetNode().(type) {
	case *pg_query.Node_ColumnRef:
		if name := lastName(n.ColumnRef.Fields); name != "" && name != "*" {
			return name, 2
		}
	case *pg_query.Node_AIndirection:
		if name := lastName(n.AIndirection.Indirection); name != "" && name != "*" {
			return name, 2
		}
		return figureColnameInternal(n.AIndirection.Arg)
	case *pg_query.Node_FuncCall:
		if name := lastName(n.FuncCall.Funcname); name != "" {
			return name, 2
		}
	case *pg_query.Node_AExpr:
		if n.AExpr.Kind == pg_query.A_Expr_Kind_AEXPR_NULLIF {
			return "nullif", 2
		}
	case *pg_query.Node_TypeCast:
		name, strength := figureColnameInternal(n.TypeCast.Arg)
		if strength <= 1 && n.TypeCast.TypeName != nil {
			if typeName := lastName(n.TypeCast.TypeName.Names); typeName != "" {
				return typeName, 1
			}
		}
		return name, strength
	case *pg_query.Node_CollateClause:
		return figureColnameInternal(n.CollateClause.Arg)
	case *pg_query.Node_GroupingFunc:
		return "grouping", 2
	case *pg_query.Node_MergeSupportFunc:
		return "merge_action", 2
	case *pg_query.Node_SubLink:
		switch n.SubLink.SubLinkType {
		case pg_query.SubLinkType_EXISTS_SUBLINK:
			return "exists", 2
		case pg_query.SubLinkType_ARRAY_SUBLINK:
			return "array", 2
		}
	case *pg_query.Node_CaseExpr:
		name, strength := figureColnameInternal(n.CaseExpr.Defresult)
		if strength <= 1 {
			return "case", 1
		}
		return name, strength
	case *pg_query.Node_AArrayExpr:
		return "array", 2
	case *pg_query.Node_RowExpr:
		return "row", 2
	case *pg_query.Node_CoalesceExpr:
		return "coalesce", 2
	case *pg_query.Node_MinMaxExpr:
		if n.MinMaxExpr.Op == pg_query.MinMaxOp_IS_LEAST {
			return "least", 2
		}
		return "greatest", 2
	case *pg_query.Node_SqlvalueFunction:
		return sqlValueColname(n.SqlvalueFunction.Op), 2
	case *pg_query.Node_XmlExpr:
		return xmlColname(n.XmlExpr.Op), 2
	case *pg_query.Node_XmlSerialize:
		return "xmlserialize", 2
	}
	return "", 0
}

// lastName is the last part of a qualified name, empty for none.
func lastName(nodes []*pg_query.Node) string {
	parts := nameParts(nodes)
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

// sqlValueColname is the name FigureColnameInternal gives a SQL-standard value function, its keyword.
func sqlValueColname(op pg_query.SQLValueFunctionOp) string {
	switch op {
	case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP, pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP_N:
		return "current_timestamp"
	default:
		return sqlValueFunctionName(op)
	}
}

func xmlColname(op pg_query.XmlExprOp) string {
	switch op {
	case pg_query.XmlExprOp_IS_XMLCONCAT:
		return "xmlconcat"
	case pg_query.XmlExprOp_IS_XMLELEMENT:
		return "xmlelement"
	case pg_query.XmlExprOp_IS_XMLFOREST:
		return "xmlforest"
	case pg_query.XmlExprOp_IS_XMLPARSE:
		return "xmlparse"
	case pg_query.XmlExprOp_IS_XMLPI:
		return "xmlpi"
	case pg_query.XmlExprOp_IS_XMLROOT:
		return "xmlroot"
	case pg_query.XmlExprOp_IS_XMLSERIALIZE:
		return "xmlserialize"
	}
	return ""
}
