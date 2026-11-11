package pgparse

import (
	"strconv"
	"strings"

	"apercu-cli/internal/pgparse/ir"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// convertExpr turns a parsed expression into the IR Expr object.
//
// It is scoped to the five places this package carries an expression:
// a column DEFAULT, a CHECK constraint, a generated-column expression, an index predicate, and an ALTER COLUMN TYPE ... USING transform.
//
// It is not a general SQL expression converter.
// It answers exactly two questions: which functions can this invoke (Calls) and does it reach a relation the statement never names (HasSubquery).
func convertExpr(node *pg_query.Node) *ir.Expr {
	if node == nil || node.Node == nil {
		return nil
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_AConst:
		return &ir.Expr{Kind: ir.ExprConst, Literal: constLiteral(n.AConst)}

	case *pg_query.Node_ColumnRef:
		return &ir.Expr{Kind: ir.ExprColumnRef, Literal: strings.Join(nameParts(n.ColumnRef.Fields), ".")}

	case *pg_query.Node_ParamRef:
		return &ir.Expr{Kind: ir.ExprParam, Literal: "$" + strconv.Itoa(int(n.ParamRef.Number))}

	case *pg_query.Node_FuncCall:
		call := &ir.Expr{Kind: ir.ExprFuncCall, Name: nameParts(n.FuncCall.Funcname)}
		call.Args = convertExprs(n.FuncCall.Args)
		// Everything else that can carry an expression is merged into Args, this is a simplification.
		// We don't have a use for knowing where exactlly the expression was called.
		if arg := convertExpr(n.FuncCall.AggFilter); arg != nil {
			call.Args = append(call.Args, *arg)
		}
		call.Args = append(call.Args, convertExprs(n.FuncCall.AggOrder)...)
		call.Args = append(call.Args, windowExprs(n.FuncCall.Over)...)
		return call

	case *pg_query.Node_AExpr:
		op := &ir.Expr{Kind: ir.ExprOperator, Name: nameParts(n.AExpr.Name), Form: operatorForm(n.AExpr)}
		if arg := convertExpr(n.AExpr.Lexpr); arg != nil {
			op.Args = append(op.Args, *arg)
		}
		if arg := convertExpr(n.AExpr.Rexpr); arg != nil {
			op.Args = append(op.Args, *arg)
		}
		return op

	case *pg_query.Node_TypeCast:
		cast := &ir.Expr{Kind: ir.ExprCast}
		if n.TypeCast.TypeName != nil {
			cast.Name = nameParts(n.TypeCast.TypeName.Names)
		}
		if arg := convertExpr(n.TypeCast.Arg); arg != nil {
			cast.Args = append(cast.Args, *arg)
		}
		return cast

	case *pg_query.Node_CollateClause:
		collate := &ir.Expr{Kind: ir.ExprCollate, Name: nameParts(n.CollateClause.Collname)}
		if arg := convertExpr(n.CollateClause.Arg); arg != nil {
			collate.Args = append(collate.Args, *arg)
		}
		return collate

	case *pg_query.Node_SqlvalueFunction:
		return &ir.Expr{Kind: ir.ExprFuncCall, Name: []string{sqlValueFunctionName(n.SqlvalueFunction.Op)}}

	case *pg_query.Node_BoolExpr:
		return &ir.Expr{Kind: ir.ExprBoolean, Name: []string{boolOpName(n.BoolExpr.Boolop)}, Args: convertExprs(n.BoolExpr.Args)}

	case *pg_query.Node_NullTest:
		test := &ir.Expr{Kind: ir.ExprNullTest, Name: []string{nullTestName(n.NullTest.Nulltesttype)}}
		if arg := convertExpr(n.NullTest.Arg); arg != nil {
			test.Args = append(test.Args, *arg)
		}
		return test

	case *pg_query.Node_CaseExpr:
		branch := &ir.Expr{Kind: ir.ExprCase}
		if arg := convertExpr(n.CaseExpr.Arg); arg != nil {
			branch.Args = append(branch.Args, *arg)
		}
		branch.Args = append(branch.Args, convertExprs(n.CaseExpr.Args)...)
		if arg := convertExpr(n.CaseExpr.Defresult); arg != nil {
			branch.Args = append(branch.Args, *arg)
		}
		return branch

	case *pg_query.Node_CoalesceExpr:
		return &ir.Expr{Kind: ir.ExprFuncCall, Name: []string{"coalesce"}, Args: convertExprs(n.CoalesceExpr.Args)}

	case *pg_query.Node_MinMaxExpr:
		name := "greatest"
		if n.MinMaxExpr.Op == pg_query.MinMaxOp_IS_LEAST {
			name = "least"
		}
		return &ir.Expr{Kind: ir.ExprFuncCall, Name: []string{name}, Args: convertExprs(n.MinMaxExpr.Args)}

	case *pg_query.Node_AArrayExpr:
		return &ir.Expr{Kind: ir.ExprList, Args: convertExprs(n.AArrayExpr.Elements)}

	case *pg_query.Node_RowExpr:
		return &ir.Expr{Kind: ir.ExprList, Args: convertExprs(n.RowExpr.Args)}

	case *pg_query.Node_List:
		return &ir.Expr{Kind: ir.ExprList, Args: convertExprs(n.List.Items)}

	case *pg_query.Node_SortBy:
		// A sort item is not an expression, it wraps one.
		// Unwrap to the sorted expression unless the item names an operator:
		// "ORDER BY x USING >" runs that operator's function like any other call.
		sorted := convertExpr(n.SortBy.Node)
		if len(n.SortBy.UseOp) == 0 {
			return sorted
		}
		using := &ir.Expr{Kind: ir.ExprOperator, Name: nameParts(n.SortBy.UseOp)}
		if sorted != nil {
			using.Args = append(using.Args, *sorted)
		}
		return using

	case *pg_query.Node_SubLink:
		// The subquery's own body stays unmodelled
		link := &ir.Expr{Kind: ir.ExprSubquery}
		tested := convertExpr(n.SubLink.Testexpr)
		if len(n.SubLink.OperName) > 0 {
			operator := ir.Expr{Kind: ir.ExprOperator, Name: nameParts(n.SubLink.OperName)}
			if tested != nil {
				operator.Args = append(operator.Args, *tested)
			}
			tested = &operator
		}
		if tested != nil {
			link.Args = append(link.Args, *tested)
		}
		return link
	}

	// For anything else, the node itself is unknown but every argument / potential call is preserved.
	return &ir.Expr{Kind: ir.ExprUnknown, Args: nestedExprs(node.ProtoReflect())}
}

// windowExprs is every expression an OVER clause carries: the partition and sort keys and the frame offsets.
// The window's name and frame mode invoke nothing, so they are dropped.
func windowExprs(window *pg_query.WindowDef) []ir.Expr {
	if window == nil {
		return nil
	}
	out := convertExprs(window.PartitionClause)
	out = append(out, convertExprs(window.OrderClause)...)
	if arg := convertExpr(window.StartOffset); arg != nil {
		out = append(out, *arg)
	}
	if arg := convertExpr(window.EndOffset); arg != nil {
		out = append(out, *arg)
	}
	return out
}

func convertExprs(nodes []*pg_query.Node) []ir.Expr {
	var out []ir.Expr
	for _, node := range nodes {
		if converted := convertExpr(node); converted != nil {
			out = append(out, *converted)
		}
	}
	return out
}

// nestedExprs walks an unmodelled message and converts every expression node hanging off it.
func nestedExprs(msg protoreflect.Message) []ir.Expr {
	var out []ir.Expr
	msg.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsList() && field.Kind() == protoreflect.MessageKind:
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				out = append(out, fromMessage(list.Get(i).Message())...)
			}
		case !field.IsList() && !field.IsMap() && field.Kind() == protoreflect.MessageKind:
			out = append(out, fromMessage(value.Message())...)
		}
		return true
	})
	return out
}

// fromMessage converts a message that is either a Node or a container to descend into.
func fromMessage(msg protoreflect.Message) []ir.Expr {
	if node, ok := msg.Interface().(*pg_query.Node); ok {
		if converted := convertExpr(node); converted != nil {
			return []ir.Expr{*converted}
		}
		return nil
	}
	return nestedExprs(msg)
}

// constLiteral renders a constant the way the statement wrote it.
func constLiteral(c *pg_query.A_Const) string {
	switch {
	case c == nil:
		return ""
	case c.Isnull:
		return "NULL"
	}
	switch v := c.Val.(type) {
	case *pg_query.A_Const_Ival:
		return strconv.Itoa(int(v.Ival.Ival))
	case *pg_query.A_Const_Fval:
		return v.Fval.Fval
	case *pg_query.A_Const_Boolval:
		return strconv.FormatBool(v.Boolval.Boolval)
	case *pg_query.A_Const_Sval:
		return "'" + v.Sval.Sval + "'"
	case *pg_query.A_Const_Bsval:
		return v.Bsval.Bsval
	default:
		return ""
	}
}

func operatorForm(expr *pg_query.A_Expr) string {
	switch expr.Kind {
	case pg_query.A_Expr_Kind_AEXPR_OP:
		return ""
	case pg_query.A_Expr_Kind_AEXPR_OP_ANY:
		return "ANY"
	case pg_query.A_Expr_Kind_AEXPR_OP_ALL:
		return "ALL"
	case pg_query.A_Expr_Kind_AEXPR_IN:
		if names := nameParts(expr.Name); len(names) == 1 && names[0] == "<>" {
			return "ALL"
		}
		return "ANY"
	case pg_query.A_Expr_Kind_AEXPR_DISTINCT:
		return "DISTINCT"
	case pg_query.A_Expr_Kind_AEXPR_NOT_DISTINCT:
		return "NOT DISTINCT"
	case pg_query.A_Expr_Kind_AEXPR_NULLIF:
		return "NULLIF"
	case pg_query.A_Expr_Kind_AEXPR_LIKE, pg_query.A_Expr_Kind_AEXPR_ILIKE, pg_query.A_Expr_Kind_AEXPR_SIMILAR:
		return "MATCH"
	default:
		return ""
	}
}

func boolOpName(op pg_query.BoolExprType) string {
	switch op {
	case pg_query.BoolExprType_AND_EXPR:
		return "AND"
	case pg_query.BoolExprType_OR_EXPR:
		return "OR"
	case pg_query.BoolExprType_NOT_EXPR:
		return "NOT"
	default:
		return "BOOL"
	}
}

func nullTestName(t pg_query.NullTestType) string {
	if t == pg_query.NullTestType_IS_NOT_NULL {
		return "IS NOT NULL"
	}
	return "IS NULL"
}

// sqlValueFunctionName spells the keyword form back as the function it resolves to, so §4.1 can
// look CURRENT_TIMESTAMP up in P-07 like any other call.
func sqlValueFunctionName(op pg_query.SQLValueFunctionOp) string {
	switch op {
	case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_DATE:
		return "current_date"
	case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIME, pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIME_N:
		return "current_time"
	case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP, pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP_N:
		return "now"
	case pg_query.SQLValueFunctionOp_SVFOP_LOCALTIME, pg_query.SQLValueFunctionOp_SVFOP_LOCALTIME_N:
		return "localtime"
	case pg_query.SQLValueFunctionOp_SVFOP_LOCALTIMESTAMP, pg_query.SQLValueFunctionOp_SVFOP_LOCALTIMESTAMP_N:
		return "localtimestamp"
	case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_ROLE:
		return "current_role"
	case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_USER:
		return "current_user"
	case pg_query.SQLValueFunctionOp_SVFOP_USER:
		return "user"
	case pg_query.SQLValueFunctionOp_SVFOP_SESSION_USER:
		return "session_user"
	case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_CATALOG:
		return "current_catalog"
	case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_SCHEMA:
		return "current_schema"
	default:
		return "unknown"
	}
}
