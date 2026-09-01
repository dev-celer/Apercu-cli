package pg_parse

import (
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ExprKind is what an expression node is.
type ExprKind uint8

const (
	// ExprUnknown is a node the converter does not model. Its children are still populated, so a call nested inside it is never lost.
	ExprUnknown ExprKind = iota
	ExprConst
	ExprColumnRef
	ExprParam
	ExprFuncCall
	// ExprOperator is an operator application, which runs the operator's own function.
	ExprOperator
	// ExprCast is x::t or CAST(x AS t), which runs the cast function.
	ExprCast
	ExprCase
	ExprBoolean
	ExprNullTest
	// ExprList is a ROW(), an ARRAY[] or an IN list.
	ExprList
	// ExprSubquery is any SELECT nested in the expression.
	ExprSubquery
	ExprCollate
)

var exprKindNames = map[ExprKind]string{
	ExprUnknown:   "UNKNOWN",
	ExprConst:     "CONST",
	ExprColumnRef: "COLUMN",
	ExprParam:     "PARAM",
	ExprFuncCall:  "FUNC",
	ExprOperator:  "OP",
	ExprCast:      "CAST",
	ExprCase:      "CASE",
	ExprBoolean:   "BOOL",
	ExprNullTest:  "NULLTEST",
	ExprList:      "LIST",
	ExprSubquery:  "SUBQUERY",
	ExprCollate:   "COLLATE",
}

func (k ExprKind) String() string {
	if name, ok := exprKindNames[k]; ok {
		return name
	}
	return "UNKNOWN"
}

// Expr is one node of a parsed expression.
type Expr struct {
	Kind ExprKind
	// Name is the qualified name of the function or operator, or the target type of a cast.
	// It is empty for the kinds that invoke nothing.
	Name []string
	Args []Expr
	// Literal is the source spelling of a constant or the dotted name of a column reference.
	Literal string
}

// String renders the node for a test failure or a finding message. It is a description, not SQL.
func (e Expr) String() string {
	switch e.Kind {
	case ExprConst:
		return e.Literal
	case ExprColumnRef:
		return e.Literal
	default:
		var b strings.Builder
		b.WriteString(e.Kind.String())
		if len(e.Name) > 0 {
			b.WriteString(":" + strings.Join(e.Name, "."))
		}
		parts := make([]string, len(e.Args))
		for i, arg := range e.Args {
			parts[i] = arg.String()
		}
		b.WriteString("(" + strings.Join(parts, ", ") + ")")
		return b.String()
	}
}

// Walk visits the node and its descendants in pre-order. Returning false from fn skips that node's children.
func (e *Expr) Walk(fn func(*Expr) bool) {
	if e == nil {
		return
	}
	if !fn(e) {
		return
	}
	for i := range e.Args {
		e.Args[i].Walk(fn)
	}
}

// Calls is every function the expression can invoke.
func (e *Expr) Calls() [][]string {
	var calls [][]string
	e.Walk(func(node *Expr) bool {
		switch node.Kind {
		case ExprFuncCall, ExprOperator, ExprCast:
			if len(node.Name) > 0 {
				calls = append(calls, node.Name)
			}
		}
		return true
	})
	return calls
}

// HasSubquery reports whether a SELECT is nested anywhere in the expression.
func (e *Expr) HasSubquery() bool {
	found := false
	e.Walk(func(node *Expr) bool {
		if node.Kind == ExprSubquery {
			found = true
			return false
		}
		return true
	})
	return found
}

// IsColumnRef reports whether the expression is nothing but a reference to the named column.
func (e *Expr) IsColumnRef(column string) bool {
	if e == nil || e.Kind != ExprColumnRef {
		return false
	}
	return strings.EqualFold(e.Literal, column)
}

// convertExpr turns a parsed expression into the IR Expr object.
//
// It is scoped to the five places this package carries an expression:
// a column DEFAULT, a CHECK constraint, a generated-column expression, an index predicate, and an ALTER COLUMN TYPE ... USING transform.
//
// It is not a general SQL expression converter.
// It answers exactly two questions: which functions can this invoke (Calls) and does it reach a relation the statement never names (HasSubquery).
func convertExpr(node *pg_query.Node) *Expr {
	if node == nil || node.Node == nil {
		return nil
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_AConst:
		return &Expr{Kind: ExprConst, Literal: constLiteral(n.AConst)}

	case *pg_query.Node_ColumnRef:
		return &Expr{Kind: ExprColumnRef, Literal: strings.Join(nameParts(n.ColumnRef.Fields), ".")}

	case *pg_query.Node_ParamRef:
		return &Expr{Kind: ExprParam, Literal: "$" + strconv.Itoa(int(n.ParamRef.Number))}

	case *pg_query.Node_FuncCall:
		call := &Expr{Kind: ExprFuncCall, Name: nameParts(n.FuncCall.Funcname)}
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
		op := &Expr{Kind: ExprOperator, Name: nameParts(n.AExpr.Name)}
		if arg := convertExpr(n.AExpr.Lexpr); arg != nil {
			op.Args = append(op.Args, *arg)
		}
		if arg := convertExpr(n.AExpr.Rexpr); arg != nil {
			op.Args = append(op.Args, *arg)
		}
		return op

	case *pg_query.Node_TypeCast:
		cast := &Expr{Kind: ExprCast}
		if n.TypeCast.TypeName != nil {
			cast.Name = nameParts(n.TypeCast.TypeName.Names)
		}
		if arg := convertExpr(n.TypeCast.Arg); arg != nil {
			cast.Args = append(cast.Args, *arg)
		}
		return cast

	case *pg_query.Node_CollateClause:
		collate := &Expr{Kind: ExprCollate, Name: nameParts(n.CollateClause.Collname)}
		if arg := convertExpr(n.CollateClause.Arg); arg != nil {
			collate.Args = append(collate.Args, *arg)
		}
		return collate

	case *pg_query.Node_SqlvalueFunction:
		return &Expr{Kind: ExprFuncCall, Name: []string{sqlValueFunctionName(n.SqlvalueFunction.Op)}}

	case *pg_query.Node_BoolExpr:
		return &Expr{Kind: ExprBoolean, Name: []string{boolOpName(n.BoolExpr.Boolop)}, Args: convertExprs(n.BoolExpr.Args)}

	case *pg_query.Node_NullTest:
		test := &Expr{Kind: ExprNullTest, Name: []string{nullTestName(n.NullTest.Nulltesttype)}}
		if arg := convertExpr(n.NullTest.Arg); arg != nil {
			test.Args = append(test.Args, *arg)
		}
		return test

	case *pg_query.Node_CaseExpr:
		branch := &Expr{Kind: ExprCase}
		if arg := convertExpr(n.CaseExpr.Arg); arg != nil {
			branch.Args = append(branch.Args, *arg)
		}
		branch.Args = append(branch.Args, convertExprs(n.CaseExpr.Args)...)
		if arg := convertExpr(n.CaseExpr.Defresult); arg != nil {
			branch.Args = append(branch.Args, *arg)
		}
		return branch

	case *pg_query.Node_CoalesceExpr:
		return &Expr{Kind: ExprFuncCall, Name: []string{"coalesce"}, Args: convertExprs(n.CoalesceExpr.Args)}

	case *pg_query.Node_MinMaxExpr:
		name := "greatest"
		if n.MinMaxExpr.Op == pg_query.MinMaxOp_IS_LEAST {
			name = "least"
		}
		return &Expr{Kind: ExprFuncCall, Name: []string{name}, Args: convertExprs(n.MinMaxExpr.Args)}

	case *pg_query.Node_AArrayExpr:
		return &Expr{Kind: ExprList, Args: convertExprs(n.AArrayExpr.Elements)}

	case *pg_query.Node_RowExpr:
		return &Expr{Kind: ExprList, Args: convertExprs(n.RowExpr.Args)}

	case *pg_query.Node_List:
		return &Expr{Kind: ExprList, Args: convertExprs(n.List.Items)}

	case *pg_query.Node_SortBy:
		// A sort item is not an expression, it wraps one.
		// Unwrap to the sorted expression unless the item names an operator:
		// "ORDER BY x USING >" runs that operator's function like any other call.
		sorted := convertExpr(n.SortBy.Node)
		if len(n.SortBy.UseOp) == 0 {
			return sorted
		}
		using := &Expr{Kind: ExprOperator, Name: nameParts(n.SortBy.UseOp)}
		if sorted != nil {
			using.Args = append(using.Args, *sorted)
		}
		return using

	case *pg_query.Node_SubLink:
		// The subquery's own body stays unmodelled
		link := &Expr{Kind: ExprSubquery}
		tested := convertExpr(n.SubLink.Testexpr)
		if len(n.SubLink.OperName) > 0 {
			operator := Expr{Kind: ExprOperator, Name: nameParts(n.SubLink.OperName)}
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
	return &Expr{Kind: ExprUnknown, Args: nestedExprs(node.ProtoReflect())}
}

// windowExprs is every expression an OVER clause carries: the partition and sort keys and the frame offsets.
// The window's name and frame mode invoke nothing, so they are dropped.
func windowExprs(window *pg_query.WindowDef) []Expr {
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

func convertExprs(nodes []*pg_query.Node) []Expr {
	var out []Expr
	for _, node := range nodes {
		if converted := convertExpr(node); converted != nil {
			out = append(out, *converted)
		}
	}
	return out
}

// nestedExprs walks an unmodelled message and converts every expression node hanging off it.
func nestedExprs(msg protoreflect.Message) []Expr {
	var out []Expr
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
func fromMessage(msg protoreflect.Message) []Expr {
	if node, ok := msg.Interface().(*pg_query.Node); ok {
		if converted := convertExpr(node); converted != nil {
			return []Expr{*converted}
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
