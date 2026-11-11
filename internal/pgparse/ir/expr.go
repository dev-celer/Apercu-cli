package ir

import (
	"strings"
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
	// Form record the kind of operator used, multiple operator share a name and differ on by kind.
	Form string
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
