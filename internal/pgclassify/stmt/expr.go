package stmt

import (
	"slices"
	"strings"

	"apercu-cli/internal/pgparse/ir"
)

// TestsNull reports whether an expression tests the column for NULL anywhere, IS NULL included: a LIST bound that accepts NULL
// has no IS NOT NULL in its partition constraint, and a CHECK spelling the IS NULL branch is aware of it.
func TestsNull(expr *ir.Expr, column string) bool {
	found := false
	expr.Walk(func(node *ir.Expr) bool {
		if node.Kind == ir.ExprNullTest && len(node.Args) > 0 && refersTo(&node.Args[0], column) {
			found = true
		}
		return !found
	})
	return found
}

// Mentions reports whether an expression reads the column anywhere.
func Mentions(expr *ir.Expr, column string) bool {
	found := false
	expr.Walk(func(node *ir.Expr) bool {
		if refersTo(node, column) {
			found = true
		}
		return !found
	})
	return found
}

// ColumnsOf lists the distinct columns an expression reads, in the order it first reads them.
func ColumnsOf(expr *ir.Expr) []string {
	var columns []string
	expr.Walk(func(node *ir.Expr) bool {
		if node.Kind == ir.ExprColumnRef {
			parts := strings.Split(node.Literal, ".")
			if column := parts[len(parts)-1]; !slices.Contains(columns, column) {
				columns = append(columns, column)
			}
		}
		return true
	})
	return columns
}

// refersTo reports whether the node is a reference to the column, qualified by its table or not.
func refersTo(node *ir.Expr, column string) bool {
	if node.Kind != ir.ExprColumnRef {
		return false
	}
	parts := strings.Split(node.Literal, ".")
	return parts[len(parts)-1] == column
}
