package pg_parse

import (
	"strconv"
	"strings"

	"apercu-cli/helper"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// nameParts flattens a list of String nodes into its parts.
func nameParts(nodes []*pg_query.Node) []string {
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		switch n := node.GetNode().(type) {
		case *pg_query.Node_String_:
			parts = append(parts, n.String_.Sval)
		case *pg_query.Node_AStar:
			parts = append(parts, "*")
		}
	}
	return parts
}

// relationRef converts a parsed table reference.
func relationRef(rv *pg_query.RangeVar) RelationRef {
	if rv == nil {
		return RelationRef{}
	}
	ref := RelationRef{
		Name: helper.FullRelationName{Schema: rv.Schemaname, Table: rv.Relname},
		Only: !rv.Inh,
	}
	if rv.Alias != nil {
		ref.Alias = rv.Alias.Aliasname
	}
	return ref
}

// objectRef converts the qualified-name form used where the grammar takes a name rather than a table reference.
func objectRef(nodes []*pg_query.Node) RelationRef {
	parts := nameParts(nodes)
	switch len(parts) {
	case 0:
		return RelationRef{}
	case 1:
		return RelationRef{Name: helper.FullRelationName{Table: parts[0]}}
	default:
		// A three-part name is catalog.schema.relation; the catalog is always the current
		// database, so dropping it loses nothing.
		return RelationRef{Name: helper.FullRelationName{
			Schema: parts[len(parts)-2],
			Table:  parts[len(parts)-1],
		}}
	}
}

// typeRef converts a type name to it's IR TypeRef Object.
func typeRef(tn *pg_query.TypeName) TypeRef {
	if tn == nil {
		return TypeRef{}
	}
	ref := TypeRef{
		Name:        nameParts(tn.Names),
		ArrayBounds: len(tn.ArrayBounds),
		PctType:     tn.PctType,
	}
	for _, mod := range tn.Typmods {
		ref.Typmods = append(ref.Typmods, literalText(mod))
	}
	return ref
}

// literalText renders a node that the grammar guarantees is a bare literal or identifier.
func literalText(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	switch n := node.GetNode().(type) {
	case *pg_query.Node_String_:
		return n.String_.Sval
	case *pg_query.Node_Integer:
		return strconv.Itoa(int(n.Integer.Ival))
	case *pg_query.Node_Float:
		return n.Float.Fval
	case *pg_query.Node_Boolean:
		return strconv.FormatBool(n.Boolean.Boolval)
	case *pg_query.Node_AConst:
		return constLiteral(n.AConst)
	case *pg_query.Node_TypeName:
		return typeRef(n.TypeName).String()
	case *pg_query.Node_List:
		parts := make([]string, 0, len(n.List.Items))
		for _, item := range n.List.Items {
			parts = append(parts, literalText(item))
		}
		return strings.Join(parts, ".")
	default:
		return ""
	}
}

// options converts a parenthesized option list.
func options(nodes []*pg_query.Node) []Option {
	var out []Option
	for _, node := range nodes {
		def, ok := node.GetNode().(*pg_query.Node_DefElem)
		if !ok {
			continue
		}
		out = append(out, Option{
			Namespace: def.DefElem.Defnamespace,
			Name:      def.DefElem.Defname,
			Value:     literalText(def.DefElem.Arg),
		})
	}
	return out
}

// findOption returns the value of an unqualified option, and whether the list named it at all.
func findOption(list []Option, name string) (string, bool) {
	for _, opt := range list {
		if opt.Namespace == "" && strings.EqualFold(opt.Name, name) {
			return opt.Value, true
		}
	}
	return "", false
}
