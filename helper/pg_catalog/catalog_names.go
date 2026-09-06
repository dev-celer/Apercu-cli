package pg_catalog

import (
	"slices"
	"strings"

	"apercu-cli/helper"
	"apercu-cli/helper/pg_contract"
)

// qualifiedName is a catalog object addressed the way a statement writes it.
type qualifiedName struct {
	namespace string
	name      string
}

// columnRef names one column of one relation across snapshots, where the OIDs do not match.
type columnRef struct {
	relation helper.FullRelationName
	column   string
}

// nameIndex is the index lookup keyed on name.
type nameIndex struct {
	schemas   map[OID]string
	types     map[qualifiedName][]Type
	procs     map[qualifiedName][]Proc
	operators map[qualifiedName][]Operator
	castsTo   map[OID][]Cast

	tablespaces       map[string]OID
	defaultTablespace OID

	// postDefaults is the column default value from the post-migration snapshot.
	postDefaults map[columnRef]ColumnDefault
}

func (n *nameIndex) build(pre, post *Snapshot) {
	n.schemas = make(map[OID]string, len(pre.Schemas))
	n.types = map[qualifiedName][]Type{}
	n.procs = map[qualifiedName][]Proc{}
	n.operators = map[qualifiedName][]Operator{}
	n.castsTo = map[OID][]Cast{}
	n.postDefaults = map[columnRef]ColumnDefault{}
	n.tablespaces = make(map[string]OID, len(pre.Tablespaces))
	n.defaultTablespace = pre.Header.DefaultTablespace

	for _, tablespace := range pre.Tablespaces {
		n.tablespaces[tablespace.Name] = tablespace.OID
	}

	for _, schema := range pre.Schemas {
		n.schemas[schema.OID] = schema.Name
	}
	for _, typ := range pre.Types {
		key := qualifiedName{typ.Namespace, typ.Name}
		n.types[key] = append(n.types[key], typ)
	}
	for _, proc := range pre.Procs {
		key := qualifiedName{proc.Namespace, proc.Name}
		n.procs[key] = append(n.procs[key], proc)
	}
	for _, operator := range pre.Operators {
		key := qualifiedName{n.schemas[operator.Namespace], operator.Name}
		n.operators[key] = append(n.operators[key], operator)
	}
	for _, cast := range pre.Casts {
		n.castsTo[cast.Target] = append(n.castsTo[cast.Target], cast)
	}

	if post == nil {
		return
	}
	names := make(map[OID]helper.FullRelationName, len(post.Relations))
	for _, rel := range post.Relations {
		names[rel.OID] = rel.RelationName()
	}
	type columnSlot struct {
		relID OID
		num   int16
	}
	columns := make(map[columnSlot]string, len(post.Columns))
	for _, column := range post.Columns {
		columns[columnSlot{column.RelID, column.Num}] = column.Name
	}
	for _, def := range post.Defaults {
		relation, ok := names[def.RelID]
		if !ok {
			continue
		}
		column, ok := columns[columnSlot{def.RelID, def.Num}]
		if !ok {
			continue
		}
		n.postDefaults[columnRef{relation, column}] = def
	}
}

// resolutionPath is the schemas a bare function, operator or type name is looked up in.
// pg_catalog is searched first unless the path names it explicitly.
func resolutionPath(searchPath []string) []string {
	if slices.Contains(searchPath, "pg_catalog") {
		return searchPath
	}
	return append([]string{"pg_catalog"}, searchPath...)
}

// TypeByName resolves a type as the statement spelled it.
func (c *Catalog) TypeByName(schema, name string, searchPath []string) (Type, bool) {
	if schema != "" {
		types := c.names.types[qualifiedName{schema, name}]
		if len(types) == 0 {
			return Type{}, false
		}
		return types[0], true
	}
	for _, candidate := range resolutionPath(searchPath) {
		if types := c.names.types[qualifiedName{candidate, name}]; len(types) > 0 {
			return types[0], true
		}
	}
	return Type{}, false
}

// ArrayOf return the array type over an element type.
func (c *Catalog) ArrayOf(element OID) (Type, bool) {
	for _, derived := range c.types.derivedFrom[element] {
		typ, ok := c.types.types[derived]
		if ok && typ.ElemID == element && strings.HasPrefix(typ.Name, "_") {
			return typ, true
		}
	}
	return Type{}, false
}

// ProcsByName lists every function a call could have meant.
func (c *Catalog) ProcsByName(schema, name string, args int, searchPath []string) []Proc {
	fits := func(candidates []Proc) []Proc {
		var out []Proc
		for _, proc := range candidates {
			if args < 0 || proc.IsVariadic ||
				(int(proc.NArgs) >= args && int(proc.NArgs)-int(proc.NArgDefaults) <= args) {
				out = append(out, proc)
			}
		}
		return out
	}

	if schema != "" {
		return fits(c.names.procs[qualifiedName{schema, name}])
	}
	for _, candidate := range resolutionPath(searchPath) {
		if procs := fits(c.names.procs[qualifiedName{candidate, name}]); len(procs) > 0 {
			return procs
		}
	}
	return nil
}

// OperatorsByName lists every operator of that name.
func (c *Catalog) OperatorsByName(schema, name string, searchPath []string) []Operator {
	if schema != "" {
		return c.names.operators[qualifiedName{schema, name}]
	}
	var out []Operator
	for _, candidate := range resolutionPath(searchPath) {
		out = append(out, c.names.operators[qualifiedName{candidate, name}]...)
	}
	return out
}

// CastsTo lists every cast that produces the type.
func (c *Catalog) CastsTo(target OID) []Cast {
	return c.names.castsTo[target]
}

// PostDefault is the default the post-migration snapshot recorded for a column.
func (c *Catalog) PostDefault(relation helper.FullRelationName, column string) (ColumnDefault, bool) {
	def, ok := c.names.postDefaults[columnRef{relation, column}]
	return def, ok
}

// tablespaceOID resolves the tablespace OID from it's name.
func (n *nameIndex) tablespaceOID(name string) (OID, bool) {
	oid, known := n.tablespaces[name]
	if !known {
		return 0, false
	}
	if oid == n.defaultTablespace {
		return 0, true
	}
	return oid, true
}

// RelationsInTablespace lists the relations in a tablespace filtered by RelationKind and, optionally, owner
//
// exact is false when the name cannot be resolved.
// The answer is then every relation that is not in the default one.
func (c *Catalog) RelationsInTablespace(name, owner string, kinds []pg_contract.RelationKind) (relations []Relation, exact bool) {
	target, exact := c.names.tablespaceOID(name)
	for _, rel := range c.pre.Relations {
		if !slices.Contains(kinds, pg_contract.RelationKindFromRelkind(rel.Kind)) {
			continue
		}
		if exact && rel.Tablespace != target {
			continue
		}
		if !exact && rel.Tablespace == 0 {
			continue
		}
		if owner != "" && rel.Owner != owner {
			continue
		}
		relations = append(relations, rel)
	}
	return relations, exact
}
