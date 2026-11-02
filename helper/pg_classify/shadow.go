package pg_classify

import (
	"apercu-cli/helper"
	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// creations maps a command that brings a relation into existence to the position of that relation in the statement's relation list.
// CREATE INDEX names its table first and the index second, everything else names only what it creates.
var creations = map[pg_contract.Command]int{
	"CREATE TABLE":             0,
	"CREATE TABLE AS":          0,
	"CREATE MATERIALIZED VIEW": 0,
	"CREATE VIEW":              0,
	"CREATE SEQUENCE":          0,
	"CREATE INDEX":             1,
}

// declare update the shadow catalog on object creation.
func (s *Session) declare(statement pg_parse.Statement, context Context) {
	if s.catalog == nil || !statement.Parsed() {
		return
	}

	position, creates := creations[statement.Command]
	if !creates || position >= len(statement.Relations) {
		return
	}

	created := statement.Relations[position]
	name := created.Name
	kind := created.Kind

	if statement.Command == "CREATE INDEX" {
		// An index lands in its table's schema, not in the first schema of the search path,
		// and it is partitioned exactly when its table is.
		table := s.catalog.Resolve(statement.Relations[0].Name, context.SearchPath)
		name.Schema = table.Name.Schema
		if pg_contract.RelationKindFromRelkind(table.Relation.Kind).IsPartitioned() {
			kind = pg_contract.RelationKindPartitionedIndex
		}
	}

	if name.Schema == "" {
		// An unqualified CREATE lands in the first schema of the search path.
		name.Schema = context.CreationSchema()
		if name.Schema == "" {
			return
		}
	}
	if relkind := kind.Relkind(); relkind != "" {
		s.catalog.Declare(helper.FullRelationName{Schema: name.Schema, Table: name.Table}, relkind)
	}
}

// link updates the shadow catalog's parent-child edges for the ones a statement draws or breaks,
// so the statements after it expand through the tree the migration has built so far.
// The server never combines ATTACH or DETACH with another clause, so the statement's own expansion
// is the tree before it, and the next one's is the tree after it.
func (s *Session) link(statement pg_parse.Statement, context Context) {
	if s.catalog == nil || !statement.Parsed() || len(statement.Relations) == 0 {
		return
	}
	resolve := func(ref pg_parse.RelationRef) (pg_catalog.RelationInfo, bool) {
		info := s.catalog.Resolve(ref.Name, context.SearchPath)
		return info, info.Exists()
	}
	named, ok := resolve(statement.Relations[0])
	if !ok {
		return
	}

	for _, sub := range statement.Subcommands {
		if len(sub.Relations) == 0 {
			continue
		}
		other, ok := resolve(sub.Relations[0])
		if !ok {
			continue
		}
		switch {
		case sub.Kind == pg_parse.SubAttachPartition && statement.Command == "CREATE TABLE":
			// CREATE TABLE … PARTITION OF names the new table first and its parent in the clause.
			s.catalog.DeclareEdge(pg_catalog.InheritEdge{Parent: other.Relation.OID, Child: named.Relation.OID,
				ParentIsPartitioned: true, IsDefaultPartition: sub.Value == "DEFAULT"})
		case sub.Kind == pg_parse.SubAttachPartition:
			// ALTER TABLE … ATTACH PARTITION, and ALTER INDEX … ATTACH PARTITION for an index tree.
			s.catalog.DeclareEdge(pg_catalog.InheritEdge{Parent: named.Relation.OID, Child: other.Relation.OID,
				ParentIsPartitioned: true, IsDefaultPartition: sub.Value == "DEFAULT"})
		case sub.Kind == pg_parse.SubDetachPartition:
			s.catalog.RemoveEdge(named.Relation.OID, other.Relation.OID)
		case sub.Kind == pg_parse.SubAddInherit:
			// INHERITS and ALTER TABLE … INHERIT both name the child first.
			s.catalog.DeclareEdge(pg_catalog.InheritEdge{Parent: other.Relation.OID, Child: named.Relation.OID})
		case sub.Kind == pg_parse.SubDropInherit:
			s.catalog.RemoveEdge(other.Relation.OID, named.Relation.OID)
		}
	}
}
