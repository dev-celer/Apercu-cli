package pg_classify

import (
	"apercu-cli/helper"
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
