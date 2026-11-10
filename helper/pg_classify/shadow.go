package pg_classify

import (
	"strings"

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

// link updates the shadow catalog's parent-child links for the ones a statement draws or breaks,
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
			s.catalog.DeclareLink(pg_catalog.Link{Parent: other.Key(), Child: named.Key(),
				ParentIsPartitioned: true, IsDefaultPartition: sub.Value == "DEFAULT"})
		case sub.Kind == pg_parse.SubAttachPartition:
			// ALTER TABLE … ATTACH PARTITION, and ALTER INDEX … ATTACH PARTITION for an index tree.
			s.catalog.DeclareLink(pg_catalog.Link{Parent: named.Key(), Child: other.Key(),
				ParentIsPartitioned: true, IsDefaultPartition: sub.Value == "DEFAULT"})
		case sub.Kind == pg_parse.SubDetachPartition:
			s.catalog.RemoveLink(named.Key(), other.Key())
		case sub.Kind == pg_parse.SubAddInherit:
			// INHERITS and ALTER TABLE … INHERIT both name the child first.
			s.catalog.DeclareLink(pg_catalog.Link{Parent: other.Key(), Child: named.Key()})
		case sub.Kind == pg_parse.SubDropInherit:
			s.catalog.RemoveLink(other.Key(), named.Key())
		}
	}
}

// relationCommands are the commands whose RENAME and SET SCHEMA act on a relation, not on a type or a function.
var relationCommands = map[string]bool{
	"TABLE": true, "INDEX": true, "VIEW": true, "MATERIALIZED VIEW": true, "SEQUENCE": true, "FOREIGN TABLE": true,
}

// moved is what a statement did to one relation of the shadow catalog: the key it went by, and the key it goes by
// now, zero when it was dropped.
type moved struct {
	from, to pg_catalog.RelationKey
}

// retire updates the shadow catalog for the relations a statement drops, renames or moves to another schema,
// and reports what it did so the state keyed by relation follows.
func (s *Session) retire(statement pg_parse.Statement, context Context) []moved {
	if s.catalog == nil || !statement.Parsed() || len(statement.Relations) == 0 {
		return nil
	}
	resolve := func(ref pg_parse.RelationRef) (pg_catalog.RelationInfo, bool) {
		info := s.catalog.Resolve(ref.Name, context.SearchPath)
		return info, info.Exists()
	}

	command := string(statement.Command)
	if object, ok := strings.CutPrefix(command, "DROP "); ok && relationCommands[object] {
		var out []moved
		for _, ref := range statement.Relations {
			if info, ok := resolve(ref); ok {
				out = append(out, s.drop(info.Key(), statement.Flags.Cascade)...)
			}
		}
		return out
	}

	object, _ := strings.CutPrefix(command, "ALTER ")
	object, renames := strings.CutSuffix(object, " RENAME")
	object, moves := strings.CutSuffix(object, " SET SCHEMA")
	if !relationCommands[object] || len(statement.Subcommands) != 1 {
		return nil
	}
	sub := statement.Subcommands[0]
	info, ok := resolve(statement.Relations[0])
	if !ok {
		return nil
	}
	switch {
	case renames && sub.Kind == pg_parse.SubRenameRelation:
		to := helper.FullRelationName{Schema: info.Name.Schema, Table: sub.NewName}
		return []moved{{from: info.Key(), to: s.catalog.Rename(info.Key(), to)}}
	case moves && sub.Kind == pg_parse.SubSetSchema:
		out := []moved{{from: info.Key(), to: s.catalog.Rename(info.Key(), helper.FullRelationName{Schema: sub.Value, Table: info.Name.Table})}}
		// A table's indexes follow it into the new schema.
		for _, index := range s.catalog.IndexesOf(info.Relation.OID) {
			if indexInfo, ok := s.catalog.RelationByKey(pg_catalog.SnapshotKey(index.IndexRelID)); ok {
				to := helper.FullRelationName{Schema: sub.Value, Table: indexInfo.Name.Table}
				out = append(out, moved{from: indexInfo.Key(), to: s.catalog.Rename(indexInfo.Key(), to)})
			}
		}
		return out
	}
	return nil
}

// drop removes a relation from the shadow catalog with what goes with it: its partitions, which the server
// always drops along, its indexes, and with CASCADE its inheritance children and the views built on it.
func (s *Session) drop(key pg_catalog.RelationKey, cascade bool) []moved {
	dropped := []pg_catalog.RelationKey{key}
	if cascade {
		dropped = append(dropped, s.catalog.Descendants(key)...)
	} else {
		dropped = append(dropped, s.catalog.PartitionDescendants(key)...)
	}

	var out []moved
	seen := map[pg_catalog.RelationKey]bool{}
	add := func(key pg_catalog.RelationKey) {
		if !seen[key] {
			seen[key] = true
			out = append(out, moved{from: key})
		}
	}
	for _, key := range dropped {
		add(key)
		oid, ok := key.OID()
		if !ok {
			continue
		}
		for _, index := range s.catalog.IndexesOf(oid) {
			add(pg_catalog.SnapshotKey(index.IndexRelID))
		}
		if cascade {
			for _, view := range s.catalog.ViewDependents(oid) {
				add(pg_catalog.SnapshotKey(view.DependentRelID))
			}
		}
	}
	for _, m := range out {
		s.catalog.Drop(m.from)
	}
	return out
}
