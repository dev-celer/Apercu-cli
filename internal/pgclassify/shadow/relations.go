package shadow

import (
	"strings"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/session"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// Relations drives the relation half of the shadow overlay: what each statement creates, links,
// drops, renames or moves.
type Relations struct {
	recorder *pgcatalog.Recorder
}

// creations maps a command that brings a relation into existence to the position of that relation in the statement's relation list.
// CREATE INDEX names its table first and the index second, everything else names only what it creates.
var creations = map[pgcontract.Command]int{
	"CREATE TABLE":             0,
	"CREATE TABLE AS":          0,
	"CREATE MATERIALIZED VIEW": 0,
	"CREATE VIEW":              0,
	"CREATE SEQUENCE":          0,
	"CREATE INDEX":             1,
}

// declare update the shadow overlay on object creation.
func (s *Relations) declare(statement ir.Statement, context session.Context) {
	if s.recorder == nil || !statement.Parsed() {
		return
	}

	position, creates := creations[statement.Command]
	if !creates {
		return
	}
	if statement.Command == "CREATE INDEX" && len(statement.Relations) > 0 && len(statement.Subcommands) > 0 {
		// An index lands in its table's schema, not in the first schema of the search path.
		if table := s.recorder.State().Resolve(statement.Relations[0].Name, context.SearchPath); table.Exists() {
			s.declareIndex(statement, table)
			return
		}
	}
	if position >= len(statement.Relations) {
		return
	}

	created := statement.Relations[position]
	name := created.Name
	if name.Schema == "" {
		// An unqualified CREATE lands in the first schema of the search path.
		name.Schema = context.CreationSchema()
		if name.Schema == "" {
			return
		}
	}
	relkind := created.Kind.Relkind()
	if relkind == "" {
		return
	}
	s.recorder.Declare(helper.FullRelationName{Schema: name.Schema, Table: name.Table}, relkind)
}

// link updates the shadow overlay's parent-child links for the ones a statement draws or breaks, and the index trees
// and trigger clones that follow a partition coming or going, so the statements after it expand through the tree the
// migration has built so far. It also records the triggers a statement creates, renames or drops.
// The server never combines ATTACH or DETACH with another clause, so the statement's own expansion
// is the tree before it, and the next one's is the tree after it.
func (s *Relations) link(statement ir.Statement, context session.Context) {
	if s.recorder == nil || !statement.Parsed() || len(statement.Relations) == 0 {
		return
	}
	named, ok := s.resolve(statement.Relations[0], context)
	if !ok {
		return
	}
	s.triggers(statement, named)

	for _, sub := range statement.Subcommands {
		if len(sub.Relations) == 0 {
			continue
		}
		other, ok := s.resolve(sub.Relations[0], context)
		if !ok {
			continue
		}
		switch {
		case sub.Kind == ir.SubAttachPartition && statement.Command == "CREATE TABLE":
			// CREATE TABLE … PARTITION OF names the new table first and its parent in the clause.
			s.recorder.Link(pgcatalog.Link{Parent: other.Key(), Child: named.Key(),
				ParentIsPartitioned: true, IsDefaultPartition: sub.Value == "DEFAULT"})
			s.cloneIndexes(other, named)
			s.cloneTriggers(other, named)
		case sub.Kind == ir.SubAttachPartition:
			// ALTER TABLE … ATTACH PARTITION, and ALTER INDEX … ATTACH PARTITION for an index tree, which draws its
			// one edge.
			s.recorder.Link(pgcatalog.Link{Parent: named.Key(), Child: other.Key(),
				ParentIsPartitioned: true, IsDefaultPartition: sub.Value == "DEFAULT"})
			if !pgcontract.RelationKindFromRelkind(named.Relation.Kind).IsIndex() {
				s.cloneIndexes(named, other)
				s.cloneTriggers(named, other)
			}
		case sub.Kind == ir.SubDetachPartition:
			s.detachTriggers(named, other)
			s.recorder.Unlink(named.Key(), other.Key())
			s.detachIndexes(named, other)
		case sub.Kind == ir.SubAddInherit:
			// INHERITS and ALTER TABLE … INHERIT both name the child first.
			s.recorder.Link(pgcatalog.Link{Parent: other.Key(), Child: named.Key()})
		case sub.Kind == ir.SubDropInherit:
			s.recorder.Unlink(other.Key(), named.Key())
		}
	}
}

// relationCommands are the commands whose RENAME and SET SCHEMA act on a relation, not on a type or a function.
var relationCommands = map[string]bool{
	"TABLE": true, "INDEX": true, "VIEW": true, "MATERIALIZED VIEW": true, "SEQUENCE": true, "FOREIGN TABLE": true,
}

// retire updates the shadow overlay for the relations a statement drops, renames or moves to another schema. A
// renamed relation keeps its key, so nothing keyed by it has to follow.
func (s *Relations) retire(statement ir.Statement, context session.Context) {
	if s.recorder == nil || !statement.Parsed() || len(statement.Relations) == 0 {
		return
	}

	command := string(statement.Command)
	if object, ok := strings.CutPrefix(command, "DROP "); ok && relationCommands[object] {
		for _, ref := range statement.Relations {
			if info, ok := s.resolve(ref, context); ok {
				s.drop(info.Key(), statement.Flags.Cascade)
			}
		}
		return
	}

	object, _ := strings.CutPrefix(command, "ALTER ")
	object, renames := strings.CutSuffix(object, " RENAME")
	object, moves := strings.CutSuffix(object, " SET SCHEMA")
	if !relationCommands[object] || len(statement.Subcommands) != 1 {
		return
	}
	sub := statement.Subcommands[0]
	info, ok := s.resolve(statement.Relations[0], context)
	if !ok {
		return
	}
	switch {
	case renames && sub.Kind == ir.SubRenameRelation:
		s.recorder.Rename(info.Key(), helper.FullRelationName{Schema: info.Name.Schema, Table: sub.NewName})
	case moves && sub.Kind == ir.SubSetSchema:
		s.recorder.Rename(info.Key(), helper.FullRelationName{Schema: sub.Value, Table: info.Name.Table})
		// A table's indexes and the sequences it owns follow it into the new schema.
		state := s.recorder.State()
		followers := state.SequencesOwnedBy(info.Key())
		for _, index := range state.IndexesOf(info.Key()) {
			followers = append(followers, index.Key)
		}
		for _, key := range followers {
			if follower, ok := state.RelationByKey(key); ok {
				s.recorder.Rename(key, helper.FullRelationName{Schema: sub.Value, Table: follower.Name.Table})
			}
		}
	}
}

// resolve finds the relation a statement names, false when nothing holds it.
func (s *Relations) resolve(ref ir.RelationRef, context session.Context) (pgcatalog.RelationInfo, bool) {
	info := s.recorder.State().Resolve(ref.Name, context.SearchPath)
	return info, info.Exists()
}

// drop removes a relation from the shadow overlay with what goes with it: its partitions, which the server
// always drops along, its indexes and the sequences its columns own, and with CASCADE its inheritance children and the views built on it.
func (s *Relations) drop(key pgcatalog.RelationKey, cascade bool) {
	state := s.recorder.State()
	dropped := []pgcatalog.RelationKey{key}
	if cascade {
		dropped = append(dropped, state.Descendants(key)...)
	} else {
		dropped = append(dropped, state.PartitionDescendants(key)...)
	}

	var out []pgcatalog.RelationKey
	seen := map[pgcatalog.RelationKey]bool{}
	add := func(key pgcatalog.RelationKey) {
		if !seen[key] {
			seen[key] = true
			out = append(out, key)
		}
	}
	for _, key := range dropped {
		add(key)
		for _, index := range state.IndexesOf(key) {
			add(index.Key)
		}
		for _, sequence := range state.SequencesOwnedBy(key) {
			add(sequence)
		}
		oid, ok := key.OID()
		if !ok {
			continue
		}
		if cascade {
			for _, view := range state.ViewDependents(oid) {
				add(pgcatalog.KeyFromOID(view.DependentRelID))
			}
		}
	}
	for _, key := range out {
		s.recorder.Drop(key)
	}
}

// Declare records in the shadow overlay what the statement creates.
func (s *Relations) Declare(statement ir.Statement, context session.Context) {
	s.declare(statement, context)
}

// Link records the partition and inheritance edges a statement that ran draws or breaks, and the index trees and
// trigger clones that follow them, and the triggers it creates, renames or drops.
func (s *Relations) Link(statement ir.Statement, context session.Context) {
	s.link(statement, context)
}

// Retire records the relations a statement that ran drops, renames or moves.
func (s *Relations) Retire(statement ir.Statement, context session.Context) {
	s.retire(statement, context)
}

// NewRelations records into recorder. A nil recorder records nothing.
func NewRelations(recorder *pgcatalog.Recorder) *Relations {
	return &Relations{recorder: recorder}
}
