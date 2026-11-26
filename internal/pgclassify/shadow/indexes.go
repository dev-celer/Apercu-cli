package shadow

import (
	"iter"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/stmt"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// declareIndex records the index a CREATE INDEX builds on a table, under the name it was given or the one the server
// generates, and, unless ONLY stops it, the index it builds or adopts on each partition.
func (s *Relations) declareIndex(statement ir.Statement, table pgcatalog.RelationInfo) {
	sub := statement.Subcommands[0]
	if sub.Index == nil {
		return
	}
	state := s.recorder.State()
	var name helper.FullRelationName
	if len(statement.Relations) > 1 {
		name = helper.FullRelationName{Schema: table.Name.Schema, Table: statement.Relations[1].Name.Table}
		if state.Holds(name) {
			return
		}
	} else {
		name = indexName(state, table.Name, sub.Index.ColumnNames())
	}

	index := s.recorder.Declare(name, indexRelkind(table))
	shape := pgcatalog.NewIndexState(table.Key(), statement)
	s.recorder.SetIndex(index, shape)
	if statement.Relations[0].Only {
		return
	}
	for _, partition := range s.partitions(table) {
		s.attachIndex(index, shape, partition)
	}
}

// cloneIndexes gives a new partition an index for each index of its parent. The constraints' indexes are left to
// the constraints, which hold their names.
func (s *Relations) cloneIndexes(parent, partition pgcatalog.RelationInfo) {
	for index, shape := range plainIndexes(s.recorder.State(), parent) {
		s.attachIndex(index, shape, partition)
	}
}

// attachIndex gives one partition its index in a tree: the one it already has in it, an index of its own that
// matches and is in no tree yet, or a new one named after the parent index's columns. Its own partitions follow.
func (s *Relations) attachIndex(parent pgcatalog.RelationKey, shape pgcatalog.IndexState, table pgcatalog.RelationInfo) {
	state := s.recorder.State()
	attached := map[pgcatalog.RelationKey]bool{}
	for _, link := range state.Children(parent) {
		attached[link.Child] = true
	}
	var index pgcatalog.RelationKey
	found := false
	for _, candidate := range state.IndexesOf(table.Key()) {
		if attached[candidate.Key] {
			return
		}
		if found || !shape.Comparable || len(state.Parents(candidate.Key)) > 0 {
			continue
		}
		if own, ok := state.IndexState(candidate.Key); ok && shape.Matches(own) {
			index, found = candidate.Key, true
		}
	}
	if !found {
		index = declareIndexOn(s.recorder, table, indexName(state, table.Name, shape.IndexNameSources), shape)
	}
	s.recorder.Link(pgcatalog.Link{Parent: parent, Child: index, ParentIsPartitioned: true})
	for _, partition := range s.partitions(table) {
		s.attachIndex(index, shape, partition)
	}
}

// detachIndexes takes a detached partition's indexes out of its former parent's trees. They stay on the table.
func (s *Relations) detachIndexes(parent, partition pgcatalog.RelationInfo) {
	state := s.recorder.State()
	for _, index := range state.IndexesOf(partition.Key()) {
		for _, link := range state.Parents(index.Key) {
			if owner, ok := state.TableOfIndex(link.Parent); ok && owner.Key() == parent.Key() {
				s.recorder.Unlink(link.Parent, index.Key)
			}
		}
	}
}

// partitions is the partitions directly below a table.
func (s *Relations) partitions(table pgcatalog.RelationInfo) []pgcatalog.RelationInfo {
	state := s.recorder.State()
	var out []pgcatalog.RelationInfo
	for _, link := range state.Children(table.Key()) {
		if partition, ok := state.RelationByKey(link.Child); ok && link.ParentIsPartitioned {
			out = append(out, partition)
		}
	}
	return out
}

// indexRelkind is the relkind of an index on a table: partitioned exactly when the table is.
func indexRelkind(table pgcatalog.RelationInfo) string {
	if stmt.PartitionedInfo(table) {
		return pgcontract.RelationKindPartitionedIndex.Relkind()
	}
	return pgcontract.RelationKindIndex.Relkind()
}

// indexName is the server's ChooseIndexName for a plain index: <table>_<columns>_idx, numbered until no relation of
// the schema goes by it.
func indexName(state *pgcatalog.State, table helper.FullRelationName, columns []string) helper.FullRelationName {
	taken := func(name string) bool {
		return state.Holds(helper.FullRelationName{Schema: table.Schema, Table: name})
	}
	return helper.FullRelationName{Schema: table.Schema, Table: chooseName(table.Table, nameAddition(columns), "idx", taken)}
}

// declareIndexOn records an index of a table under a name, of the kind the table calls for, and returns its key.
func declareIndexOn(recorder *pgcatalog.Recorder, table pgcatalog.RelationInfo, name helper.FullRelationName, shape pgcatalog.IndexState) pgcatalog.RelationKey {
	index := recorder.Declare(name, indexRelkind(table))
	shape.Table = table.Key()
	recorder.SetIndex(index, shape)
	return index
}

// plainIndexes is the indexes of a table that back no constraint, with their shape: the ones a copy of the table
// gets under names of its own.
func plainIndexes(state *pgcatalog.State, table pgcatalog.RelationInfo) iter.Seq2[pgcatalog.RelationKey, pgcatalog.IndexState] {
	return func(yield func(pgcatalog.RelationKey, pgcatalog.IndexState) bool) {
		for _, index := range state.IndexesOf(table.Key()) {
			if backsConstraint(state, table, index) {
				continue
			}
			if shape, ok := state.IndexState(index.Key); ok && !yield(index.Key, shape) {
				return
			}
		}
	}
}

// backsConstraint reports whether an index enforces one of its table's PRIMARY KEY, UNIQUE or EXCLUDE: the server
// keeps the two under the same name.
func backsConstraint(state *pgcatalog.State, table pgcatalog.RelationInfo, index pgcatalog.IndexInfo) bool {
	info, ok := state.RelationByKey(index.Key)
	if !ok {
		return false
	}
	constraint, ok := state.ConstraintByName(table.Key(), info.Name.Table)
	return ok && constraint.BackedByIndex()
}
