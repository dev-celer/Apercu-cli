package shadow

import (
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgparse/ir"
)

// A partitioned table's row trigger is cloned under its own name onto every partition below it, a partition that
// comes later included, and the clones are renamed and dropped with it and leave with a partition DETACH takes away.
// No other trigger of a partition can take a name its parent's trigger holds, so a clone is known by its name.

// triggers records what CREATE TRIGGER, DROP TRIGGER and ALTER TRIGGER … RENAME do to a table's triggers.
func (s *Relations) triggers(statement ir.Statement, table pgcatalog.RelationInfo) {
	if len(statement.Subcommands) != 1 {
		return
	}
	sub := statement.Subcommands[0]
	switch statement.Command {
	case "CREATE TRIGGER":
		// CREATE OR REPLACE turning a row trigger into a statement one leaves the clones where they are.
		trigger := pgcatalog.TriggerState{Row: statement.Flags.Row}
		holders := []pgcatalog.RelationKey{table.Key()}
		if trigger.Row {
			holders = withPartitions(s.recorder.State(), table.Key())
		}
		for _, holder := range holders {
			s.recorder.SetTrigger(holder, sub.Name, trigger)
		}
	case "DROP TRIGGER":
		for _, holder := range s.triggerHolders(table.Key(), sub.Name) {
			s.recorder.DeleteTrigger(holder, sub.Name)
		}
	case "ALTER TRIGGER RENAME":
		for _, holder := range s.triggerHolders(table.Key(), sub.Name) {
			s.recorder.RenameTrigger(holder, sub.Name, sub.NewName)
		}
	}
}

// cloneTriggers gives a new partition, and the partitions below it, a clone of each of its parent's row triggers.
func (s *Relations) cloneTriggers(parent, partition pgcatalog.RelationInfo) {
	state := s.recorder.State()
	holders := withPartitions(state, partition.Key())
	for _, trigger := range state.TriggersOf(parent.Key()) {
		if !trigger.Row {
			continue
		}
		for _, holder := range holders {
			s.recorder.SetTrigger(holder, trigger.Name, trigger.TriggerState)
		}
	}
}

// detachTriggers takes the clones of its former parent's row triggers off a detached partition and the partitions
// below it.
func (s *Relations) detachTriggers(parent, partition pgcatalog.RelationInfo) {
	state := s.recorder.State()
	holders := withPartitions(state, partition.Key())
	for _, trigger := range state.TriggersOf(parent.Key()) {
		if !trigger.Row {
			continue
		}
		for _, holder := range holders {
			if _, ok := state.TriggerByName(holder, trigger.Name); ok {
				s.recorder.DeleteTrigger(holder, trigger.Name)
			}
		}
	}
}

// triggerHolders is a table holding a trigger, and the partitions below it holding its clones.
func (s *Relations) triggerHolders(table pgcatalog.RelationKey, name string) []pgcatalog.RelationKey {
	state := s.recorder.State()
	if _, ok := state.TriggerByName(table, name); !ok {
		return nil
	}
	var out []pgcatalog.RelationKey
	for _, holder := range withPartitions(state, table) {
		if _, ok := state.TriggerByName(holder, name); ok {
			out = append(out, holder)
		}
	}
	return out
}

// withPartitions is a table and every partition below it.
func withPartitions(state *pgcatalog.State, table pgcatalog.RelationKey) []pgcatalog.RelationKey {
	return append([]pgcatalog.RelationKey{table}, state.PartitionDescendants(table)...)
}
