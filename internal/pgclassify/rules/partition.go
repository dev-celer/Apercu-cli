package rules

import (
	"fmt"

	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/prove"
	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// ruleAttachPartition is R-AT-ATTACH.
func ruleAttachPartition(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-ATTACH", "")
	parent := s.Relation()

	attached := "the new partition"
	proven := false
	if len(sub.Relations) > 0 {
		partition := s.Resolve(sub.Relations[0])
		attached = partition.Name.String()
		proven = prove.BoundProven(s.Scope, parent, partition, sub.Bound)
		op := pgcontract.OpKindScan
		if proven {
			op = pgcontract.OpKindMetadata
		}
		e.Touches = append(e.Touches, s.MovedPartition(partition, op))
	}
	if proven {
		e.Message = fmt.Sprintf("the validated CHECK constraints of %s already imply the partition bound, so it is not read", attached)
	} else {
		e.Message = fmt.Sprintf("%s is read in full to prove every row belongs inside the partition bound", attached) +
			"; the scan is skipped only by a validated CHECK that implies the whole bound, and the bound includes \"partition key IS NOT NULL\""
	}

	// The default partition has to be re-proved too, rows that used to fall into it may now belong to the partition being attached.
	if def, ok := s.DefaultPartitionOf(parent); ok {
		e.Message += fmt.Sprintf("; the default partition %s is scanned with it", def.Name)
	}
	e.Touches = append(e.Touches, s.PartitionAdded(parent)...)
	// Under an intermediate parent, the bound the newcomer has to satisfy includes every ancestor's.
	e.Touches = append(e.Touches, s.Ancestors(parent)...)
	return e
}

// ruleDetachPartition is R-AT-DETACH.
func ruleDetachPartition(s engine.Scope, sub ir.Subcommand) engine.Effect {
	partition := pgcatalog.RelationInfo{}
	if len(sub.Relations) > 0 {
		partition = s.Resolve(sub.Relations[0])
	}

	switch {
	case sub.Flags.Finalize:
		e := engine.NewEffect(s, "R-AT-DETACH-FIN", "an interrupted concurrent detach is completed; the catalog edge is already half gone, and the partition ends at ACCESS EXCLUSIVE")
		if partition.Name.Table != "" {
			e.Touches = append(e.Touches, s.MovedPartition(partition, pgcontract.OpKindMetadata))
		}
		e.Touches = append(e.Touches, s.KeysLeaving(partition)...)
		return e
	case sub.Flags.Concurrently:
		e := engine.NewEffect(s, "R-AT-DETACH-CONC", "the parent is held at SHARE UPDATE EXCLUSIVE across two internal transactions, waiting between them for every transaction still using it; the partition ends at ACCESS EXCLUSIVE")
		return detaching(e, s, partition)
	}

	e := engine.NewEffect(s, "R-AT-DETACH", "the partition is separated from the parent; no row moves")
	return detaching(e, s, partition)
}

// detaching is what a partition leaving its parent takes: its own tree, the default partition, and
// the foreign keys that stop being inherited.
func detaching(e engine.Effect, s engine.Scope, partition pgcatalog.RelationInfo) engine.Effect {
	if partition.Name.Table != "" {
		e.Touches = append(e.Touches, s.MovedPartition(partition, pgcontract.OpKindMetadata))
	}
	e.Touches = append(e.Touches, s.DefaultPartition(s.Relation(), pgcontract.OpKindMetadata, engine.Nowhere)...)
	e.Touches = append(e.Touches, s.KeysLeaving(partition)...)
	e.Touches = append(e.Touches, s.KeysInto(partition)...)
	return e
}
