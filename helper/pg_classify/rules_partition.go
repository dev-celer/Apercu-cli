package pg_classify

import (
	"fmt"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// ruleAttachPartition is R-AT-ATTACH.
func ruleAttachPartition(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-ATTACH", "")
	parent := s.relation()

	attached := "the new partition"
	proven := false
	if len(sub.Relations) > 0 {
		partition := s.resolve(sub.Relations[0])
		attached = partition.Name.String()
		proven = s.boundProven(parent, partition, sub.Bound)
		op := pg_contract.OpKindScan
		if proven {
			op = pg_contract.OpKindMetadata
		}
		e.touches = append(e.touches, s.movedPartition(partition, op))
	}
	if proven {
		e.message = fmt.Sprintf("the validated CHECK constraints of %s already imply the partition bound, so it is not read", attached)
	} else {
		e.message = fmt.Sprintf("%s is read in full to prove every row belongs inside the partition bound", attached) +
			"; the scan is skipped only by a validated CHECK that implies the whole bound, and the bound includes \"partition key IS NOT NULL\""
	}

	// The default partition has to be re-proved too, rows that used to fall into it may now belong to the partition being attached.
	if def, ok := s.defaultPartitionOf(parent); ok {
		e.message += fmt.Sprintf("; the default partition %s is scanned with it", def.Name)
	}
	e.touches = append(e.touches, s.partitionAdded(parent)...)
	// Under an intermediate parent, the bound the newcomer has to satisfy includes every ancestor's.
	e.touches = append(e.touches, s.ancestors(parent)...)
	return e
}

// ruleDetachPartition is R-AT-DETACH.
func ruleDetachPartition(s scope, sub pg_parse.Subcommand) effect {
	partition := pg_catalog.RelationInfo{}
	if len(sub.Relations) > 0 {
		partition = s.resolve(sub.Relations[0])
	}

	switch {
	case sub.Flags.Finalize:
		e := newEffect(s, "R-AT-DETACH-FIN", "an interrupted concurrent detach is completed; the catalog edge is already half gone, and the partition ends at ACCESS EXCLUSIVE")
		if partition.Name.Table != "" {
			e.touches = append(e.touches, s.movedPartition(partition, pg_contract.OpKindMetadata))
		}
		e.touches = append(e.touches, s.keysLeaving(partition)...)
		return e
	case sub.Flags.Concurrently:
		e := newEffect(s, "R-AT-DETACH-CONC", "the parent is held at SHARE UPDATE EXCLUSIVE across two internal transactions, waiting between them for every transaction still using it; the partition ends at ACCESS EXCLUSIVE")
		return e.detaching(s, partition)
	}

	e := newEffect(s, "R-AT-DETACH", "the partition is separated from the parent; no row moves")
	return e.detaching(s, partition)
}

// detaching is what a partition leaving its parent takes: its own tree, the default partition, and
// the foreign keys that stop being inherited.
func (e effect) detaching(s scope, partition pg_catalog.RelationInfo) effect {
	if partition.Name.Table != "" {
		e.touches = append(e.touches, s.movedPartition(partition, pg_contract.OpKindMetadata))
	}
	e.touches = append(e.touches, s.defaultPartition(s.relation(), pg_contract.OpKindMetadata, nowhere)...)
	e.touches = append(e.touches, s.keysLeaving(partition)...)
	e.touches = append(e.touches, s.keysInto(partition)...)
	return e
}
