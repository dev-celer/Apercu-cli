package pg_classify

import (
	"fmt"

	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// ruleAttachPartition is R-AT-ATTACH.
func ruleAttachPartition(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-ATTACH", "")
	e.lock = pg_contract.LockShareUpdateExclusive
	e.recursion = parentOnly

	attached := "the new partition"
	if len(sub.Relations) > 0 {
		partition := s.resolve(sub.Relations[0])
		attached = partition.Name.String()
		e.extra = append(e.extra, pg_contract.Target{
			Relation: contract(partition),
			Lock:     pg_contract.LockAccessExclusive,
			OpKind:   pg_contract.OpKindScan,
			Role:     pg_contract.TargetRoleDirect,
		})
	}
	e.message = fmt.Sprintf("%s is read in full to prove every row belongs inside the partition bound", attached) +
		"; the scan is skipped only by a validated CHECK that implies the whole bound, and the bound includes \"partition key IS NOT NULL\""

	// The default partition has to be re-proved too, rows that used to fall into it may now belong to the partition being attached.
	if s.relation().Exists() {
		if oid, ok := s.catalog.DefaultPartition(s.relation().Relation.OID); ok {
			if rel, ok := s.catalog.ByOID(oid); ok {
				e.extra = append(e.extra, pg_contract.Target{
					Relation: rel.Contract(),
					Lock:     pg_contract.LockAccessExclusive,
					OpKind:   pg_contract.OpKindScan,
					Role:     pg_contract.TargetRoleImplicit,
				})
				e.message += fmt.Sprintf("; the default partition %s is scanned with it", rel.RelationName())
			}
		}
	}
	return e
}

// ruleDetachPartition is R-AT-DETACH.
func ruleDetachPartition(s scope, sub pg_parse.Subcommand) effect {
	switch {
	case sub.Flags.Finalize:
		e := newEffect(s, "R-AT-DETACH-FIN", "an interrupted concurrent detach is completed; the catalog edge is already half gone")
		e.lock = pg_contract.LockShareUpdateExclusive
		e.recursion = parentOnly
		return e
	case sub.Flags.Concurrently:
		return detachConcurrently(s, sub)
	}

	e := newEffect(s, "R-AT-DETACH", "the partition is separated from the parent; no row moves")
	e.recursion = parentOnly
	e.extra = append(e.extra, s.partitionTargets(sub, pg_contract.LockAccessExclusive)...)
	return e
}

// detachConcurrently is R-AT-DETACH-CONC.
func detachConcurrently(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect(s, "R-AT-DETACH-CONC", "the parent is held at SHARE UPDATE EXCLUSIVE across two internal transactions, waiting between them for every transaction still using it; the partition ends at ACCESS EXCLUSIVE")
	e.lock = pg_contract.LockShareUpdateExclusive
	e.recursion = parentOnly
	e.extra = append(e.extra, s.partitionTargets(sub, pg_contract.LockAccessExclusive)...)

	if s.relation().Exists() {
		if _, hasDefault := s.catalog.DefaultPartition(s.relation().Relation.OID); hasDefault {
			return e.reject("DETACH PARTITION … CONCURRENTLY is refused while the partitioned table has a default partition", pg_contract.AnyVersion)
		}
	}
	return e
}

// partitionTargets is the partition a DETACH names, plus the default partition.
func (s scope) partitionTargets(sub pg_parse.Subcommand, lock pg_contract.Lock) []pg_contract.Target {
	var targets []pg_contract.Target
	if len(sub.Relations) > 0 {
		targets = append(targets, pg_contract.Target{
			Relation: contract(s.resolve(sub.Relations[0])),
			Lock:     lock,
			OpKind:   pg_contract.OpKindMetadata,
			Role:     pg_contract.TargetRoleDirect,
		})
	}
	if !s.relation().Exists() {
		return targets
	}
	oid, ok := s.catalog.DefaultPartition(s.relation().Relation.OID)
	if !ok {
		return targets
	}
	rel, ok := s.catalog.ByOID(oid)
	if !ok {
		return targets
	}
	return append(targets, pg_contract.Target{
		Relation: rel.Contract(),
		Lock:     lock,
		OpKind:   pg_contract.OpKindMetadata,
		Role:     pg_contract.TargetRoleImplicit,
	})
}
