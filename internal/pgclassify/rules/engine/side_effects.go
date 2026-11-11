package engine

import (
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgcontract"
)

// The catalog changes the server makes on its own behalf, each spelled as the touches it takes.
// A foreign key is a set of triggers on both of its tables, cloned onto every partition of a
// partitioned side, so every foreign key lock comes down to triggers being added or removed.

// AddTrigger is a table having triggers added to it, on every partition when it is partitioned.
func (s Scope) AddTrigger(info pgcatalog.RelationInfo) Touch {
	return Touch{
		Info:  info,
		Lock:  pgcontract.LockShareRowExclusive,
		Op:    pgcontract.OpKindMetadata,
		Role:  pgcontract.TargetRoleImplicit,
		Below: ToPartitions,
	}
}

// RemoveTrigger is a table losing triggers or the constraint rows that own them, on every partition.
func (s Scope) RemoveTrigger(info pgcatalog.RelationInfo) Touch {
	return Touch{
		Info:  info,
		Lock:  pgcontract.LockAccessExclusive,
		Op:    pgcontract.OpKindMetadata,
		Role:  pgcontract.TargetRoleImplicit,
		Below: ToPartitions,
	}
}

// ValidateForeignKey is the referenced side of the query that proves existing rows: the table is
// held at ROW SHARE, and the partitions the query reads at ACCESS SHARE.
func (s Scope) ValidateForeignKey(referenced pgcatalog.RelationInfo) Touch {
	return Touch{
		Info:  referenced,
		Lock:  pgcontract.LockRowShare,
		Op:    pgcontract.OpKindMetadata,
		Role:  pgcontract.TargetRoleImplicit,
		Below: Below{Partitions: Locked, PartitionLock: pgcontract.LockAccessShare},
	}
}

// DefaultPartition is the default partition of a parent at ACCESS EXCLUSIVE, which every change
// to the parent's partitions takes; reach says whether the server works inside it too.
func (s Scope) DefaultPartition(parent pgcatalog.RelationInfo, op pgcontract.OpKind, reach Below) []Touch {
	info, ok := s.DefaultPartitionOf(parent)
	if !ok {
		return nil
	}
	return []Touch{{
		Info:  info,
		Lock:  pgcontract.LockAccessExclusive,
		Op:    op,
		Role:  pgcontract.TargetRoleImplicit,
		Below: reach,
	}}
}

// DefaultPartitionOf is the default partition of a parent, if it has one.
func (s Scope) DefaultPartitionOf(parent pgcatalog.RelationInfo) (pgcatalog.RelationInfo, bool) {
	if !parent.Exists() {
		return pgcatalog.RelationInfo{}, false
	}
	key, ok := s.Catalog.DefaultPartition(parent.Key())
	if !ok {
		return pgcatalog.RelationInfo{}, false
	}
	return s.Catalog.RelationByKey(key)
}

// MovedPartition is the table an ATTACH or a DETACH moves: ACCESS EXCLUSIVE on its whole tree.
func (s Scope) MovedPartition(partition pgcatalog.RelationInfo, op pgcontract.OpKind) Touch {
	return Touch{
		Info:  partition,
		Lock:  pgcontract.LockAccessExclusive,
		Op:    op,
		Role:  pgcontract.TargetRoleDirect,
		Below: ToPartitions,
	}
}

// PartitionAdded is what a parent gaining a partition takes beyond the parent itself: the default
// partition is re-proved down to its leaves, and every foreign key of the parent, in either
// direction, is cloned onto the newcomer under SHARE ROW EXCLUSIVE on the table at the other end.
func (s Scope) PartitionAdded(parent pgcatalog.RelationInfo) []Touch {
	touches := s.DefaultPartition(parent, pgcontract.OpKindScan, ToPartitions)
	if !parent.Exists() {
		return touches
	}
	for _, key := range s.Catalog.ForeignKeysFrom(parent.Relation.OID) {
		if referenced, ok := s.RelationByOID(key.ForeignRelID); ok {
			touches = append(touches, s.partner(referenced, pgcontract.LockShareRowExclusive, pgcontract.OpKindMetadata))
		}
	}
	for _, key := range s.Catalog.ForeignKeysTo(parent.Relation.OID) {
		if key.ParentID != 0 {
			continue
		}
		if referencing, ok := s.RelationByOID(key.RelID); ok {
			touches = append(touches, s.partner(referencing, pgcontract.LockShareRowExclusive, pgcontract.OpKindMetadata))
		}
	}
	return touches
}

// KeysLeaving is a detached partition's cloned foreign keys becoming keys of their own: each one
// gets its own triggers on the referenced table, under SHARE ROW EXCLUSIVE.
func (s Scope) KeysLeaving(partition pgcatalog.RelationInfo) []Touch {
	if !partition.Exists() {
		return nil
	}
	var touches []Touch
	for _, key := range s.Catalog.ForeignKeysFrom(partition.Relation.OID) {
		if key.ParentID == 0 {
			continue
		}
		if referenced, ok := s.RelationByOID(key.ForeignRelID); ok {
			touches = append(touches, s.partner(referenced, pgcontract.LockShareRowExclusive, pgcontract.OpKindMetadata))
		}
	}
	return touches
}

// KeysInto is a referenced partition leaving its tree: every referencing table is read against
// the partition to prove none of its rows still points into it, then loses the constraint rows that did.
func (s Scope) KeysInto(partition pgcatalog.RelationInfo) []Touch {
	if !partition.Exists() {
		return nil
	}
	var touches []Touch
	keys := s.Catalog.ForeignKeysTo(partition.Relation.OID)
	if len(keys) > 0 {
		touches = append(touches, Touch{Info: partition, Lock: pgcontract.LockAccessExclusive, Op: pgcontract.OpKindScan, Role: pgcontract.TargetRoleDirect})
	}
	for _, key := range keys {
		referencing, ok := s.RelationByOID(key.RelID)
		if !ok {
			continue
		}
		touches = append(touches,
			s.partner(referencing, pgcontract.LockShare, pgcontract.OpKindScan),
			s.partner(referencing, pgcontract.LockAccessExclusive, pgcontract.OpKindMetadata))
	}
	return touches
}

// Ancestors is every partitioned parent above a relation, read at ACCESS SHARE to build the
// partition constraint a new partition has to satisfy.
func (s Scope) Ancestors(info pgcatalog.RelationInfo) []Touch {
	var touches []Touch
	seen := map[pgcatalog.RelationKey]bool{}
	for info.Exists() && !seen[info.Key()] {
		seen[info.Key()] = true
		var parent pgcatalog.RelationInfo
		for _, link := range s.Catalog.Parents(info.Key()) {
			if rel, ok := s.Catalog.RelationByKey(link.Parent); ok && link.ParentIsPartitioned {
				parent = rel
			}
		}
		if !parent.Exists() {
			break
		}
		touches = append(touches, s.partner(parent, pgcontract.LockAccessShare, pgcontract.OpKindMetadata))
		info = parent
	}
	return touches
}

// partner is a table the server opens on the side, without reaching below it.
func (s Scope) partner(info pgcatalog.RelationInfo, lock pgcontract.Lock, op pgcontract.OpKind) Touch {
	return Touch{Info: info, Lock: lock, Op: op, Role: pgcontract.TargetRoleImplicit}
}

// ReferencingTables is the tables that hold a foreign key to this relation, each losing the
// key's constraint rows on every one of its partitions. A clone is reached through its original.
func (s Scope) ReferencingTables(info pgcatalog.RelationInfo) []Touch {
	if !info.Exists() {
		return nil
	}
	var targets []Touch
	for _, key := range s.Catalog.ForeignKeysTo(info.Relation.OID) {
		if key.ParentID != 0 {
			continue
		}
		if referencing, ok := s.RelationByOID(key.RelID); ok {
			targets = append(targets, s.RemoveTrigger(referencing))
		}
	}
	return targets
}
