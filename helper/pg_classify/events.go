package pg_classify

import (
	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
)

// The catalog changes the server makes on its own behalf, each spelled as the touches it takes.
// A foreign key is a set of triggers on both of its tables, cloned onto every partition of a
// partitioned side, so every foreign key lock comes down to triggers being added or removed.

// addTrigger is a table having triggers added to it, on every partition when it is partitioned.
func (s scope) addTrigger(info pg_catalog.RelationInfo) touch {
	return touch{
		info:  info,
		lock:  pg_contract.LockShareRowExclusive,
		op:    pg_contract.OpKindMetadata,
		role:  pg_contract.TargetRoleImplicit,
		below: toPartitions,
	}
}

// removeTrigger is a table losing triggers or the constraint rows that own them, on every partition.
func (s scope) removeTrigger(info pg_catalog.RelationInfo) touch {
	return touch{
		info:  info,
		lock:  pg_contract.LockAccessExclusive,
		op:    pg_contract.OpKindMetadata,
		role:  pg_contract.TargetRoleImplicit,
		below: toPartitions,
	}
}

// validateForeignKey is the referenced side of the query that proves existing rows: the table is
// held at ROW SHARE, and the partitions the query reads at ACCESS SHARE.
func (s scope) validateForeignKey(referenced pg_catalog.RelationInfo) touch {
	return touch{
		info:  referenced,
		lock:  pg_contract.LockRowShare,
		op:    pg_contract.OpKindMetadata,
		role:  pg_contract.TargetRoleImplicit,
		below: below{partitions: locked, partitionLock: pg_contract.LockAccessShare},
	}
}

// defaultPartition is the default partition of a parent at ACCESS EXCLUSIVE, which every change
// to the parent's partitions takes; reach says whether the server works inside it too.
func (s scope) defaultPartition(parent pg_catalog.RelationInfo, op pg_contract.OpKind, reach below) []touch {
	info, ok := s.defaultPartitionOf(parent)
	if !ok {
		return nil
	}
	return []touch{{
		info:  info,
		lock:  pg_contract.LockAccessExclusive,
		op:    op,
		role:  pg_contract.TargetRoleImplicit,
		below: reach,
	}}
}

// defaultPartitionOf is the default partition of a parent, if it has one.
func (s scope) defaultPartitionOf(parent pg_catalog.RelationInfo) (pg_catalog.RelationInfo, bool) {
	if !parent.Exists() {
		return pg_catalog.RelationInfo{}, false
	}
	key, ok := s.catalog.DefaultPartition(parent.Key())
	if !ok {
		return pg_catalog.RelationInfo{}, false
	}
	return s.catalog.RelationByKey(key)
}

// movedPartition is the table an ATTACH or a DETACH moves: ACCESS EXCLUSIVE on its whole tree.
func (s scope) movedPartition(partition pg_catalog.RelationInfo, op pg_contract.OpKind) touch {
	return touch{
		info:  partition,
		lock:  pg_contract.LockAccessExclusive,
		op:    op,
		role:  pg_contract.TargetRoleDirect,
		below: toPartitions,
	}
}

// partitionAdded is what a parent gaining a partition takes beyond the parent itself: the default
// partition is re-proved down to its leaves, and every foreign key of the parent, in either
// direction, is cloned onto the newcomer under SHARE ROW EXCLUSIVE on the table at the other end.
func (s scope) partitionAdded(parent pg_catalog.RelationInfo) []touch {
	touches := s.defaultPartition(parent, pg_contract.OpKindScan, toPartitions)
	if !parent.Exists() {
		return touches
	}
	for _, key := range s.catalog.ForeignKeysFrom(parent.Relation.OID) {
		if referenced, ok := s.relationByOID(key.ForeignRelID); ok {
			touches = append(touches, s.partner(referenced, pg_contract.LockShareRowExclusive, pg_contract.OpKindMetadata))
		}
	}
	for _, key := range s.catalog.ForeignKeysTo(parent.Relation.OID) {
		if key.ParentID != 0 {
			continue
		}
		if referencing, ok := s.relationByOID(key.RelID); ok {
			touches = append(touches, s.partner(referencing, pg_contract.LockShareRowExclusive, pg_contract.OpKindMetadata))
		}
	}
	return touches
}

// keysLeaving is a detached partition's cloned foreign keys becoming keys of their own: each one
// gets its own triggers on the referenced table, under SHARE ROW EXCLUSIVE.
func (s scope) keysLeaving(partition pg_catalog.RelationInfo) []touch {
	if !partition.Exists() {
		return nil
	}
	var touches []touch
	for _, key := range s.catalog.ForeignKeysFrom(partition.Relation.OID) {
		if key.ParentID == 0 {
			continue
		}
		if referenced, ok := s.relationByOID(key.ForeignRelID); ok {
			touches = append(touches, s.partner(referenced, pg_contract.LockShareRowExclusive, pg_contract.OpKindMetadata))
		}
	}
	return touches
}

// keysInto is a referenced partition leaving its tree: every referencing table is read against
// the partition to prove none of its rows still points into it, then loses the constraint rows that did.
func (s scope) keysInto(partition pg_catalog.RelationInfo) []touch {
	if !partition.Exists() {
		return nil
	}
	var touches []touch
	keys := s.catalog.ForeignKeysTo(partition.Relation.OID)
	if len(keys) > 0 {
		touches = append(touches, touch{info: partition, lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindScan, role: pg_contract.TargetRoleDirect})
	}
	for _, key := range keys {
		referencing, ok := s.relationByOID(key.RelID)
		if !ok {
			continue
		}
		touches = append(touches,
			s.partner(referencing, pg_contract.LockShare, pg_contract.OpKindScan),
			s.partner(referencing, pg_contract.LockAccessExclusive, pg_contract.OpKindMetadata))
	}
	return touches
}

// ancestors is every partitioned parent above a relation, read at ACCESS SHARE to build the
// partition constraint a new partition has to satisfy.
func (s scope) ancestors(info pg_catalog.RelationInfo) []touch {
	var touches []touch
	seen := map[pg_catalog.RelationKey]bool{}
	for info.Exists() && !seen[info.Key()] {
		seen[info.Key()] = true
		var parent pg_catalog.RelationInfo
		for _, link := range s.catalog.Parents(info.Key()) {
			if rel, ok := s.catalog.RelationByKey(link.Parent); ok && link.ParentIsPartitioned {
				parent = rel
			}
		}
		if !parent.Exists() {
			break
		}
		touches = append(touches, s.partner(parent, pg_contract.LockAccessShare, pg_contract.OpKindMetadata))
		info = parent
	}
	return touches
}

// partner is a table the server opens on the side, without reaching below it.
func (s scope) partner(info pg_catalog.RelationInfo, lock pg_contract.Lock, op pg_contract.OpKind) touch {
	return touch{info: info, lock: lock, op: op, role: pg_contract.TargetRoleImplicit}
}

// relationByOID is a relation the catalog knows by OID, in the shape a touch carries.
func (s scope) relationByOID(oid pg_catalog.OID) (pg_catalog.RelationInfo, bool) {
	rel, ok := s.catalog.ByOID(oid)
	if !ok {
		return pg_catalog.RelationInfo{}, false
	}
	return s.infoOf(rel), true
}
