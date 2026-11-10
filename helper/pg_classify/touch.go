package pg_classify

import (
	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
)

// reach is how far a touch goes into one kind of table below its relation.
type reach uint8

const (
	// untouched leaves the tables below unopened.
	untouched reach = iota
	// locked opens them at the touch's lock and does none of its work in them.
	locked
	// worked opens them and does the touch's work in them too.
	worked
	// refused only appears under ONLY: the server errors when such tables exist.
	refused
)

// rank orders the reaches from the weakest reading to the strongest. A refusal ranks lowest,
// so any version that accepts the clause wins over one that refuses it.
func (r reach) rank() int {
	if r == refused {
		return -1
	}
	return int(r)
}

// below is how a touch reaches the tables under its relation.
type below struct {
	partitions reach
	// children are the classic INHERITS children.
	children reach
	// partitionLock replaces the touch's lock on the partitions; LockNone keeps it.
	partitionLock pg_contract.Lock
}

// touch is one relation a rule reaches, before the server's own propagation is applied.
type touch struct {
	info  pg_catalog.RelationInfo
	lock  pg_contract.Lock
	op    pg_contract.OpKind
	role  pg_contract.TargetRole
	below below
	// tableLock is, for an index, the lock on the table that owns it, at every level of the index tree.
	tableLock pg_contract.Lock
	// leafOp, when set, decides the work on each leaf the touch reaches; the lock is unchanged.
	leafOp func(leaf pg_catalog.RelationInfo) pg_contract.OpKind
}

// expand turns touches into targets: each touched relation first, then everything the server
// reaches under them.
func (s scope) expand(touches []touch) []pg_contract.Target {
	return append(s.ownTargets(touches), s.belowTargets(touches)...)
}

// ownTargets is each touched relation itself, and the table behind a touched index.
func (s scope) ownTargets(touches []touch) []pg_contract.Target {
	var targets []pg_contract.Target
	for _, t := range touches {
		targets = append(targets, pg_contract.Target{
			Relation: contract(t.info),
			Lock:     t.lock,
			OpKind:   opOn(t.op, partitionedInfo(t.info)),
			Role:     t.role,
		})
		targets = append(targets, s.owningTable(t, t.info, pg_contract.TargetRoleResolved)...)
	}
	return targets
}

// belowTargets is everything the server reaches under the touched relations.
func (s scope) belowTargets(touches []touch) []pg_contract.Target {
	var targets []pg_contract.Target
	for _, t := range touches {
		targets = s.walk(t, t.info, map[pg_catalog.RelationKey]bool{}, targets)
	}
	return targets
}

// walk follows the partition and inheritance edges under one relation, the way the server
// does when it works inside a tree: every level at the touch's lock, the work on the leaves.
func (s scope) walk(t touch, parent pg_catalog.RelationInfo, seen map[pg_catalog.RelationKey]bool, targets []pg_contract.Target) []pg_contract.Target {
	if !parent.Exists() {
		return targets
	}
	for _, link := range s.catalog.Children(parent.Key()) {
		reaches := t.below.children
		if link.ParentIsPartitioned {
			reaches = t.below.partitions
		}
		if (reaches != locked && reaches != worked) || seen[link.Child] {
			continue
		}
		seen[link.Child] = true
		child, ok := s.catalog.RelationByKey(link.Child)
		if !ok {
			continue
		}

		lock := t.lock
		if link.ParentIsPartitioned && t.below.partitionLock != pg_contract.LockNone {
			lock = t.below.partitionLock
		}
		op := pg_contract.OpKindMetadata
		if reaches == worked {
			op = t.op
			if t.leafOp != nil && !partitionedInfo(child) {
				op = t.leafOp(child)
			}
		}
		targets = append(targets, pg_contract.Target{
			Relation: contract(child),
			Lock:     lock,
			OpKind:   opOn(op, partitionedInfo(child)),
			Role:     belowRole(t.role),
		})
		targets = append(targets, s.owningTable(t, child, belowRole(t.role))...)
		targets = s.walk(t, child, seen, targets)
	}
	return targets
}

// belowRole is the role of what the server reaches under a touched relation: an expansion of what
// the statement names, or as implicit as the relation it hangs off.
func belowRole(role pg_contract.TargetRole) pg_contract.TargetRole {
	if role == pg_contract.TargetRoleImplicit {
		return pg_contract.TargetRoleImplicit
	}
	return pg_contract.TargetRoleExpanded
}

// owningTable is the table an index belongs to, locked the way the touch says.
func (s scope) owningTable(t touch, index pg_catalog.RelationInfo, role pg_contract.TargetRole) []pg_contract.Target {
	if t.tableLock == pg_contract.LockNone || !index.Exists() {
		return nil
	}
	if !pg_contract.RelationKindFromRelkind(index.Relation.Kind).IsIndex() {
		return nil
	}
	table, ok := s.catalog.TableOfIndex(index.Relation.OID)
	if !ok {
		return nil
	}
	return []pg_contract.Target{{
		Relation: table.Contract(),
		Lock:     t.tableLock,
		OpKind:   pg_contract.OpKindMetadata,
		Role:     role,
	}}
}

// hasPartitions reports whether a relation has partitions of its own.
func (s scope) hasPartitions(info pg_catalog.RelationInfo) bool {
	if !info.Exists() {
		return false
	}
	for _, link := range s.catalog.Children(info.Key()) {
		if link.ParentIsPartitioned {
			return true
		}
	}
	return false
}

// infoOf is a relation the catalog handed out by OID, in the shape a touch carries.
func (s scope) infoOf(rel pg_catalog.Relation) pg_catalog.RelationInfo {
	// By OID, so a relation the migration renamed is found under its new name.
	if info, ok := s.catalog.RelationByKey(pg_catalog.SnapshotKey(rel.OID)); ok {
		return info
	}
	return pg_catalog.RelationInfo{Name: rel.RelationName(), ProdBytes: -1}
}
