package engine

import (
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/stmt"
	"apercu-cli/internal/pgcontract"
)

// Touch is one relation a rule reaches, before the server's own propagation is applied.
type Touch struct {
	Info  pgcatalog.RelationInfo
	Lock  pgcontract.Lock
	Op    pgcontract.OpKind
	Role  pgcontract.TargetRole
	Below Below
	// tableLock is, for an index, the lock on the table that owns it, at every level of the index tree.
	TableLock pgcontract.Lock
	// leafOp, when set, decides the work on each leaf the touch reaches; the lock is unchanged.
	LeafOp func(leaf pgcatalog.RelationInfo) pgcontract.OpKind
}

// Expand turns touches into targets: each touched relation first, then everything the server
// reaches under them.
func (s Scope) Expand(touches []Touch) []pgcontract.Target {
	return append(s.ownTargets(touches), s.belowTargets(touches)...)
}

// ownTargets is each touched relation itself, and the table behind a touched index.
func (s Scope) ownTargets(touches []Touch) []pgcontract.Target {
	var targets []pgcontract.Target
	for _, t := range touches {
		targets = append(targets, pgcontract.Target{
			Relation: stmt.Contract(t.Info),
			Lock:     t.Lock,
			OpKind:   OpOn(t.Op, stmt.PartitionedInfo(t.Info)),
			Role:     t.Role,
		})
		targets = append(targets, s.owningTable(t, t.Info, pgcontract.TargetRoleResolved)...)
	}
	return targets
}

// belowTargets is everything the server reaches under the touched relations.
func (s Scope) belowTargets(touches []Touch) []pgcontract.Target {
	var targets []pgcontract.Target
	for _, t := range touches {
		targets = s.walk(t, t.Info, map[pgcatalog.RelationKey]bool{}, targets)
	}
	return targets
}

// walk follows the partition and inheritance edges under one relation, the way the server
// does when it works inside a tree: every level at the touch's lock, the work on the leaves.
func (s Scope) walk(t Touch, parent pgcatalog.RelationInfo, seen map[pgcatalog.RelationKey]bool, targets []pgcontract.Target) []pgcontract.Target {
	if !parent.Exists() {
		return targets
	}
	for _, link := range s.Catalog.Children(parent.Key()) {
		reaches := t.Below.Children
		if link.ParentIsPartitioned {
			reaches = t.Below.Partitions
		}
		if (reaches != Locked && reaches != Worked) || seen[link.Child] {
			continue
		}
		seen[link.Child] = true
		child, ok := s.Catalog.RelationByKey(link.Child)
		if !ok {
			continue
		}

		lock := t.Lock
		if link.ParentIsPartitioned && t.Below.PartitionLock != pgcontract.LockNone {
			lock = t.Below.PartitionLock
		}
		op := pgcontract.OpKindMetadata
		if reaches == Worked {
			op = t.Op
			if t.LeafOp != nil && !stmt.PartitionedInfo(child) {
				op = t.LeafOp(child)
			}
		}
		targets = append(targets, pgcontract.Target{
			Relation: stmt.Contract(child),
			Lock:     lock,
			OpKind:   OpOn(op, stmt.PartitionedInfo(child)),
			Role:     belowRole(t.Role),
		})
		targets = append(targets, s.owningTable(t, child, belowRole(t.Role))...)
		targets = s.walk(t, child, seen, targets)
	}
	return targets
}

// belowRole is the role of what the server reaches under a touched relation: an expansion of what
// the statement names, or as implicit as the relation it hangs off.
func belowRole(role pgcontract.TargetRole) pgcontract.TargetRole {
	if role == pgcontract.TargetRoleImplicit {
		return pgcontract.TargetRoleImplicit
	}
	return pgcontract.TargetRoleExpanded
}

// owningTable is the table an index belongs to, locked the way the touch says.
func (s Scope) owningTable(t Touch, index pgcatalog.RelationInfo, role pgcontract.TargetRole) []pgcontract.Target {
	if t.TableLock == pgcontract.LockNone || !index.Exists() {
		return nil
	}
	if !pgcontract.RelationKindFromRelkind(index.Relation.Kind).IsIndex() {
		return nil
	}
	table, ok := s.Catalog.TableOfIndex(index.Relation.OID)
	if !ok {
		return nil
	}
	return []pgcontract.Target{{
		Relation: table.Contract(),
		Lock:     t.TableLock,
		OpKind:   pgcontract.OpKindMetadata,
		Role:     role,
	}}
}

// hasPartitions reports whether a relation has partitions of its own.
func (s Scope) hasPartitions(info pgcatalog.RelationInfo) bool {
	if !info.Exists() {
		return false
	}
	for _, link := range s.Catalog.Children(info.Key()) {
		if link.ParentIsPartitioned {
			return true
		}
	}
	return false
}
