package pgcatalog

import (
	"slices"
	"sort"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog/snapshot"
)

// Declare records an object the migration creates.
func (c *Catalog) Declare(name helper.FullRelationName, kind string) {
	if _, existed := c.byName[name]; existed && !c.isRetired(name) {
		return
	}
	if _, renamed := c.renamed[name]; renamed {
		return
	}
	delete(c.postOnly, name)
	if _, declared := c.createdByMigration[name]; declared {
		return
	}
	c.createdByMigration[name] = snapshot.Relation{Namespace: name.Schema, Name: name.Table, Kind: kind}
	delete(c.unresolved, name)
}

// Drop forgets a relation the migration drops, and every partition and inheritance edge it had.
// A later CREATE of the same name declares a new relation instead of finding the snapshot row.
func (c *Catalog) Drop(key RelationKey) {
	if oid, ok := key.OID(); ok {
		c.droppedOIDs[oid] = struct{}{}
		name := c.nameOf(oid)
		c.retired[name] = struct{}{}
		delete(c.renamed, name)
	} else {
		delete(c.createdByMigration, key.declared)
	}
	c.relations.forgetEdges(key)
}

// Rename moves a relation the migration renames or moves to another schema to its new name, and returns its key
// under that name: unchanged for a snapshot relation, which is keyed by OID, the new name for a declared one.
func (c *Catalog) Rename(key RelationKey, to helper.FullRelationName) RelationKey {
	delete(c.unresolved, to)
	if oid, ok := key.OID(); ok {
		from := c.nameOf(oid)
		c.retired[from] = struct{}{}
		delete(c.renamed, from)
		c.renamed[to] = oid
		c.currentName[oid] = to
		return key
	}
	rel, ok := c.createdByMigration[key.declared]
	if !ok {
		return key
	}
	delete(c.createdByMigration, key.declared)
	rel.Namespace, rel.Name = to.Schema, to.Table
	c.createdByMigration[to] = rel
	moved := DeclaredKey(to)
	c.relations.rekeyEdges(key, moved)
	return moved
}

// DeclareLink records a parent-child link the migration draws: ATTACH, PARTITION OF, INHERIT.
func (c *Catalog) DeclareLink(link Link) {
	c.RemoveLink(link.Parent, link.Child)
	c.relations.children[link.Parent] = append(c.relations.children[link.Parent], link)
	c.relations.parents[link.Child] = append(c.relations.parents[link.Child], link)
}

// RemoveLink forgets a parent-child link the migration breaks: DETACH, NO INHERIT.
func (c *Catalog) RemoveLink(parent, child RelationKey) {
	c.relations.removeLink(parent, child)
}

func (r *relationIndex) removeLink(parent, child RelationKey) {
	r.children[parent] = slices.DeleteFunc(r.children[parent], func(link Link) bool { return link.Child == child })
	r.parents[child] = slices.DeleteFunc(r.parents[child], func(link Link) bool { return link.Parent == parent })
}

// forgetEdges removes every edge a relation has, to its parents and to its children.
func (r *relationIndex) forgetEdges(key RelationKey) {
	for _, link := range slices.Clone(r.children[key]) {
		r.removeLink(key, link.Child)
	}
	for _, link := range slices.Clone(r.parents[key]) {
		r.removeLink(link.Parent, key)
	}
	delete(r.children, key)
	delete(r.parents, key)
}

// rekeyEdges moves every edge of a relation to the key it goes by after a rename.
func (r *relationIndex) rekeyEdges(from, to RelationKey) {
	children, parents := slices.Clone(r.children[from]), slices.Clone(r.parents[from])
	r.forgetEdges(from)
	for _, link := range children {
		link.Parent = to
		r.children[to] = append(r.children[to], link)
		r.parents[link.Child] = append(r.parents[link.Child], link)
	}
	for _, link := range parents {
		link.Child = to
		r.children[link.Parent] = append(r.children[link.Parent], link)
		r.parents[to] = append(r.parents[to], link)
	}
}

// CreatedByMigration reports whether the migration created this relation earlier in the file.
func (c *Catalog) CreatedByMigration(name helper.FullRelationName) bool {
	_, created := c.createdByMigration[name]
	return created
}

// Holds reports whether a relation of that exact name exists at this point of the migration, without counting it as
// a name nothing explained. What only the post-migration snapshot knows has not been created yet.
func (c *Catalog) Holds(name helper.FullRelationName) bool {
	if _, renamed := c.renamed[name]; renamed {
		return true
	}
	if _, ok := c.byName[name]; ok && !c.isRetired(name) {
		return true
	}
	if _, onlyPost := c.postOnly[name]; onlyPost {
		return false
	}
	_, created := c.createdByMigration[name]
	return created
}

func (c *Catalog) isRetired(name helper.FullRelationName) bool {
	_, retired := c.retired[name]
	return retired
}

// nameOf is the name a snapshot relation goes by at this point of the migration.
func (c *Catalog) nameOf(oid snapshot.OID) helper.FullRelationName {
	if name, ok := c.currentName[oid]; ok {
		return name
	}
	return c.byOID[oid].RelationName()
}

// Unresolved lists the distinct names no snapshot explained.
func (c *Catalog) Unresolved() []helper.FullRelationName {
	names := make([]helper.FullRelationName, 0, len(c.unresolved))
	for name := range c.unresolved {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		if names[i].Schema != names[j].Schema {
			return names[i].Schema < names[j].Schema
		}
		return names[i].Table < names[j].Table
	})
	return names
}
