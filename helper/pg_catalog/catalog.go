package pg_catalog

import (
	"apercu-cli/helper"
	"apercu-cli/helper/pg_contract"
	"fmt"
	"sort"
	"strings"
)

// Catalog is the derived lookup layer, P-01…P-20. It is built once from the snapshots
// and then answers every question the rules ask.
type Catalog struct {
	pre  *Snapshot
	post *Snapshot
	prod *Snapshot

	// byName resolves a namespace-qualified name to its Relation.
	byName map[helper.FullRelationName]Relation
	byOID  map[OID]Relation

	// createdByMigration holds the Relation the migration itself create.
	createdByMigration map[helper.FullRelationName]Relation

	// unresolved counts the distinct names no snapshot could found.
	unresolved map[helper.FullRelationName]struct{}

	previewSizes map[OID]int64                     // Relation size in bytes on the preview database.
	prodSizes    map[helper.FullRelationName]int64 // Relation size in bytes on the production database.

	relations  relationIndex
	types      typeIndex
	settings   map[string]Setting
	tableStats map[helper.FullRelationName]TableStat // Table usage statistics from the production database.
}

// CatalogOptions are the snapshots the lookups are built from. Only Pre is mandatory.
type CatalogOptions struct {
	// Pre is the preview captured before the migration.
	Pre *Snapshot
	// Post is the preview captured after the migration.
	Post *Snapshot
	// Prod is the production capture. If missing the catalog will work in degraded mode
	// (no known version, no classification by activity/size)
	Prod *Snapshot
}

func NewCatalog(opts CatalogOptions) (*Catalog, error) {
	if opts.Pre == nil {
		return nil, fmt.Errorf("Failed to build the catalog: the pre-migration snapshot is required")
	}
	if opts.Pre.PIT != PITPre {
		return nil, fmt.Errorf("Failed to build the catalog: the pre-migration snapshot is a %q capture", opts.Pre.PIT)
	}
	if opts.Post != nil && opts.Post.PIT != PITPost {
		return nil, fmt.Errorf("Failed to build the catalog: the post-migration snapshot is a %q capture", opts.Post.PIT)
	}
	if opts.Prod != nil && opts.Prod.Source != SourceProd {
		return nil, fmt.Errorf("Failed to build the catalog: the production snapshot comes from %q", opts.Prod.Source)
	}

	c := &Catalog{
		pre:                opts.Pre,
		post:               opts.Post,
		prod:               opts.Prod,
		byName:             make(map[helper.FullRelationName]Relation, len(opts.Pre.Relations)),
		byOID:              make(map[OID]Relation, len(opts.Pre.Relations)),
		createdByMigration: map[helper.FullRelationName]Relation{},
		unresolved:         map[helper.FullRelationName]struct{}{},
		settings:           map[string]Setting{},
		tableStats:         map[helper.FullRelationName]TableStat{},
		previewSizes:       make(map[OID]int64, len(opts.Pre.Relations)),
		prodSizes:          map[helper.FullRelationName]int64{},
	}

	for _, rel := range opts.Pre.Relations {
		c.byName[rel.RelationName()] = rel
		c.byOID[rel.OID] = rel
	}
	if opts.Post != nil {
		for _, rel := range opts.Post.Relations {
			name := rel.RelationName()
			if _, existed := c.byName[name]; !existed {
				c.createdByMigration[name] = rel
			}
		}
	}

	for _, setting := range opts.Pre.Settings {
		c.settings[setting.Name] = setting
	}

	c.relations.build(opts.Pre)
	c.types.build(opts.Pre)

	rawProdSize := map[helper.FullRelationName]int64{}
	if opts.Prod != nil {
		for _, stat := range opts.Prod.TableStats {
			name := helper.FullRelationName{Schema: stat.Namespace, Table: stat.Name}
			c.tableStats[name] = stat
			rawProdSize[name] = stat.TotalBytes
		}
	}

	c.buildSize(rawProdSize)

	return c, nil
}

// Contract converts a Relation into the vocabulary the findings speak.
func (r Relation) Contract() pg_contract.Relation {
	return pg_contract.Relation{
		Name: r.RelationName(),
		Kind: pg_contract.RelationKindFromRelkind(r.Kind),
	}
}

// Origin is where a resolved relation came from
type Origin uint8

const (
	// OriginUnknown is a name no snapshot explains.
	OriginUnknown Origin = iota
	// OriginExisting is a relation that was already there before the migration.
	OriginExisting
	// OriginCreated is a relation the migration itself creates.
	OriginCreated
)

func (o Origin) String() string {
	switch o {
	case OriginExisting:
		return "EXISTING"
	case OriginCreated:
		return "CREATED"
	default:
		return "UNKNOWN"
	}
}

// RelationInfo group all the information about a relation
type RelationInfo struct {
	// Name is the qualified name the lookup settled on.
	Name     helper.FullRelationName
	Relation Relation
	Origin   Origin
	// PreviewBytes represent the relation size in bytes, or, for partitioned parent the sum of it children size.
	// This is taken on the preview database.
	PreviewBytes int64
	// ProdBytes represent the relation size in bytes, or, for partitioned parent the sum of it children size.
	// This is taken on the production database, -1 if unavailable.
	ProdBytes int64
}

// Exists reports whether the relation is found from the snapshot or the shadow catalog.
func (r RelationInfo) Exists() bool { return r.Origin != OriginUnknown }

// Resolve turns a name from a statement into a relation.
//
// An unqualified name is looked up through the search path.
// searchPath is optional, if provided it will override the catalog current recorded search path
func (c *Catalog) Resolve(name helper.FullRelationName, searchPath []string) RelationInfo {
	if name.Schema != "" {
		return c.lookup(name)
	}

	if searchPath == nil {
		searchPath = c.SearchPath()
	}
	for _, schema := range searchPath {
		candidate := helper.FullRelationName{Schema: schema, Table: name.Table}
		if info := c.lookup(candidate); info.Exists() {
			return info
		}
	}

	// Nothing on the path holds it. Report it against the first schema on the path, which is where the statement would have created or expected it.
	unqualified := name
	if len(searchPath) > 0 {
		unqualified.Schema = searchPath[0]
	}
	c.unresolved[unqualified] = struct{}{}
	return RelationInfo{Name: unqualified, ProdBytes: -1}
}

// lookup answers an exact, fully qualified name.
func (c *Catalog) lookup(name helper.FullRelationName) RelationInfo {
	if rel, ok := c.byName[name]; ok {
		prodBytes := int64(-1)
		if size, ok := c.prodSizes[rel.RelationName()]; ok {
			prodBytes = size
		}
		return RelationInfo{
			Name:         name,
			Relation:     rel,
			Origin:       OriginExisting,
			PreviewBytes: c.previewSizes[rel.OID],
			ProdBytes:    prodBytes,
		}
	}
	// Created by the migration.
	if rel, ok := c.createdByMigration[name]; ok {
		return RelationInfo{Name: name, Relation: rel, Origin: OriginCreated, ProdBytes: -1}
	}
	c.unresolved[name] = struct{}{}
	return RelationInfo{Name: name, ProdBytes: -1}
}

// ByOID resolves a relation the catalog already handed out an OID for.
func (c *Catalog) ByOID(oid OID) (Relation, bool) {
	rel, ok := c.byOID[oid]
	return rel, ok
}

// Declare records an object the migration creates.
func (c *Catalog) Declare(name helper.FullRelationName, kind string) {
	if _, existed := c.byName[name]; existed {
		return
	}
	if _, declared := c.createdByMigration[name]; declared {
		return
	}
	c.createdByMigration[name] = Relation{Namespace: name.Schema, Name: name.Table, Kind: kind}
	delete(c.unresolved, name)
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

// buildSize will build the previewSizes map and, if available, the prodSizes map.
func (c *Catalog) buildSize(rawProdSize map[helper.FullRelationName]int64) {
	for oid, rel := range c.byOID {
		c.previewSizes[oid] = c.previewSizeOf(rel)
	}

	if len(rawProdSize) > 0 {
		for _, rel := range c.byOID {
			if size, known := c.prodSizeOf(rel, rawProdSize); known {
				c.prodSizes[rel.RelationName()] = size
			}
		}
	}
}

// previewSizeOf is the size the schema snapshot reports.
func (c *Catalog) previewSizeOf(rel Relation) int64 {
	if !pg_contract.RelationKindFromRelkind(rel.Kind).IsPartitioned() {
		return storageBytes(rel)
	}
	var total int64
	for _, leaf := range c.relations.partitionDescendants(rel.OID) {
		if child, ok := c.byOID[leaf]; ok {
			total += storageBytes(child)
		}
	}
	return total
}

// storageBytes is what one relation occupies.
// Get total size for table / matview and heap size for all the other relation.
func storageBytes(rel Relation) int64 {
	if rel.TotalBytes > 0 {
		return rel.TotalBytes
	}
	return rel.HeapBytes
}

// prodSizeOf is the size production reports for the same relation.
func (c *Catalog) prodSizeOf(rel Relation, rawProdSize map[helper.FullRelationName]int64) (int64, bool) {
	if !pg_contract.RelationKindFromRelkind(rel.Kind).IsPartitioned() {
		size, ok := rawProdSize[rel.RelationName()]
		return size, ok
	}

	var total int64
	var known bool
	for _, leaf := range c.relations.partitionDescendants(rel.OID) {
		child, ok := c.byOID[leaf]
		if !ok {
			continue
		}
		if size, found := rawProdSize[child.RelationName()]; found {
			total += size
			known = true
		}
	}
	return total, known
}

// Version is the version the rules are gated on.
func (c *Catalog) Version() pg_contract.Version {
	if c.prod == nil {
		return pg_contract.VersionUnknown
	}
	return c.prod.Header.Version
}

// PreviewVersion is the version the schema was read from.
func (c *Catalog) PreviewVersion() pg_contract.Version {
	return c.pre.Header.Version
}

// VersionRange is the span of versions the rules must hold across.
func (c *Catalog) VersionRange() pg_contract.VersionRange {
	if version := c.Version(); version != pg_contract.VersionUnknown {
		return pg_contract.Exactly(version)
	}
	return pg_contract.Between(pg_contract.MinSupportedVersion, pg_contract.MaxSupportedVersion)
}

// Setting is the session default settings as the preview reported it.
func (c *Catalog) Setting(name string) (string, bool) {
	setting, ok := c.settings[name]
	return setting.Value, ok
}

// SearchPath is the session default search_path, split into schemas.
func (c *Catalog) SearchPath() []string {
	value, ok := c.Setting("search_path")
	if !ok {
		value = c.pre.Header.SearchPath
	}
	return ParseSearchPath(value, c.pre.Header.User)
}

// ParseSearchPath splits a search_path setting into the schemas it names, substituting
// "$user" with the given user and dropping it when there is none.
func ParseSearchPath(value, user string) []string {
	schemas := make([]string, 0, 2)
	for _, entry := range strings.Split(value, ",") {
		schema := strings.Trim(strings.TrimSpace(entry), `"`)
		if schema == "$user" {
			schema = user
		}
		if schema == "" {
			continue
		}
		schemas = append(schemas, schema)
	}
	return schemas
}

// Heat is how busy a table is on production.
func (c *Catalog) Heat(name helper.FullRelationName) (TableStat, bool) {
	stat, ok := c.tableStats[name]
	return stat, ok
}

// HasProdActivity reports whether production activity can be relied on for severity grading.
func (c *Catalog) HasProdActivity() bool {
	return c.prod != nil && !c.prod.Header.FromReplica && len(c.tableStats) > 0
}
