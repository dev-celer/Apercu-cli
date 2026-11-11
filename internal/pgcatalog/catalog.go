// Package pgcatalog is the derived lookup layer built on top of the snapshots, and the shadow
// overlay the classifier updates as the migration goes.
package pgcatalog

import (
	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgcontract"
	"fmt"
	"slices"
	"strings"
)

// Catalog is the derived lookup layer, P-01…P-20. It is built once from the snapshots
// and then answers every question the rules ask.
type Catalog struct {
	pre  *snapshot.Snapshot
	post *snapshot.Snapshot
	prod *snapshot.Snapshot

	// byName resolves a namespace-qualified name to its Relation.
	byName map[helper.FullRelationName]snapshot.Relation
	byOID  map[snapshot.OID]snapshot.Relation

	// Shadow Catalog. Record change made by the migration
	// createdByMigration holds the Relation the migration itself create.
	createdByMigration map[helper.FullRelationName]snapshot.Relation
	// postOnly holds the names createdByMigration knows from the post-migration snapshot alone, until a statement creates them.
	postOnly map[helper.FullRelationName]struct{}

	// retired holds the snapshot names that no longer resolve to their snapshot row: dropped or renamed away.
	retired map[helper.FullRelationName]struct{}
	// droppedOIDs holds the snapshot relations the migration dropped.
	droppedOIDs map[snapshot.OID]struct{}
	// renamed resolves the name a snapshot relation was renamed to, and currentName is the reverse.
	renamed     map[helper.FullRelationName]snapshot.OID
	currentName map[snapshot.OID]helper.FullRelationName

	// unresolved counts the distinct names no snapshot could found.
	unresolved map[helper.FullRelationName]struct{}

	previewSizes map[snapshot.OID]int64            // Relation size in bytes on the preview database.
	prodSizes    map[helper.FullRelationName]int64 // Relation size in bytes on the production database.

	relations  relationIndex
	types      typeIndex
	names      nameIndex
	settings   map[string]snapshot.Setting
	tableStats map[helper.FullRelationName]snapshot.TableStat // Table usage statistics from the production database.
	extensions map[string]snapshot.Extension                  // Installed extensions, from production when it was captured.
}

// CatalogOptions are the snapshots the lookups are built from. Only Pre is mandatory.
type CatalogOptions struct {
	// Pre is the preview captured before the migration.
	Pre *snapshot.Snapshot
	// Post is the preview captured after the migration.
	Post *snapshot.Snapshot
	// Prod is the production capture. If missing the catalog will work in degraded mode
	// (no known version, no classification by activity/size)
	Prod *snapshot.Snapshot
}

func NewCatalog(opts CatalogOptions) (*Catalog, error) {
	if opts.Pre == nil {
		return nil, fmt.Errorf("Failed to build the catalog: the pre-migration snapshot is required")
	}
	if opts.Pre.PIT != snapshot.PITPre {
		return nil, fmt.Errorf("Failed to build the catalog: the pre-migration snapshot is a %q capture", opts.Pre.PIT)
	}
	if opts.Post != nil && opts.Post.PIT != snapshot.PITPost {
		return nil, fmt.Errorf("Failed to build the catalog: the post-migration snapshot is a %q capture", opts.Post.PIT)
	}
	if opts.Prod != nil && opts.Prod.Source != snapshot.SourceProd {
		return nil, fmt.Errorf("Failed to build the catalog: the production snapshot comes from %q", opts.Prod.Source)
	}

	c := &Catalog{
		pre:                opts.Pre,
		post:               opts.Post,
		prod:               opts.Prod,
		byName:             make(map[helper.FullRelationName]snapshot.Relation, len(opts.Pre.Relations)),
		byOID:              make(map[snapshot.OID]snapshot.Relation, len(opts.Pre.Relations)),
		createdByMigration: map[helper.FullRelationName]snapshot.Relation{},
		postOnly:           map[helper.FullRelationName]struct{}{},
		retired:            map[helper.FullRelationName]struct{}{},
		droppedOIDs:        map[snapshot.OID]struct{}{},
		renamed:            map[helper.FullRelationName]snapshot.OID{},
		currentName:        map[snapshot.OID]helper.FullRelationName{},
		unresolved:         map[helper.FullRelationName]struct{}{},
		settings:           map[string]snapshot.Setting{},
		extensions:         map[string]snapshot.Extension{},
		tableStats:         map[helper.FullRelationName]snapshot.TableStat{},
		previewSizes:       make(map[snapshot.OID]int64, len(opts.Pre.Relations)),
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
				c.postOnly[name] = struct{}{}
			}
		}
	}

	for _, setting := range opts.Pre.Settings {
		c.settings[setting.Name] = setting
	}

	extensionSource := opts.Pre
	if opts.Prod != nil {
		extensionSource = opts.Prod
	}
	for _, extension := range extensionSource.Extensions {
		c.extensions[extension.Name] = extension
	}

	c.relations.build(opts.Pre)
	c.types.build(opts.Pre)
	c.names.build(opts.Pre, opts.Post)

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
	Relation snapshot.Relation
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

// Key is the relation's node in the partition and inheritance graph: its OID when the pre-migration
// snapshot holds it, its qualified name when the migration created it, nothing when it is unknown.
func (r RelationInfo) Key() RelationKey {
	switch r.Origin {
	case OriginExisting:
		return SnapshotKey(r.Relation.OID)
	case OriginCreated:
		return DeclaredKey(r.Name)
	default:
		return RelationKey{}
	}
}

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

// lookup find a relation in the catalog / shadow catalog from it's qualified name.
func (c *Catalog) lookup(name helper.FullRelationName) RelationInfo {
	if oid, ok := c.renamed[name]; ok {
		return c.existing(name, c.byOID[oid])
	}
	if rel, ok := c.byName[name]; ok && !c.isRetired(name) {
		return c.existing(name, rel)
	}
	// Created by the migration.
	if rel, ok := c.createdByMigration[name]; ok {
		return RelationInfo{Name: name, Relation: rel, Origin: OriginCreated, ProdBytes: -1}
	}
	c.unresolved[name] = struct{}{}
	return RelationInfo{Name: name, ProdBytes: -1}
}

// existing is a snapshot relation under the name it currently goes by. Its sizes are keyed by its snapshot name.
func (c *Catalog) existing(name helper.FullRelationName, rel snapshot.Relation) RelationInfo {
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

// RelationByKey resolves a node of the partition and inheritance graph back to its relation.
func (c *Catalog) RelationByKey(key RelationKey) (RelationInfo, bool) {
	if oid, ok := key.OID(); ok {
		if _, found := c.byOID[oid]; !found {
			return RelationInfo{}, false
		}
		if _, dropped := c.droppedOIDs[oid]; dropped {
			return RelationInfo{}, false
		}
		return c.lookup(c.nameOf(oid)), true
	}
	if _, declared := c.createdByMigration[key.declared]; !declared {
		return RelationInfo{}, false
	}
	return c.lookup(key.declared), true
}

// ByOID resolves a relation the catalog already handed out an OID for.
func (c *Catalog) ByOID(oid snapshot.OID) (snapshot.Relation, bool) {
	rel, ok := c.byOID[oid]
	return rel, ok
}

// RelationsInSchema lists the relations of a schema, filtered by RelationKind.
// An empty schema name answers with every relation the snapshot holds, which is the target set of
// a database-wide statement.
func (c *Catalog) RelationsInSchema(schema string, kinds []pgcontract.RelationKind) []snapshot.Relation {
	var relations []snapshot.Relation
	for _, rel := range c.pre.Relations {
		if _, dropped := c.droppedOIDs[rel.OID]; dropped {
			continue
		}
		if schema != "" && c.nameOf(rel.OID).Schema != schema {
			continue
		}
		if !slices.Contains(kinds, pgcontract.RelationKindFromRelkind(rel.Kind)) {
			continue
		}
		relations = append(relations, rel)
	}
	return relations
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
func (c *Catalog) previewSizeOf(rel snapshot.Relation) int64 {
	if !pgcontract.RelationKindFromRelkind(rel.Kind).IsPartitioned() {
		return storageBytes(rel)
	}
	var total int64
	for _, leaf := range c.relations.partitionDescendants(SnapshotKey(rel.OID)) {
		if child, ok := c.byOID[leaf.oid]; ok {
			total += storageBytes(child)
		}
	}
	return total
}

// storageBytes is what one relation occupies.
// Get total size for table / matview and heap size for all the other relation.
func storageBytes(rel snapshot.Relation) int64 {
	if rel.TotalBytes > 0 {
		return rel.TotalBytes
	}
	return rel.HeapBytes
}

// prodSizeOf is the size production reports for the same relation.
func (c *Catalog) prodSizeOf(rel snapshot.Relation, rawProdSize map[helper.FullRelationName]int64) (int64, bool) {
	if !pgcontract.RelationKindFromRelkind(rel.Kind).IsPartitioned() {
		size, ok := rawProdSize[rel.RelationName()]
		return size, ok
	}

	var total int64
	var known bool
	for _, leaf := range c.relations.partitionDescendants(SnapshotKey(rel.OID)) {
		child, ok := c.byOID[leaf.oid]
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
func (c *Catalog) Version() pgcontract.Version {
	if c.prod == nil {
		return pgcontract.VersionUnknown
	}
	return c.prod.Header.Version
}

// PreviewVersion is the version the schema was read from.
func (c *Catalog) PreviewVersion() pgcontract.Version {
	return c.pre.Header.Version
}

// VersionRange is the span of versions the rules must hold across.
func (c *Catalog) VersionRange() pgcontract.VersionRange {
	if version := c.Version(); version != pgcontract.VersionUnknown {
		return pgcontract.Exactly(version)
	}
	return pgcontract.Between(pgcontract.MinSupportedVersion, pgcontract.MaxSupportedVersion)
}

// Setting is the session default settings as the preview reported it.
func (c *Catalog) Setting(name string) (string, bool) {
	setting, ok := c.settings[name]
	return setting.Value, ok
}

// Extension is the installed extension of that name. Extensions belong to the database, not to a schema.
func (c *Catalog) Extension(name string) (snapshot.Extension, bool) {
	extension, ok := c.extensions[name]
	return extension, ok
}

// SearchPath is the session default search_path, split into schemas.
func (c *Catalog) SearchPath() []string {
	value, ok := c.Setting("search_path")
	if !ok {
		value = c.pre.Header.SearchPath
	}
	return ParseSearchPath(value, c.pre.Header.User)
}

// SnapshotUser is the role the preview was captured as.
func (c *Catalog) SnapshotUser() string {
	return c.pre.Header.User
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
func (c *Catalog) Heat(name helper.FullRelationName) (snapshot.TableStat, bool) {
	stat, ok := c.tableStats[name]
	return stat, ok
}

// HasProdActivity reports whether production activity can be relied on for severity grading.
func (c *Catalog) HasProdActivity() bool {
	return c.prod != nil && !c.prod.Header.FromReplica && len(c.tableStats) > 0
}

// describe is a debugging aid, not an output format.
func (r RelationInfo) String() string {
	if !r.Exists() {
		return fmt.Sprintf("%s (unknown)", r.Name)
	}
	size := fmt.Sprintf("%d bytes preview", r.PreviewBytes)
	if r.ProdBytes >= 0 {
		size += fmt.Sprintf(", %d bytes prod", r.ProdBytes)
	}
	return fmt.Sprintf("%s %s/%s %s", r.Name, r.Relation.Kind, strings.ToLower(r.Origin.String()), size)
}
