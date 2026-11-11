// Package stmt is one statement as the rules see it: the catalog, the context it runs under, and
// the relations it names, resolved through the search path.
package stmt

import (
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgclassify/session"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// View is one statement as the rules see it: the catalog, the context it runs under, and the
// relations it names, resolved.
type View struct {
	Catalog   *pgcatalog.Catalog
	Context   session.Context
	Statement ir.Statement
	// version is production's. It is VersionUnknown when production was unreachable.
	Version pgcontract.Version
	// relations is statement.Relations resolved through the search path.
	Relations []pgcatalog.RelationInfo
}

// New resolves the relations a statement names, through the search path in force.
func New(catalog *pgcatalog.Catalog, context session.Context, statement ir.Statement) View {
	v := View{
		Catalog:   catalog,
		Context:   context,
		Statement: statement,
		Version:   catalog.Version(),
	}
	for _, named := range statement.Relations {
		v.Relations = append(v.Relations, catalog.Resolve(named.Name, context.SearchPath))
	}
	return v
}

// Relation is the first relation the statement names.
func (s View) Relation() pgcatalog.RelationInfo {
	if len(s.Relations) == 0 {
		return pgcatalog.RelationInfo{}
	}
	return s.Relations[0]
}

// Only is the ONLY keyword, which stops the statement from reaching partitions and children.
func (s View) Only() bool {
	return len(s.Statement.Relations) > 0 && s.Statement.Relations[0].Only
}

// Partitioned reports whether the named relation is a partitioned parent.
func (s View) Partitioned() bool {
	return PartitionedInfo(s.Relation())
}

// PartitionedInfo is the same question for any resolved relation.
func PartitionedInfo(info pgcatalog.RelationInfo) bool {
	return pgcontract.RelationKindFromRelkind(info.Relation.Kind).IsPartitioned()
}

// Resolve turns a name a subcommand wrote into a relation, through the search path in force.
func (s View) Resolve(ref ir.RelationRef) pgcatalog.RelationInfo {
	return s.Catalog.Resolve(ref.Name, s.Context.SearchPath)
}

func (s View) AtLeast18() bool {
	return s.Version == pgcontract.VersionUnknown || s.Version >= pgcontract.Version18
}

func (s View) VersionsWhen(conditional pgcontract.VersionRange) pgcontract.VersionRange {
	if s.Version != pgcontract.VersionUnknown {
		return pgcontract.AnyVersion
	}
	return conditional
}

// Contract is a resolved relation in the vocabulary a finding speaks.
func Contract(info pgcatalog.RelationInfo) pgcontract.Relation {
	return pgcontract.Relation{
		Name: info.Name,
		Kind: pgcontract.RelationKindFromRelkind(info.Relation.Kind),
	}
}

// InfoOf is a relation the catalog handed out by OID, in the shape a touch carries.
func (s View) InfoOf(rel snapshot.Relation) pgcatalog.RelationInfo {
	// By OID, so a relation the migration renamed is found under its new name.
	if info, ok := s.Catalog.RelationByKey(pgcatalog.SnapshotKey(rel.OID)); ok {
		return info
	}
	return pgcatalog.RelationInfo{Name: rel.RelationName(), ProdBytes: -1}
}

// RelationByOID is a relation the catalog knows by OID, in the shape a touch carries.
func (s View) RelationByOID(oid snapshot.OID) (pgcatalog.RelationInfo, bool) {
	rel, ok := s.Catalog.ByOID(oid)
	if !ok {
		return pgcatalog.RelationInfo{}, false
	}
	return s.InfoOf(rel), true
}

// ByName is a relation a lookup already answered with, back in the shape the rules pass around.
func (s View) ByName(relation pgcontract.Relation) pgcatalog.RelationInfo {
	return s.Catalog.Resolve(relation.Name, s.Context.SearchPath)
}

// Descendant is one relation below another, for the rules that list a tree rather than lock it:
// locking goes through expand.
type Descendant struct {
	Relation pgcontract.Relation
	// partition separates declarative partitioning from classic INHERITS.
	Partition bool
}

// DescendantsOf is every partition and inherited child below a relation.
func (s View) DescendantsOf(info pgcatalog.RelationInfo) []Descendant {
	if !info.Exists() {
		return nil
	}
	partitions := map[pgcatalog.RelationKey]bool{}
	for _, key := range s.Catalog.PartitionDescendants(info.Key()) {
		partitions[key] = true
	}
	var out []Descendant
	for _, key := range s.Catalog.Descendants(info.Key()) {
		child, ok := s.Catalog.RelationByKey(key)
		if !ok {
			continue
		}
		out = append(out, Descendant{Relation: Contract(child), Partition: partitions[key]})
	}
	return out
}

// Column resolves one column of the named table.
func (s View) Column(name string) (pgcatalog.ColumnInfo, bool) {
	if !s.Relation().Exists() || name == "" {
		return pgcatalog.ColumnInfo{}, false
	}
	return s.Catalog.Column(s.Relation().Relation.OID, name)
}

// TableOfIndex return, for an index, the table it belongs to.
func (s View) TableOfIndex(index pgcatalog.RelationInfo) (pgcontract.Relation, bool) {
	if !index.Exists() {
		return pgcontract.Relation{}, false
	}
	table, ok := s.Catalog.TableOfIndex(index.Relation.OID)
	if !ok {
		return pgcontract.Relation{}, false
	}
	return table.Contract(), true
}

// RowTrigger reports whether a trigger of the named table fires per row, which is what clones it
// onto the partitions. A trigger the snapshot does not know, or a clause naming them all, reads as one that does.
func (s View) RowTrigger(name string) bool {
	if name == "" || !s.Relation().Exists() {
		return true
	}
	trigger, ok := s.Catalog.TriggerByName(s.Relation().Relation.OID, name)
	return !ok || trigger.Row()
}

// Sources is every relation the CREATE TABLE / CREATE VIEW statement names after the one it creates.
func (s View) Sources() []pgcatalog.RelationInfo {
	if len(s.Relations) < 2 {
		return nil
	}
	return s.Relations[1:]
}
