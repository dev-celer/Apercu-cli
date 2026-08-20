// Package pg_catalog captures the PostgreSQL catalog snapshots the classifier
// reasons over (§2). A snapshot is a flat, consistent copy of the catalog rows
// a migration can be graded against; the derived lookups (P-01…P-20) are built
// on top of it.
package pg_catalog

import (
	"apercu-cli/helper/pg_contract"
	"time"
)

// OID is a PostgreSQL object identifier. Zero means "none" — pg_class.reloftype,
// pg_constraint.confrelid and friends all use 0 rather than NULL for absence.
type OID uint32

// Source is the database a snapshot was taken from. The two are not
// interchangeable: the baseline carries the schema, production carries the
// sizes and the activity (§2.1 S-SOURCE).
type Source string

const (
	// SourceBaseline is the pre-migration subsetted or anonymized database.
	SourceBaseline Source = "baseline"
	// SourceProd is the real production database, when it is reachable.
	SourceProd Source = "prod"
)

// PIT is when the snapshot was taken relative to the migration (§2.1 S-PIT).
type PIT string

const (
	PITPre  PIT = "pre"
	PITPost PIT = "post"
)

// Snapshot is one capture. Which tables are populated depends on the source and
// point in time it was taken at — a production snapshot carries the header,
// replication state and activity statistics, a baseline snapshot carries the
// schema. Use Collected to tell "not captured" from "captured and empty".
type Snapshot struct {
	Source     Source    `json:"source"`
	PIT        PIT       `json:"pit"`
	CapturedAt time.Time `json:"captured_at"`
	// Collected lists the item ids actually run for this snapshot ("S-00", …).
	Collected []string `json:"collected"`

	// Header describes the database this snapshot came from. It is always
	// captured, because the collector needs the version to decide which columns
	// exist; which header is *authoritative* for version gating is decided when
	// the snapshots are combined.
	Header Header `json:"header"` // S-00

	Schemas         []Schema         `json:"schemas"`          // S-01
	Relations       []Relation       `json:"relations"`        // S-02
	Columns         []Column         `json:"columns"`          // S-03
	Defaults        []ColumnDefault  `json:"defaults"`         // S-04
	Constraints     []Constraint     `json:"constraints"`      // S-05
	Indexes         []Index          `json:"indexes"`          // S-06
	Inherits        []InheritEdge    `json:"inherits"`         // S-07
	Sequences       []Sequence       `json:"sequences"`        // S-08
	Types           []Type           `json:"types"`            // S-09
	Triggers        []Trigger        `json:"triggers"`         // S-10
	Policies        []Policy         `json:"policies"`         // S-10
	ViewDeps        []ViewDep        `json:"view_deps"`        // S-11
	Depends         []DependEdge     `json:"depends"`          // S-12
	Procs           []Proc           `json:"procs"`            // S-13
	Casts           []Cast           `json:"casts"`            // S-14
	Operators       []Operator       `json:"operators"`        // S-14
	Publications    []Publication    `json:"publications"`     // S-15
	PublicationRels []PublicationRel `json:"publication_rels"` // S-15
	Extensions      []Extension      `json:"extensions"`       // S-15
	Settings        []Setting        `json:"settings"`         // S-16
	TableStats      []TableStat      `json:"table_stats"`      // S-17
	Roles           []Role           `json:"roles"`            // S-18
	RelACLs         []RelACL         `json:"rel_acls"`         // S-18
}

// Header is S-00. Version-sensitive classification depends on it, so a run that
// cannot read it from production falls back to bounding the version by the
// syntax observed.
type Header struct {
	ServerVersionNum int                 `json:"server_version_num"`
	Version          pg_contract.Version `json:"version"`
	Database         string              `json:"database"`
	User             string              `json:"user"`
	SearchPath       string              `json:"search_path"`
	TimeZone         string              `json:"timezone"`
	// FromReplica is pg_is_in_recovery(). Activity statistics (S-17) are
	// meaningless when it is true.
	FromReplica bool `json:"from_replica"`
}

// Schema is S-01.
type Schema struct {
	OID   OID    `json:"oid"`
	Name  string `json:"name"`
	Owner string `json:"owner"`
}

// Relation is S-02: every lockable relkind in one table.
type Relation struct {
	OID          OID      `json:"oid"`
	Namespace    string   `json:"namespace"`
	Name         string   `json:"name"`
	Kind         string   `json:"kind"`        // pg_class.relkind, one char
	Persistence  string   `json:"persistence"` // 'p' permanent, 'u' unlogged, 't' temp
	IsPartition  bool     `json:"is_partition"`
	RowSecurity  bool     `json:"row_security"`
	HasSubclass  bool     `json:"has_subclass"`
	Tuples       float64  `json:"tuples"`
	Pages        int64    `json:"pages"`
	Options      []string `json:"options,omitempty"`
	Tablespace   OID      `json:"tablespace"`
	OfType       OID      `json:"of_type"`
	AccessMethod string   `json:"access_method,omitempty"`
	Owner        string   `json:"owner"`
	// HeapBytes and TotalBytes are both 0 for a partitioned parent, which has no
	// storage of its own; P-01 sums the tree instead.
	HeapBytes      int64  `json:"heap_bytes"`
	TotalBytes     int64  `json:"total_bytes"`
	PartitionBound string `json:"partition_bound,omitempty"`
	PartitionKey   string `json:"partition_key,omitempty"`
}

// Column is S-03.
type Column struct {
	RelID         OID    `json:"relid"`
	Num           int16  `json:"num"`
	Name          string `json:"name"`
	TypeID        OID    `json:"type_id"`
	FormattedType string `json:"formatted_type"`
	TypeMod       int32  `json:"type_mod"`
	// NotNull is authoritative up to PG 17. On 18 a NOT NULL can exist as an
	// unenforced constraint while attnotnull is still true, so P-14 reads S-05.
	NotNull     bool   `json:"not_null"`
	Generated   string `json:"generated"` // '' none, 's' stored, 'v' virtual (18+)
	Identity    string `json:"identity"`
	Storage     string `json:"storage"`
	Compression string `json:"compression"`
	IsLocal     bool   `json:"is_local"`
	InhCount    int32  `json:"inh_count"`
	Collation   OID    `json:"collation"`
	// StatsTarget is nil when the column uses the database default. The catalog
	// spells that -1 on PG 15/16 and NULL on 17/18; both normalize to nil here.
	StatsTarget *int32 `json:"stats_target,omitempty"`
}

// ColumnDefault is S-04.
type ColumnDefault struct {
	RelID OID    `json:"relid"`
	Num   int16  `json:"num"`
	Expr  string `json:"expr"`
	// ReferencedProcs is empty for built-in functions: PostgreSQL records no
	// dependency on pinned objects. It pins the exact overload of a user
	// function, and never proves the absence of a function call.
	ReferencedProcs     []OID `json:"referenced_procs,omitempty"`
	ReferencedOperators []OID `json:"referenced_operators,omitempty"`
}

// Constraint is S-05.
type Constraint struct {
	OID          OID     `json:"oid"`
	Name         string  `json:"name"`
	RelID        OID     `json:"relid"`         // conrelid
	ForeignRelID OID     `json:"foreign_relid"` // confrelid
	Type         string  `json:"type"`          // contype: c p u f t x n
	Validated    bool    `json:"validated"`
	Deferrable   bool    `json:"deferrable"`
	Deferred     bool    `json:"deferred"`
	IsLocal      bool    `json:"is_local"`
	InhCount     int32   `json:"inh_count"`
	NoInherit    bool    `json:"no_inherit"`
	Key          []int16 `json:"key,omitempty"`         // conkey
	ForeignKey   []int16 `json:"foreign_key,omitempty"` // confkey
	IndexID      OID     `json:"index_id"`
	FKUpdateType string  `json:"fk_update_type"`
	FKDeleteType string  `json:"fk_delete_type"`
	// TypeID is non-zero for a domain constraint rather than a table one.
	TypeID OID    `json:"type_id"`
	Def    string `json:"def"`
	// Period is PG 18+; false on older versions, where the column does not exist.
	Period bool `json:"period"`
	// Enforced is PG 18+; true on older versions, where every constraint is
	// enforced by definition.
	Enforced bool `json:"enforced"`
}

// Index is S-06.
type Index struct {
	IndexRelID  OID  `json:"index_relid"`
	RelID       OID  `json:"relid"`
	IsUnique    bool `json:"is_unique"`
	IsPrimary   bool `json:"is_primary"`
	IsExclusion bool `json:"is_exclusion"`
	// IsValid false means a CREATE INDEX CONCURRENTLY failed and left debris.
	IsValid     bool    `json:"is_valid"`
	IsReady     bool    `json:"is_ready"`
	IsLive      bool    `json:"is_live"`
	IsClustered bool    `json:"is_clustered"`
	NAtts       int16   `json:"n_atts"`
	NKeyAtts    int16   `json:"n_key_atts"`
	Columns     []int16 `json:"columns,omitempty"`
	Def         string  `json:"def"`
	Predicate   string  `json:"predicate,omitempty"`
}

// InheritEdge is S-07: one edge of the inheritance or partition graph. The
// transitive closure is computed at load time (P-06).
type InheritEdge struct {
	Parent OID   `json:"parent"`
	Child  OID   `json:"child"`
	SeqNo  int32 `json:"seq_no"`
	// DetachPending marks an interrupted DETACH CONCURRENTLY.
	DetachPending bool `json:"detach_pending"`
	// IsDefaultPartition marks the implicit AEL target of ATTACH and DETACH.
	IsDefaultPartition bool `json:"is_default_partition"`
	// ParentIsPartitioned separates declarative partitioning from classic
	// INHERITS, which differ for VACUUM and ANALYZE scope.
	ParentIsPartitioned bool `json:"parent_is_partitioned"`
}

// Sequence is S-08.
type Sequence struct {
	SeqRelID OID   `json:"seq_relid"`
	TypeID   OID   `json:"type_id"`
	Max      int64 `json:"max"`
	Cycle    bool  `json:"cycle"`
	// OwnerTable is 0 for a standalone sequence.
	OwnerTable  OID    `json:"owner_table"`
	OwnerAttNum int16  `json:"owner_attnum"`
	DepType     string `json:"dep_type"` // 'a' OWNED BY / serial, 'i' identity
}

// Type is S-09: types, domains and enums.
type Type struct {
	OID        OID      `json:"oid"`
	Namespace  string   `json:"namespace"`
	Name       string   `json:"name"`
	Type       string   `json:"type"` // typtype: b d e c r m
	BaseTypeID OID      `json:"base_type_id"`
	TypeMod    int32    `json:"type_mod"`
	NotNull    bool     `json:"not_null"`
	ElemID     OID      `json:"elem_id"`
	RelID      OID      `json:"relid"`
	Category   string   `json:"category"`
	Len        int16    `json:"len"`
	Input      OID      `json:"input"`
	Output     OID      `json:"output"`
	EnumLabels []string `json:"enum_labels,omitempty"`
	// DomainConstraints > 0 is the whole of P-11: a constrained domain forces
	// ADD COLUMN to rewrite.
	DomainConstraints int `json:"domain_constraints"`
}

// Trigger is S-10. Internal triggers are excluded: FK enforcement triggers are
// already visible through S-05.
type Trigger struct {
	OID           OID    `json:"oid"`
	RelID         OID    `json:"relid"`
	Name          string `json:"name"`
	Enabled       string `json:"enabled"` // tgenabled: O D R A
	Type          int16  `json:"type"`    // tgtype bitmask
	ConstraintOID OID    `json:"constraint_oid"`
	Def           string `json:"def"`
}

// Policy is S-10.
type Policy struct {
	OID        OID    `json:"oid"`
	RelID      OID    `json:"relid"`
	Name       string `json:"name"`
	Cmd        string `json:"cmd"`
	Permissive bool   `json:"permissive"`
}

// ViewDep is S-11: a view or matview depending on a relation, and on which
// column of it. The attnum is what separates a DROP COLUMN that cascades from
// one that does not.
type ViewDep struct {
	DependentRelID   OID   `json:"dependent_relid"`
	ReferencedRelID  OID   `json:"referenced_relid"`
	ReferencedAttNum int16 `json:"referenced_attnum"`
}

// DependEdge is S-12: the catch-all CASCADE blast radius that S-11 misses.
type DependEdge struct {
	ClassID     OID    `json:"classid"`
	ObjID       OID    `json:"objid"`
	ObjSubID    int32  `json:"objsubid"`
	RefClassID  OID    `json:"refclassid"`
	RefObjID    OID    `json:"refobjid"`
	RefObjSubID int32  `json:"refobjsubid"`
	DepType     string `json:"deptype"`
}

// Proc is S-13. This table, not pg_depend, is the authority on volatility.
type Proc struct {
	OID          OID    `json:"oid"`
	Namespace    string `json:"namespace"`
	Name         string `json:"name"`
	Volatility   string `json:"volatility"` // provolatile: i s v
	IsStrict     bool   `json:"is_strict"`
	Kind         string `json:"kind"` // prokind: f p a w
	NArgs        int16  `json:"n_args"`
	NArgDefaults int16  `json:"n_arg_defaults"`
	IsVariadic   bool   `json:"is_variadic"`
	IdentityArgs string `json:"identity_args"`
}

// Cast is S-14. Method 'b' is binary coercible, which is what lets ALTER COLUMN
// TYPE skip the rewrite (P-12).
type Cast struct {
	Source  OID    `json:"source"`
	Target  OID    `json:"target"`
	Method  string `json:"method"`
	Context string `json:"context"`
	Func    OID    `json:"func"`
}

// Operator is S-14. Its volatility is the volatility of oprcode (P-08).
type Operator struct {
	OID       OID    `json:"oid"`
	Name      string `json:"name"`
	Namespace OID    `json:"namespace"`
	Left      OID    `json:"left"`
	Right     OID    `json:"right"`
	Code      OID    `json:"code"` // oprcode
}

// Publication is S-15. A published table changes the risk profile of a rewrite.
type Publication struct {
	OID       OID    `json:"oid"`
	Name      string `json:"name"`
	AllTables bool   `json:"all_tables"`
	Insert    bool   `json:"insert"`
	Update    bool   `json:"update"`
	Delete    bool   `json:"delete"`
}

// PublicationRel is S-15.
type PublicationRel struct {
	PubID OID `json:"pub_id"`
	RelID OID `json:"rel_id"`
}

// Extension is S-15.
type Extension struct {
	OID       OID    `json:"oid"`
	Name      string `json:"name"`
	Version   string `json:"version"`
	Namespace OID    `json:"namespace"`
}

// Setting is S-16. These are the *collector's* session values: the migration
// runner may use different ones, so they are defaults that the migration's own
// SET statements override.
type Setting struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Source string `json:"source"`
}

// TableStat is S-17. Severity input only — no classification may depend on it,
// and it is meaningless on a replica or a restored dump.
type TableStat struct {
	RelID          OID        `json:"relid"`
	Namespace      string     `json:"namespace"`
	Name           string     `json:"name"`
	SeqScan        int64      `json:"seq_scan"`
	IdxScan        int64      `json:"idx_scan"`
	TupIns         int64      `json:"tup_ins"`
	TupUpd         int64      `json:"tup_upd"`
	TupDel         int64      `json:"tup_del"`
	LiveTup        int64      `json:"live_tup"`
	DeadTup        int64      `json:"dead_tup"`
	LastAutovacuum *time.Time `json:"last_autovacuum,omitempty"`
	LastAnalyze    *time.Time `json:"last_analyze,omitempty"`
}

// Role is S-18.
type Role struct {
	OID      OID    `json:"oid"`
	Name     string `json:"name"`
	Super    bool   `json:"super"`
	CanLogin bool   `json:"can_login"`
}

// RelACL is S-18: the access control list of one relation.
type RelACL struct {
	RelID OID      `json:"relid"`
	ACL   []string `json:"acl,omitempty"`
}

// GatingVersion is the version range §8 evaluates against.
//
// With a production snapshot the version is known exactly. Without one, the
// baseline's version says nothing about production — the two databases can
// legitimately differ, which is the whole reason the parser reports version
// errors at all — so the range starts fully open across the supported versions
// and is narrowed later by the syntax the migration actually uses. A range
// wider than one version is what inhibits version-sensitive errors and makes
// version-ambiguous findings carry the range they apply to.
func GatingVersion(snapshots ...*Snapshot) pg_contract.VersionRange {
	for _, snapshot := range snapshots {
		if snapshot == nil || snapshot.Source != SourceProd {
			continue
		}
		if version := snapshot.Header.Version; version.IsSupported() {
			return pg_contract.Exactly(version)
		}
	}
	return pg_contract.Between(pg_contract.MinSupportedVersion, pg_contract.MaxSupportedVersion)
}
