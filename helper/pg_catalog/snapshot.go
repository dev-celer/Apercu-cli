// Package pg_catalog captures the PostgreSQL catalog snapshots. The derived lookup are built on top of this.
package pg_catalog

import (
	"apercu-cli/helper/pg_contract"
	"time"
)

// OID is a PostgreSQL object identifier. Zero means "none".
type OID uint32

// Source is the database a snapshot was taken from.
type Source string

const (
	// SourceBaseline is the pre-migration subsetted or anonymized database.
	SourceBaseline Source = "baseline"
	// SourceProd is the real production database.
	SourceProd Source = "prod"
)

// PIT (for Point In Time) is when the snapshot was taken relative to the migration.
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
	// captured, because the collector needs the version to decide which columns exist
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
	Collations      []Collation      `json:"collations"`       // S-19
}

// Header is S-00. General information about the server / the database.
// Version-sensitive snapshotting depend on this
type Header struct {
	ServerVersionNum int                 `json:"server_version_num"`
	Version          pg_contract.Version `json:"version"`
	Database         string              `json:"database"`
	User             string              `json:"user"`
	SearchPath       string              `json:"search_path"`
	TimeZone         string              `json:"timezone"`
	// FromReplica is pg_is_in_recovery(). Activity statistics (S-17) are meaningless when it is true.
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
	Kind         string   `json:"kind"`        // pg_class.relkind, accepted kind: ('r','p','i','I','m','v','S','f','c')
	Persistence  string   `json:"persistence"` // 'p' permanent, 'u' unlogged, 't' temp
	IsPartition  bool     `json:"is_partition"`
	RowSecurity  bool     `json:"row_security"`
	HasSubclass  bool     `json:"has_subclass"`
	Tuples       float64  `json:"tuples"`
	Pages        int64    `json:"pages"`
	Options      []string `json:"options,omitempty"`
	Tablespace   OID      `json:"tablespace"`
	OfType       OID      `json:"of_type"` // For typed table only, link to the composite Type
	AccessMethod string   `json:"access_method,omitempty"`
	Owner        string   `json:"owner"`
	// HeapBytes and TotalBytes are both 0 for a partitioned parent, which has no storage of its own.
	HeapBytes      int64  `json:"heap_bytes"`
	TotalBytes     int64  `json:"total_bytes"`
	PartitionBound string `json:"partition_bound,omitempty"`
	PartitionKey   string `json:"partition_key,omitempty"`
}

// Column is S-03.
type Column struct {
	RelID         OID    `json:"relid"` // Link to the table (Relation) the column appear in
	Num           int16  `json:"num"`
	Name          string `json:"name"`
	TypeID        OID    `json:"type_id"` // Link to the Type of the column
	FormattedType string `json:"formatted_type"`
	TypeMod       int32  `json:"type_mod"`
	// NotNull is authoritative up to PG 17. On 18 a NOT NULL can exist as an unenforced constraint while attnotnull is still true.
	NotNull     bool   `json:"not_null"`
	Generated   string `json:"generated"` // '' none, 's' stored, 'v' virtual (18+)
	Identity    string `json:"identity"`
	Storage     string `json:"storage"`
	Compression string `json:"compression"`
	IsLocal     bool   `json:"is_local"`
	InhCount    int32  `json:"inh_count"`
	// Collation is 0 when the column type is not collatable, else the OID link to Collation.
	Collation OID `json:"collation"`
	// StatsTarget is nil when the column uses the database default.
	// The catalog spells that -1 on PG 15/16 and NULL on 17/18; both normalize to nil here.
	StatsTarget *int32 `json:"stats_target,omitempty"`
}

// ColumnDefault is S-04.
type ColumnDefault struct {
	RelID OID    `json:"relid"` // Link to the table (Relation) the column appear in
	Num   int16  `json:"num"`   // Link to the Column Num
	Expr  string `json:"expr"`
	// ReferencedProcs record only the referenced user functions, it is empty for pinned functions.
	// For user functions, the OID link to Proc
	ReferencedProcs []OID `json:"referenced_procs,omitempty"`
	// ReferencedOperators record only the referenced user operators, it is empty for pinned operator.
	// for user operators, the OID link to Operator
	ReferencedOperators []OID `json:"referenced_operators,omitempty"`
}

// Constraint is S-05.
type Constraint struct {
	OID          OID     `json:"oid"`
	Name         string  `json:"name"`
	RelID        OID     `json:"relid"`         // For table constraint, the OID link to Relation
	ForeignRelID OID     `json:"foreign_relid"` // For foreign key, the OID link to Relation and represent the referenced table
	Type         string  `json:"type"`          // contype: c p u f t x n
	Validated    bool    `json:"validated"`
	Deferrable   bool    `json:"deferrable"`
	Deferred     bool    `json:"deferred"`
	IsLocal      bool    `json:"is_local"`
	InhCount     int32   `json:"inh_count"`
	NoInherit    bool    `json:"no_inherit"`
	Key          []int16 `json:"key,omitempty"`         // For table constraint, array of the constraint columns num, link to Column.Num
	ForeignKey   []int16 `json:"foreign_key,omitempty"` // For foreign key, array of the referenced columns num, link to Column.Num
	IndexID      OID     `json:"index_id"`              // If backed by an index, the OID link to Index & Relation
	FKUpdateType string  `json:"fk_update_type"`
	FKDeleteType string  `json:"fk_delete_type"`
	TypeID       OID     `json:"type_id"` // For domain constraint, the OID link to Type
	Def          string  `json:"def"`
	// Period is PG 18+; false on older versions, where the column does not exist.
	Period bool `json:"period"`
	// Enforced is PG 18+; true on older versions, where every constraint is enforced by definition.
	Enforced bool `json:"enforced"`
}

// Index is S-06.
type Index struct {
	IndexRelID  OID   `json:"index_relid"` // Indexes are also present in the relation table, so OID link to Relation
	RelID       OID   `json:"relid"`       // This represents the table the index is on, the OID link to Relation
	IsUnique    bool  `json:"is_unique"`
	IsPrimary   bool  `json:"is_primary"`
	IsExclusion bool  `json:"is_exclusion"`
	IsValid     bool  `json:"is_valid"` // IsValid false means a CREATE INDEX CONCURRENTLY failed and left debris.
	IsReady     bool  `json:"is_ready"`
	IsLive      bool  `json:"is_live"`
	IsClustered bool  `json:"is_clustered"`
	NAtts       int16 `json:"n_atts"`     // Total number of columns in the index
	NKeyAtts    int16 `json:"n_key_atts"` // Number of key columns in the index
	// Array of column num for the index, key column come first
	// This link to Column.Num, except 0 which indicate an expression column
	Columns []int16 `json:"columns,omitempty"`
	// Array of collation OID, one entry per Columns entry and in the same order.
	// 0 indicate a column that is not collatable, else the OID link to Collation.
	Collations []OID  `json:"collations,omitempty"`
	Def        string `json:"def"`
	Predicate  string `json:"predicate,omitempty"`
}

// InheritEdge is S-07.
type InheritEdge struct {
	Parent             OID   `json:"parent"` // This OID link to Relation
	Child              OID   `json:"child"`  // This OID link to Relation
	SeqNo              int32 `json:"seq_no"`
	DetachPending      bool  `json:"detach_pending"` // DetachPending marks an interrupted DETACH CONCURRENTLY.
	IsDefaultPartition bool  `json:"is_default_partition"`
	// ParentIsPartitioned separates declarative partitioning from classic INHERITS.
	ParentIsPartitioned bool `json:"parent_is_partitioned"`
}

// Sequence is S-08.
type Sequence struct {
	SeqRelID    OID    `json:"seq_relid"`
	TypeID      OID    `json:"type_id"` // The OID link to Type
	Max         int64  `json:"max"`
	Cycle       bool   `json:"cycle"`
	OwnerTable  OID    `json:"owner_table"` // OwnerTable is 0 for a standalone sequence, else this OID link to Relation.
	OwnerAttNum int16  `json:"owner_attnum"`
	DepType     string `json:"dep_type"` // 'a' OWNED BY / serial, 'i' identity
}

// Type is S-09.
type Type struct {
	OID               OID      `json:"oid"`
	Namespace         string   `json:"namespace"`
	Name              string   `json:"name"`
	Type              string   `json:"type"`         // typtype: b d e c r m
	BaseTypeID        OID      `json:"base_type_id"` // For domain type, the OID link to Type
	TypeMod           int32    `json:"type_mod"`
	NotNull           bool     `json:"not_null"`
	ElemID            OID      `json:"elem_id"` // For array-like type, the OID link to Type
	RelID             OID      `json:"relid"`   // For composite type, the OID link to Relation
	Category          string   `json:"category"`
	Len               int16    `json:"len"`
	Input             OID      `json:"input"`  // This represents the text to type conversion function, the OID link to Proc
	Output            OID      `json:"output"` // This represents the type to text conversion function, the OID link to Proc
	EnumLabels        []string `json:"enum_labels,omitempty"`
	DomainConstraints int      `json:"domain_constraints"`
}

// Trigger is S-10.
type Trigger struct {
	OID           OID    `json:"oid"`
	RelID         OID    `json:"relid"` // The OID link to Relation
	Name          string `json:"name"`
	Enabled       string `json:"enabled"`        // tgenabled: O D R A
	Type          int16  `json:"type"`           // tgtype bitmask
	ConstraintOID OID    `json:"constraint_oid"` // If the trigger is for a constraint, the OID link to Constraint
	Def           string `json:"def"`
}

// Policy is S-10.
type Policy struct {
	OID        OID    `json:"oid"`
	RelID      OID    `json:"relid"` // The table this policy apply to, the OID link to Relation
	Name       string `json:"name"`
	Cmd        string `json:"cmd"`
	Permissive bool   `json:"permissive"`
}

// ViewDep is S-11.
type ViewDep struct {
	DependentRelID   OID   `json:"dependent_relid"`   // The OID link to Relation
	ReferencedRelID  OID   `json:"referenced_relid"`  // The OID link to Relation
	ReferencedAttNum int16 `json:"referenced_attnum"` // This link to the Column.Num of the referenced Relation
}

// DependEdge is S-12.
type DependEdge struct {
	ClassID     OID    `json:"classid"`
	ObjID       OID    `json:"objid"`
	ObjSubID    int32  `json:"objsubid"`
	RefClassID  OID    `json:"refclassid"`
	RefObjID    OID    `json:"refobjid"`
	RefObjSubID int32  `json:"refobjsubid"`
	DepType     string `json:"deptype"`
}

// Proc is S-13.
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

// Cast is S-14.
type Cast struct {
	Source  OID    `json:"source"` // The OID link to Type
	Target  OID    `json:"target"` // The OID link to Type
	Method  string `json:"method"` // 'f': Function cast, 'b': binary coercible, 'i': use input/output type function
	Context string `json:"context"`
	Func    OID    `json:"func"` // For function cast, The OID link to Proc
}

// Operator is S-14.
type Operator struct {
	OID       OID    `json:"oid"`
	Name      string `json:"name"`
	Namespace OID    `json:"namespace"`
	Left      OID    `json:"left"`  // This OID link to Type
	Right     OID    `json:"right"` // This OID link to Type
	Code      OID    `json:"code"`  // This represents the function used, the OID link to Proc
}

// Publication is S-15.
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
	PubID OID `json:"pub_id"` // The OID link to Publication
	RelID OID `json:"rel_id"` // The OID link to Relation
}

// Extension is S-15.
type Extension struct {
	OID       OID    `json:"oid"`
	Name      string `json:"name"`
	Version   string `json:"version"`
	Namespace OID    `json:"namespace"`
}

// Setting is S-16.
type Setting struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Source string `json:"source"`
}

// TableStat is S-17.
type TableStat struct {
	RelID          OID        `json:"relid"` // The OID link to Relation
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

// RelACL is S-18.
type RelACL struct {
	RelID OID      `json:"relid"` // The OID link to Relation
	ACL   []string `json:"acl,omitempty"`
}

// Collation is S-19.
type Collation struct {
	OID       OID    `json:"oid"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	// Encoding is -1 when the collation applies to every encoding, else a pg_encoding number.
	Encoding int32 `json:"encoding"`
}
