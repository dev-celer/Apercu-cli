package pg_contract

import (
	"apercu-cli/helper"
	"fmt"
)

// RelationKind mirrors pg_class.relkind.
type RelationKind uint8

const (
	RelationKindUnknown RelationKind = iota
	RelationKindTable
	RelationKindPartitionedTable
	RelationKindIndex
	RelationKindPartitionedIndex
	RelationKindView
	RelationKindMaterializedView
	RelationKindSequence
	RelationKindForeignTable
	RelationKindCompositeType
	RelationKindToastTable
)

var relationKindNames = map[RelationKind]string{
	RelationKindUnknown:          "UNKNOWN",
	RelationKindTable:            "TABLE",
	RelationKindPartitionedTable: "PARTITIONED_TABLE",
	RelationKindIndex:            "INDEX",
	RelationKindPartitionedIndex: "PARTITIONED_INDEX",
	RelationKindView:             "VIEW",
	RelationKindMaterializedView: "MATERIALIZED_VIEW",
	RelationKindSequence:         "SEQUENCE",
	RelationKindForeignTable:     "FOREIGN_TABLE",
	RelationKindCompositeType:    "COMPOSITE_TYPE",
	RelationKindToastTable:       "TOAST_TABLE",
}

var relationKindAliases = buildAliases(relationKindNames, map[string]RelationKind{
	"": RelationKindUnknown,
})

// relkinds maps pg_class.relkind to RelationKind. It is keyed by byte rather
// than going through normalizeEnumKey because relkind is case-sensitive:
// 'i' is an index and 'I' a partitioned one.
var relkinds = map[byte]RelationKind{
	'r': RelationKindTable,
	'p': RelationKindPartitionedTable,
	'i': RelationKindIndex,
	'I': RelationKindPartitionedIndex,
	'v': RelationKindView,
	'm': RelationKindMaterializedView,
	'S': RelationKindSequence,
	'f': RelationKindForeignTable,
	'c': RelationKindCompositeType,
	't': RelationKindToastTable,
}

// RelationKindFromRelkind converts a pg_class.relkind value. An unrecognised or empty value is RelationKindUnknown.
func RelationKindFromRelkind(relkind string) RelationKind {
	if len(relkind) != 1 {
		return RelationKindUnknown
	}
	return relkinds[relkind[0]]
}

func (k RelationKind) String() string {
	if name, ok := relationKindNames[k]; ok {
		return name
	}
	return "UNKNOWN"
}

// IsTable reports whether if the relation is a table, partitioned parents included.
func (k RelationKind) IsTable() bool {
	return k == RelationKindTable || k == RelationKindPartitionedTable
}

// IsIndex reports whether the relation is an index, partitioned or not.
func (k RelationKind) IsIndex() bool {
	return k == RelationKindIndex || k == RelationKindPartitionedIndex
}

// IsPartitioned reports whether the relation is a partitioned parent, which has no storage of its own.
func (k RelationKind) IsPartitioned() bool {
	return k == RelationKindPartitionedTable || k == RelationKindPartitionedIndex
}

// ParseRelationKind resolves any spelling of a relation kind.
func ParseRelationKind(s string) (RelationKind, error) {
	return parseEnum(s, relationKindAliases)
}

func (k RelationKind) MarshalText() ([]byte, error) {
	return marshalEnum(k, relationKindNames)
}

func (k *RelationKind) UnmarshalText(data []byte) error {
	parsed, err := ParseRelationKind(string(data))
	if err != nil {
		return err
	}
	*k = parsed
	return nil
}

func (k RelationKind) MarshalYAML() (any, error) {
	return k.String(), nil
}

func (k *RelationKind) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	return k.UnmarshalText([]byte(s))
}

// Relation is a schema-qualified object a statement locks.
type Relation struct {
	Name helper.FullRelationName `json:"name" yaml:"name"`
	Kind RelationKind            `json:"kind,omitempty" yaml:"kind,omitempty"`
}

func NewRelation(schema, name string, kind RelationKind) Relation {
	return Relation{
		Name: helper.FullRelationName{Schema: schema, Table: name},
		Kind: kind,
	}
}

func (r Relation) String() string {
	return r.Name.String()
}

func (r Relation) IsZero() bool {
	return r.Name.Schema == "" && r.Name.Table == ""
}

// TargetRole records why a relation ended up in a target list.
type TargetRole uint8

const (
	// TargetRoleDirect is named literally in the statement.
	TargetRoleDirect TargetRole = iota
	// TargetRoleResolved the named object is not a table and the table behind it had to be looked up.
	TargetRoleResolved
	// TargetRoleExpanded is a descendant implied by the named table.
	TargetRoleExpanded
	// TargetRoleImplicit is locked but never named: the other side of a foreign key, a default partition.
	TargetRoleImplicit
)

var targetRoleNames = map[TargetRole]string{
	TargetRoleDirect:   "DIRECT",
	TargetRoleResolved: "RESOLVED",
	TargetRoleExpanded: "EXPANDED",
	TargetRoleImplicit: "IMPLICIT",
}

var targetRoleAliases = buildAliases(targetRoleNames, nil)

func (r TargetRole) String() string {
	if name, ok := targetRoleNames[r]; ok {
		return name
	}
	return "UNKNOWN"
}

// ParseTargetRole resolves any spelling of a target role.
func ParseTargetRole(s string) (TargetRole, error) {
	return parseEnum(s, targetRoleAliases)
}

func (r TargetRole) MarshalText() ([]byte, error) {
	return marshalEnum(r, targetRoleNames)
}

func (r *TargetRole) UnmarshalText(data []byte) error {
	parsed, err := ParseTargetRole(string(data))
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}

func (r TargetRole) MarshalYAML() (any, error) {
	return r.String(), nil
}

func (r *TargetRole) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	return r.UnmarshalText([]byte(s))
}

// Target is one relation a statement acts on, with the lock it takes and the work it does under that lock.
type Target struct {
	Relation Relation   `json:"relation" yaml:"relation"`
	Lock     Lock       `json:"lock" yaml:"lock"`
	OpKind   OpKind     `json:"op_kind" yaml:"op_kind"`
	Role     TargetRole `json:"role" yaml:"role"`
}

func (t Target) String() string {
	return fmt.Sprintf("%s %s/%s (%s)", t.Relation, t.Lock.Short(), t.OpKind, t.Role)
}
