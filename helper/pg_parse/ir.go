// Package pg_parse turns the SQL the proxy captured into the intermediate representation the classifier reads.
package pg_parse

import (
	"strings"

	"apercu-cli/helper"
	"apercu-cli/helper/pg_contract"
)

// Statement is the intermediate representation of an SQL statement.
type Statement struct {
	RawSQL string
	// Command is the top-level command, "ALTER TABLE", "CREATE INDEX"...
	Command pg_contract.Command
	// Relations are the relations the statement names literally, in the order it names them.
	Relations []RelationRef
	// Subcommands is the decomposition of a statement that carries a subcommand list.
	Subcommands []Subcommand
	// Flags are the statement-level modifiers a rule branches on.
	Flags Flags
	// Options is the parenthesized option list of VACUUM, ANALYZE, REINDEX, CREATE TABLE...
	Options []Option
	// Features is the version-bound syntax the statement uses.
	// This is also used by the PG-18 shim to reinject the stripped properties after normalization
	Features []Feature
	// featureDepth says from which list the features injection should happen
	// 0 for an ALTER TABLE statement's top-level list.
	// 1 for the parenthesized element list of a CREATE TABLE.
	featureDepth int
	Unparsed     string
}

// Parsed reports whether the statement produced an IR at all.
func (s Statement) Parsed() bool { return s.Unparsed == "" }

// Uses reports whether the statement carries the given feature.
func (s Statement) Uses(name FeatureName) bool {
	for _, f := range s.Features {
		if f.Name == name {
			return true
		}
	}
	return false
}

// MinVersion is the oldest server that accepts every construct the statement uses.
func (s Statement) MinVersion() pg_contract.Version {
	lowest := pg_contract.MinSupportedVersion
	for _, f := range s.Features {
		if f.Since > lowest {
			lowest = f.Since
		}
	}
	return lowest
}

// RelationRef is a relation as the statement names it. Schema may be empty.
type RelationRef struct {
	Name helper.FullRelationName
	// Kind is what the statement asserts the relation is.
	Kind pg_contract.RelationKind
	// Only records that the statement wrote ONLY, which suppresses recursion to partitions and inheritance children.
	Only bool
	// Alias is the correlation name a DML statement gave the relation, empty otherwise.
	Alias string
}

func (r RelationRef) String() string {
	if r.Only {
		return "ONLY " + r.Name.String()
	}
	return r.Name.String()
}

// Flags are the modifiers that decide which rule applies.
type Flags struct {
	IfExists     bool
	IfNotExists  bool
	Concurrently bool
	Cascade      bool
	OrReplace    bool
	Unique       bool
	// Finalize is ALTER TABLE … DETACH PARTITION … FINALIZE.
	Finalize bool
	// Nowait is the NOWAIT of LOCK TABLE and of ALTER … ALL IN TABLESPACE.
	Nowait bool
	// Persistence is the TEMP / UNLOGGED a CREATE carries.
	Persistence Persistence
	// WithData records CREATE MATERIALIZED VIEW … WITH [NO] DATA
	WithData bool
	// RestartIdentity is TRUNCATE … RESTART IDENTITY.
	RestartIdentity bool
}

// Option is one entry of a parenthesised option list: a storage parameter, an attribute option, a VACUUM option.
type Option struct {
	// Namespace may be empty
	Namespace string
	Name      string
	Value     string
}

// Subcommand is one comma-separated clause of an ALTER statement. A statement that carries no
// clause list is represented by a single subcommand describing the whole statement.
type Subcommand struct {
	Kind SubKind
	// Name is the object the clause acts on: a column, a constraint, a trigger, a rule.
	// Empty when the clause acts on the relation as a whole.
	Name string
	// NewName is the target of a RENAME.
	NewName string
	// Column carries the definition of ADD COLUMN and the new type of ALTER COLUMN TYPE.
	Column *ColumnDef
	// Constraint carries the definition of ADD CONSTRAINT and the attributes of ALTER CONSTRAINT.
	Constraint *ConstraintDef
	// Relations are the extra relations the clause names.
	Relations []RelationRef
	// Options is the parenthesized list of a SET/RESET clause, empty otherwise.
	Options []Option
	// Expr is the clause's expression: a SET DEFAULT value, a SET EXPRESSION body.
	Expr *Expr
	// Value is the single bare argument the clause's kind implies and that has no better attribute.
	// the tablespace of SET TABLESPACE, the access method of SET ACCESS METHOD, the role of OWNER TO,
	// the mode of SET STORAGE, the method of SET COMPRESSION, the target of SET STATISTICS,
	// the variant of REPLICA IDENTITY, the schema of SET SCHEMA.
	Value string
	// Flags are the clause's own modifiers.
	Flags Flags
	// loc is where the clause starts in the parsed text.
	// The shim uses it to attach a feature it had to blank out to the clause the feature belonged to.
	loc int
}

// Persistence is the storage durability a CREATE asks for.
type Persistence uint8

const (
	PersistencePermanent Persistence = iota
	PersistenceTemporary
	PersistenceUnlogged
)

func (p Persistence) String() string {
	switch p {
	case PersistenceTemporary:
		return "TEMPORARY"
	case PersistenceUnlogged:
		return "UNLOGGED"
	default:
		return "PERMANENT"
	}
}

// ColumnDef is a column as the statement declares it.
type ColumnDef struct {
	Name string
	Type TypeRef
	// Collation is the COLLATE clause, empty when the statement omitted it.
	Collation []string
	// Constraints are the inline column constraints, in the order written.
	Constraints []ConstraintDef
	// Generated is how the column produces its value.
	Generated Generated
	// GeneratedExpr is the body of GENERATED … AS (…), nil for an identity or plain column.
	GeneratedExpr *Expr
	// Default is the DEFAULT expression, nil when the column has none.
	Default *Expr
	// Using is the USING expression of ALTER COLUMN TYPE, nil when the statement omitted it.
	Using *Expr
	// loc is where the definition starts in the parsed text, for the shim's reinjection only.
	loc int
}

// Generated is how a column's value is produced.
type Generated uint8

const (
	GeneratedNone Generated = iota
	// GeneratedStored materialises the expression on disk.
	GeneratedStored
	// GeneratedVirtual computes the expression on read. PG18+.
	GeneratedVirtual
	GeneratedIdentityAlways
	GeneratedIdentityByDefault
)

func (g Generated) String() string {
	switch g {
	case GeneratedStored:
		return "STORED"
	case GeneratedVirtual:
		return "VIRTUAL"
	case GeneratedIdentityAlways:
		return "IDENTITY ALWAYS"
	case GeneratedIdentityByDefault:
		return "IDENTITY BY DEFAULT"
	default:
		return "NONE"
	}
}

// IsIdentity reports whether the column draws its value from a sequence.
func (g Generated) IsIdentity() bool {
	return g == GeneratedIdentityAlways || g == GeneratedIdentityByDefault
}

// TypeRef is a type name as the statement wrote it, resolved to libpg_query's spelling: an
// unqualified "int" arrives as pg_catalog.int4.
type TypeRef struct {
	// Name is the qualified name, schema first.
	Name []string
	// Typmods are the parenthesized arguments as written: {"6"} for timestamp(6), {"10","2"} for numeric(10,2).
	// Empty means the statement gave no precision.
	Typmods []string
	// ArrayBounds is how many array dimensions the type has, 0 for a scalar.
	ArrayBounds int
	// PctType is true for the %TYPE form.
	PctType bool
}

// Base is the type name without its schema.
func (t TypeRef) Base() string {
	if len(t.Name) == 0 {
		return ""
	}
	return t.Name[len(t.Name)-1]
}

// Schema is the type's schema, empty when the statement did not qualify it.
func (t TypeRef) Schema() string {
	if len(t.Name) < 2 {
		return ""
	}
	return strings.Join(t.Name[:len(t.Name)-1], ".")
}

func (t TypeRef) IsZero() bool { return len(t.Name) == 0 }

func (t TypeRef) String() string {
	var b strings.Builder
	b.WriteString(strings.Join(t.Name, "."))
	if len(t.Typmods) > 0 {
		b.WriteString("(" + strings.Join(t.Typmods, ",") + ")")
	}
	b.WriteString(strings.Repeat("[]", t.ArrayBounds))
	return b.String()
}

// ConstraintType is what a constraint enforces.
type ConstraintType uint8

const (
	ConstraintUnknown ConstraintType = iota
	ConstraintNotNull
	ConstraintNull
	ConstraintDefault
	ConstraintCheck
	ConstraintPrimaryKey
	ConstraintUnique
	ConstraintExclusion
	ConstraintForeignKey
	ConstraintIdentity
	ConstraintGenerated
)

var constraintTypeNames = map[ConstraintType]string{
	ConstraintNotNull:    "NOT NULL",
	ConstraintNull:       "NULL",
	ConstraintDefault:    "DEFAULT",
	ConstraintCheck:      "CHECK",
	ConstraintPrimaryKey: "PRIMARY KEY",
	ConstraintUnique:     "UNIQUE",
	ConstraintExclusion:  "EXCLUDE",
	ConstraintForeignKey: "FOREIGN KEY",
	ConstraintIdentity:   "IDENTITY",
	ConstraintGenerated:  "GENERATED",
}

func (c ConstraintType) String() string {
	if name, ok := constraintTypeNames[c]; ok {
		return name
	}
	return "UNKNOWN"
}

// ConstraintDef is a constraint as the statement declares it.
type ConstraintDef struct {
	Name string
	Type ConstraintType
	// Columns are the constrained columns.
	Columns []string
	// KeyExprs holds the EXCLUDE elements that are expressions rather than bare columns.
	KeyExprs []*Expr
	// Expr is the CHECK body or the DEFAULT value, nil for the other kinds.
	Expr *Expr
	// References is the table a FOREIGN KEY points at, zero otherwise.
	References RelationRef
	// ReferencedColumns are the columns on the other side of a FOREIGN KEY.
	ReferencedColumns []string
	// UsingIndex is the index a PRIMARY KEY or UNIQUE adopts instead of building its own.
	// Empty when the constraint builds one.
	UsingIndex string
	// NotValid marks a constraint added without validating the existing rows.
	NotValid bool
	// NotEnforced marks a constraint that is never checked at all. PG18+.
	NotEnforced bool
	// NoInherit stops a CHECK or NOT NULL from reaching inheritance children.
	NoInherit bool
	// Inherit and NoInheritSet record ALTER CONSTRAINT … {INHERIT|NO INHERIT}, PG18+.
	// The second field distinguishes "the clause said INHERIT" from "the clause said nothing".
	Inherit      bool
	NoInheritSet bool
	Deferrable   bool
	InitiallyDef bool
	DeferralSet  bool
	// EnforcementSet records that ALTER CONSTRAINT named ENFORCED or NOT ENFORCED, PG18+.
	EnforcementSet bool
	// WithoutOverlaps is the temporal-key form of a PRIMARY KEY or UNIQUE, PG18+.
	WithoutOverlaps bool
	// Period is the FOREIGN KEY … PERIOD form, PG18+.
	Period bool
	// Storage parameters of the index a PRIMARY KEY or UNIQUE builds, plus its tablespace.
	Options    []Option
	Tablespace string
	// generatedWhen is the parser's ALWAYS / BY DEFAULT marker for an identity constraint.
	generatedWhen string
	// loc is where the constraint starts in the parsed text, for the shim's reinjection only.
	loc int
}

func (c ConstraintDef) Validated() bool { return !c.NotValid && !c.NotEnforced }
