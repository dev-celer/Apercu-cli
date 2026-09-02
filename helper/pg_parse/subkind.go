package pg_parse

// SubKind names one clause of an ALTER TABLE statement
type SubKind uint8

const (
	SubUnknown SubKind = iota

	// columns.
	SubAddColumn
	SubDropColumn
	SubAlterColumnType
	SubSetDefault
	SubDropDefault
	SubSetNotNull
	SubDropNotNull
	SubSetExpression
	SubDropExpression
	SubAddIdentity
	SubSetIdentity
	SubDropIdentity
	SubSetStatistics
	SubSetAttributeOptions
	SubResetAttributeOptions
	SubSetStorage
	SubSetCompression
	SubRenameColumn

	// constraints.
	SubAddConstraint
	SubValidateConstraint
	SubDropConstraint
	SubAlterConstraint
	SubRenameConstraint

	// triggers, rules, row-level security.
	SubEnableTrigger
	SubDisableTrigger
	SubEnableRule
	SubDisableRule
	SubEnableRowSecurity
	SubDisableRowSecurity
	SubForceRowSecurity
	SubNoForceRowSecurity

	// storage, layout, ownership.
	SubSetRelOptions
	SubResetRelOptions
	SubSetTablespace
	SubSetAccessMethod
	SubSetLogged
	SubSetUnlogged
	SubClusterOn
	SubDropCluster
	SubDropOids
	SubChangeOwner
	SubReplicaIdentity
	SubRenameRelation
	SubSetSchema

	// partitions and inheritance.
	SubAttachPartition
	SubDetachPartition
	SubAddInherit
	SubDropInherit
	SubAddOf
	SubDropOf

	// clauses that are not ALTER TABLE subcommands but reach the IR the same way, so a rule reads one shape rather than two.
	SubCreateIndex
	SubLike
	SubAddEnumValue
	SubRenameEnumValue

	// Foreign-table clauses.
	SubGenericOptions
	SubAlterColumnGenericOptions

	// SET / RESET statement forms.
	SubSetVariable
	SubResetVariable
	SubSetVariableCurrent
	SubSetTransaction
)

var subKindNames = map[SubKind]string{
	SubUnknown:                   "UNKNOWN",
	SubAddColumn:                 "ADD COLUMN",
	SubDropColumn:                "DROP COLUMN",
	SubAlterColumnType:           "ALTER COLUMN TYPE",
	SubSetDefault:                "SET DEFAULT",
	SubDropDefault:               "DROP DEFAULT",
	SubSetNotNull:                "SET NOT NULL",
	SubDropNotNull:               "DROP NOT NULL",
	SubSetExpression:             "SET EXPRESSION",
	SubDropExpression:            "DROP EXPRESSION",
	SubAddIdentity:               "ADD IDENTITY",
	SubSetIdentity:               "SET IDENTITY",
	SubDropIdentity:              "DROP IDENTITY",
	SubSetStatistics:             "SET STATISTICS",
	SubSetAttributeOptions:       "SET ATTRIBUTE OPTIONS",
	SubResetAttributeOptions:     "RESET ATTRIBUTE OPTIONS",
	SubSetStorage:                "SET STORAGE",
	SubSetCompression:            "SET COMPRESSION",
	SubRenameColumn:              "RENAME COLUMN",
	SubAddConstraint:             "ADD CONSTRAINT",
	SubValidateConstraint:        "VALIDATE CONSTRAINT",
	SubDropConstraint:            "DROP CONSTRAINT",
	SubAlterConstraint:           "ALTER CONSTRAINT",
	SubRenameConstraint:          "RENAME CONSTRAINT",
	SubEnableTrigger:             "ENABLE TRIGGER",
	SubDisableTrigger:            "DISABLE TRIGGER",
	SubEnableRule:                "ENABLE RULE",
	SubDisableRule:               "DISABLE RULE",
	SubEnableRowSecurity:         "ENABLE ROW LEVEL SECURITY",
	SubDisableRowSecurity:        "DISABLE ROW LEVEL SECURITY",
	SubForceRowSecurity:          "FORCE ROW LEVEL SECURITY",
	SubNoForceRowSecurity:        "NO FORCE ROW LEVEL SECURITY",
	SubSetRelOptions:             "SET",
	SubResetRelOptions:           "RESET",
	SubSetTablespace:             "SET TABLESPACE",
	SubSetAccessMethod:           "SET ACCESS METHOD",
	SubSetLogged:                 "SET LOGGED",
	SubSetUnlogged:               "SET UNLOGGED",
	SubClusterOn:                 "CLUSTER ON",
	SubDropCluster:               "SET WITHOUT CLUSTER",
	SubDropOids:                  "SET WITHOUT OIDS",
	SubChangeOwner:               "OWNER TO",
	SubReplicaIdentity:           "REPLICA IDENTITY",
	SubRenameRelation:            "RENAME TO",
	SubSetSchema:                 "SET SCHEMA",
	SubAttachPartition:           "ATTACH PARTITION",
	SubDetachPartition:           "DETACH PARTITION",
	SubAddInherit:                "INHERIT",
	SubDropInherit:               "NO INHERIT",
	SubAddOf:                     "OF",
	SubDropOf:                    "NOT OF",
	SubCreateIndex:               "CREATE INDEX",
	SubLike:                      "LIKE",
	SubAddEnumValue:              "ADD VALUE",
	SubRenameEnumValue:           "RENAME VALUE",
	SubGenericOptions:            "OPTIONS",
	SubAlterColumnGenericOptions: "ALTER COLUMN OPTIONS",
	SubSetVariable:               "SET VARIABLE",
	SubResetVariable:             "RESET VARIABLE",
	SubSetVariableCurrent:        "SET VARIABLE FROM CURRENT",
	SubSetTransaction:            "SET TRANSACTION",
}

func (k SubKind) String() string {
	if name, ok := subKindNames[k]; ok {
		return name
	}
	return "UNKNOWN"
}

// Exclusive reports whether the clause can never share a statement with another one.
// A statement holding one of these has exactly one subcommand.
func (k SubKind) Exclusive() bool {
	switch k {
	case SubRenameRelation, SubRenameColumn, SubRenameConstraint, SubSetSchema,
		SubAttachPartition, SubDetachPartition, SubRenameEnumValue:
		return true
	default:
		return false
	}
}
