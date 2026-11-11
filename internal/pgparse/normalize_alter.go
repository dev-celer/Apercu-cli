package pgparse

import (
	"strconv"
	"strings"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// normalizeAlterTable decompose the pg_query ALTER TABLE, and the other statement that reuse AlterTableStmt,
// and transform it into an IR Statement Object.
func normalizeAlterTable(stmt *pg_query.AlterTableStmt, s ir.Statement) ir.Statement {
	s.Command = pgcontract.Command("ALTER " + objectWord(stmt.Objtype))
	s.Flags.IfExists = stmt.MissingOk

	target := relationRef(stmt.Relation)
	target.Kind = relationKindOf(stmt.Objtype)
	if stmt.Objtype == pg_query.ObjectType_OBJECT_TYPE {
		// For ALTER TYPE the parser leaves inh false, which relationRef() understand as Only = true. We need to manually reset it.
		target.Only = false
	}
	s.Relations = []ir.RelationRef{target}

	for _, cmd := range stmt.Cmds {
		alter, ok := cmd.GetNode().(*pg_query.Node_AlterTableCmd)
		if !ok {
			continue
		}
		s.Subcommands = append(s.Subcommands, alterTableCmd(alter.AlterTableCmd))
	}
	return s
}

// alterTableCmd converts one subcommand into it's IR Subcommand object.
func alterTableCmd(cmd *pg_query.AlterTableCmd) ir.Subcommand {
	sub := ir.Subcommand{Name: cmd.Name}
	sub.Flags.IfExists = cmd.MissingOk
	sub.Flags.Cascade = cmd.Behavior == pg_query.DropBehavior_DROP_CASCADE

	switch cmd.Subtype {
	case pg_query.AlterTableType_AT_AddColumn, pg_query.AlterTableType_AT_AddColumnToView:
		sub.Kind = ir.SubAddColumn
		if column := columnDef(cmd.Def); column != nil {
			sub.Column = column
			sub.Name = column.Name
		}
		// ADD COLUMN use MissingOK to mean IF NOT EXISTS instead of IF EXISTS
		sub.Flags.IfNotExists = cmd.MissingOk
		sub.Flags.IfExists = false

	case pg_query.AlterTableType_AT_DropColumn:
		sub.Kind = ir.SubDropColumn

	case pg_query.AlterTableType_AT_AlterColumnType:
		sub.Kind = ir.SubAlterColumnType
		if column := columnDef(cmd.Def); column != nil {
			column.Name = cmd.Name
			column.Using = column.Default
			column.Default = nil
			sub.Column = column
		}

	case pg_query.AlterTableType_AT_ColumnDefault, pg_query.AlterTableType_AT_CookedColumnDefault:
		// One subtype for both directions: an expression means SET, its absence means DROP.
		if cmd.Def == nil {
			sub.Kind = ir.SubDropDefault
		} else {
			sub.Kind = ir.SubSetDefault
			sub.Expr = convertExpr(cmd.Def)
		}

	case pg_query.AlterTableType_AT_SetNotNull, pg_query.AlterTableType_AT_CheckNotNull:
		sub.Kind = ir.SubSetNotNull
	case pg_query.AlterTableType_AT_DropNotNull:
		sub.Kind = ir.SubDropNotNull

	case pg_query.AlterTableType_AT_SetExpression:
		sub.Kind = ir.SubSetExpression
		sub.Expr = convertExpr(cmd.Def)
	case pg_query.AlterTableType_AT_DropExpression:
		sub.Kind = ir.SubDropExpression

	case pg_query.AlterTableType_AT_AddIdentity:
		sub.Kind = ir.SubAddIdentity
		if constraint := constraintDef(cmd.Def); constraint != nil {
			sub.Options = constraint.Options
			sub.Value = identityWhen(generatedWhen(cmd.Def))
		}
	case pg_query.AlterTableType_AT_SetIdentity:
		sub.Kind = ir.SubSetIdentity
		sub.Options = options(listItems(cmd.Def))
		for i, option := range sub.Options {
			// The parser reports SET GENERATED's argument as the character code PostgreSQL
			// stores; the IR exists so no rule ever has to know that.
			if strings.EqualFold(option.Name, "generated") {
				sub.Options[i].Value = identityWhen(charCode(option.Value))
				sub.Value = sub.Options[i].Value
			}
		}
	case pg_query.AlterTableType_AT_DropIdentity:
		sub.Kind = ir.SubDropIdentity

	case pg_query.AlterTableType_AT_SetStatistics:
		sub.Kind = ir.SubSetStatistics
		sub.Value = literalText(cmd.Def)
		if sub.Value == "" {
			// SET STATISTICS DEFAULT carries no argument.
			sub.Value = "DEFAULT"
		}
		if sub.Name == "" && cmd.Num != 0 {
			// An index column is named by its ordinal rather than by a name.
			sub.Name = "#" + strconv.Itoa(int(cmd.Num))
		}

	case pg_query.AlterTableType_AT_SetOptions:
		sub.Kind = ir.SubSetAttributeOptions
		sub.Options = options(listItems(cmd.Def))
	case pg_query.AlterTableType_AT_ResetOptions:
		sub.Kind = ir.SubResetAttributeOptions
		sub.Options = options(listItems(cmd.Def))

	case pg_query.AlterTableType_AT_SetStorage:
		sub.Kind = ir.SubSetStorage
		sub.Value = strings.ToUpper(literalText(cmd.Def))
	case pg_query.AlterTableType_AT_SetCompression:
		sub.Kind = ir.SubSetCompression
		sub.Value = literalText(cmd.Def)

	case pg_query.AlterTableType_AT_AddConstraint, pg_query.AlterTableType_AT_ReAddConstraint,
		pg_query.AlterTableType_AT_AddIndexConstraint, pg_query.AlterTableType_AT_ReAddDomainConstraint:
		sub.Kind = ir.SubAddConstraint
		if constraint := constraintDef(cmd.Def); constraint != nil {
			sub.Constraint = constraint
			sub.Name = constraint.Name
			if constraint.References.Name.Table != "" {
				sub.Relations = append(sub.Relations, constraint.References)
			}
		}

	case pg_query.AlterTableType_AT_ValidateConstraint:
		sub.Kind = ir.SubValidateConstraint
	case pg_query.AlterTableType_AT_DropConstraint:
		sub.Kind = ir.SubDropConstraint

	case pg_query.AlterTableType_AT_AlterConstraint:
		sub.Kind = ir.SubAlterConstraint
		constraint := constraintDef(cmd.Def)
		if constraint == nil {
			constraint = &ir.ConstraintDef{}
		}
		// The parser reports every ALTER CONSTRAINT as if it were a foreign key, so the type is irrelevant.
		constraint.Type = ir.ConstraintUnknown
		constraint.DeferralSet = true
		sub.Constraint = constraint
		sub.Name = constraint.Name

	case pg_query.AlterTableType_AT_AddIndex, pg_query.AlterTableType_AT_ReAddIndex:
		sub.Kind = ir.SubAddConstraint

	case pg_query.AlterTableType_AT_EnableTrig, pg_query.AlterTableType_AT_EnableTrigAll,
		pg_query.AlterTableType_AT_EnableTrigUser:
		sub.Kind = ir.SubEnableTrigger
		sub.Value = triggerScope(cmd.Subtype, cmd.Name)
	case pg_query.AlterTableType_AT_EnableAlwaysTrig:
		sub.Kind = ir.SubEnableTrigger
		sub.Value = "ALWAYS"
	case pg_query.AlterTableType_AT_EnableReplicaTrig:
		sub.Kind = ir.SubEnableTrigger
		sub.Value = "REPLICA"
	case pg_query.AlterTableType_AT_DisableTrig, pg_query.AlterTableType_AT_DisableTrigAll,
		pg_query.AlterTableType_AT_DisableTrigUser:
		sub.Kind = ir.SubDisableTrigger
		sub.Value = triggerScope(cmd.Subtype, cmd.Name)

	case pg_query.AlterTableType_AT_EnableRule:
		sub.Kind = ir.SubEnableRule
	case pg_query.AlterTableType_AT_EnableAlwaysRule:
		sub.Kind = ir.SubEnableRule
		sub.Value = "ALWAYS"
	case pg_query.AlterTableType_AT_EnableReplicaRule:
		sub.Kind = ir.SubEnableRule
		sub.Value = "REPLICA"
	case pg_query.AlterTableType_AT_DisableRule:
		sub.Kind = ir.SubDisableRule

	case pg_query.AlterTableType_AT_EnableRowSecurity:
		sub.Kind = ir.SubEnableRowSecurity
	case pg_query.AlterTableType_AT_DisableRowSecurity:
		sub.Kind = ir.SubDisableRowSecurity
	case pg_query.AlterTableType_AT_ForceRowSecurity:
		sub.Kind = ir.SubForceRowSecurity
	case pg_query.AlterTableType_AT_NoForceRowSecurity:
		sub.Kind = ir.SubNoForceRowSecurity

	case pg_query.AlterTableType_AT_SetRelOptions, pg_query.AlterTableType_AT_ReplaceRelOptions:
		sub.Kind = ir.SubSetRelOptions
		sub.Options = options(listItems(cmd.Def))
	case pg_query.AlterTableType_AT_ResetRelOptions:
		sub.Kind = ir.SubResetRelOptions
		sub.Options = options(listItems(cmd.Def))

	case pg_query.AlterTableType_AT_SetTableSpace:
		sub.Kind = ir.SubSetTablespace
		sub.Value, sub.Name = cmd.Name, ""
	case pg_query.AlterTableType_AT_SetAccessMethod:
		sub.Kind = ir.SubSetAccessMethod
		sub.Value, sub.Name = cmd.Name, ""
		if sub.Value == "" {
			sub.Value = "DEFAULT"
		}

	case pg_query.AlterTableType_AT_SetLogged:
		sub.Kind = ir.SubSetLogged
	case pg_query.AlterTableType_AT_SetUnLogged:
		sub.Kind = ir.SubSetUnlogged
	case pg_query.AlterTableType_AT_ClusterOn:
		sub.Kind = ir.SubClusterOn
	case pg_query.AlterTableType_AT_DropCluster:
		sub.Kind = ir.SubDropCluster
	case pg_query.AlterTableType_AT_DropOids:
		sub.Kind = ir.SubDropOids

	case pg_query.AlterTableType_AT_ChangeOwner:
		sub.Kind = ir.SubChangeOwner
		sub.Value = roleName(cmd.Newowner)

	case pg_query.AlterTableType_AT_ReplicaIdentity:
		sub.Kind = ir.SubReplicaIdentity
		if def, ok := cmd.Def.GetNode().(*pg_query.Node_ReplicaIdentityStmt); ok {
			sub.Value = replicaIdentityName(def.ReplicaIdentityStmt.IdentityType)
			if name := def.ReplicaIdentityStmt.Name; name != "" {
				sub.Name = name
				sub.Relations = append(sub.Relations, ir.RelationRef{
					Name: helper.FullRelationName{Table: name},
					Kind: pgcontract.RelationKindIndex,
				})
			}
		}

	case pg_query.AlterTableType_AT_AttachPartition:
		sub.Kind = ir.SubAttachPartition
		sub = withPartition(sub, cmd.Def)
	case pg_query.AlterTableType_AT_DetachPartition:
		sub.Kind = ir.SubDetachPartition
		sub = withPartition(sub, cmd.Def)
	case pg_query.AlterTableType_AT_DetachPartitionFinalize:
		sub.Kind = ir.SubDetachPartition
		sub.Flags.Finalize = true
		sub = withPartition(sub, cmd.Def)

	case pg_query.AlterTableType_AT_AddInherit:
		sub.Kind = ir.SubAddInherit
		sub.Relations = append(sub.Relations, defRelation(cmd.Def))
	case pg_query.AlterTableType_AT_DropInherit:
		sub.Kind = ir.SubDropInherit
		sub.Relations = append(sub.Relations, defRelation(cmd.Def))
	case pg_query.AlterTableType_AT_AddOf:
		sub.Kind = ir.SubAddOf
		if def, ok := cmd.Def.GetNode().(*pg_query.Node_TypeName); ok {
			sub.Value = typeRef(def.TypeName).String()
		}
	case pg_query.AlterTableType_AT_DropOf:
		sub.Kind = ir.SubDropOf

	case pg_query.AlterTableType_AT_GenericOptions:
		sub.Kind = ir.SubGenericOptions
		sub.Options = options(cmd.Def.GetList().GetItems())
	case pg_query.AlterTableType_AT_AlterColumnGenericOptions:
		sub.Kind = ir.SubAlterColumnGenericOptions
		sub.Options = options(cmd.Def.GetList().GetItems())
	}

	return sub
}

// withPartition fills in the partition an ATTACH or DETACH names, and the CONCURRENTLY that only DETACH accepts.
func withPartition(sub ir.Subcommand, def *pg_query.Node) ir.Subcommand {
	partition, ok := def.GetNode().(*pg_query.Node_PartitionCmd)
	if !ok {
		return sub
	}
	ref := relationRef(partition.PartitionCmd.Name)
	sub.Relations = append(sub.Relations, ref)
	sub.Name = ref.Name.Table
	sub.Flags.Concurrently = partition.PartitionCmd.Concurrent
	if bound := partition.PartitionCmd.Bound; bound != nil {
		sub.Bound = partitionBound(bound)
		if bound.IsDefault {
			sub.Value = "DEFAULT"
		}
	}
	return sub
}

// partitionBound turns a FOR VALUES clause into the IR.
func partitionBound(spec *pg_query.PartitionBoundSpec) *ir.PartitionBound {
	bound := &ir.PartitionBound{Default: spec.IsDefault}
	if spec.IsDefault {
		return bound
	}
	switch spec.Strategy {
	case "r":
		bound.Strategy = "RANGE"
	case "l":
		bound.Strategy = "LIST"
	case "h":
		bound.Strategy = "HASH"
	}
	bound.In = convertExprs(spec.Listdatums)
	bound.From = convertExprs(spec.Lowerdatums)
	bound.To = convertExprs(spec.Upperdatums)
	return bound
}

func defRelation(def *pg_query.Node) ir.RelationRef {
	if rv, ok := def.GetNode().(*pg_query.Node_RangeVar); ok {
		return relationRef(rv.RangeVar)
	}
	return ir.RelationRef{}
}

// triggerScope names what an ENABLE/DISABLE TRIGGER applies to. ALL and USER are the two forms
// that name no trigger.
func triggerScope(subtype pg_query.AlterTableType, name string) string {
	switch subtype {
	case pg_query.AlterTableType_AT_EnableTrigAll, pg_query.AlterTableType_AT_DisableTrigAll:
		return "ALL"
	case pg_query.AlterTableType_AT_EnableTrigUser, pg_query.AlterTableType_AT_DisableTrigUser:
		return "USER"
	default:
		if name == "" {
			return "ALL"
		}
		return ""
	}
}

// identityWhen spells the ALWAYS / BY DEFAULT marker an identity column carries.
func identityWhen(when string) string {
	switch when {
	case "a":
		return "ALWAYS"
	case "d":
		return "BY DEFAULT"
	default:
		return ""
	}
}

// charCode turns the decimal a DefElem carries back into the single character it encodes.
func charCode(value string) string {
	code, err := strconv.Atoi(value)
	if err != nil || code <= 0 || code > 127 {
		return value
	}
	return string(rune(code))
}

func replicaIdentityName(identity string) string {
	switch identity {
	case "f":
		return "FULL"
	case "n":
		return "NOTHING"
	case "i":
		return "USING INDEX"
	default:
		return "DEFAULT"
	}
}

func roleName(spec *pg_query.RoleSpec) string {
	if spec == nil {
		return ""
	}
	switch spec.Roletype {
	case pg_query.RoleSpecType_ROLESPEC_CURRENT_ROLE:
		return "CURRENT_ROLE"
	case pg_query.RoleSpecType_ROLESPEC_CURRENT_USER:
		return "CURRENT_USER"
	case pg_query.RoleSpecType_ROLESPEC_SESSION_USER:
		return "SESSION_USER"
	case pg_query.RoleSpecType_ROLESPEC_PUBLIC:
		return "PUBLIC"
	default:
		return spec.Rolename
	}
}

func listItems(node *pg_query.Node) []*pg_query.Node {
	if list, ok := node.GetNode().(*pg_query.Node_List); ok {
		return list.List.Items
	}
	if node == nil {
		return nil
	}
	return []*pg_query.Node{node}
}

// normalizeRename covers every RENAME.
func normalizeRename(stmt *pg_query.RenameStmt, s ir.Statement) ir.Statement {
	s.Flags.IfExists = stmt.MissingOk

	sub := ir.Subcommand{Name: stmt.Subname, NewName: stmt.Newname}
	switch stmt.RenameType {
	case pg_query.ObjectType_OBJECT_COLUMN, pg_query.ObjectType_OBJECT_ATTRIBUTE:
		sub.Kind = ir.SubRenameColumn
	case pg_query.ObjectType_OBJECT_TABCONSTRAINT, pg_query.ObjectType_OBJECT_DOMCONSTRAINT:
		sub.Kind = ir.SubRenameConstraint
	default:
		sub.Kind = ir.SubRenameRelation
	}

	// relationType is unreliable, the parser leaves it at its zero value for a plain RENAME TO.
	// In that case, the object being renamed is what names the command.
	subject := stmt.RenameType
	if sub.Kind != ir.SubRenameRelation {
		subject = stmt.RelationType
		if subject == pg_query.ObjectType_OBJECT_ACCESS_METHOD || subject == pg_query.ObjectType_OBJECT_TABLE {
			subject = pg_query.ObjectType_OBJECT_TABLE
		}
	}
	s.Command = pgcontract.Command("ALTER " + objectWord(subject) + " RENAME")

	if stmt.Relation != nil {
		target := relationRef(stmt.Relation)
		target.Kind = relationKindOf(subject)
		s.Relations = []ir.RelationRef{target}
	} else {
		s.Relations = []ir.RelationRef{objectRef(objectNameList(stmt.Object))}
	}
	s.Subcommands = []ir.Subcommand{sub}
	return s
}

// normalizeSetSchema is ALTER … SET SCHEMA.
func normalizeSetSchema(stmt *pg_query.AlterObjectSchemaStmt, s ir.Statement) ir.Statement {
	s.Command = pgcontract.Command("ALTER " + objectWord(stmt.ObjectType) + " SET SCHEMA")
	s.Flags.IfExists = stmt.MissingOk
	if stmt.Relation != nil {
		target := relationRef(stmt.Relation)
		target.Kind = relationKindOf(stmt.ObjectType)
		s.Relations = []ir.RelationRef{target}
	} else {
		s.Relations = []ir.RelationRef{objectRef(objectNameList(stmt.Object))}
	}
	s.Subcommands = []ir.Subcommand{{Kind: ir.SubSetSchema, Value: stmt.Newschema}}
	return s
}

// normalizeAlterOwner is the OWNER TO form for objects that are not tables;
func normalizeAlterOwner(stmt *pg_query.AlterOwnerStmt, s ir.Statement) ir.Statement {
	s.Command = pgcontract.Command("ALTER " + objectWord(stmt.ObjectType) + " OWNER")
	s.Relations = []ir.RelationRef{objectRef(objectNameList(stmt.Object))}
	s.Subcommands = []ir.Subcommand{{Kind: ir.SubChangeOwner, Value: roleName(stmt.Newowner)}}
	return s
}

// normalizeMoveAll names no relation at all: the target set is every table or index in the source tablespace.
func normalizeMoveAll(stmt *pg_query.AlterTableMoveAllStmt, s ir.Statement) ir.Statement {
	s.Command = pgcontract.Command("ALTER " + objectWord(stmt.Objtype) + " ALL IN TABLESPACE")
	s.Flags.Nowait = stmt.Nowait
	sub := ir.Subcommand{
		Kind:  ir.SubSetTablespace,
		Name:  stmt.OrigTablespacename,
		Value: stmt.NewTablespacename,
	}
	for _, role := range stmt.Roles {
		if spec, ok := role.GetNode().(*pg_query.Node_RoleSpec); ok {
			sub.Options = append(sub.Options, ir.Option{Name: "owned_by", Value: roleName(spec.RoleSpec)})
		}
	}
	s.Subcommands = []ir.Subcommand{sub}
	return s
}

// objectNameList unwraps the two spellings the grammar uses for a qualified object name.
func objectNameList(object *pg_query.Node) []*pg_query.Node {
	switch n := object.GetNode().(type) {
	case *pg_query.Node_List:
		return n.List.Items
	case *pg_query.Node_String_:
		return []*pg_query.Node{object}
	default:
		return nil
	}
}
