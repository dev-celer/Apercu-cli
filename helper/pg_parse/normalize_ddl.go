package pg_parse

import (
	"strings"

	"apercu-cli/helper"
	"apercu-cli/helper/pg_contract"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func normalizeCreateIndex(stmt *pg_query.IndexStmt, s Statement) Statement {
	s.Command = "CREATE INDEX"
	s.Flags.Unique = stmt.Unique
	s.Flags.Concurrently = stmt.Concurrent
	s.Flags.IfNotExists = stmt.IfNotExists

	table := relationRef(stmt.Relation)
	table.Kind = pg_contract.RelationKindTable
	s.Relations = []RelationRef{table}
	if stmt.Idxname != "" {
		s.Relations = append(s.Relations, RelationRef{
			Name: helper.FullRelationName{Schema: stmt.Relation.GetSchemaname(), Table: stmt.Idxname},
			Kind: pg_contract.RelationKindIndex,
		})
	}

	sub := Subcommand{
		Kind:    SubCreateIndex,
		Name:    stmt.Idxname,
		Value:   stmt.AccessMethod,
		Options: options(stmt.Options),
	}
	sub.Expr = convertExpr(stmt.WhereClause)
	if stmt.TableSpace != "" {
		sub.Options = append(sub.Options, Option{Name: "tablespace", Value: stmt.TableSpace})
	}
	s.Subcommands = []Subcommand{sub}
	return s
}

func normalizeReindex(stmt *pg_query.ReindexStmt, s Statement) Statement {
	scope := strings.TrimPrefix(stmt.Kind.String(), "REINDEX_OBJECT_")
	s.Command = pg_contract.Command("REINDEX " + scope)
	s.Options = options(stmt.Params)
	if _, ok := findOption(s.Options, "concurrently"); ok {
		s.Flags.Concurrently = true
	}

	if stmt.Relation != nil {
		target := relationRef(stmt.Relation)
		if stmt.Kind == pg_query.ReindexObjectType_REINDEX_OBJECT_INDEX {
			target.Kind = pg_contract.RelationKindIndex
		} else {
			target.Kind = pg_contract.RelationKindTable
		}
		s.Relations = []RelationRef{target}
	} else if stmt.Name != "" {
		// The name of a SCHEMA / DATABASE / SYSTEM reindex is not a relation
		s.Subcommands = []Subcommand{{Kind: SubUnknown, Name: stmt.Name}}
	}
	return s
}

func normalizeCreateTable(stmt *pg_query.CreateStmt, s Statement) Statement {
	s.Command = "CREATE TABLE"
	s.Flags.IfNotExists = stmt.IfNotExists
	s.Flags.Persistence = persistence(stmt.Relation.GetRelpersistence())
	s.Options = options(stmt.Options)

	// For shim attachFeatures
	s.featureDepth = 1

	s.Relations = []RelationRef{{Name: relationRef(stmt.Relation).Name, Kind: pg_contract.RelationKindTable}}

	for _, element := range stmt.TableElts {
		switch element.GetNode().(type) {
		case *pg_query.Node_ColumnDef:
			sub := Subcommand{Kind: SubAddColumn, Column: columnDef(element)}
			if sub.Column != nil {
				sub.Name = sub.Column.Name
				for _, constraint := range sub.Column.Constraints {
					if constraint.Type == ConstraintForeignKey {
						sub.Relations = append(sub.Relations, constraint.References)
					}
				}
			}
			s.Subcommands = append(s.Subcommands, sub)
		case *pg_query.Node_Constraint:
			sub := Subcommand{Kind: SubAddConstraint, Constraint: constraintDef(element)}
			if sub.Constraint != nil {
				sub.Name = sub.Constraint.Name
				if sub.Constraint.References.Name.Table != "" {
					sub.Relations = append(sub.Relations, sub.Constraint.References)
				}
			}
			s.Subcommands = append(s.Subcommands, sub)
		case *pg_query.Node_TableLikeClause:
			like := element.GetTableLikeClause()
			ref := relationRef(like.Relation)
			s.Subcommands = append(s.Subcommands, Subcommand{Kind: SubLike, Name: ref.Name.Table, Relations: []RelationRef{ref}})
			s.Relations = append(s.Relations, ref)
		}
	}

	// PARTITION OF and INHERITS share the parent list, only a partition bound tells them apart.
	kind := SubAddInherit
	if stmt.Partbound != nil {
		kind = SubAttachPartition
	}
	for _, parent := range stmt.InhRelations {
		if rv, ok := parent.GetNode().(*pg_query.Node_RangeVar); ok {
			ref := relationRef(rv.RangeVar)
			sub := Subcommand{Kind: kind, Relations: []RelationRef{ref}, Name: ref.Name.Table}
			if stmt.Partbound != nil && stmt.Partbound.IsDefault {
				sub.Value = "DEFAULT"
			}
			s.Subcommands = append(s.Subcommands, sub)
			s.Relations = append(s.Relations, ref)
		}
	}
	return s
}

// normalizeCreateTableAs covers CREATE TABLE AS, SELECT INTO and CREATE MATERIALIZED VIEW.
func normalizeCreateTableAs(stmt *pg_query.CreateTableAsStmt, s Statement) Statement {
	s.Command = "CREATE TABLE AS"
	kind := pg_contract.RelationKindTable
	if stmt.Objtype == pg_query.ObjectType_OBJECT_MATVIEW {
		s.Command = "CREATE MATERIALIZED VIEW"
		kind = pg_contract.RelationKindMaterializedView
	}
	s.Flags.WithData = true

	if into := stmt.Into; into != nil {
		s.Flags.WithData = !into.SkipData
		s.Flags.Persistence = persistence(into.Rel.GetRelpersistence())
		s.Options = options(into.Options)
		s.Relations = []RelationRef{{Name: relationRef(into.Rel).Name, Kind: kind}}
	}
	s.Relations = append(s.Relations, queryRelations(stmt.Query)...)
	return s
}

func normalizeCreateView(stmt *pg_query.ViewStmt, s Statement) Statement {
	s.Command = "CREATE VIEW"
	s.Flags.OrReplace = stmt.Replace
	s.Options = options(stmt.Options)
	s.Relations = []RelationRef{{Name: relationRef(stmt.View).Name, Kind: pg_contract.RelationKindView}}
	s.Relations = append(s.Relations, queryRelations(stmt.Query)...)
	return s
}

func normalizeRefreshMatView(stmt *pg_query.RefreshMatViewStmt, s Statement) Statement {
	s.Command = "REFRESH MATERIALIZED VIEW"
	s.Flags.Concurrently = stmt.Concurrent
	s.Flags.WithData = !stmt.SkipData
	s.Relations = []RelationRef{{Name: relationRef(stmt.Relation).Name, Kind: pg_contract.RelationKindMaterializedView}}
	return s
}

func normalizeCreateSeq(stmt *pg_query.CreateSeqStmt, s Statement) Statement {
	s.Command = "CREATE SEQUENCE"
	s.Flags.IfNotExists = stmt.IfNotExists
	s.Options = options(stmt.Options)
	s.Relations = []RelationRef{{Name: relationRef(stmt.Sequence).Name, Kind: pg_contract.RelationKindSequence}}
	return s
}

func normalizeAlterSeq(stmt *pg_query.AlterSeqStmt, s Statement) Statement {
	s.Command = "ALTER SEQUENCE"
	s.Flags.IfExists = stmt.MissingOk
	s.Options = options(stmt.Options)
	s.Relations = []RelationRef{{Name: relationRef(stmt.Sequence).Name, Kind: pg_contract.RelationKindSequence}}
	return s
}

// normalizeDrop covers every DROP that names relations, plus the ones that name objects hanging off a relation.
func normalizeDrop(stmt *pg_query.DropStmt, s Statement) Statement {
	s.Command = pg_contract.Command("DROP " + objectWord(stmt.RemoveType))
	s.Flags.IfExists = stmt.MissingOk
	s.Flags.Cascade = stmt.Behavior == pg_query.DropBehavior_DROP_CASCADE
	s.Flags.Concurrently = stmt.Concurrent

	kind := relationKindOf(stmt.RemoveType)
	for _, object := range stmt.Objects {
		names := objectNameList(object)
		switch stmt.RemoveType {
		case pg_query.ObjectType_OBJECT_TRIGGER, pg_query.ObjectType_OBJECT_POLICY,
			pg_query.ObjectType_OBJECT_RULE:
			// These spell the object last and the table it hangs off before it.
			if len(names) < 2 {
				continue
			}
			ref := objectRef(names[:len(names)-1])
			ref.Kind = pg_contract.RelationKindTable
			s.Relations = append(s.Relations, ref)
			s.Subcommands = append(s.Subcommands, Subcommand{
				Kind: SubUnknown,
				Name: nameParts(names[len(names)-1:])[0],
			})
		default:
			// A DROP TYPE or DROP EXTENSION spells its object as a type name rather than as a relation.
			if len(names) == 0 {
				continue
			}
			ref := objectRef(names)
			ref.Kind = kind
			s.Relations = append(s.Relations, ref)
		}
	}
	return s
}

func normalizeCreateTrigger(stmt *pg_query.CreateTrigStmt, s Statement) Statement {
	s.Command = "CREATE TRIGGER"
	s.Flags.OrReplace = stmt.Replace
	s.Relations = []RelationRef{relationRef(stmt.Relation)}
	if stmt.Constrrel != nil {
		s.Relations = append(s.Relations, relationRef(stmt.Constrrel))
	}
	s.Subcommands = []Subcommand{{Kind: SubEnableTrigger, Name: stmt.Trigname}}
	return s
}

func normalizePolicy(table *pg_query.RangeVar, command pg_contract.Command, name string, s Statement) Statement {
	s.Command = command
	s.Relations = []RelationRef{relationRef(table)}
	s.Subcommands = []Subcommand{{Kind: SubEnableRowSecurity, Name: name}}
	return s
}

func normalizeRule(stmt *pg_query.RuleStmt, s Statement) Statement {
	s.Command = "CREATE RULE"
	s.Flags.OrReplace = stmt.Replace
	s.Relations = []RelationRef{relationRef(stmt.Relation)}
	s.Subcommands = []Subcommand{{Kind: SubEnableRule, Name: stmt.Rulename}}
	return s
}

func normalizeCreateStats(stmt *pg_query.CreateStatsStmt, s Statement) Statement {
	s.Command = "CREATE STATISTICS"
	s.Flags.IfNotExists = stmt.IfNotExists
	s.Relations = rangeRelations(stmt.Relations)
	s.Subcommands = []Subcommand{{Kind: SubSetStatistics, Name: strings.Join(nameParts(stmt.Defnames), ".")}}
	return s
}

func normalizeComment(stmt *pg_query.CommentStmt, s Statement) Statement {
	s.Command = pg_contract.Command("COMMENT ON " + objectWord(stmt.Objtype))
	names := objectNameList(stmt.Object)

	switch stmt.Objtype {
	case pg_query.ObjectType_OBJECT_COLUMN, pg_query.ObjectType_OBJECT_TABCONSTRAINT,
		pg_query.ObjectType_OBJECT_TRIGGER, pg_query.ObjectType_OBJECT_POLICY,
		pg_query.ObjectType_OBJECT_RULE:
		if len(names) < 2 {
			return s
		}
		ref := objectRef(names[:len(names)-1])
		ref.Kind = pg_contract.RelationKindTable
		s.Relations = []RelationRef{ref}
		s.Subcommands = []Subcommand{{Kind: SubUnknown, Name: nameParts(names[len(names)-1:])[0]}}
	case pg_query.ObjectType_OBJECT_TABLE, pg_query.ObjectType_OBJECT_INDEX,
		pg_query.ObjectType_OBJECT_VIEW, pg_query.ObjectType_OBJECT_MATVIEW,
		pg_query.ObjectType_OBJECT_SEQUENCE, pg_query.ObjectType_OBJECT_FOREIGN_TABLE:
		ref := objectRef(names)
		ref.Kind = relationKindOf(stmt.Objtype)
		s.Relations = []RelationRef{ref}
	}
	return s
}

func normalizeGrant(stmt *pg_query.GrantStmt, s Statement) Statement {
	s.Command = "GRANT"
	if !stmt.IsGrant {
		s.Command = "REVOKE"
	}
	s.Flags.Cascade = stmt.Behavior == pg_query.DropBehavior_DROP_CASCADE

	if stmt.Targtype == pg_query.GrantTargetType_ACL_TARGET_OBJECT {
		kind := relationKindOf(stmt.Objtype)
		for _, object := range stmt.Objects {
			switch n := object.GetNode().(type) {
			case *pg_query.Node_RangeVar:
				ref := relationRef(n.RangeVar)
				ref.Kind = kind
				s.Relations = append(s.Relations, ref)
			case *pg_query.Node_List, *pg_query.Node_String_:
				ref := objectRef(objectNameList(object))
				ref.Kind = kind
				s.Relations = append(s.Relations, ref)
			}
		}
	}

	for _, privilege := range stmt.Privileges {
		if priv, ok := privilege.GetNode().(*pg_query.Node_AccessPriv); ok {
			s.Options = append(s.Options, Option{Name: priv.AccessPriv.PrivName})
		}
	}
	return s
}

// normalizeAlterEnum
func normalizeAlterEnum(stmt *pg_query.AlterEnumStmt, s Statement) Statement {
	s.Command = "ALTER TYPE ADD VALUE"
	sub := Subcommand{Kind: SubAddEnumValue, Name: strings.Join(nameParts(stmt.TypeName), "."), Value: stmt.NewVal}
	sub.Flags.IfNotExists = stmt.SkipIfNewValExists
	if stmt.OldVal != "" {
		s.Command = "ALTER TYPE RENAME VALUE"
		sub.Kind = SubRenameEnumValue
		sub.NewName = stmt.NewVal
		sub.Value = stmt.OldVal
	}
	s.Subcommands = []Subcommand{sub}
	return s
}

func normalizeAlterDomain(stmt *pg_query.AlterDomainStmt, s Statement) Statement {
	s.Command = "ALTER DOMAIN"
	sub := Subcommand{Name: strings.Join(nameParts(stmt.TypeName), ".")}
	switch stmt.Subtype {
	case "C":
		sub.Kind = SubAddConstraint
		sub.Constraint = constraintDef(stmt.Def)
	case "X":
		sub.Kind = SubDropConstraint
		sub.Name = stmt.Name
	case "V":
		sub.Kind = SubValidateConstraint
		sub.Name = stmt.Name
	case "T":
		sub.Kind = SubSetDefault
		sub.Expr = convertExpr(stmt.Def)
		if stmt.Def == nil {
			sub.Kind = SubDropDefault
		}
	case "N":
		sub.Kind = SubDropNotNull
	case "O":
		sub.Kind = SubSetNotNull
	}
	sub.Flags.Cascade = stmt.Behavior == pg_query.DropBehavior_DROP_CASCADE
	s.Subcommands = []Subcommand{sub}
	return s
}

func publicationRelations(objects []*pg_query.Node) []RelationRef {
	var refs []RelationRef
	for _, object := range objects {
		spec, ok := object.GetNode().(*pg_query.Node_PublicationObjSpec)
		if !ok || spec.PublicationObjSpec.Pubtable == nil {
			continue
		}
		refs = append(refs, relationRef(spec.PublicationObjSpec.Pubtable.Relation))
	}
	return refs
}

func persistence(relpersistence string) Persistence {
	switch relpersistence {
	case "t":
		return PersistenceTemporary
	case "u":
		return PersistenceUnlogged
	default:
		return PersistencePermanent
	}
}
