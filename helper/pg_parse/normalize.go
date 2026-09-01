package pg_parse

import (
	"strings"

	"apercu-cli/helper/pg_contract"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// normalizeNode converts one parsed statement into the IR.
func normalizeNode(node *pg_query.Node, s Statement) Statement {
	switch n := node.GetNode().(type) {
	// ALTER TABLE and the clauses the grammar gives their own statement node.
	case *pg_query.Node_AlterTableStmt:
		s = normalizeAlterTable(n.AlterTableStmt, s)
	case *pg_query.Node_RenameStmt:
		s = normalizeRename(n.RenameStmt, s)
	case *pg_query.Node_AlterObjectSchemaStmt:
		s = normalizeSetSchema(n.AlterObjectSchemaStmt, s)
	case *pg_query.Node_AlterOwnerStmt:
		s = normalizeAlterOwner(n.AlterOwnerStmt, s)
	case *pg_query.Node_AlterTableMoveAllStmt:
		s = normalizeMoveAll(n.AlterTableMoveAllStmt, s)

	// index DDL.
	case *pg_query.Node_IndexStmt:
		s = normalizeCreateIndex(n.IndexStmt, s)
	case *pg_query.Node_ReindexStmt:
		s = normalizeReindex(n.ReindexStmt, s)

	// creation and destruction.
	case *pg_query.Node_CreateStmt:
		s = normalizeCreateTable(n.CreateStmt, s)
	case *pg_query.Node_CreateTableAsStmt:
		s = normalizeCreateTableAs(n.CreateTableAsStmt, s)
	case *pg_query.Node_ViewStmt:
		s = normalizeCreateView(n.ViewStmt, s)
	case *pg_query.Node_RefreshMatViewStmt:
		s = normalizeRefreshMatView(n.RefreshMatViewStmt, s)
	case *pg_query.Node_CreateSeqStmt:
		s = normalizeCreateSeq(n.CreateSeqStmt, s)
	case *pg_query.Node_AlterSeqStmt:
		s = normalizeAlterSeq(n.AlterSeqStmt, s)
	case *pg_query.Node_DropStmt:
		s = normalizeDrop(n.DropStmt, s)

	// the objects that hang off a table.
	case *pg_query.Node_CreateTrigStmt:
		s = normalizeCreateTrigger(n.CreateTrigStmt, s)
	case *pg_query.Node_CreatePolicyStmt:
		s = normalizePolicy(n.CreatePolicyStmt.Table, "CREATE POLICY", n.CreatePolicyStmt.PolicyName, s)
	case *pg_query.Node_AlterPolicyStmt:
		s = normalizePolicy(n.AlterPolicyStmt.Table, "ALTER POLICY", n.AlterPolicyStmt.PolicyName, s)
	case *pg_query.Node_RuleStmt:
		s = normalizeRule(n.RuleStmt, s)
	case *pg_query.Node_CreateStatsStmt:
		s = normalizeCreateStats(n.CreateStatsStmt, s)
	case *pg_query.Node_AlterStatsStmt:
		s.Command = "ALTER STATISTICS"
		s.Subcommands = []Subcommand{{Kind: SubSetStatistics, Name: strings.Join(nameParts(n.AlterStatsStmt.Defnames), ".")}}
	case *pg_query.Node_CommentStmt:
		s = normalizeComment(n.CommentStmt, s)
	case *pg_query.Node_GrantStmt:
		s = normalizeGrant(n.GrantStmt, s)
	case *pg_query.Node_AlterDefaultPrivilegesStmt:
		s.Command = "ALTER DEFAULT PRIVILEGES"

	// types and domains.
	case *pg_query.Node_AlterEnumStmt:
		s = normalizeAlterEnum(n.AlterEnumStmt, s)
	case *pg_query.Node_AlterDomainStmt:
		s = normalizeAlterDomain(n.AlterDomainStmt, s)
	case *pg_query.Node_DefineStmt:
		s.Command = pg_contract.Command("CREATE " + objectWord(n.DefineStmt.Kind))

	// publications, subscriptions, extensions, schemas.
	case *pg_query.Node_CreatePublicationStmt:
		s.Command = "CREATE PUBLICATION"
		s.Relations = publicationRelations(n.CreatePublicationStmt.Pubobjects)
	case *pg_query.Node_AlterPublicationStmt:
		s.Command = "ALTER PUBLICATION"
		s.Relations = publicationRelations(n.AlterPublicationStmt.Pubobjects)
	case *pg_query.Node_CreateSubscriptionStmt:
		s.Command = "CREATE SUBSCRIPTION"
	case *pg_query.Node_AlterSubscriptionStmt:
		s.Command = "ALTER SUBSCRIPTION"
	case *pg_query.Node_CreateExtensionStmt:
		s.Command = "CREATE EXTENSION"
		s.Flags.IfNotExists = n.CreateExtensionStmt.IfNotExists
	case *pg_query.Node_AlterExtensionStmt:
		s.Command = "ALTER EXTENSION"
	case *pg_query.Node_CreateSchemaStmt:
		s.Command = "CREATE SCHEMA"
		s.Flags.IfNotExists = n.CreateSchemaStmt.IfNotExists

	// maintenance.
	case *pg_query.Node_VacuumStmt:
		s = normalizeVacuum(n.VacuumStmt, s)
	case *pg_query.Node_ClusterStmt:
		s = normalizeCluster(n.ClusterStmt, s)
	case *pg_query.Node_TruncateStmt:
		s = normalizeTruncate(n.TruncateStmt, s)
	case *pg_query.Node_LockStmt:
		s = normalizeLock(n.LockStmt, s)
	case *pg_query.Node_CopyStmt:
		s = normalizeCopy(n.CopyStmt, s)

	// data.
	case *pg_query.Node_InsertStmt:
		s.Command = "INSERT"
		s.Relations = append(s.Relations, relationRef(n.InsertStmt.Relation))
		s.Relations = append(s.Relations, queryRelations(n.InsertStmt.SelectStmt)...)
	case *pg_query.Node_UpdateStmt:
		s.Command = "UPDATE"
		s.Relations = append(s.Relations, relationRef(n.UpdateStmt.Relation))
		s.Relations = append(s.Relations, rangeRelations(n.UpdateStmt.FromClause)...)
	case *pg_query.Node_DeleteStmt:
		s.Command = "DELETE"
		s.Relations = append(s.Relations, relationRef(n.DeleteStmt.Relation))
		s.Relations = append(s.Relations, rangeRelations(n.DeleteStmt.UsingClause)...)
	case *pg_query.Node_MergeStmt:
		s.Command = "MERGE"
		s.Relations = append(s.Relations, relationRef(n.MergeStmt.Relation))
		s.Relations = append(s.Relations, rangeRelations([]*pg_query.Node{n.MergeStmt.SourceRelation})...)
		if len(n.MergeStmt.ReturningList) > 0 {
			s.Features = addFeature(s.Features, FeatureMergeReturning)
		}
	case *pg_query.Node_SelectStmt:
		s = normalizeSelect(n.SelectStmt, s)

	// transaction and session control.
	case *pg_query.Node_TransactionStmt:
		s = normalizeTransaction(n.TransactionStmt, s)
	case *pg_query.Node_VariableSetStmt:
		s = normalizeVariableSet(n.VariableSetStmt, s)
	case *pg_query.Node_VariableShowStmt:
		s.Command = "SHOW"

	default:
		// A statement the IR does not model
		s.Command = pg_contract.Command(strings.ToUpper(unmodelledName(node)))
	}

	return attachFeatures(deriveFeatures(s))
}

// unmodelledName turns a node type the dispatch does not handle into something readable.
func unmodelledName(node *pg_query.Node) string {
	name := string(node.ProtoReflect().WhichOneof(node.ProtoReflect().Descriptor().Oneofs().Get(0)).Name())
	name = strings.TrimSuffix(name, "_stmt")
	return strings.ReplaceAll(name, "_", " ")
}

// deriveFeatures records the version-bound syntax that are accepted by libpg_query 17.
func deriveFeatures(s Statement) Statement {
	for _, sub := range s.Subcommands {
		switch {
		case sub.Kind == SubSetExpression:
			s.Features = addFeature(s.Features, FeatureSetExpression)
		case sub.Kind == SubSetStatistics && sub.Value == "DEFAULT":
			s.Features = addFeature(s.Features, FeatureStatisticsDefault)
		case sub.Kind == SubSetAccessMethod && sub.Value == "DEFAULT":
			s.Features = addFeature(s.Features, FeatureAccessMethodDefault)
		case sub.Kind == SubSetStorage && sub.Value == "DEFAULT":
			s.Features = addFeature(s.Features, FeatureStorageDefault)
		}
	}
	return s
}

// attachFeatures reconstruct the original statement the user wrote from its features.
func attachFeatures(s Statement) Statement {
	for _, feature := range s.Features {
		index := feature.clause
		if s.featureDepth == 1 {
			index = feature.subclause
		}
		if index < 0 {
			continue
		}

		// VACUUM and ANALYZE carry their ONLY on the relation rather than on a clause.
		switch feature.Name {
		case FeatureVacuumOnly, FeatureAnalyzeOnly:
			if index < len(s.Relations) {
				s.Relations[index].Only = true
			}
			continue
		}

		if index >= len(s.Subcommands) {
			continue
		}
		sub := &s.Subcommands[index]

		switch feature.Name {
		case FeatureNotEnforced:
			if sub.Constraint == nil {
				sub.Constraint = new(ConstraintDef{})
			}
			sub.Constraint.NotEnforced = feature.Detail != "ENFORCED"
			if sub.Kind == SubAlterConstraint {
				sub.Constraint.EnforcementSet = true
				// An "ALTER CONSTRAINT c" call without attribute is interpreted as NOT DEFERRABLE by the PG17 parser.
				// Since the shim blanked out (NOT) ENFORCED, it can be wrongly interpreted as NOT DEFERRABLE.
				// The feature recorded if DEFERRABLE was named before shimming, so we can fix this case.
				sub.Constraint.DeferralSet = feature.deferralNamed
			}

		case FeatureNotNullConstraint:
			// This replaces the shimmed CHECK constraint with the PG18+ NOT NULL constraint
			if sub.Constraint == nil {
				sub.Constraint = new(ConstraintDef{})
			}
			sub.Constraint.Type = ConstraintNotNull
			sub.Constraint.Columns = []string{feature.Detail}
			sub.Constraint.Expr = nil

		case FeatureConstraintInherit:
			if sub.Constraint == nil {
				sub.Constraint = new(ConstraintDef{})
			}
			sub.Constraint.NoInheritSet = true
			sub.Constraint.Inherit = feature.Detail == "INHERIT"
			// An "ALTER CONSTRAINT c" call without attribute is interpreted as NOT DEFERRABLE by the PG17 parser.
			// Since the shim blanked out [NO] INHERIT, it can be wrongly interpreted as NOT DEFERRABLE.
			// The feature recorded if DEFERRABLE was named before shimming, so we can fix this case.
			sub.Constraint.DeferralSet = feature.deferralNamed

		case FeatureVirtualGenerated, FeatureBareGenerated:
			if sub.Column != nil {
				sub.Column.Generated = GeneratedVirtual
			}

		case FeatureWithoutOverlaps:
			if sub.Constraint != nil {
				sub.Constraint.WithoutOverlaps = true
			}

		case FeatureForeignKeyPeriod:
			if sub.Constraint != nil {
				sub.Constraint.Period = true
			}
		}
	}
	return s
}

// objectWord spells a parsed object type the way the command that names it does.
func objectWord(objtype pg_query.ObjectType) string {
	switch objtype {
	case pg_query.ObjectType_OBJECT_MATVIEW:
		return "MATERIALIZED VIEW"
	case pg_query.ObjectType_OBJECT_STATISTIC_EXT:
		return "STATISTICS"
	case pg_query.ObjectType_OBJECT_TABCONSTRAINT, pg_query.ObjectType_OBJECT_DOMCONSTRAINT:
		return "CONSTRAINT"
	case pg_query.ObjectType_OBJECT_FOREIGN_TABLE:
		return "FOREIGN TABLE"
	case pg_query.ObjectType_OBJECT_ACCESS_METHOD:
		return "ACCESS METHOD"
	}
	return strings.ReplaceAll(strings.TrimPrefix(objtype.String(), "OBJECT_"), "_", " ")
}

// relationKindOf maps the object type a statement declares to the catalog's relkind vocabulary.
func relationKindOf(objtype pg_query.ObjectType) pg_contract.RelationKind {
	switch objtype {
	case pg_query.ObjectType_OBJECT_TABLE:
		return pg_contract.RelationKindTable
	case pg_query.ObjectType_OBJECT_INDEX:
		return pg_contract.RelationKindIndex
	case pg_query.ObjectType_OBJECT_VIEW:
		return pg_contract.RelationKindView
	case pg_query.ObjectType_OBJECT_MATVIEW:
		return pg_contract.RelationKindMaterializedView
	case pg_query.ObjectType_OBJECT_SEQUENCE:
		return pg_contract.RelationKindSequence
	case pg_query.ObjectType_OBJECT_FOREIGN_TABLE:
		return pg_contract.RelationKindForeignTable
	default:
		return pg_contract.RelationKindUnknown
	}
}
