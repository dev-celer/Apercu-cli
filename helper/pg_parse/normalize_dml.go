package pg_parse

import (
	"strings"

	"apercu-cli/helper/pg_contract"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// normalizeVacuum covers VACUUM and ANALYZE, which the grammar folds into one node. A form that names no relation means "every table".
func normalizeVacuum(stmt *pg_query.VacuumStmt, s Statement) Statement {
	s.Command = "ANALYZE"
	if stmt.IsVacuumcmd {
		s.Command = "VACUUM"
	}
	s.Options = options(stmt.Options)

	for _, rel := range stmt.Rels {
		vacuum, ok := rel.GetNode().(*pg_query.Node_VacuumRelation)
		if !ok {
			continue
		}
		s.Relations = append(s.Relations, relationRef(vacuum.VacuumRelation.Relation))
	}
	return s
}

// normalizeCluster handle the CLUSTER command.
// With no relation it clusters every table that has a marked index.
func normalizeCluster(stmt *pg_query.ClusterStmt, s Statement) Statement {
	s.Command = "CLUSTER"
	s.Options = options(stmt.Params)
	if stmt.Relation != nil {
		s.Relations = []RelationRef{relationRef(stmt.Relation)}
	}
	if stmt.Indexname != "" {
		s.Subcommands = []Subcommand{{Kind: SubClusterOn, Name: stmt.Indexname}}
	}
	return s
}

func normalizeTruncate(stmt *pg_query.TruncateStmt, s Statement) Statement {
	s.Command = "TRUNCATE"
	s.Flags.Cascade = stmt.Behavior == pg_query.DropBehavior_DROP_CASCADE
	s.Flags.RestartIdentity = stmt.RestartSeqs
	s.Relations = rangeRelations(stmt.Relations)
	return s
}

func normalizeLock(stmt *pg_query.LockStmt, s Statement) Statement {
	s.Command = "LOCK"
	s.Flags.Nowait = stmt.Nowait
	s.Relations = rangeRelations(stmt.Relations)
	s.Subcommands = []Subcommand{{Kind: SubUnknown, Value: pg_contract.Lock(stmt.Mode).String()}}
	return s
}

func normalizeCopy(stmt *pg_query.CopyStmt, s Statement) Statement {
	s.Command = "COPY TO"
	if stmt.IsFrom {
		s.Command = "COPY FROM"
	}
	s.Options = options(stmt.Options)
	if stmt.Relation != nil {
		s.Relations = []RelationRef{relationRef(stmt.Relation)}
	}
	s.Relations = append(s.Relations, queryRelations(stmt.Query)...)
	return s
}

func normalizeSelect(stmt *pg_query.SelectStmt, s Statement) Statement {
	s.Command = "SELECT"
	s.Relations = selectRelations(stmt)
	for _, locking := range stmt.LockingClause {
		clause, ok := locking.GetNode().(*pg_query.Node_LockingClause)
		if !ok {
			continue
		}
		s.Command = "SELECT FOR"
		s.Subcommands = append(s.Subcommands, Subcommand{
			Kind:  SubUnknown,
			Value: lockingStrength(clause.LockingClause.Strength),
		})
	}
	return s
}

func lockingStrength(strength pg_query.LockClauseStrength) string {
	switch strength {
	case pg_query.LockClauseStrength_LCS_FORKEYSHARE:
		return "KEY SHARE"
	case pg_query.LockClauseStrength_LCS_FORSHARE:
		return "SHARE"
	case pg_query.LockClauseStrength_LCS_FORNOKEYUPDATE:
		return "NO KEY UPDATE"
	case pg_query.LockClauseStrength_LCS_FORUPDATE:
		return "UPDATE"
	default:
		return "NONE"
	}
}

func normalizeTransaction(stmt *pg_query.TransactionStmt, s Statement) Statement {
	switch stmt.Kind {
	case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN, pg_query.TransactionStmtKind_TRANS_STMT_START:
		s.Command = "BEGIN"
	case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT:
		s.Command = "COMMIT"
	case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK:
		s.Command = "ROLLBACK"
	case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO:
		s.Command = "ROLLBACK TO SAVEPOINT"
	case pg_query.TransactionStmtKind_TRANS_STMT_SAVEPOINT:
		s.Command = "SAVEPOINT"
	case pg_query.TransactionStmtKind_TRANS_STMT_RELEASE:
		s.Command = "RELEASE SAVEPOINT"
	case pg_query.TransactionStmtKind_TRANS_STMT_PREPARE:
		s.Command = "PREPARE TRANSACTION"
	case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT_PREPARED:
		s.Command = "COMMIT PREPARED"
	case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK_PREPARED:
		s.Command = "ROLLBACK PREPARED"
	default:
		s.Command = "TRANSACTION"
	}

	s.Options = options(stmt.Options)
	if stmt.SavepointName != "" {
		s.Subcommands = []Subcommand{{Kind: SubUnknown, Name: stmt.SavepointName}}
	}
	return s
}

// normalizeVariableSet handle the SET / RESET variables statement.
func normalizeVariableSet(stmt *pg_query.VariableSetStmt, s Statement) Statement {
	sub := Subcommand{Kind: SubSetVariable, Name: stmt.Name}
	sub.Value = strings.Join(variableValues(stmt.Args), ", ")

	switch stmt.Kind {
	case pg_query.VariableSetKind_VAR_RESET:
		s.Command = "RESET"
		sub.Kind = SubResetVariable
	case pg_query.VariableSetKind_VAR_RESET_ALL:
		s.Command = "RESET ALL"
		sub.Kind = SubResetVariable
	case pg_query.VariableSetKind_VAR_SET_DEFAULT:
		s.Command = "SET"
		if stmt.IsLocal {
			s.Command = "SET LOCAL"
		}
		sub.Kind = SubResetVariable
	default:
		s.Command = "SET"
		if stmt.IsLocal {
			s.Command = "SET LOCAL"
		}
		switch stmt.Kind {
		case pg_query.VariableSetKind_VAR_SET_CURRENT:
			sub.Kind = SubSetVariableCurrent
		case pg_query.VariableSetKind_VAR_SET_MULTI:
			// SET TRANSACTION / SET SESSION CHARACTERISTICS. The grammar files the modes under a
			// parameter named TRANSACTION, which is not a GUC anything tracks.
			sub.Kind = SubSetTransaction
		}
	}
	s.Subcommands = []Subcommand{sub}
	return s
}

func variableValues(args []*pg_query.Node) []string {
	values := make([]string, 0, len(args))
	for _, arg := range args {
		if text := literalText(arg); text != "" {
			values = append(values, strings.Trim(text, "'"))
		}
	}
	return values
}

// rangeRelations converts a list of plain table references.
func rangeRelations(nodes []*pg_query.Node) []RelationRef {
	var refs []RelationRef
	for _, node := range nodes {
		refs = append(refs, fromClauseRelations(node)...)
	}
	return refs
}

// queryRelations lists the relations a nested query reads.
func queryRelations(node *pg_query.Node) []RelationRef {
	if node == nil {
		return nil
	}
	if selectStmt, ok := node.GetNode().(*pg_query.Node_SelectStmt); ok {
		return selectRelations(selectStmt.SelectStmt)
	}
	return nil
}

func selectRelations(stmt *pg_query.SelectStmt) []RelationRef {
	if stmt == nil {
		return nil
	}
	refs := rangeRelations(stmt.FromClause)
	// A set operation keeps its arms in larg/rarg rather than in the FROM clause.
	refs = append(refs, selectRelations(stmt.Larg)...)
	refs = append(refs, selectRelations(stmt.Rarg)...)
	if stmt.WithClause != nil {
		for _, cte := range stmt.WithClause.Ctes {
			if common, ok := cte.GetNode().(*pg_query.Node_CommonTableExpr); ok {
				refs = append(refs, queryRelations(common.CommonTableExpr.Ctequery)...)
			}
		}
	}
	return refs
}

// fromClauseRelations descends a FROM entry, which may be a join tree or a subquery rather than
// a plain table.
func fromClauseRelations(node *pg_query.Node) []RelationRef {
	switch n := node.GetNode().(type) {
	case *pg_query.Node_RangeVar:
		return []RelationRef{relationRef(n.RangeVar)}
	case *pg_query.Node_JoinExpr:
		return append(fromClauseRelations(n.JoinExpr.Larg), fromClauseRelations(n.JoinExpr.Rarg)...)
	case *pg_query.Node_RangeSubselect:
		return queryRelations(n.RangeSubselect.Subquery)
	default:
		return nil
	}
}
