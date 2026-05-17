package engines

import (
	"apercu-cli/helper/warning"
	"apercu-cli/output"
	"database/sql"
	"fmt"
)

type RewriteEngine struct {
	db                *sql.DB
	preMigrationNode  map[string]uint32
	postMigrationNode map[string]uint32
}

func NewRewriteEngine(db *sql.DB) *RewriteEngine {
	return &RewriteEngine{
		db:                db,
		preMigrationNode:  make(map[string]uint32),
		postMigrationNode: make(map[string]uint32),
	}
}

func (e *RewriteEngine) CollectPreMigrationMetrics() error {
	nodes, err := e.getRelNode()
	if err != nil {
		return err
	}

	e.preMigrationNode = nodes
	return nil
}

func (e *RewriteEngine) SendPgProxyLogs(s string) {}

func (e *RewriteEngine) CollectPostMigrationMetrics() error {
	nodes, err := e.getRelNode()
	if err != nil {
		return err
	}

	e.postMigrationNode = nodes
	return nil
}

func (e *RewriteEngine) StoreMetricsToOutput(metrics *output.OutputDatabaseMetrics) error {
	for table, nodeId := range e.postMigrationNode {
		if preMigrationNode, ok := e.preMigrationNode[table]; ok {
			if preMigrationNode != nodeId {
				metrics.RewrittenTable = append(metrics.RewrittenTable, table)
			}
		}
	}
	return nil
}

func (e *RewriteEngine) GetWarnings() []warning.Warning {
	return nil
}

func (e *RewriteEngine) getRelNode() (map[string]uint32, error) {
	rows, err := e.db.Query("select s.schemaname, s.relname, c.relfilenode from pg_class c inner join pg_stat_user_tables s on s.relid = c.oid")
	if err != nil {
		return nil, fmt.Errorf("failed to query preview database for node id: %w", err)
	}
	defer rows.Close()

	nodes := make(map[string]uint32)
	for rows.Next() {
		var schema, table string
		var relfilenode uint32
		if err := rows.Scan(&schema, &table, &relfilenode); err != nil {
			return nil, fmt.Errorf("failed to scan row for node id: %w", err)
		}
		nodes[fmt.Sprintf("%s.%s", schema, table)] = relfilenode
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan rows for node id: %w", err)
	}

	return nodes, nil
}
