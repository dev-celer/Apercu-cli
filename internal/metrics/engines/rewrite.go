package engines

import (
	databasehelper "apercu-cli/helper"
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"apercu-cli/output"
	"database/sql"
	"fmt"
)

type RewriteEngine struct {
	db                *sql.DB
	preMigrationNode  map[databasehelper.FullTableName]uint32
	postMigrationNode map[databasehelper.FullTableName]uint32
	warnings          []warning.Warning
	prodMetrics       map[databasehelper.FullTableName]metricshelper.TableMetrics
}

func NewRewriteEngine(db *sql.DB, prodMetrics map[databasehelper.FullTableName]metricshelper.TableMetrics) *RewriteEngine {
	return &RewriteEngine{
		db:                db,
		preMigrationNode:  make(map[databasehelper.FullTableName]uint32),
		postMigrationNode: make(map[databasehelper.FullTableName]uint32),
		warnings:          make([]warning.Warning, 0),
		prodMetrics:       prodMetrics,
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

func (e *RewriteEngine) SendPgProxyLogs(_ string) {}

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

				// Get prod database metrics
				prod, ok := e.prodMetrics[table]

				// Generate warning
				if ok {
					e.warnings = append(e.warnings, warning.NewRewriteWarning(table, &prod))
				} else {
					e.warnings = append(e.warnings, warning.NewRewriteWarning(table, nil))
				}
			}
		}
	}
	return nil
}

func (e *RewriteEngine) GetWarnings() []warning.Warning {
	return e.warnings
}

func (e *RewriteEngine) getRelNode() (map[databasehelper.FullTableName]uint32, error) {
	rows, err := e.db.Query("select s.schemaname, s.relname, c.relfilenode from pg_class c inner join pg_stat_user_tables s on s.relid = c.oid")
	if err != nil {
		return nil, fmt.Errorf("failed to query preview database for node id: %w", err)
	}
	defer rows.Close()

	nodes := make(map[databasehelper.FullTableName]uint32)
	for rows.Next() {
		var schema, table string
		var relfilenode uint32
		if err := rows.Scan(&schema, &table, &relfilenode); err != nil {
			return nil, fmt.Errorf("failed to scan row for node id: %w", err)
		}

		nodes[databasehelper.FullTableName{
			Schema: schema,
			Table:  table,
		}] = relfilenode
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan rows for node id: %w", err)
	}

	return nodes, nil
}
