//go:build integration

package pg_classify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Initialization script for integration tests
const txnBlockDDL = `CREATE INDEX events_at_idx ON events (at);`

type txnBlockScript struct {
	sql     string
	subject string
}

var txnBlockScripts = []txnBlockScript{
	{sql: "CREATE INDEX CONCURRENTLY v01_cic ON orders (status)", subject: "CREATE INDEX CONCURRENTLY"},
	{sql: "CREATE INDEX v01_plain ON orders (status)"},
	{sql: "DROP INDEX CONCURRENTLY orders_status_idx", subject: "DROP INDEX CONCURRENTLY"},
	{sql: "DROP INDEX orders_status_idx"},

	{sql: "REINDEX INDEX CONCURRENTLY orders_status_idx", subject: "REINDEX CONCURRENTLY"},
	{sql: "REINDEX TABLE CONCURRENTLY orders", subject: "REINDEX CONCURRENTLY"},
	{sql: "REINDEX SCHEMA " + oracleSchema, subject: "REINDEX SCHEMA"},
	{sql: "REINDEX DATABASE app", subject: "REINDEX DATABASE"},
	{sql: "REINDEX SYSTEM app", subject: "REINDEX SYSTEM"},
	{sql: "REINDEX TABLE events", subject: "REINDEX TABLE"},
	{sql: "REINDEX INDEX events_at_idx", subject: "REINDEX INDEX"},
	{sql: "REINDEX TABLE orders"},
	{sql: "REINDEX INDEX orders_status_idx"},

	{sql: "VACUUM", subject: "VACUUM"},
	{sql: "VACUUM orders", subject: "VACUUM"},
	{sql: "VACUUM FULL orders", subject: "VACUUM"},
	{sql: "VACUUM (ANALYZE) orders", subject: "VACUUM"},
	{sql: "ANALYZE orders"},

	{sql: "CLUSTER", subject: "CLUSTER"},
	{sql: "CLUSTER events USING events_at_idx", subject: "CLUSTER"},
	{sql: "CLUSTER orders USING orders_status_idx"},

	{sql: "ALTER TABLE events DETACH PARTITION events_2025 CONCURRENTLY", subject: "ALTER TABLE ... DETACH CONCURRENTLY"},
	{sql: "ALTER TABLE events DETACH PARTITION events_2025"},
	{sql: "ALTER TABLE events DETACH PARTITION events_2025 FINALIZE"},

	{sql: "CREATE DATABASE v01_db", subject: "CREATE DATABASE"},
	{sql: "DROP DATABASE v01_db", subject: "DROP DATABASE"},
	{sql: "CREATE TABLESPACE v01_ts LOCATION '/tmp/v01_ts'", subject: "CREATE TABLESPACE"},
	{sql: "DROP TABLESPACE v01_ts", subject: "DROP TABLESPACE"},
}

func TestTxnBlockSafetyOracle(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	for _, version := range integrationVersions {
		wg.Add(1)
		go func(version pg_contract.Version) {
			defer wg.Done()
			t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
				db := startPostgres(t, version)
				_, err := db.Exec(oracleDDL)
				require.NoError(t, err)
				_, err = db.Exec(txnBlockDDL)
				require.NoError(t, err)

				catalog := oracleCatalog(t, db)
				require.Equal(t, version, catalog.Version())

				for _, script := range txnBlockScripts {
					t.Run(script.sql, func(t *testing.T) { explicitTxnBlock(t, db, catalog, script) })
				}
				t.Run("an implicit transaction refuses them the same way", func(t *testing.T) {
					implicitTxnBlock(t, db, catalog)
				})
			})
		}(version)
	}
	wg.Wait()
}

func explicitTxnBlock(t *testing.T, db *sql.DB, catalog *pg_catalog.Catalog, script txnBlockScript) {
	t.Helper()
	ctx := context.Background()

	statements := pg_parse.Parse("BEGIN; " + script.sql)
	require.Len(t, statements, 2)
	classifier := NewClassifier(catalog)
	classifier.Next(statements[0])
	analysis := classifier.Next(statements[1])

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.ExecContext(ctx, "SET search_path TO "+oracleSchema)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)
	defer func() { _, _ = conn.ExecContext(ctx, "ROLLBACK") }()

	_, execErr := conn.ExecContext(ctx, script.sql)
	refused, serverMessage := refusedInTxnBlock(execErr)

	predicted := filterTxnBlockErrors(analysis)
	assert.Equalf(t, script.subject != "", refused,
		"the server %s %q inside a block (said: %v), the corpus says %q",
		accepted(execErr), script.sql, execErr, script.subject)
	require.Equalf(t, script.subject != "", len(predicted) == 1,
		"V-01 %v on %q, the corpus says %q", predicted, script.sql, script.subject)

	if script.subject == "" {
		return
	}
	assert.Equal(t, serverMessage, predicted[0].Message, "V-01 reports the server's own wording")
	assert.Emptyf(t, analysis.Findings, "%q never reaches a relation, so it takes no lock", script.sql)
}

func refusedInTxnBlock(err error) (bool, string) {
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) || pqErr.Code != "25001" {
		return false, ""
	}
	return true, pqErr.Message
}

func filterTxnBlockErrors(analysis pg_contract.StatementAnalysis) []pg_contract.Error {
	var out []pg_contract.Error
	for _, err := range analysis.Errors {
		if err.Code == txnBlockCode {
			out = append(out, err)
		}
	}
	return out
}

func implicitTxnBlock(t *testing.T, db *sql.DB, catalog *pg_catalog.Catalog) {
	t.Helper()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.ExecContext(ctx, "SET search_path TO "+oracleSchema)
	require.NoError(t, err)
	defer func() { _, _ = conn.ExecContext(ctx, "ROLLBACK") }()

	// A VACUUM runs fine on its own and is refused the moment it shares a message with anything.
	_, execErr := conn.ExecContext(ctx, "VACUUM orders; ANALYZE orders;")
	refused, serverMessage := refusedInTxnBlock(execErr)
	require.Truef(t, refused, "the server accepted a VACUUM sharing a message (said: %v)", execErr)

	shared := Analyze(catalog, cycle(0, event("VACUUM orders", 1), event("ANALYZE orders", 1)))
	require.Len(t, shared.Statements, 2)
	predicted := filterTxnBlockErrors(shared.Statements[0])
	require.Lenf(t, predicted, 1, "V-01 %v on a VACUUM sharing a cycle", predicted)
	assert.Equal(t, serverMessage, predicted[0].Message)
	assert.Empty(t, shared.Statements[0].Findings, "it never reaches the relation, so it takes no lock")
	assert.Empty(t, filterTxnBlockErrors(shared.Statements[1]), "the ANALYZE is safe in a block")

	// The same VACUUM with the message to itself is what the server accepted above.
	alone := Analyze(catalog, apart(event("VACUUM orders", 1)))
	require.Len(t, alone.Statements, 1)
	assert.Empty(t, filterTxnBlockErrors(alone.Statements[0]))
	assert.NotEmpty(t, alone.Statements[0].Findings, "and it takes its lock")
}
