//go:build integration

package pg_classify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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

const queueProbeTimeout = "100ms"
const queueHeldFor = 1500 * time.Millisecond

var queueThresholdScripts = []string{
	"LOCK TABLE orders IN ACCESS SHARE MODE",
	"LOCK TABLE orders IN ROW SHARE MODE",
	"LOCK TABLE orders IN ROW EXCLUSIVE MODE",
	"LOCK TABLE orders IN SHARE UPDATE EXCLUSIVE MODE",
	"LOCK TABLE orders IN SHARE MODE",
	"LOCK TABLE orders IN SHARE ROW EXCLUSIVE MODE",
	"LOCK TABLE orders IN EXCLUSIVE MODE",
	"LOCK TABLE orders IN ACCESS EXCLUSIVE MODE",
	"UPDATE orders SET status = 'done' WHERE id < 10",
	"ALTER TABLE orders VALIDATE CONSTRAINT orders_total_nv",
	"CREATE INDEX v02_idx ON orders (status)",
	"ALTER TABLE orders ADD CONSTRAINT v02_fk FOREIGN KEY (user_id) REFERENCES users (id) NOT VALID",
	"CREATE TABLE v02_fk (id bigint REFERENCES users (id))",
	"DROP INDEX orders_status_idx",
	"ALTER TABLE orders ADD COLUMN v02 int",
	"ALTER TABLE events ADD COLUMN v02 int",
}

type timeoutScopeScript struct {
	name     string
	messages []string
	gap      string
}

const v02AddColumn = "ALTER TABLE " + oracleSchema + ".orders ADD COLUMN v02 int"

var timeoutScopeScripts = []timeoutScopeScript{
	{name: "no timeout at all", messages: []string{v02AddColumn}},
	{name: "a session timeout", messages: []string{"SET lock_timeout = '200ms'", v02AddColumn}},
	{name: "a timeout of zero is disabled", messages: []string{"SET lock_timeout = 0", v02AddColumn}},
	{name: "SET LOCAL outside a block applies nothing", messages: []string{"SET LOCAL lock_timeout = '200ms'", v02AddColumn}},
	{name: "SET LOCAL inside the block", messages: []string{"BEGIN", "SET LOCAL lock_timeout = '200ms'", v02AddColumn}},
	{name: "SET LOCAL ends with its block", messages: []string{"BEGIN", "SET LOCAL lock_timeout = '200ms'", "COMMIT", v02AddColumn}},
	{name: "a session SET survives the COMMIT", messages: []string{"BEGIN", "SET lock_timeout = '200ms'", "COMMIT", v02AddColumn}},
	{name: "and not a ROLLBACK", messages: []string{"BEGIN", "SET lock_timeout = '200ms'", "ROLLBACK", v02AddColumn}},
	{name: "RESET", messages: []string{"SET lock_timeout = '200ms'", "RESET lock_timeout", v02AddColumn}},
	{name: "RESET ALL", messages: []string{"SET lock_timeout = '200ms'", "RESET ALL", v02AddColumn}},
	{
		name:     "a savepoint unwinds the timeout",
		messages: []string{"BEGIN", "SAVEPOINT s", "SET LOCAL lock_timeout = '200ms'", "ROLLBACK TO SAVEPOINT s", v02AddColumn},
	},
	{
		name:     "SET LOCAL to zero overrides a session timeout",
		messages: []string{"SET lock_timeout = '200ms'", "BEGIN", "SET LOCAL lock_timeout = 0", v02AddColumn},
	},
	{name: "NOWAIT fails instead of queueing", messages: []string{"BEGIN", "LOCK TABLE orders IN ACCESS EXCLUSIVE MODE NOWAIT"}},
	{name: "a COMMIT part-way through a message ends SET LOCAL", messages: []string{"SET LOCAL lock_timeout = '200ms'; COMMIT; " + v02AddColumn}},
	{
		name:     "SET LOCAL bounds the rest of its own message",
		messages: []string{"SET LOCAL lock_timeout = '200ms'; " + v02AddColumn},
		gap:      "C-04 does not model the implicit transaction of a shared cycle yet, the server bounds this wait",
	},
}

func TestUnboundedQueueOracle(t *testing.T) {
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
				catalog := oracleCatalog(t, db)

				for _, sql := range queueThresholdScripts {
					t.Run("threshold/"+sql, func(t *testing.T) { queueThreshold(t, db, catalog, sql) })
				}
				for _, script := range timeoutScopeScripts {
					t.Run("scope/"+script.name, func(t *testing.T) { timeoutScope(t, db, catalog, script) })
				}
				t.Run("a covered lock never waits", func(t *testing.T) { coveredNeverWaits(t, db, catalog) })
			})
		}(version)
	}
	wg.Wait()
}

// queueThreshold assert that the lock block reader/writer as the classifier expect it.
func queueThreshold(t *testing.T, db *sql.DB, catalog *pg_catalog.Catalog, sql string) {
	t.Helper()

	statements := pg_parse.Parse(sql)
	require.Len(t, statements, 1)
	analysis := NewClassifier(catalog).Next(statements[0])

	named := map[pg_contract.Relation]bool{}
	if finding := filterV02(analysis); finding != nil {
		for _, target := range finding.Targets {
			named[target.Relation] = true
		}
	}

	for _, locked := range analysis.LockedRelations() {
		if !locked.Relation.Kind.IsTable() {
			continue
		}
		t.Run(locked.Relation.Name.Table, func(t *testing.T) {
			table := pq.QuoteIdentifier(locked.Relation.Name.Schema) + "." + pq.QuoteIdentifier(locked.Relation.Name.Table)
			readerBlocked, writerBlocked := queueBehind(t, db, sql, table)

			assert.Equalf(t, named[locked.Relation], readerBlocked || writerBlocked,
				"V-02 names %s=%v, a queued %s blocked readers=%v writers=%v", table, named[locked.Relation], locked.Lock.Short(), readerBlocked, writerBlocked)
			assert.Equalf(t, locked.Lock.IsReadBlocking(), readerBlocked, "a queued %s on %s blocked readers=%v", locked.Lock.Short(), table, readerBlocked)
		})
	}
}

// queueBehind runs sql against the test database and return if the passed table was block for write and read.
func queueBehind(t *testing.T, db *sql.DB, sql, table string) (readerBlocked, writerBlocked bool) {
	t.Helper()
	ctx := context.Background()

	holdShareUpdateExclusive(t, db, table)

	conn := oracleConn(t, db)
	_, err := conn.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)
	defer func() { _, _ = conn.ExecContext(ctx, "ROLLBACK") }()

	statement := start(t, conn, sql)
	defer statement.stop(t, db)
	if !statement.queued(t, db) {
		require.NoErrorf(t, statement.err, "%s", sql)
	}

	readerBlocked = probeBlocked(t, db, "SELECT 1 FROM "+table+" LIMIT 1")
	writerBlocked = probeBlocked(t, db, "LOCK TABLE "+table+" IN ROW EXCLUSIVE MODE")
	return readerBlocked, writerBlocked
}

// timeoutScope assert that a lock_timeout is behaving has expected by the classifier.
func timeoutScope(t *testing.T, db *sql.DB, catalog *pg_catalog.Catalog, script timeoutScopeScript) {
	t.Helper()
	if script.gap != "" {
		t.Skip(script.gap)
	}
	ctx := context.Background()

	events := make([]pg_contract.QueryEvent, 0, len(script.messages))
	for _, message := range script.messages {
		events = append(events, event(message, 1))
	}
	analysis := Analyze(catalog, apart(events...))
	last := analysis.Statements[len(analysis.Statements)-1]
	flagged := filterV02(last) != nil

	holdShareUpdateExclusive(t, db, "orders")

	conn := oracleConn(t, db)
	defer func() { _, _ = conn.ExecContext(ctx, "ROLLBACK") }()
	for _, message := range script.messages[:len(script.messages)-1] {
		_, err := conn.ExecContext(ctx, message)
		require.NoErrorf(t, err, "%s", message)
	}

	statement := start(t, conn, script.messages[len(script.messages)-1])
	defer statement.stop(t, db)
	bounded := statement.finish(queueHeldFor)
	if bounded {
		assert.Truef(t, lockTimedOut(statement.err), "%q ended with %v instead of giving up on its lock", last.RawSQL, statement.err)
	} else {
		assert.Truef(t, hasLockWaiting(t, db, statement.pid), "%q is still running but not on a lock", last.RawSQL)
	}
	assert.Equalf(t, !bounded, flagged, "the server bounded the wait of %q: %v, V-02 fired: %v", last.RawSQL, bounded, flagged)
}

func coveredNeverWaits(t *testing.T, db *sql.DB, catalog *pg_catalog.Catalog) {
	t.Helper()
	ctx := context.Background()
	const lockAll = "LOCK TABLE orders IN ACCESS EXCLUSIVE MODE"

	analysis := Analyze(catalog, apart(event("BEGIN", 1), event(lockAll, 1), event(v02AddColumn, 1)))
	require.Len(t, analysis.Statements, 3)
	assert.NotNil(t, filterV02(analysis.Statements[1]), "the LOCK can queue")
	assert.Nil(t, filterV02(analysis.Statements[2]), "the ALTER holds its lock already")

	conn := oracleConn(t, db)
	defer func() { _, _ = conn.ExecContext(ctx, "ROLLBACK") }()
	for _, sql := range []string{"BEGIN", lockAll} {
		_, err := conn.ExecContext(ctx, sql)
		require.NoError(t, err)
	}

	holder := oracleConn(t, db)
	_, err := holder.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)
	defer func() { _, _ = holder.ExecContext(ctx, "ROLLBACK") }()
	waiting := start(t, holder, "LOCK TABLE orders IN SHARE UPDATE EXCLUSIVE MODE")
	defer waiting.stop(t, db)
	require.True(t, waiting.queued(t, db), "the holder queues behind the transaction's lock")

	alter := start(t, conn, v02AddColumn)
	defer alter.stop(t, db)
	require.True(t, alter.finish(queueHeldFor), "the ALTER waited for a lock its own transaction holds")
	assert.NoError(t, alter.err)
}

// oracleConn give a new cleaned up connection, ready to be used for tests.
func oracleConn(t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = conn.ExecContext(ctx, "DISCARD ALL")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "SET search_path TO "+oracleSchema)
	require.NoError(t, err)
	return conn
}

func holdShareUpdateExclusive(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	ctx := context.Background()

	holder := oracleConn(t, db)
	_, err := holder.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = holder.ExecContext(ctx, "ROLLBACK") })
	_, err = holder.ExecContext(ctx, "LOCK TABLE "+table+" IN SHARE UPDATE EXCLUSIVE MODE")
	require.NoError(t, err)
}

// probeBlocked runs sql under a short lock_timeout and reports whether it gave up waiting for its lock.
func probeBlocked(t *testing.T, db *sql.DB, sql string) bool {
	t.Helper()

	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.Exec("SET LOCAL lock_timeout = '" + queueProbeTimeout + "'")
	require.NoError(t, err)

	_, err = tx.Exec(sql)
	if lockTimedOut(err) {
		return true
	}
	require.NoErrorf(t, err, "%s", sql)
	return false
}

func lockTimedOut(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "55P03"
}

func hasLockWaiting(t *testing.T, db *sql.DB, pid int) bool {
	t.Helper()

	waiting := false
	require.NoError(t, db.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND NOT granted)", pid).Scan(&waiting))
	return waiting
}

// pending is a statement left running on its own connection.
type pending struct {
	pid      int
	done     chan error
	finished bool
	err      error
}

func start(t *testing.T, conn *sql.Conn, sql string) *pending {
	t.Helper()

	p := &pending{done: make(chan error, 1)}
	require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT pg_backend_pid()").Scan(&p.pid))
	go func() {
		_, err := conn.ExecContext(context.Background(), sql)
		p.done <- err
	}()
	return p
}

// finish waits up to within for the statement to end and reports whether it did.
func (p *pending) finish(within time.Duration) bool {
	if p.finished {
		return true
	}
	select {
	case p.err = <-p.done:
		p.finished = true
	case <-time.After(within):
	}
	return p.finished
}

// queued polls until the statement waits for a lock, false when it finished first.
func (p *pending) queued(t *testing.T, db *sql.DB) bool {
	t.Helper()

	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if p.finish(10 * time.Millisecond) {
			return false
		}
		if hasLockWaiting(t, db, p.pid) {
			return true
		}
	}
	require.FailNow(t, "the statement neither queued for a lock nor finished")
	return false
}

// stop cancels the statement if it is still running and waits for it to end, so its connection can be reused.
func (p *pending) stop(t *testing.T, db *sql.DB) {
	t.Helper()
	if p.finished {
		return
	}
	_, err := db.Exec("SELECT pg_cancel_backend($1)", p.pid)
	assert.NoError(t, err)
	p.err = <-p.done
	p.finished = true
}
