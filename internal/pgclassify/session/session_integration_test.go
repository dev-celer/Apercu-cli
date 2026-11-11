//go:build integration

package session_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgclassify/session"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// observedSettings is what the server says the session looks like after a statement.
type observedSettings struct {
	searchPath       string
	timeZone         string
	lockTimeout      string
	statementTimeout string
	replicationRole  string
}

const observeQuery = `SELECT current_setting('search_path'), current_setting('TimeZone'),
       current_setting('lock_timeout'), current_setting('statement_timeout'),
       current_setting('session_replication_role')`

func observe(t *testing.T, conn *sql.Conn) observedSettings {
	t.Helper()

	got := observedSettings{}
	require.NoError(t, conn.QueryRowContext(context.Background(), observeQuery).Scan(
		&got.searchPath, &got.timeZone, &got.lockTimeout, &got.statementTimeout, &got.replicationRole))
	return got
}

// baselineCatalog builds a catalog from the settings a connection starts with.
func baselineCatalog(t *testing.T, start observedSettings, user string) *pgcatalog.Catalog {
	t.Helper()

	catalog, err := pgcatalog.NewCatalog(pgcatalog.CatalogOptions{Pre: &snapshot.Snapshot{
		Source: snapshot.SourcePreview,
		PIT:    snapshot.PITPre,
		Header: snapshot.Header{Version: pgcontract.Version17, User: user, SearchPath: start.searchPath},
		Settings: []snapshot.Setting{
			{Name: "search_path", Value: start.searchPath},
			{Name: "TimeZone", Value: start.timeZone},
			{Name: "lock_timeout", Value: start.lockTimeout},
			{Name: "statement_timeout", Value: start.statementTimeout},
			{Name: "session_replication_role", Value: start.replicationRole},
		},
	}})
	require.NoError(t, err)
	return catalog
}

var sessionScripts = []string{
	// SET LOCAL with no block around it, in a Query of its own since replay sends one statement per message.
	// The server warns and applies nothing.
	"SET LOCAL lock_timeout = '5s'; SELECT 1",
	// SET LOCAL inside a block, and what is left of it after the COMMIT.
	"BEGIN; SET LOCAL lock_timeout = '5s'; SELECT 1; COMMIT; SELECT 1",
	// A session SET survives the COMMIT.
	"BEGIN; SET lock_timeout = '5s'; SELECT 1; COMMIT; SELECT 1",
	// And does not survive a ROLLBACK: settings are transactional.
	"BEGIN; SET lock_timeout = '5s'; SELECT 1; ROLLBACK; SELECT 1",
	// LOCAL outranks session inside the block whichever order they were written in.
	"BEGIN; SET lock_timeout = '9s'; SET LOCAL lock_timeout = '5s'; SELECT 1; COMMIT; SELECT 1",
	"BEGIN; SET LOCAL lock_timeout = '5s'; SET lock_timeout = '9s'; SELECT 1; COMMIT; SELECT 1",
	// RESET and its long spelling.
	"SET lock_timeout = '5s'; RESET lock_timeout; SELECT 1",
	"SET lock_timeout = '5s'; SET lock_timeout TO DEFAULT; SELECT 1",
	"SET lock_timeout = '5s'; SET search_path = public; RESET ALL; SELECT 1",
	// A LOCAL reset has to outrank the session value it is overriding, then stop at the COMMIT.
	"SET lock_timeout = '5s'; BEGIN; SET LOCAL lock_timeout TO DEFAULT; SELECT 1; COMMIT; SELECT 1",
	// A session-level reset is still the last write, so it takes a LOCAL value with it.
	"BEGIN; SET LOCAL lock_timeout = '5s'; RESET ALL; SELECT 1; COMMIT; SELECT 1",
	"BEGIN; SET LOCAL lock_timeout = '5s'; RESET lock_timeout; SELECT 1; COMMIT; SELECT 1",
	"BEGIN; SET lock_timeout = '9s'; SET LOCAL lock_timeout = '5s'; RESET lock_timeout; SELECT 1; COMMIT; SELECT 1",
	// AND CHAIN ends the block, so the LOCAL values written in it go even though no COMMIT of the ordinary kind was ever issued.
	"SET lock_timeout = '9s'; BEGIN; SET LOCAL lock_timeout = '5s'; COMMIT AND CHAIN; SELECT 1; COMMIT",
	"SET lock_timeout = '9s'; BEGIN; SET lock_timeout = '5s'; ROLLBACK AND CHAIN; SELECT 1; COMMIT",
	// FROM CURRENT writes back what is already in force.
	"SET lock_timeout = '5s'; SET lock_timeout FROM CURRENT; SELECT 1",
	// Savepoints unwind settings, and only down to the savepoint named.
	"BEGIN; SET lock_timeout = '1s'; SAVEPOINT s; SET lock_timeout = '9s'; ROLLBACK TO SAVEPOINT s; SELECT 1; COMMIT; SELECT 1",
	"BEGIN; SAVEPOINT s; SET lock_timeout = '9s'; ROLLBACK TO SAVEPOINT s; SELECT 1; COMMIT; SELECT 1",
	"BEGIN; SAVEPOINT s; SET lock_timeout = '9s'; RELEASE SAVEPOINT s; SELECT 1; COMMIT; SELECT 1",
	"BEGIN; SAVEPOINT s; SET lock_timeout = '9s'; RELEASE SAVEPOINT s; ROLLBACK; SELECT 1",
	// The innermost savepoint of a shared name is the one that answers.
	"BEGIN; SET lock_timeout = '1s'; SAVEPOINT s; SET lock_timeout = '2s'; SAVEPOINT s; SET lock_timeout = '3s'; ROLLBACK TO SAVEPOINT s; SELECT 1; COMMIT; SELECT 1",
	// A savepoint can be rolled back to more than once.
	"BEGIN; SAVEPOINT s; SET lock_timeout = '9s'; ROLLBACK TO SAVEPOINT s; SET lock_timeout = '8s'; ROLLBACK TO SAVEPOINT s; SELECT 1; COMMIT",
	// A LOCAL write is unwound by a partial rollback like any other.
	"BEGIN; SET LOCAL lock_timeout = '1s'; SAVEPOINT s; SET LOCAL lock_timeout = '9s'; ROLLBACK TO SAVEPOINT s; SELECT 1; COMMIT; SELECT 1",
	// The other tracked parameters follow the same scoping.
	"BEGIN; SET LOCAL search_path = public; SELECT 1; COMMIT; SELECT 1",
	`BEGIN; SET LOCAL search_path = "$user", public; SELECT 1; COMMIT`,
	"SET session_replication_role = replica; SELECT 1; RESET session_replication_role; SELECT 1",
	"BEGIN; SET LOCAL TimeZone = 'Europe/Paris'; SELECT 1; COMMIT; SELECT 1",
	"SET statement_timeout = '2min'; BEGIN; SET LOCAL statement_timeout = '250ms'; SELECT 1; COMMIT; SELECT 1",
}

// TestSessionContextMatchesServer test every statement in sessionScripts against real servers for every supported version.
// It than compare the values recorded by Session and the real value the server give.
func TestSessionContextMatchesServer(t *testing.T) {
	t.Parallel()

	wg := sync.WaitGroup{}
	for _, version := range classifytest.Versions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
				db := classifytest.StartPostgres(t, version)
				for _, script := range sessionScripts {
					t.Run(script, func(t *testing.T) { replay(t, db, script) })
				}
			})
		}()
	}
	wg.Wait()
}

// replay runs one script on a connection of its own, comparing the context the session hands out with what the server reports, statement by statement.
func replay(t *testing.T, db *sql.DB, script string) {
	t.Helper()
	ctx := context.Background()

	// One connection per script.
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// The pool can hand back an already used connection, we need to clean it up before using it.
	_, err = conn.ExecContext(ctx, "DISCARD ALL")
	require.NoError(t, err)

	user := ""
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT current_user").Scan(&user))
	catalog := baselineCatalog(t, observe(t, conn), user)
	sess := session.NewSession(catalog)

	for _, statement := range pgparse.Parse(script) {
		got := sess.Next(statement)
		assertContext(t, catalog, observe(t, conn), got, fmt.Sprintf("entering %q", statement.RawSQL))

		_, err := conn.ExecContext(ctx, statement.RawSQL)
		require.NoErrorf(t, err, "the server refused %q", statement.RawSQL)
	}
}

// assertContext compares the context the session handed out with what the server reports.
func assertContext(t *testing.T, catalog *pgcatalog.Catalog, want observedSettings, got session.Context, where string) {
	t.Helper()

	assert.Equalf(t, pgcatalog.ParseSearchPath(want.searchPath, catalog.SnapshotUser()), got.SearchPath, "search_path %s", where)
	assert.Equalf(t, want.timeZone, got.TimeZone, "TimeZone %s", where)
	assert.Equalf(t, session.ParseTimeout(want.lockTimeout).Duration, got.LockTimeout.Duration, "lock_timeout %s", where)
	assert.Equalf(t, session.ParseTimeout(want.statementTimeout).Duration, got.StatementTimeout.Duration, "statement_timeout %s", where)
	assert.Equalf(t, session.ParseReplicationRole(want.replicationRole), got.ReplicationRole, "session_replication_role %s", where)
}

var cycleScripts = [][]classifytest.CycleSpec{
	// A multi-statement Query is one implicit transaction, SET LOCAL holds until the end of the message.
	{classifytest.Simple("SET LOCAL lock_timeout = '5s'; SELECT 1"), classifytest.Simple("SELECT 1")},
	// A pipeline is one too. The server warns that SET LOCAL needs a transaction block, and applies it anyway.
	{classifytest.Pipe("SET LOCAL lock_timeout = '5s'; SELECT 1"), classifytest.Simple("SELECT 1")},
	{classifytest.Pipe("SET LOCAL lock_timeout = '5s'; SELECT 1"), classifytest.Pipe("SELECT 1")},
	// Leaving the implicit transaction falls back to the session value, not to the baseline.
	{classifytest.Simple("SET lock_timeout = '9s'"), classifytest.Simple("SET LOCAL lock_timeout = '5s'; SELECT 1"), classifytest.Simple("SELECT 1")},
	{classifytest.Simple("SET lock_timeout = '9s'"), classifytest.Pipe("SET LOCAL lock_timeout = '5s'; SELECT 1"), classifytest.Simple("SELECT 1")},
	{classifytest.Simple("SET lock_timeout = '9s'"), classifytest.Simple("SET LOCAL lock_timeout TO DEFAULT; SELECT 1"), classifytest.Simple("SELECT 1")},
	// A message of its own is a transaction of its own, a SET LOCAL there applies to nothing.
	{classifytest.Simple("SET LOCAL lock_timeout = '5s'"), classifytest.Simple("SELECT 1")},
	{classifytest.Simple("SELECT 1; SET LOCAL lock_timeout = '5s'"), classifytest.Simple("SELECT 1")},
	// COMMIT and ROLLBACK end the implicit transaction in the middle of the message.
	{classifytest.Simple("SET LOCAL lock_timeout = '5s'; COMMIT; SELECT 1")},
	{classifytest.Simple("SET LOCAL lock_timeout = '5s'; ROLLBACK; SELECT 1")},
	{classifytest.Pipe("SET LOCAL lock_timeout = '5s'; COMMIT; SELECT 1")},
	// The statements after it run in an implicit transaction of their own.
	{classifytest.Simple("SET LOCAL lock_timeout = '5s'; COMMIT; SET LOCAL lock_timeout = '9s'; SELECT 1"), classifytest.Simple("SELECT 1")},
	{classifytest.Pipe("SET LOCAL lock_timeout = '5s'; COMMIT; SET LOCAL lock_timeout = '9s'; SELECT 1"), classifytest.Simple("SELECT 1")},
	// The implicit transaction is still a transaction: its ROLLBACK undoes a session SET, its COMMIT keeps it.
	{classifytest.Simple("SET lock_timeout = '5s'; ROLLBACK; SELECT 1"), classifytest.Simple("SELECT 1")},
	{classifytest.Pipe("SET lock_timeout = '5s'; ROLLBACK; SELECT 1"), classifytest.Simple("SELECT 1")},
	{classifytest.Simple("SET lock_timeout = '5s'; COMMIT; SELECT 1"), classifytest.Simple("SELECT 1")},
	// BEGIN turns the implicit transaction into the block, with what it already wrote.
	{classifytest.Simple("SET LOCAL lock_timeout = '5s'; BEGIN; SELECT 1"), classifytest.Simple("SELECT 1; COMMIT"), classifytest.Simple("SELECT 1")},
	{classifytest.Pipe("SET LOCAL lock_timeout = '5s'; BEGIN; SELECT 1"), classifytest.Pipe("SELECT 1; COMMIT"), classifytest.Simple("SELECT 1")},
	// So the block's ROLLBACK undoes the writes made before the BEGIN.
	{classifytest.Simple("SET lock_timeout = '5s'; BEGIN; ROLLBACK; SELECT 1"), classifytest.Simple("SELECT 1")},
	{classifytest.Pipe("SET lock_timeout = '5s'; BEGIN; ROLLBACK; SELECT 1"), classifytest.Simple("SELECT 1")},
	// An explicit block outlives the messages it spans.
	{classifytest.Simple("BEGIN; SET LOCAL lock_timeout = '5s'"), classifytest.Simple("SELECT 1; COMMIT; SELECT 1")},
	{classifytest.Pipe("BEGIN; SET LOCAL lock_timeout = '5s'"), classifytest.Pipe("SELECT 1; COMMIT; SELECT 1")},
	// The other tracked parameters follow the same scoping.
	{classifytest.Simple("SET LOCAL search_path = public; SELECT 1"), classifytest.Simple("SELECT 1")},
	{classifytest.Pipe("SET LOCAL TimeZone = 'Europe/Paris'; SELECT 1"), classifytest.Simple("SELECT 1")},
}

// TestSessionContextAcrossCyclesMatchesServer replays every script in cycleScripts against real servers for every
// supported version, sending each cycle the way its client would, and compares the session with what the server reports.
func TestSessionContextAcrossCyclesMatchesServer(t *testing.T) {
	t.Parallel()

	wg := sync.WaitGroup{}
	for _, version := range classifytest.Versions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
				url := classifytest.StartServer(t, version)
				for _, script := range cycleScripts {
					t.Run(classifytest.CycleScriptName(script), func(t *testing.T) { replayCycles(t, url, script) })
				}
			})
		}()
	}
	wg.Wait()
}

// replayCycles runs one script on a connection of its own, comparing the context the session hands out with what the server reports, statement by statement.
func replayCycles(t *testing.T, url string, script []classifytest.CycleSpec) {
	t.Helper()
	ctx := context.Background()

	conn, err := pgconn.Connect(ctx, url)
	require.NoError(t, err)
	defer func() { _ = conn.Close(ctx) }()

	users, err := conn.Exec(ctx, "SELECT current_user").ReadAll()
	require.NoError(t, err)
	catalog := baselineCatalog(t, observeConn(t, conn), string(users[0].Rows[0][0]))
	sess := session.NewSession(catalog)

	for cycle, spec := range script {
		statements := pgparse.Parse(spec.SQL)
		require.NotEmpty(t, statements)

		entering := []observedSettings{observeConn(t, conn)}
		for _, result := range classifytest.SendCycle(t, conn, spec, statements, observeQuery) {
			entering = append(entering, settingsOf(t, result))
		}
		for i, statement := range statements {
			got := sess.NextInCycle(statement, cycle)
			assertContext(t, catalog, entering[i], got, fmt.Sprintf("entering %q in cycle %d", statement.RawSQL, cycle))
		}
	}
}

// observeConn is observe on a protocol-level connection, in a Query message of its own.
func observeConn(t *testing.T, conn *pgconn.PgConn) observedSettings {
	t.Helper()

	results, err := conn.Exec(context.Background(), observeQuery).ReadAll()
	require.NoError(t, err)
	require.Len(t, results, 1)
	return settingsOf(t, results[0])
}

// settingsOf reads the row of one observeQuery.
func settingsOf(t *testing.T, result *pgconn.Result) observedSettings {
	t.Helper()

	require.NoError(t, result.Err)
	require.Len(t, result.Rows, 1)
	row := result.Rows[0]
	return observedSettings{
		searchPath:       string(row[0]),
		timeZone:         string(row[1]),
		lockTimeout:      string(row[2]),
		statementTimeout: string(row[3]),
		replicationRole:  string(row[4]),
	}
}
