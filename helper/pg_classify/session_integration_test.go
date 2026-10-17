//go:build integration

package pg_classify

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var integrationVersions = []pg_contract.Version{15, 16, 17, 18}

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

// startServer starts a server of the given version and answers with its connection string.
func startServer(t *testing.T, version pg_contract.Version) string {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx,
		fmt.Sprintf("postgres:%d-alpine", version),
		postgres.WithDatabase("app"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("pg"),
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	return url
}

func startPostgres(t *testing.T, version pg_contract.Version) *sql.DB {
	t.Helper()

	db, err := sql.Open("postgres", startServer(t, version))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.Ping())
	return db
}

func observe(t *testing.T, conn *sql.Conn) observedSettings {
	t.Helper()

	got := observedSettings{}
	require.NoError(t, conn.QueryRowContext(context.Background(), observeQuery).Scan(
		&got.searchPath, &got.timeZone, &got.lockTimeout, &got.statementTimeout, &got.replicationRole))
	return got
}

// baselineCatalog builds a catalog from the settings a connection starts with.
func baselineCatalog(t *testing.T, start observedSettings, user string) *pg_catalog.Catalog {
	t.Helper()

	catalog, err := pg_catalog.NewCatalog(pg_catalog.CatalogOptions{Pre: &pg_catalog.Snapshot{
		Source: pg_catalog.SourcePreview,
		PIT:    pg_catalog.PITPre,
		Header: pg_catalog.Header{Version: pg_contract.Version17, User: user, SearchPath: start.searchPath},
		Settings: []pg_catalog.Setting{
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
	for _, version := range integrationVersions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
				db := startPostgres(t, version)
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
	session := NewSession(catalog)

	for _, statement := range pg_parse.Parse(script) {
		got := session.Next(statement)
		assertContext(t, catalog, observe(t, conn), got, fmt.Sprintf("entering %q", statement.RawSQL))

		_, err := conn.ExecContext(ctx, statement.RawSQL)
		require.NoErrorf(t, err, "the server refused %q", statement.RawSQL)
	}
}

// assertContext compares the context the session handed out with what the server reports.
func assertContext(t *testing.T, catalog *pg_catalog.Catalog, want observedSettings, got Context, where string) {
	t.Helper()

	assert.Equalf(t, pg_catalog.ParseSearchPath(want.searchPath, catalog.SnapshotUser()), got.SearchPath, "search_path %s", where)
	assert.Equalf(t, want.timeZone, got.TimeZone, "TimeZone %s", where)
	assert.Equalf(t, parseTimeout(want.lockTimeout).Duration, got.LockTimeout.Duration, "lock_timeout %s", where)
	assert.Equalf(t, parseTimeout(want.statementTimeout).Duration, got.StatementTimeout.Duration, "statement_timeout %s", where)
	assert.Equalf(t, parseReplicationRole(want.replicationRole), got.ReplicationRole, "session_replication_role %s", where)
}

// protocol is how a cycle reaches the server.
type protocol string

const (
	protocolQuery    protocol = "Q"
	protocolPipeline protocol = "P"
)

// cycleSpec is one client message. The server runs its statements in one implicit transaction.
type cycleSpec struct {
	protocol protocol
	sql      string
}

func simple(sql string) cycleSpec { return cycleSpec{protocol: protocolQuery, sql: sql} }
func pipe(sql string) cycleSpec   { return cycleSpec{protocol: protocolPipeline, sql: sql} }

// cycleScriptName is a script as its subtest name: Q(...) for a Query, P(...) for a pipeline.
func cycleScriptName(script []cycleSpec) string {
	parts := make([]string, 0, len(script))
	for _, spec := range script {
		parts = append(parts, fmt.Sprintf("%s(%s)", spec.protocol, spec.sql))
	}
	return strings.Join(parts, " | ")
}

var cycleScripts = [][]cycleSpec{
	// A multi-statement Query is one implicit transaction, SET LOCAL holds until the end of the message.
	{simple("SET LOCAL lock_timeout = '5s'; SELECT 1"), simple("SELECT 1")},
	// A pipeline is one too. The server warns that SET LOCAL needs a transaction block, and applies it anyway.
	{pipe("SET LOCAL lock_timeout = '5s'; SELECT 1"), simple("SELECT 1")},
	{pipe("SET LOCAL lock_timeout = '5s'; SELECT 1"), pipe("SELECT 1")},
	// Leaving the implicit transaction falls back to the session value, not to the baseline.
	{simple("SET lock_timeout = '9s'"), simple("SET LOCAL lock_timeout = '5s'; SELECT 1"), simple("SELECT 1")},
	{simple("SET lock_timeout = '9s'"), pipe("SET LOCAL lock_timeout = '5s'; SELECT 1"), simple("SELECT 1")},
	{simple("SET lock_timeout = '9s'"), simple("SET LOCAL lock_timeout TO DEFAULT; SELECT 1"), simple("SELECT 1")},
	// A message of its own is a transaction of its own, a SET LOCAL there applies to nothing.
	{simple("SET LOCAL lock_timeout = '5s'"), simple("SELECT 1")},
	{simple("SELECT 1; SET LOCAL lock_timeout = '5s'"), simple("SELECT 1")},
	// COMMIT and ROLLBACK end the implicit transaction in the middle of the message.
	{simple("SET LOCAL lock_timeout = '5s'; COMMIT; SELECT 1")},
	{simple("SET LOCAL lock_timeout = '5s'; ROLLBACK; SELECT 1")},
	{pipe("SET LOCAL lock_timeout = '5s'; COMMIT; SELECT 1")},
	// The statements after it run in an implicit transaction of their own.
	{simple("SET LOCAL lock_timeout = '5s'; COMMIT; SET LOCAL lock_timeout = '9s'; SELECT 1"), simple("SELECT 1")},
	{pipe("SET LOCAL lock_timeout = '5s'; COMMIT; SET LOCAL lock_timeout = '9s'; SELECT 1"), simple("SELECT 1")},
	// The implicit transaction is still a transaction: its ROLLBACK undoes a session SET, its COMMIT keeps it.
	{simple("SET lock_timeout = '5s'; ROLLBACK; SELECT 1"), simple("SELECT 1")},
	{pipe("SET lock_timeout = '5s'; ROLLBACK; SELECT 1"), simple("SELECT 1")},
	{simple("SET lock_timeout = '5s'; COMMIT; SELECT 1"), simple("SELECT 1")},
	// BEGIN turns the implicit transaction into the block, with what it already wrote.
	{simple("SET LOCAL lock_timeout = '5s'; BEGIN; SELECT 1"), simple("SELECT 1; COMMIT"), simple("SELECT 1")},
	{pipe("SET LOCAL lock_timeout = '5s'; BEGIN; SELECT 1"), pipe("SELECT 1; COMMIT"), simple("SELECT 1")},
	// So the block's ROLLBACK undoes the writes made before the BEGIN.
	{simple("SET lock_timeout = '5s'; BEGIN; ROLLBACK; SELECT 1"), simple("SELECT 1")},
	{pipe("SET lock_timeout = '5s'; BEGIN; ROLLBACK; SELECT 1"), simple("SELECT 1")},
	// An explicit block outlives the messages it spans.
	{simple("BEGIN; SET LOCAL lock_timeout = '5s'"), simple("SELECT 1; COMMIT; SELECT 1")},
	{pipe("BEGIN; SET LOCAL lock_timeout = '5s'"), pipe("SELECT 1; COMMIT; SELECT 1")},
	// The other tracked parameters follow the same scoping.
	{simple("SET LOCAL search_path = public; SELECT 1"), simple("SELECT 1")},
	{pipe("SET LOCAL TimeZone = 'Europe/Paris'; SELECT 1"), simple("SELECT 1")},
}

// TestSessionContextAcrossCyclesMatchesServer replays every script in cycleScripts against real servers for every
// supported version, sending each cycle the way its client would, and compares the session with what the server reports.
func TestSessionContextAcrossCyclesMatchesServer(t *testing.T) {
	t.Parallel()

	wg := sync.WaitGroup{}
	for _, version := range integrationVersions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
				url := startServer(t, version)
				for _, script := range cycleScripts {
					t.Run(cycleScriptName(script), func(t *testing.T) { replayCycles(t, url, script) })
				}
			})
		}()
	}
	wg.Wait()
}

// replayCycles runs one script on a connection of its own, comparing the context the session hands out with what the server reports, statement by statement.
func replayCycles(t *testing.T, url string, script []cycleSpec) {
	t.Helper()
	ctx := context.Background()

	conn, err := pgconn.Connect(ctx, url)
	require.NoError(t, err)
	defer func() { _ = conn.Close(ctx) }()

	users, err := conn.Exec(ctx, "SELECT current_user").ReadAll()
	require.NoError(t, err)
	catalog := baselineCatalog(t, observeConn(t, conn), string(users[0].Rows[0][0]))
	session := NewSession(catalog)

	for cycle, spec := range script {
		statements := pg_parse.Parse(spec.sql)
		require.NotEmpty(t, statements)

		entering := append([]observedSettings{observeConn(t, conn)}, sendCycle(t, conn, spec, statements)...)
		for i, statement := range statements {
			got := session.NextInCycle(statement, cycle)
			assertContext(t, catalog, entering[i], got, fmt.Sprintf("entering %q in cycle %d", statement.RawSQL, cycle))
		}
	}
}

// sendCycle runs one cycle with an observeQuery in front of every statement but the first, and answers with what each
// of those statements was entering. The first one is observed before the cycle, in a message of its own: injecting a
// query into a single-statement cycle would turn it into an implicit transaction, the very thing under test.
func sendCycle(t *testing.T, conn *pgconn.PgConn, spec cycleSpec, statements []pg_parse.Statement) []observedSettings {
	t.Helper()

	var sent []string
	var observed []bool
	for i, statement := range statements {
		if i > 0 {
			sent = append(sent, observeQuery)
			observed = append(observed, true)
		}
		sent = append(sent, statement.RawSQL)
		observed = append(observed, false)
	}

	var results []*pgconn.Result
	switch spec.protocol {
	case protocolQuery:
		var err error
		results, err = conn.Exec(context.Background(), strings.Join(sent, "; ")).ReadAll()
		require.NoErrorf(t, err, "the server refused %q", spec.sql)
	case protocolPipeline:
		results = runPipeline(t, conn, sent)
	}
	require.Len(t, results, len(sent))

	var entering []observedSettings
	for i, result := range results {
		require.NoErrorf(t, result.Err, "the server refused %q", sent[i])
		if observed[i] {
			entering = append(entering, settingsOf(t, result))
		}
	}
	return entering
}

// runPipeline sends every query as an Execute of its own and closes them all with a single Sync.
func runPipeline(t *testing.T, conn *pgconn.PgConn, queries []string) []*pgconn.Result {
	t.Helper()

	p := conn.StartPipeline(context.Background())
	for _, query := range queries {
		p.SendQueryParams(query, nil, nil, nil, nil)
	}
	p.SendPipelineSync()
	require.NoError(t, p.Flush())

	var results []*pgconn.Result
	for {
		got, err := p.GetResults()
		require.NoError(t, err)
		if _, synced := got.(*pgconn.PipelineSync); synced {
			break
		}
		reader, ok := got.(*pgconn.ResultReader)
		require.Truef(t, ok, "unexpected pipeline result %T", got)
		results = append(results, reader.Read())
	}
	require.NoError(t, p.Close())
	return results
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
