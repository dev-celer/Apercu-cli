//go:build integration

package pg_classify

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"

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

func startPostgres(t *testing.T, version pg_contract.Version) *sql.DB {
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

	db, err := sql.Open("postgres", url)
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

// baselineCatalog builds a catalog from the baseline settings of the selected connection.
func baselineCatalog(t *testing.T, conn *sql.Conn) *pg_catalog.Catalog {
	t.Helper()

	start := observe(t, conn)
	user := ""
	require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT current_user").Scan(&user))

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
	// SET LOCAL with no block around it. The server warns and applies nothing.
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

	catalog := baselineCatalog(t, conn)
	session := NewSession(catalog)

	for _, statement := range pg_parse.Parse(script) {
		got := session.Next(statement)
		want := observe(t, conn)

		where := fmt.Sprintf("entering %q", statement.RawSQL)
		assert.Equalf(t, pg_catalog.ParseSearchPath(want.searchPath, catalog.SnapshotUser()), got.SearchPath, "search_path %s", where)
		assert.Equalf(t, want.timeZone, got.TimeZone, "TimeZone %s", where)
		assert.Equalf(t, parseTimeout(want.lockTimeout).Duration, got.LockTimeout.Duration, "lock_timeout %s", where)
		assert.Equalf(t, parseTimeout(want.statementTimeout).Duration, got.StatementTimeout.Duration, "statement_timeout %s", where)
		assert.Equalf(t, parseReplicationRole(want.replicationRole), got.ReplicationRole, "session_replication_role %s", where)

		_, err := conn.ExecContext(ctx, statement.RawSQL)
		require.NoErrorf(t, err, "the server refused %q", statement.RawSQL)
	}
}
