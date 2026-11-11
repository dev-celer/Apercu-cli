//go:build integration

package classifytest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Versions are the servers every oracle runs against.
var Versions = []pgcontract.Version{15, 16, 17, 18}

// StartServer starts a server of the given version and answers with its connection string.
func StartServer(t *testing.T, version pgcontract.Version) string {
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

func StartPostgres(t *testing.T, version pgcontract.Version) *sql.DB {
	t.Helper()

	db, err := sql.Open("postgres", StartServer(t, version))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.Ping())
	return db
}

// Protocol is how a cycle reaches the server.
type Protocol string

const (
	ProtocolQuery    Protocol = "Q"
	ProtocolPipeline Protocol = "P"
)

// CycleSpec is one client message. The server runs its statements in one implicit transaction.
type CycleSpec struct {
	Protocol Protocol
	SQL      string
}

func Simple(sql string) CycleSpec { return CycleSpec{Protocol: ProtocolQuery, SQL: sql} }

func Pipe(sql string) CycleSpec { return CycleSpec{Protocol: ProtocolPipeline, SQL: sql} }

// CycleScriptName is a script as its subtest name: Q(...) for a Query, P(...) for a pipeline.
func CycleScriptName(script []CycleSpec) string {
	parts := make([]string, 0, len(script))
	for _, spec := range script {
		parts = append(parts, fmt.Sprintf("%s(%s)", spec.Protocol, spec.SQL))
	}
	return strings.Join(parts, " | ")
}

// SendCycle runs one cycle with the observe query in front of every statement but the first, and answers with what that
// query read entering each of those statements. The first one is observed before the cycle, in a message of its own:
// injecting a query into a single-statement cycle would turn it into an implicit transaction, the very thing under test.
func SendCycle(t *testing.T, conn *pgconn.PgConn, spec CycleSpec, statements []ir.Statement, observe string) []*pgconn.Result {
	t.Helper()

	var sent []string
	var observed []bool
	for i, statement := range statements {
		if i > 0 {
			sent = append(sent, observe)
			observed = append(observed, true)
		}
		sent = append(sent, statement.RawSQL)
		observed = append(observed, false)
	}

	var results []*pgconn.Result
	switch spec.Protocol {
	case ProtocolQuery:
		var err error
		results, err = conn.Exec(context.Background(), strings.Join(sent, "; ")).ReadAll()
		require.NoErrorf(t, err, "the server refused %q", spec.SQL)
	case ProtocolPipeline:
		results = runPipeline(t, conn, sent)
	}
	require.Len(t, results, len(sent))

	var entering []*pgconn.Result
	for i, result := range results {
		require.NoErrorf(t, result.Err, "the server refused %q", sent[i])
		if observed[i] {
			entering = append(entering, result)
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

// OracleSchema is the schema the lock oracle runs against.
const OracleSchema = "apercu_lock_oracle"

const OracleDDL = `
CREATE SCHEMA ` + OracleSchema + `;
SET search_path TO ` + OracleSchema + `;

CREATE TABLE users (id bigint PRIMARY KEY, email text NOT NULL, created_at timestamptz);
CREATE TABLE orders (
    id      bigint PRIMARY KEY,
    user_id bigint REFERENCES users(id),
    total   numeric(10,2),
    status  text,
    code    text
);
CREATE INDEX orders_status_idx ON orders (status);
CREATE UNIQUE INDEX orders_code_uidx ON orders (code);
ALTER TABLE orders ADD CONSTRAINT orders_status_nn CHECK (status IS NOT NULL);
ALTER TABLE orders ADD CONSTRAINT orders_total_nv CHECK (total >= 0) NOT VALID;

CREATE FUNCTION next_code() RETURNS text LANGUAGE sql VOLATILE AS $$ SELECT 'c' $$;
CREATE FUNCTION bump() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER orders_bump BEFORE UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION bump();

CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);
CREATE DOMAIN loose_text AS text;

-- events has no default partition, so a concurrent detach is allowed on it.
CREATE TABLE events (id bigint NOT NULL, at timestamptz NOT NULL) PARTITION BY RANGE (at);
CREATE TABLE events_2025 PARTITION OF events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE events_2026 (id bigint NOT NULL, at timestamptz NOT NULL, CONSTRAINT events_id_check CHECK (id > 0));
ALTER TABLE events ADD CONSTRAINT events_id_check CHECK (id > 0);

-- parted has one, which is what R-AT-ATTACH rescans and R-AT-DETACH-CONC refuses to work with.
CREATE TABLE parted (id bigint NOT NULL) PARTITION BY RANGE (id);
CREATE TABLE parted_1 PARTITION OF parted FOR VALUES FROM (1) TO (100);
CREATE TABLE parted_default PARTITION OF parted DEFAULT;
CREATE TABLE parted_2 (id bigint NOT NULL);

CREATE TABLE legacy_parent (id bigint NOT NULL);
CREATE TABLE legacy_child () INHERITS (legacy_parent);
CREATE TABLE orphan (id bigint NOT NULL, CONSTRAINT legacy_id_check CHECK (id > 0));
ALTER TABLE legacy_parent ADD CONSTRAINT legacy_id_check CHECK (id > 0);

-- A generated column of each storage kind: R-AT-SETEXPR and R-AT-DROPEXPR branch on it, and the
-- virtual half of the branch exists only on 18 ⟨oracleDDL18⟩.
CREATE TABLE computed (
    id           bigint PRIMARY KEY,
    base         numeric(10,2),
    stored_total numeric(10,2) GENERATED ALWAYS AS (base * 2) STORED
);

CREATE VIEW active_users AS SELECT id, email FROM users;

-- A second role, so OWNER TO actually changes something: PostgreSQL returns early when the
-- new owner is the one already recorded, and then locks nothing at all.
CREATE ROLE apercu_lock_oracle_owner;

-- The matview reads user_id alone: a view over a column is enough to make ALTER COLUMN TYPE on it a hard error.
CREATE MATERIALIZED VIEW order_totals AS SELECT user_id, count(*) AS n FROM orders GROUP BY user_id;

CREATE UNIQUE INDEX order_totals_uidx ON order_totals (user_id);
CREATE SEQUENCE standalone_seq;

CREATE TABLE ticket (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, note text);
CREATE TABLE ticket_line (ticket_id bigint REFERENCES ticket(id));

-- One composite type of each usage: bound to a typed table, used in an ordinary column, unused.
CREATE TYPE address AS (street text, city text);
CREATE TABLE places OF address;
CREATE TYPE col_comp AS (a text, b text);
CREATE TABLE uses_comp (c col_comp);
CREATE TYPE plain_comp AS (a text, b text);
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE profiles (user_id bigint PRIMARY KEY, score positive_int, feeling mood);

CREATE TABLE stats_tbl (a int, b int);
CREATE STATISTICS stats_tbl_stats ON a, b FROM stats_tbl;

CREATE TABLE policy_tbl (id bigint);
CREATE POLICY policy_tbl_p ON policy_tbl USING (true);
CREATE RULE policy_tbl_r AS ON DELETE TO policy_tbl DO INSTEAD NOTHING;

CREATE TABLE spare (id bigint);
CREATE INDEX spare_id_idx ON spare (id);
CREATE INDEX spare_spare_idx ON spare (id, id);
CREATE TABLE spare_child () INHERITS (spare);

CREATE TABLE pub_tbl (id bigint);
CREATE PUBLICATION oracle_pub FOR TABLE pub_tbl;

-- ON ONLY leaves the parent index unattached, which is the only state R-IX-ATTACH can run against.
CREATE TABLE ixparted (id bigint NOT NULL) PARTITION BY RANGE (id);
CREATE TABLE ixparted_1 PARTITION OF ixparted FOR VALUES FROM (1) TO (100);
CREATE TABLE ixparted_2 PARTITION OF ixparted FOR VALUES FROM (100) TO (200);
CREATE INDEX ixparted_only_idx ON ONLY ixparted (id);
CREATE INDEX ixparted_2_id_idx ON ixparted_2 (id);

INSERT INTO users SELECT i, 'u' || i, now() FROM generate_series(1, 200) i;
INSERT INTO orders SELECT i, (i % 200) + 1, i, 'open', 'c' || i FROM generate_series(1, 200) i;
INSERT INTO events SELECT i, '2025-06-01'::timestamptz FROM generate_series(1, 100) i;
INSERT INTO parted SELECT i FROM generate_series(1, 99) i;
INSERT INTO computed (id, base) SELECT i, i FROM generate_series(1, 200) i;
INSERT INTO ticket (note) SELECT 'n' || i FROM generate_series(1, 50) i;
INSERT INTO profiles SELECT i, i, 'ok' FROM generate_series(1, 50) i;
INSERT INTO spare SELECT i FROM generate_series(1, 50) i;
INSERT INTO ixparted SELECT i FROM generate_series(1, 150) i;
REFRESH MATERIALIZED VIEW order_totals;
ANALYZE;
`

// LockModes maps the spelling pg_locks uses onto the enum §1.2 defines.
var LockModes = map[string]pgcontract.Lock{
	"AccessShareLock":          pgcontract.LockAccessShare,
	"RowShareLock":             pgcontract.LockRowShare,
	"RowExclusiveLock":         pgcontract.LockRowExclusive,
	"ShareUpdateExclusiveLock": pgcontract.LockShareUpdateExclusive,
	"ShareLock":                pgcontract.LockShare,
	"ShareRowExclusiveLock":    pgcontract.LockShareRowExclusive,
	"ExclusiveLock":            pgcontract.LockExclusive,
	"AccessExclusiveLock":      pgcontract.LockAccessExclusive,
}

// observedLocks reads what one backend is actually holding, from another session.
const observedLocksQuery = `
SELECT n.nspname, c.relname, c.relkind, l.mode
  FROM pg_locks l
  JOIN pg_class c ON c.oid = l.relation
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE l.pid = $1 AND l.locktype = 'relation' AND l.granted
   AND n.nspname = $2`

// LockedRelation is one row of that.
type LockedRelation struct {
	Name helper.FullRelationName
	Kind string
	Lock pgcontract.Lock
}

// OracleCatalog captures the seeded database the way the CLI would, as both the preview and the
// production snapshot, so P-18 answers and G-07 can branch.
func OracleCatalog(t *testing.T, db *sql.DB) *pgcatalog.Catalog {
	t.Helper()
	return OracleCatalogs(t, db)()
}

// OracleCatalogs captures the database once and hands out a fresh catalog on every call: the
// shadow catalog learns what each classified statement creates and attaches, and a script the
// server rolled back must not leave that behind for the next one.
func OracleCatalogs(t *testing.T, db *sql.DB) func() *pgcatalog.Catalog {
	t.Helper()
	ctx := context.Background()

	pre, err := snapshot.Collect(ctx, db, snapshot.CollectOptions{Source: snapshot.SourcePreview, PIT: snapshot.PITPre})
	require.NoError(t, err)
	prod, err := snapshot.Collect(ctx, db, snapshot.CollectOptions{Source: snapshot.SourceProd, PIT: snapshot.PITPre, ProdAvailable: true})
	require.NoError(t, err)

	return func() *pgcatalog.Catalog {
		catalog, err := pgcatalog.NewCatalog(pgcatalog.CatalogOptions{Pre: pre, Prod: prod})
		require.NoError(t, err)
		return catalog
	}
}

// Observation is what the server did while the statement's transaction was still open.
type Observation struct {
	Locks map[helper.FullRelationName]LockedRelation
	// Work is the relations with storage the statement read or rewrote.
	Work map[helper.FullRelationName]bool
}

// RunObserved runs one statement on its own connection and reads back its locks and its work
// before rolling it back.
func RunObserved(t *testing.T, db *sql.DB, statement string, outsideTransaction bool) (Observation, error) {
	t.Helper()
	return RunObservedSetup(t, db, "", statement, outsideTransaction)
}

// RunObservedSetup is RunObserved after setup statements that run in the same transaction but are not measured.
func RunObservedSetup(t *testing.T, db *sql.DB, setup, statement string, outsideTransaction bool) (Observation, error) {
	t.Helper()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.ExecContext(ctx, "SET search_path TO "+OracleSchema)
	require.NoError(t, err)

	pid := 0
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid))

	if outsideTransaction {
		_, execErr := conn.ExecContext(ctx, statement)
		return Observation{}, execErr
	}
	_, err = conn.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)
	defer func() { _, _ = conn.ExecContext(ctx, "ROLLBACK") }()

	if setup != "" {
		_, err = conn.ExecContext(ctx, setup)
		require.NoError(t, err, "setup: %s", setup)
	}
	before := readWork(t, conn)
	if _, execErr := conn.ExecContext(ctx, statement); execErr != nil {
		return Observation{}, execErr
	}
	after := readWork(t, conn)

	work := map[helper.FullRelationName]bool{}
	for name, now := range after {
		if was, existed := before[name]; existed && (now.scans > was.scans || now.filenode != was.filenode) {
			work[name] = true
		}
	}
	return Observation{Locks: readLocks(t, db, pid), Work: work}, nil
}

// storageState is one relation's scan count and file inside the running transaction.
type storageState struct {
	scans    int64
	filenode uint32
}

const workQuery = `
SELECT n.nspname, c.relname, pg_stat_get_xact_numscans(c.oid), c.relfilenode
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = $1 AND c.relkind IN ('r', 'm')`

// readWork reads, from inside the transaction, how often each relation has been scanned and which file holds it.
func readWork(t *testing.T, conn *sql.Conn) map[helper.FullRelationName]storageState {
	t.Helper()

	rows, err := conn.QueryContext(context.Background(), workQuery, OracleSchema)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	states := map[helper.FullRelationName]storageState{}
	for rows.Next() {
		var schema, name string
		var state storageState
		require.NoError(t, rows.Scan(&schema, &name, &state.scans, &state.filenode))
		states[helper.FullRelationName{Schema: schema, Table: name}] = state
	}
	require.NoError(t, rows.Err())
	return states
}

// readLocks is the second session: it reads what the first one is holding while it still holds it.
func readLocks(t *testing.T, db *sql.DB, pid int) map[helper.FullRelationName]LockedRelation {
	t.Helper()

	rows, err := db.QueryContext(context.Background(), observedLocksQuery, pid, OracleSchema)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	held := map[helper.FullRelationName]LockedRelation{}
	modes := map[helper.FullRelationName]pgcontract.LockSet{}
	for rows.Next() {
		var schema, name, kind, mode string
		require.NoError(t, rows.Scan(&schema, &name, &kind, &mode))
		lock, known := LockModes[mode]
		require.Truef(t, known, "pg_locks reported an unknown mode %q", mode)

		relation := helper.FullRelationName{Schema: schema, Table: name}
		modes[relation] = modes[relation].With(lock)
		if previous, seen := held[relation]; seen {
			lock = pgcontract.MaxLock(lock, previous.Lock)
		}
		held[relation] = LockedRelation{Name: relation, Kind: kind, Lock: lock}
	}
	require.NoError(t, rows.Err())

	// The classifier keeps one lock per relation and statement, the strongest. That loses nothing only while the
	// strongest mode the statement took blocks everything the others do.
	for relation, locked := range held {
		assert.Equalf(t, modes[relation].Conflicts(), pgcontract.LockSetOf(locked.Lock).Conflicts(),
			"%s is held at %v, which blocks more than its strongest mode %s", relation, modes[relation], locked.Lock.Short())
	}
	return held
}

func Accepted(err error) string {
	if err != nil {
		return "refused"
	}
	return "accepted"
}

func Expected(refused bool) string {
	if refused {
		return "refuses it"
	}
	return "runs"
}

// OracleConn give a new cleaned up connection, ready to be used for tests.
func OracleConn(t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = conn.ExecContext(ctx, "DISCARD ALL")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "SET search_path TO "+OracleSchema)
	require.NoError(t, err)
	return conn
}

func LockTimedOut(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "55P03"
}

func HasLockWaiting(t *testing.T, db *sql.DB, pid int) bool {
	t.Helper()

	waiting := false
	require.NoError(t, db.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND NOT granted)", pid).Scan(&waiting))
	return waiting
}

// Pending is a statement left running on its own connection.
type Pending struct {
	PID      int
	done     chan error
	finished bool
	Err      error
}

func Start(t *testing.T, conn *sql.Conn, sql string) *Pending {
	t.Helper()

	p := &Pending{done: make(chan error, 1)}
	require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT pg_backend_pid()").Scan(&p.PID))
	go func() {
		_, err := conn.ExecContext(context.Background(), sql)
		p.done <- err
	}()
	return p
}

// Finish waits up to within for the statement to end and reports whether it did.
func (p *Pending) Finish(within time.Duration) bool {
	if p.finished {
		return true
	}
	select {
	case p.Err = <-p.done:
		p.finished = true
	case <-time.After(within):
	}
	return p.finished
}

// Queued polls until the statement waits for a lock, false when it finished first.
func (p *Pending) Queued(t *testing.T, db *sql.DB) bool {
	t.Helper()

	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if p.Finish(10 * time.Millisecond) {
			return false
		}
		if HasLockWaiting(t, db, p.PID) {
			return true
		}
	}
	require.FailNow(t, "the statement neither queued for a lock nor finished")
	return false
}

// Stop cancels the statement if it is still running and waits for it to end, so its connection can be reused.
func (p *Pending) Stop(t *testing.T, db *sql.DB) {
	t.Helper()
	if p.finished {
		return
	}
	_, err := db.Exec("SELECT pg_cancel_backend($1)", p.PID)
	assert.NoError(t, err)
	p.Err = <-p.done
	p.finished = true
}
