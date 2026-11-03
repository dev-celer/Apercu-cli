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

	"apercu-cli/helper"
	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	"CREATE INDEX v01_idx ON orders (status)",
	"ALTER TABLE orders ADD CONSTRAINT v01_fk FOREIGN KEY (user_id) REFERENCES users (id) NOT VALID",
	"CREATE TABLE v01_fk (id bigint REFERENCES users (id))",
	"DROP INDEX orders_status_idx",
	"ALTER TABLE orders ADD COLUMN v02 int",
	"ALTER TABLE events ADD COLUMN v02 int",
}

type timeoutScopeScript struct {
	name     string
	messages []string
}

const v02AddColumn = "ALTER TABLE " + oracleSchema + ".orders ADD COLUMN v02 int"

var timeoutScopeScripts = []timeoutScopeScript{
	{name: "no timeout at all", messages: []string{v02AddColumn}},
	{name: "a session timeout", messages: []string{"SET lock_timeout = '200ms'", v02AddColumn}},
	{name: "a timeout of zero is disabled", messages: []string{"SET lock_timeout = 0", v02AddColumn}},
	{name: "SET LOCAL in a message of its own applies nothing", messages: []string{"SET LOCAL lock_timeout = '200ms'", v02AddColumn}},
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
	{name: "SET LOCAL bounds the rest of its own message", messages: []string{"SET LOCAL lock_timeout = '200ms'; " + v02AddColumn}},
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
				for _, script := range coverScripts {
					t.Run("covered/"+script.name, func(t *testing.T) { coveredWaits(t, db, catalog, script) })
				}
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
	if finding := filterV01(analysis); finding != nil {
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
				"V-01 names %s=%v, a queued %s blocked readers=%v writers=%v", table, named[locked.Relation], locked.Lock.Short(), readerBlocked, writerBlocked)
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
	ctx := context.Background()

	events := make([]pg_contract.QueryEvent, 0, len(script.messages))
	for _, message := range script.messages {
		events = append(events, event(message))
	}
	analysis := Analyze(catalog, apart(events...))
	last := analysis.Statements[len(analysis.Statements)-1]
	flagged := filterV01(last) != nil

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
	assert.Equalf(t, !bounded, flagged, "the server bounded the wait of %q: %v, V-01 fired: %v", last.RawSQL, bounded, flagged)
}

// coverScript is a statement taken after others in one transaction, with the sessions around it set up so that it
// waits if what the transaction holds does not cover its lock.
type coverScript struct {
	name string
	// held runs first in the block.
	held []string
	// queued is a request another session leaves waiting behind what the block holds.
	queued string
	// shared is a lock another session holds alongside the block.
	shared string
	// then is the statement checked, on orders.
	then string
}

var coverScripts = []coverScript{
	{
		name:   "a covered lock never waits, even behind a queued request",
		held:   []string{"LOCK TABLE orders IN ACCESS EXCLUSIVE MODE"},
		queued: "LOCK TABLE orders IN SHARE UPDATE EXCLUSIVE MODE",
		then:   v02AddColumn,
	},
	{
		name:   "locks held together cover one neither covers alone",
		held:   []string{"LOCK TABLE orders IN SHARE MODE", "UPDATE orders SET status = status WHERE false"},
		queued: "LOCK TABLE orders IN ROW EXCLUSIVE MODE",
		then:   "CREATE TRIGGER v01_cover BEFORE UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION bump()",
	},
	{
		name:   "SHARE does not cover a write, which waits for another SHARE",
		held:   []string{"LOCK TABLE orders IN SHARE MODE"},
		shared: "LOCK TABLE orders IN SHARE MODE",
		then:   "UPDATE orders SET status = status WHERE false",
	},
	{
		name:   "SHARE does not cover SHARE UPDATE EXCLUSIVE, which waits for another SHARE",
		held:   []string{"LOCK TABLE orders IN SHARE MODE"},
		shared: "LOCK TABLE orders IN SHARE MODE",
		then:   "ANALYZE orders",
	},
}

// coveredWaits runs a coverScript and checks that the statement waits exactly where its envelope is not covered, and
// that V-01 is silent on the covered ones.
func coveredWaits(t *testing.T, db *sql.DB, catalog *pg_catalog.Catalog, script coverScript) {
	t.Helper()
	ctx := context.Background()

	events := []pg_contract.QueryEvent{event("BEGIN")}
	for _, sql := range script.held {
		events = append(events, event(sql))
	}
	analysis := Analyze(catalog, apart(append(events, event(script.then))...))
	last := len(analysis.Statements) - 1
	var envelope *pg_contract.LockEnvelope
	for _, opened := range analysis.EnvelopesOn(helper.FullRelationName{Schema: oracleSchema, Table: "orders"}) {
		if opened.OpenedBy == last {
			envelope = &opened
		}
	}
	require.NotNilf(t, envelope, "%q opened no envelope on orders, %v", script.then, analysis.Envelopes)
	if envelope.Covered {
		assert.Nil(t, filterV01(analysis.Statements[last]), "a covered lock never queues")
	}

	conn := oracleConn(t, db)
	defer func() { _, _ = conn.ExecContext(ctx, "ROLLBACK") }()
	for _, sql := range append([]string{"BEGIN"}, script.held...) {
		_, err := conn.ExecContext(ctx, sql)
		require.NoErrorf(t, err, "%s", sql)
	}

	if script.shared != "" {
		other := oracleConn(t, db)
		for _, sql := range []string{"BEGIN", script.shared} {
			_, err := other.ExecContext(ctx, sql)
			require.NoErrorf(t, err, "%s", sql)
		}
		defer func() { _, _ = other.ExecContext(ctx, "ROLLBACK") }()
	}
	if script.queued != "" {
		holder := oracleConn(t, db)
		_, err := holder.ExecContext(ctx, "BEGIN")
		require.NoError(t, err)
		defer func() { _, _ = holder.ExecContext(ctx, "ROLLBACK") }()
		waiting := start(t, holder, script.queued)
		defer waiting.stop(t, db)
		require.Truef(t, waiting.queued(t, db), "%q queues behind the transaction's locks", script.queued)
	}

	statement := start(t, conn, script.then)
	defer statement.stop(t, db)
	waited := statement.queued(t, db)
	if !waited {
		require.NoErrorf(t, statement.err, "%s", script.then)
	}
	assert.Equalf(t, !envelope.Covered, waited, "%q waited=%v, its envelope says covered=%v", script.then, waited, envelope.Covered)
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

// nullableKeyDDL is V-03's fixture: parents whose partition key can hold NULL, and tables waiting to be attached
// with a CHECK that proves the bound, one that omits IS NOT NULL, and the shapes in between.
const nullableKeyDDL = `
CREATE SCHEMA IF NOT EXISTS ` + oracleSchema + `;
SET search_path TO ` + oracleSchema + `;

CREATE TABLE v03_range (id bigint NOT NULL, at int) PARTITION BY RANGE (at);
CREATE TABLE v03_list (id bigint NOT NULL, at int) PARTITION BY LIST (at);

CREATE TABLE v03_bound (id bigint NOT NULL, at int, CONSTRAINT v03_bound_b CHECK (at >= 100 AND at < 200));
CREATE TABLE v03_proved (id bigint NOT NULL, at int, CONSTRAINT v03_proved_b CHECK (at IS NOT NULL AND at >= 100 AND at < 200));
CREATE TABLE v03_split (id bigint NOT NULL, at int, CONSTRAINT v03_split_b CHECK (at >= 100 AND at < 200),
    CONSTRAINT v03_split_nn CHECK (at IS NOT NULL));
CREATE TABLE v03_notnull (id bigint NOT NULL, at int NOT NULL, CONSTRAINT v03_notnull_b CHECK (at >= 100 AND at < 200));
CREATE TABLE v03_plain (id bigint NOT NULL, at int);
CREATE TABLE v03_inlist (id bigint NOT NULL, at int, CONSTRAINT v03_inlist_b CHECK (at IN (1, 2)));
CREATE TABLE v03_inlist_null (id bigint NOT NULL, at int, CONSTRAINT v03_inlist_null_b CHECK (at IS NULL OR at IN (1, 2)));

INSERT INTO v03_bound VALUES (1, 150);
INSERT INTO v03_proved VALUES (1, 150);
INSERT INTO v03_split VALUES (1, 150);
INSERT INTO v03_notnull VALUES (1, 150);
INSERT INTO v03_plain VALUES (1, 150);
INSERT INTO v03_inlist VALUES (1, 1);
INSERT INTO v03_inlist_null VALUES (1, NULL);
ANALYZE;
`

type nullableKeyScript struct {
	sql       string
	partition string
	// scanned is what the server does: ATTACH reads the partition unless its CHECKs prove the bound.
	scanned bool
	// fires is V-03's verdict. It may only fire where the server scanned.
	fires bool
}

var nullableKeyScripts = []nullableKeyScript{
	{sql: "ALTER TABLE v03_range ATTACH PARTITION v03_bound FOR VALUES FROM (100) TO (200)", partition: "v03_bound", scanned: true, fires: true},
	{sql: "ALTER TABLE v03_range ATTACH PARTITION v03_proved FOR VALUES FROM (100) TO (200)", partition: "v03_proved"},
	{sql: "ALTER TABLE v03_range ATTACH PARTITION v03_split FOR VALUES FROM (100) TO (200)", partition: "v03_split"},
	{sql: "ALTER TABLE v03_range ATTACH PARTITION v03_notnull FOR VALUES FROM (100) TO (200)", partition: "v03_notnull"},
	// Nothing tried to prove the bound: the scan is the plain cost of ATTACH, not a CHECK that fell short.
	{sql: "ALTER TABLE v03_range ATTACH PARTITION v03_plain FOR VALUES FROM (100) TO (200)", partition: "v03_plain", scanned: true},
	{sql: "ALTER TABLE v03_list ATTACH PARTITION v03_inlist FOR VALUES IN (1, 2)", partition: "v03_inlist", scanned: true, fires: true},
	{sql: "ALTER TABLE v03_list ATTACH PARTITION v03_inlist_null FOR VALUES IN (NULL, 1, 2)", partition: "v03_inlist_null"},
}

// TestUnprovenNullableKeyOracle measures V-03's premise on a live 15/16/17/18: a CHECK bounding a nullable key
// without IS NOT NULL leaves the ATTACH scan in place, and saying it, in that CHECK or another, removes the scan.
func TestUnprovenNullableKeyOracle(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	for _, version := range integrationVersions {
		wg.Add(1)
		go func(version pg_contract.Version) {
			defer wg.Done()
			t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
				db := startPostgres(t, version)
				_, err := db.Exec(nullableKeyDDL)
				require.NoError(t, err)
				catalogs := oracleCatalogs(t, db)

				for _, script := range nullableKeyScripts {
					t.Run(script.sql, func(t *testing.T) {
						observed, err := runObserved(t, db, script.sql, false)
						require.NoError(t, err)
						partition := helper.FullRelationName{Schema: oracleSchema, Table: script.partition}
						assert.Equal(t, script.scanned, observed.work[partition], "the server's scan of %s", partition)

						statements := pg_parse.Parse("SET search_path TO " + oracleSchema + "; " + script.sql)
						analyses := NewClassifier(catalogs()).Analyze(statements)
						fired := filterV03(analyses[len(analyses)-1]) != nil
						assert.Equal(t, script.fires, fired, "V-03")
						if fired {
							assert.True(t, observed.work[partition], "V-03 fired on an ATTACH the server did not scan")
						}
					})
				}
			})
		}(version)
	}
	wg.Wait()
}

// boundProofDDL is the fixture for R-AT-ATTACH's CHECK branch: parents of each key type the proof models, and tables
// whose CHECKs prove their bound, fall short of it, or say it in a shape the proof has to read.
const boundProofDDL = `
CREATE SCHEMA IF NOT EXISTS ` + oracleSchema + `;
SET search_path TO ` + oracleSchema + `;

CREATE TABLE bp_int (id bigint NOT NULL, k int) PARTITION BY RANGE (k);
CREATE TABLE bp_ts (id bigint NOT NULL, at timestamptz) PARTITION BY RANGE (at);
CREATE TABLE bp_day (id bigint NOT NULL, d date) PARTITION BY RANGE (d);
CREATE TABLE bp_text (id bigint NOT NULL, v text) PARTITION BY LIST (v);
CREATE TABLE bp_sub (id bigint NOT NULL, k int) PARTITION BY RANGE (k);
CREATE TABLE bp_sub_1 PARTITION OF bp_sub FOR VALUES FROM (0) TO (1000) PARTITION BY RANGE (k);

CREATE TABLE bp_exact (id bigint NOT NULL, k int, CHECK (k IS NOT NULL AND k >= 100 AND k < 200));
CREATE TABLE bp_tighter (id bigint NOT NULL, k int, CHECK (k IS NOT NULL AND k > 120 AND k <= 150));
CREATE TABLE bp_looser (id bigint NOT NULL, k int, CHECK (k IS NOT NULL AND k >= 50 AND k < 200));
CREATE TABLE bp_between (id bigint NOT NULL, k int NOT NULL, CHECK (k BETWEEN 100 AND 199));
CREATE TABLE bp_flipped (id bigint NOT NULL, k int, CHECK (100 <= k AND 200 > k AND NOT (k IS NULL)));
CREATE TABLE bp_split (id bigint NOT NULL, k int NOT NULL, CHECK (k >= 100), CHECK (k < 200));
CREATE TABLE bp_lower (id bigint NOT NULL, k int NOT NULL, CHECK (k < 200));
CREATE TABLE bp_fraction (id bigint NOT NULL, k int NOT NULL, CHECK (k >= 99.5 AND k < 200));
CREATE TABLE bp_or (id bigint NOT NULL, k int NOT NULL, CHECK (k >= 100 AND k < 150 OR k >= 150 AND k < 200));
CREATE TABLE bp_in (id bigint NOT NULL, k int NOT NULL, CHECK (k IN (100, 150, 199)));
CREATE TABLE bp_notvalid (id bigint NOT NULL, k int NOT NULL);
ALTER TABLE bp_notvalid ADD CONSTRAINT bp_notvalid_b CHECK (k >= 100 AND k < 200) NOT VALID;
CREATE TABLE bp_ts_day (id bigint NOT NULL, at timestamptz NOT NULL, CHECK (at >= '2025-01-01' AND at < '2026-01-01'));
CREATE TABLE bp_ts_utc (id bigint NOT NULL, at timestamptz NOT NULL, CHECK (at >= '2025-01-01 00:00:00+00' AND at < '2026-01-01 00:00:00+00'));
CREATE TABLE bp_day_ok (id bigint NOT NULL, d date NOT NULL, CHECK (d >= '2025-03-01' AND d < '2025-04-01'));
CREATE TABLE bp_text_ok (id bigint NOT NULL, v text NOT NULL, CHECK (v IN ('a', 'b')));
CREATE TABLE bp_text_one (id bigint NOT NULL, v text NOT NULL, CHECK (v = 'a'));
CREATE TABLE bp_text_null (id bigint NOT NULL, v text, CHECK (v IS NULL OR v = 'a'));
CREATE TABLE bp_sub_new (id bigint NOT NULL, k int NOT NULL, CHECK (k >= 100 AND k < 200));
-- Proves its own bound but not bp_sub_1's, which the implied constraint carries too.
CREATE TABLE bp_sub_wide (id bigint NOT NULL, k int NOT NULL, CHECK (k >= 900 AND k < 1100));
-- Bounds another column than the key.
CREATE TABLE bp_other (id bigint NOT NULL, k int NOT NULL, CHECK (id >= 100 AND id < 200));

INSERT INTO bp_exact VALUES (1, 150);
INSERT INTO bp_tighter VALUES (1, 130);
INSERT INTO bp_looser VALUES (1, 150);
INSERT INTO bp_between VALUES (1, 150);
INSERT INTO bp_flipped VALUES (1, 150);
INSERT INTO bp_split VALUES (1, 150);
INSERT INTO bp_lower VALUES (1, 150);
INSERT INTO bp_fraction VALUES (1, 150);
INSERT INTO bp_or VALUES (1, 150);
INSERT INTO bp_in VALUES (1, 150);
INSERT INTO bp_notvalid VALUES (1, 150);
INSERT INTO bp_ts_day VALUES (1, '2025-06-01');
INSERT INTO bp_ts_utc VALUES (1, '2025-06-01');
INSERT INTO bp_day_ok VALUES (1, '2025-03-15');
INSERT INTO bp_text_ok VALUES (1, 'a');
INSERT INTO bp_text_one VALUES (1, 'a');
INSERT INTO bp_text_null VALUES (1, NULL);
INSERT INTO bp_sub_new VALUES (1, 150);
INSERT INTO bp_sub_wide VALUES (1, 950);
INSERT INTO bp_other VALUES (150, 150);
ANALYZE;
`

type boundProofScript struct {
	sql       string
	partition string
	// skipped is what the server does: the partition's constraints imply the bound, so it reads nothing.
	skipped bool
}

var boundProofScripts = []boundProofScript{
	{sql: "ALTER TABLE bp_int ATTACH PARTITION bp_exact FOR VALUES FROM (100) TO (200)", partition: "bp_exact", skipped: true},
	{sql: "ALTER TABLE bp_int ATTACH PARTITION bp_tighter FOR VALUES FROM (100) TO (200)", partition: "bp_tighter", skipped: true},
	{sql: "ALTER TABLE bp_int ATTACH PARTITION bp_looser FOR VALUES FROM (100) TO (200)", partition: "bp_looser"},
	{sql: "ALTER TABLE bp_int ATTACH PARTITION bp_looser FOR VALUES FROM (MINVALUE) TO (200)", partition: "bp_looser", skipped: true},
	{sql: "ALTER TABLE bp_int ATTACH PARTITION bp_between FOR VALUES FROM (100) TO (200)", partition: "bp_between", skipped: true},
	{sql: "ALTER TABLE bp_int ATTACH PARTITION bp_flipped FOR VALUES FROM (100) TO (200)", partition: "bp_flipped", skipped: true},
	{sql: "ALTER TABLE bp_int ATTACH PARTITION bp_split FOR VALUES FROM (100) TO (200)", partition: "bp_split", skipped: true},
	{sql: "ALTER TABLE bp_int ATTACH PARTITION bp_lower FOR VALUES FROM (100) TO (200)", partition: "bp_lower"},
	{sql: "ALTER TABLE bp_int ATTACH PARTITION bp_lower FOR VALUES FROM (MINVALUE) TO (200)", partition: "bp_lower", skipped: true},
	{sql: "ALTER TABLE bp_int ATTACH PARTITION bp_fraction FOR VALUES FROM (100) TO (200)", partition: "bp_fraction"},
	{sql: "ALTER TABLE bp_int ATTACH PARTITION bp_or FOR VALUES FROM (100) TO (200)", partition: "bp_or", skipped: true},
	{sql: "ALTER TABLE bp_int ATTACH PARTITION bp_in FOR VALUES FROM (100) TO (200)", partition: "bp_in", skipped: true},
	{sql: "ALTER TABLE bp_int ATTACH PARTITION bp_notvalid FOR VALUES FROM (100) TO (200)", partition: "bp_notvalid"},
	{sql: "ALTER TABLE bp_ts ATTACH PARTITION bp_ts_day FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')", partition: "bp_ts_day", skipped: true},
	{sql: "ALTER TABLE bp_ts ATTACH PARTITION bp_ts_utc FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2026-01-01 00:00:00+00')", partition: "bp_ts_utc", skipped: true},
	{sql: "ALTER TABLE bp_day ATTACH PARTITION bp_day_ok FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')", partition: "bp_day_ok", skipped: true},
	{sql: "ALTER TABLE bp_day ATTACH PARTITION bp_day_ok FOR VALUES FROM ('2025-03-02') TO ('2026-01-01')", partition: "bp_day_ok"},
	{sql: "ALTER TABLE bp_text ATTACH PARTITION bp_text_ok FOR VALUES IN ('a', 'b', 'c')", partition: "bp_text_ok", skipped: true},
	{sql: "ALTER TABLE bp_text ATTACH PARTITION bp_text_ok FOR VALUES IN ('a')", partition: "bp_text_ok"},
	{sql: "ALTER TABLE bp_text ATTACH PARTITION bp_text_one FOR VALUES IN ('a', 'b')", partition: "bp_text_one", skipped: true},
	{sql: "ALTER TABLE bp_text ATTACH PARTITION bp_text_null FOR VALUES IN (NULL, 'a')", partition: "bp_text_null", skipped: true},
	{sql: "ALTER TABLE bp_sub_1 ATTACH PARTITION bp_sub_new FOR VALUES FROM (100) TO (200)", partition: "bp_sub_new", skipped: true},
	{sql: "ALTER TABLE bp_sub_1 ATTACH PARTITION bp_sub_wide FOR VALUES FROM (900) TO (1100)", partition: "bp_sub_wide"},
	{sql: "ALTER TABLE bp_int ATTACH PARTITION bp_other FOR VALUES FROM (100) TO (200)", partition: "bp_other"},
}

// TestAttachBoundProofOracle measures R-AT-ATTACH's CHECK branch on a live 15/16/17/18: the classifier reports METADATA on
// the partition exactly where the server reads nothing of it. Each script runs in a transaction that is rolled back.
func TestAttachBoundProofOracle(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	for _, version := range integrationVersions {
		wg.Add(1)
		go func(version pg_contract.Version) {
			defer wg.Done()
			t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
				db := startPostgres(t, version)
				_, err := db.Exec(boundProofDDL)
				require.NoError(t, err)
				catalogs := oracleCatalogs(t, db)

				for _, script := range boundProofScripts {
					t.Run(script.sql, func(t *testing.T) {
						observed, err := runObserved(t, db, script.sql, false)
						require.NoError(t, err)
						partition := helper.FullRelationName{Schema: oracleSchema, Table: script.partition}
						assert.Equal(t, !script.skipped, observed.work[partition], "the server's scan of %s", partition)

						statements := pg_parse.Parse("SET search_path TO " + oracleSchema + "; " + script.sql)
						analyses := NewClassifier(catalogs()).Analyze(statements)
						var op pg_contract.OpKind
						for _, finding := range analyses[len(analyses)-1].Findings {
							for _, target := range finding.Targets {
								if finding.Code == "R-AT-ATTACH" && target.Relation.Name == partition {
									op = target.OpKind
								}
							}
						}
						assert.Equal(t, observed.work[partition], op == pg_contract.OpKindScan,
							"the classifier says %s, the server scanned: %v", op, observed.work[partition])
					})
				}
			})
		}(version)
	}
	wg.Wait()
}
