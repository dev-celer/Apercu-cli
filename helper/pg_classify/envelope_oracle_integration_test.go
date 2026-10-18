//go:build integration

package pg_classify

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"apercu-cli/helper"
	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	envelopeOrders = "ALTER TABLE " + oracleSchema + ".orders ADD COLUMN IF NOT EXISTS envelope int"
	envelopeUsers  = "ALTER TABLE " + oracleSchema + ".users ADD COLUMN IF NOT EXISTS envelope int"
	envelopeTouch  = "UPDATE " + oracleSchema + ".orders SET status = status WHERE false"
	envelopeShare  = "LOCK TABLE " + oracleSchema + ".orders IN SHARE MODE"
	envelopeSRE    = "LOCK TABLE " + oracleSchema + ".orders IN SHARE ROW EXCLUSIVE MODE"
)

// ownLocksQuery reads the relation locks the connection running it holds. Sent inside a message, it reports what the
// message's transaction holds at that point.
const ownLocksQuery = `
SELECT c.relname, c.relkind, l.mode
  FROM pg_locks l
  JOIN pg_class c ON c.oid = l.relation
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE l.pid = pg_backend_pid() AND l.locktype = 'relation' AND l.granted
   AND n.nspname = '` + oracleSchema + `'`

var envelopeScripts = [][]cycleSpec{
	// A message holds every lock its statements take until it ends.
	{simple(envelopeTouch + "; " + envelopeOrders + "; " + envelopeUsers + "; SELECT 1"), simple("SELECT 1")},
	{pipe(envelopeTouch + "; " + envelopeOrders + "; " + envelopeUsers + "; SELECT 1"), simple("SELECT 1")},
	// A COMMIT or a ROLLBACK part-way through releases them, the statements after it start from nothing.
	{simple(envelopeOrders + "; COMMIT; " + envelopeUsers + "; SELECT 1")},
	{pipe(envelopeOrders + "; COMMIT; " + envelopeUsers + "; SELECT 1")},
	{simple(envelopeOrders + "; ROLLBACK; " + envelopeUsers + "; SELECT 1")},
	{pipe(envelopeOrders + "; ROLLBACK; " + envelopeUsers + "; SELECT 1")},
	// So the same lock taken again after it is not covered by the one released.
	{simple(envelopeOrders + "; COMMIT; " + envelopeOrders + "; SELECT 1")},
	{pipe(envelopeOrders + "; COMMIT; " + envelopeOrders + "; SELECT 1")},
	// A BEGIN part-way through keeps what the message already holds, until the block ends.
	{simple(envelopeOrders + "; BEGIN; " + envelopeUsers), simple("SELECT 1"), simple("COMMIT"), simple("SELECT 1")},
	{pipe(envelopeOrders + "; BEGIN; " + envelopeUsers), pipe("SELECT 1; COMMIT"), simple("SELECT 1")},
	// An explicit block holds its locks across messages, and its COMMIT releases them part-way through one.
	{simple("BEGIN"), simple(envelopeOrders), simple("SELECT 1"), simple("COMMIT; " + envelopeUsers + "; SELECT 1")},
	{simple("BEGIN"), simple(envelopeOrders), pipe("SELECT 1; COMMIT; " + envelopeUsers + "; SELECT 1")},
	// A statement alone in its message releases its locks when it ends.
	{simple(envelopeOrders), simple(envelopeUsers), simple("SELECT 1")},
	// The server holds every mode a transaction takes, not the strongest one: SHARE then a write holds both. There is
	// no pipeline here, LOCK TABLE refuses one: it runs as an implicit transaction, not as a block.
	{simple(envelopeShare + "; " + envelopeTouch + "; " + envelopeSRE + "; SELECT 1"), simple("SELECT 1")},
	{simple("BEGIN"), simple(envelopeShare), simple(envelopeTouch), simple(envelopeSRE), simple("COMMIT")},
}

// TestLockEnvelopesMatchTheServer is the envelope oracle. It replays every script in envelopeScripts on 15, 16, 17 and
// 18 and checks, entering every statement, that the envelopes whose window reaches it are exactly the locks the server holds.
func TestLockEnvelopesMatchTheServer(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	for _, version := range integrationVersions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
				url := startServer(t, version)
				db, err := sql.Open("postgres", url)
				require.NoError(t, err)
				t.Cleanup(func() { _ = db.Close() })
				_, err = db.Exec(oracleDDL)
				require.NoError(t, err)
				catalog := oracleCatalog(t, db)

				for _, script := range envelopeScripts {
					t.Run(cycleScriptName(script), func(t *testing.T) { replayEnvelopes(t, url, catalog, script) })
				}
			})
		}()
	}
	wg.Wait()
}

// replayEnvelopes runs one script on a connection of its own, sending each cycle the way the proxy would publish it,
// and compares what the server holds entering each statement with the envelopes Analyze opened over it.
func replayEnvelopes(t *testing.T, url string, catalog *pg_catalog.Catalog, script []cycleSpec) {
	t.Helper()
	ctx := context.Background()

	var events []pg_contract.QueryEvent
	cycles := make([][]pg_parse.Statement, 0, len(script))
	for cycle, spec := range script {
		statements := pg_parse.Parse(spec.sql)
		require.NotEmpty(t, statements)
		cycles = append(cycles, statements)
		switch spec.protocol {
		case protocolQuery:
			events = append(events, pg_contract.QueryEvent{SQL: spec.sql, Cycle: cycle})
		case protocolPipeline:
			for _, statement := range statements {
				events = append(events, pg_contract.QueryEvent{SQL: statement.RawSQL, Cycle: cycle})
			}
		}
	}
	analysis := Analyze(catalog, events)

	conn, err := pgconn.Connect(ctx, url)
	require.NoError(t, err)
	defer func() { _ = conn.Close(ctx) }()

	position := 0
	for cycle, statements := range cycles {
		first, err := conn.Exec(ctx, ownLocksQuery).ReadAll()
		require.NoError(t, err)
		require.Len(t, first, 1)

		entering := []map[helper.FullRelationName]pg_contract.LockSet{ownLocks(t, first[0])}
		for _, result := range sendCycle(t, conn, script[cycle], statements, ownLocksQuery) {
			entering = append(entering, ownLocks(t, result))
		}
		for i, statement := range statements {
			assert.Equalf(t, entering[i], heldEntering(analysis.Envelopes, position+i),
				"entering %q in cycle %d: what the server holds, against what the envelopes say", statement.RawSQL, cycle)
		}
		position += len(statements)
	}
	require.Equal(t, len(analysis.Statements), position)
}

// ownLocks reads one ownLocksQuery result: every mode held on each table of the oracle schema. Indexes are left out,
// they are locked with their table and no envelope names them.
func ownLocks(t *testing.T, result *pgconn.Result) map[helper.FullRelationName]pg_contract.LockSet {
	t.Helper()

	require.NoError(t, result.Err)
	held := map[helper.FullRelationName]pg_contract.LockSet{}
	for _, row := range result.Rows {
		kind := pg_contract.RelationKindFromRelkind(string(row[1]))
		if !kind.IsTable() && kind != pg_contract.RelationKindMaterializedView {
			continue
		}
		lock, known := lockModes[string(row[2])]
		require.Truef(t, known, "pg_locks reported an unknown mode %q", row[2])

		name := helper.FullRelationName{Schema: oracleSchema, Table: string(row[0])}
		held[name] = held[name].With(lock)
	}
	return held
}

// heldEntering is what the envelopes say the transaction holds when the statement at this position starts: the lock
// of every envelope opened before it whose window still reaches it.
func heldEntering(envelopes []pg_contract.LockEnvelope, position int) map[helper.FullRelationName]pg_contract.LockSet {
	held := map[helper.FullRelationName]pg_contract.LockSet{}
	for _, envelope := range envelopes {
		if envelope.OpenedBy < position && position < envelope.OpenedBy+envelope.StatementsWindow {
			held[envelope.Relation.Name] = held[envelope.Relation.Name].With(envelope.Lock)
		}
	}
	return held
}
