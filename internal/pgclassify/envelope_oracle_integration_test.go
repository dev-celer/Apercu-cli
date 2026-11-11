//go:build integration

package pgclassify_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify"
	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse"
	"apercu-cli/internal/pgparse/ir"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	envelopeOrders = "ALTER TABLE " + classifytest.OracleSchema + ".orders ADD COLUMN IF NOT EXISTS envelope int"
	envelopeUsers  = "ALTER TABLE " + classifytest.OracleSchema + ".users ADD COLUMN IF NOT EXISTS envelope int"
	envelopeTouch  = "UPDATE " + classifytest.OracleSchema + ".orders SET status = status WHERE false"
	envelopeShare  = "LOCK TABLE " + classifytest.OracleSchema + ".orders IN SHARE MODE"
	envelopeSRE    = "LOCK TABLE " + classifytest.OracleSchema + ".orders IN SHARE ROW EXCLUSIVE MODE"
)

// ownLocksQuery reads the relation locks the connection running it holds. Sent inside a message, it reports what the
// message's transaction holds at that point.
const ownLocksQuery = `
SELECT c.relname, c.relkind, l.mode
  FROM pg_locks l
  JOIN pg_class c ON c.oid = l.relation
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE l.pid = pg_backend_pid() AND l.locktype = 'relation' AND l.granted
   AND n.nspname = '` + classifytest.OracleSchema + `'`

var envelopeScripts = [][]classifytest.CycleSpec{
	// A message holds every lock its statements take until it ends.
	{classifytest.Simple(envelopeTouch + "; " + envelopeOrders + "; " + envelopeUsers + "; SELECT 1"), classifytest.Simple("SELECT 1")},
	{classifytest.Pipe(envelopeTouch + "; " + envelopeOrders + "; " + envelopeUsers + "; SELECT 1"), classifytest.Simple("SELECT 1")},
	// A COMMIT or a ROLLBACK part-way through releases them, the statements after it start from nothing.
	{classifytest.Simple(envelopeOrders + "; COMMIT; " + envelopeUsers + "; SELECT 1")},
	{classifytest.Pipe(envelopeOrders + "; COMMIT; " + envelopeUsers + "; SELECT 1")},
	{classifytest.Simple(envelopeOrders + "; ROLLBACK; " + envelopeUsers + "; SELECT 1")},
	{classifytest.Pipe(envelopeOrders + "; ROLLBACK; " + envelopeUsers + "; SELECT 1")},
	// So the same lock taken again after it is not covered by the one released.
	{classifytest.Simple(envelopeOrders + "; COMMIT; " + envelopeOrders + "; SELECT 1")},
	{classifytest.Pipe(envelopeOrders + "; COMMIT; " + envelopeOrders + "; SELECT 1")},
	// A BEGIN part-way through keeps what the message already holds, until the block ends.
	{classifytest.Simple(envelopeOrders + "; BEGIN; " + envelopeUsers), classifytest.Simple("SELECT 1"), classifytest.Simple("COMMIT"), classifytest.Simple("SELECT 1")},
	{classifytest.Pipe(envelopeOrders + "; BEGIN; " + envelopeUsers), classifytest.Pipe("SELECT 1; COMMIT"), classifytest.Simple("SELECT 1")},
	// An explicit block holds its locks across messages, and its COMMIT releases them part-way through one.
	{classifytest.Simple("BEGIN"), classifytest.Simple(envelopeOrders), classifytest.Simple("SELECT 1"), classifytest.Simple("COMMIT; " + envelopeUsers + "; SELECT 1")},
	{classifytest.Simple("BEGIN"), classifytest.Simple(envelopeOrders), classifytest.Pipe("SELECT 1; COMMIT; " + envelopeUsers + "; SELECT 1")},
	// A statement alone in its message releases its locks when it ends.
	{classifytest.Simple(envelopeOrders), classifytest.Simple(envelopeUsers), classifytest.Simple("SELECT 1")},
	// The server holds every mode a transaction takes, not the strongest one: SHARE then a write holds both. There is
	// no pipeline here, LOCK TABLE refuses one: it runs as an implicit transaction, not as a block.
	{classifytest.Simple(envelopeShare + "; " + envelopeTouch + "; " + envelopeSRE + "; SELECT 1"), classifytest.Simple("SELECT 1")},
	{classifytest.Simple("BEGIN"), classifytest.Simple(envelopeShare), classifytest.Simple(envelopeTouch), classifytest.Simple(envelopeSRE), classifytest.Simple("COMMIT")},
}

// TestLockEnvelopesMatchTheServer is the envelope oracle. It replays every script in envelopeScripts on 15, 16, 17 and
// 18 and checks, entering every statement, that the envelopes whose window reaches it are exactly the locks the server holds.
func TestLockEnvelopesMatchTheServer(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	for _, version := range classifytest.Versions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
				url := classifytest.StartServer(t, version)
				db, err := sql.Open("postgres", url)
				require.NoError(t, err)
				t.Cleanup(func() { _ = db.Close() })
				_, err = db.Exec(classifytest.OracleDDL)
				require.NoError(t, err)
				catalog := classifytest.OracleCatalog(t, db)

				for _, script := range envelopeScripts {
					t.Run(classifytest.CycleScriptName(script), func(t *testing.T) { replayEnvelopes(t, url, catalog, script) })
				}
			})
		}()
	}
	wg.Wait()
}

// replayEnvelopes runs one script on a connection of its own, sending each cycle the way the proxy would publish it,
// and compares what the server holds entering each statement with the envelopes Analyze opened over it.
func replayEnvelopes(t *testing.T, url string, catalog *pgcatalog.Catalog, script []classifytest.CycleSpec) {
	t.Helper()
	ctx := context.Background()

	var events []pgcontract.QueryEvent
	cycles := make([][]ir.Statement, 0, len(script))
	for cycle, spec := range script {
		statements := pgparse.Parse(spec.SQL)
		require.NotEmpty(t, statements)
		cycles = append(cycles, statements)
		switch spec.Protocol {
		case classifytest.ProtocolQuery:
			events = append(events, pgcontract.QueryEvent{SQL: spec.SQL, Cycle: cycle})
		case classifytest.ProtocolPipeline:
			for _, statement := range statements {
				events = append(events, pgcontract.QueryEvent{SQL: statement.RawSQL, Cycle: cycle})
			}
		}
	}
	analysis := pgclassify.Analyze(catalog, events)

	conn, err := pgconn.Connect(ctx, url)
	require.NoError(t, err)
	defer func() { _ = conn.Close(ctx) }()

	position := 0
	for cycle, statements := range cycles {
		first, err := conn.Exec(ctx, ownLocksQuery).ReadAll()
		require.NoError(t, err)
		require.Len(t, first, 1)

		entering := []map[helper.FullRelationName]pgcontract.LockSet{ownLocks(t, first[0])}
		for _, result := range classifytest.SendCycle(t, conn, script[cycle], statements, ownLocksQuery) {
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
func ownLocks(t *testing.T, result *pgconn.Result) map[helper.FullRelationName]pgcontract.LockSet {
	t.Helper()

	require.NoError(t, result.Err)
	held := map[helper.FullRelationName]pgcontract.LockSet{}
	for _, row := range result.Rows {
		kind := pgcontract.RelationKindFromRelkind(string(row[1]))
		if !kind.IsTable() && kind != pgcontract.RelationKindMaterializedView {
			continue
		}
		lock, known := classifytest.LockModes[string(row[2])]
		require.Truef(t, known, "pg_locks reported an unknown mode %q", row[2])

		name := helper.FullRelationName{Schema: classifytest.OracleSchema, Table: string(row[0])}
		held[name] = held[name].With(lock)
	}
	return held
}

// heldEntering is what the envelopes say the transaction holds when the statement at this position starts: the lock
// of every envelope opened before it whose window still reaches it.
func heldEntering(envelopes []pgcontract.LockEnvelope, position int) map[helper.FullRelationName]pgcontract.LockSet {
	held := map[helper.FullRelationName]pgcontract.LockSet{}
	for _, envelope := range envelopes {
		if envelope.OpenedBy < position && position < envelope.OpenedBy+envelope.StatementsWindow {
			held[envelope.Relation.Name] = held[envelope.Relation.Name].With(envelope.Lock)
		}
	}
	return held
}
