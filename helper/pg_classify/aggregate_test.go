package pg_classify

import (
	"slices"
	"strings"
	"testing"

	"apercu-cli/helper"
	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// event is one proxy event: a simple Query, which may hold several statements, or one Execute.
func event(sql string) pg_contract.QueryEvent {
	return pg_contract.QueryEvent{SQL: sql}
}

// cycle stamps one protocol cycle on a run of events, which is what one client message becomes.
// Several events in one cycle is an extended-protocol pipeline: an Execute each, closed by one Sync,
// still one implicit transaction on the server.
func cycle(id int, events ...pg_contract.QueryEvent) []pg_contract.QueryEvent {
	for i := range events {
		events[i].Cycle = id
	}
	return events
}

// apart is a proxy event list the client sent as separate messages, one cycle each.
func apart(events ...pg_contract.QueryEvent) []pg_contract.QueryEvent {
	for i := range events {
		events[i].Cycle = i
	}
	return events
}

func relationName(name string) helper.FullRelationName {
	return helper.FullRelationName{Schema: "apercu_snapshot_test", Table: name}
}

// envelopeOn is the envelope that raised a relation to a lock, the covered ones aside.
func envelopeOn(t *testing.T, analysis pg_contract.MigrationAnalysis, table string, lock pg_contract.Lock) pg_contract.LockEnvelope {
	t.Helper()

	envelopes := analysis.EnvelopesOn(relationName(table))
	for _, envelope := range pg_contract.EffectiveEnvelopes(envelopes) {
		if envelope.Lock == lock {
			return envelope
		}
	}
	require.Failf(t, "missing envelope", "nothing raised %s on %s, only %v", lock.Short(), table, envelopes)
	return pg_contract.LockEnvelope{}
}

// coveredOn is the envelopes on a relation the transaction was already blocking at least as hard.
func coveredOn(analysis pg_contract.MigrationAnalysis, table string) []pg_contract.LockEnvelope {
	var out []pg_contract.LockEnvelope
	for _, envelope := range analysis.EnvelopesOn(relationName(table)) {
		if envelope.Covered {
			out = append(out, envelope)
		}
	}
	return out
}

func TestMultipleLockLevel(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("BEGIN"),
		event("UPDATE orders SET status = 'done'"),
		event("ALTER TABLE orders ADD COLUMN z int"),
		event("ANALYZE orders"),
		event("COMMIT"),
	))

	require.Len(t, analysis.Statements, 5)
	for _, statement := range analysis.Statements {
		assert.Equal(t, pg_contract.TxnGroup(1), statement.TxnGroup, "%q", statement.RawSQL)
	}

	assert.Len(t, analysis.EnvelopesOn(relationName("orders")), 3)

	covered := coveredOn(analysis, "orders")
	require.Len(t, covered, 1)
	assert.Equal(t, pg_contract.LockShareUpdateExclusive, covered[0].Lock)
	assert.Equal(t, 3, covered[0].OpenedBy)
	assert.Equal(t, 2, covered[0].StatementsWindow, "the ANALYZE and the COMMIT")

	write := envelopeOn(t, analysis, "orders", pg_contract.LockRowExclusive)
	assert.Equal(t, 1, write.OpenedBy)
	assert.Equal(t, 4, write.StatementsWindow)

	exclusive := envelopeOn(t, analysis, "orders", pg_contract.LockAccessExclusive)
	assert.Equal(t, 2, exclusive.OpenedBy)
	assert.Equal(t, 3, exclusive.StatementsWindow)
}

func TestCoveredLock(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("BEGIN"),
		event("ALTER TABLE orders ADD COLUMN z int"),
		event("UPDATE orders SET status = 'done'"),
		event("ANALYZE orders"),
		event("COMMIT"),
	))

	envelopes := analysis.EnvelopesOn(relationName("orders"))
	require.Len(t, envelopes, 3)

	effective := pg_contract.EffectiveEnvelopes(envelopes)
	require.Len(t, effective, 1, "only the ALTER raised anything, %v", envelopes)
	assert.Equal(t, pg_contract.LockAccessExclusive, effective[0].Lock)
	assert.Equal(t, 1, effective[0].OpenedBy)

	covered := coveredOn(analysis, "orders")
	require.Len(t, covered, 2)
	assert.Equal(t, pg_contract.LockRowExclusive, covered[0].Lock)
	assert.Equal(t, 3, covered[0].StatementsWindow)
	assert.Equal(t, pg_contract.LockShareUpdateExclusive, covered[1].Lock)
	assert.Equal(t, 2, covered[1].StatementsWindow)
}

func TestCoveredEqualLocks(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("BEGIN"),
		event("UPDATE orders SET status = 'done'"),
		event("DELETE FROM orders WHERE status = 'stale'"),
		event("COMMIT"),
	))

	envelopes := pg_contract.EffectiveEnvelopes(analysis.EnvelopesOn(relationName("orders")))
	require.Len(t, envelopes, 1)
	assert.Equal(t, pg_contract.LockRowExclusive, envelopes[0].Lock)
	assert.Equal(t, 1, envelopes[0].OpenedBy)
	assert.Equal(t, 3, envelopes[0].StatementsWindow)

	covered := coveredOn(analysis, "orders")
	require.Len(t, covered, 1)
	assert.Equal(t, pg_contract.LockRowExclusive, covered[0].Lock)
	assert.Equal(t, 2, covered[0].OpenedBy)
	assert.Equal(t, 2, covered[0].StatementsWindow)
}

func TestNoEnvelope(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("ALTER TABLE orders ADD COLUMN z int"),
		event("ALTER TABLE users ADD COLUMN z int"),
	))

	require.Len(t, analysis.Statements, 2)
	assert.Empty(t, analysis.Envelopes, "neither statement holds a lock past its own end")

	assert.Equal(t, pg_contract.LockAccessExclusive, analysis.Statements[0].LockOn(relationName("orders")))
}

func TestImplicitMultiStatementQuery(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("ALTER TABLE orders ADD COLUMN z int; ALTER TABLE users ADD COLUMN z int"),
	))

	require.Len(t, analysis.Statements, 2, "one event, split into its statements")

	orders := envelopeOn(t, analysis, "orders", pg_contract.LockAccessExclusive)
	assert.Equal(t, 2, orders.StatementsWindow, "held while the second ALTER runs")

	users := envelopeOn(t, analysis, "users", pg_contract.LockAccessExclusive)
	assert.Equal(t, 1, users.StatementsWindow)
}

func TestImplicitExtendedProtocol(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), slices.Concat(
		cycle(0, event("ALTER TABLE orders ADD COLUMN z int"), event("ANALYZE orders")),
		cycle(1, event("ALTER TABLE users ADD COLUMN z int")),
	))

	require.Len(t, analysis.Statements, 3)

	orders := envelopeOn(t, analysis, "orders", pg_contract.LockAccessExclusive)
	assert.Equal(t, 2, orders.StatementsWindow, "stops at the end of its own message")

	assert.Empty(t, analysis.EnvelopesOn(relationName("users")), "the last message is one statement on its own")
}

// TestImplicitTransactionEndsPartWay is a COMMIT or a ROLLBACK inside a message: it ends the implicit transaction there,
// releasing its locks, and the statements after it run in another one.
func TestImplicitTransactionEndsPartWay(t *testing.T) {
	t.Parallel()

	for _, end := range []string{"COMMIT", "ROLLBACK"} {
		statements := []string{"ALTER TABLE orders ADD COLUMN z int", end, "ALTER TABLE users ADD COLUMN z int", "ANALYZE users"}
		pipeline := make([]pg_contract.QueryEvent, 0, len(statements))
		for _, sql := range statements {
			pipeline = append(pipeline, event(sql))
		}

		cases := []struct {
			name   string
			events []pg_contract.QueryEvent
		}{
			{name: end + " in a multi-statement query", events: apart(event(strings.Join(statements, "; ")))},
			{name: end + " in a pipeline", events: cycle(0, pipeline...)},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				analysis := Analyze(testCatalog(t), tc.events)
				require.Len(t, analysis.Statements, 4)

				orders := envelopeOn(t, analysis, "orders", pg_contract.LockAccessExclusive)
				assert.Equal(t, 2, orders.StatementsWindow, "the ALTER and the %s", end)

				users := envelopeOn(t, analysis, "users", pg_contract.LockAccessExclusive)
				assert.Equal(t, 2, users.OpenedBy)
				assert.Equal(t, 2, users.StatementsWindow, "the ALTER and the ANALYZE")
			})
		}
	}
}

func TestEnvelopeEnd(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("BEGIN"),
		event("ALTER TABLE orders ADD COLUMN z int"),
		event("COMMIT"),
		event("BEGIN"),
		event("ALTER TABLE users ADD COLUMN z int"),
		event("COMMIT"),
	))

	orders := envelopeOn(t, analysis, "orders", pg_contract.LockAccessExclusive)
	assert.Equal(t, 2, orders.StatementsWindow, "the ALTER and the COMMIT")

	users := envelopeOn(t, analysis, "users", pg_contract.LockAccessExclusive)
	assert.Equal(t, 2, users.StatementsWindow)
	assert.NotEqual(t, orders.TxnGroup, users.TxnGroup)
}

func TestLevelGradingCreatedRelation(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("CREATE TABLE fresh (id bigint)"),
		event("ALTER TABLE fresh ADD COLUMN z int, ADD CONSTRAINT c CHECK (id >= 0)"),
		event("ALTER TABLE orders ADD COLUMN z int"),
	))
	require.Len(t, analysis.Statements, 3)

	for _, finding := range analysis.Statements[1].Findings {
		assert.Equal(t, pg_contract.LevelLow, finding.Level, "%s only touches a table this migration created", finding.Code)
	}

	// An existing table still waits for production's size and traffic to be graded.
	for _, finding := range rulesOnly(analysis.Statements[2]).Findings {
		assert.Equal(t, pg_contract.LevelUnset, finding.Level, "%s touches a table that was already there", finding.Code)
	}
}

func TestShadowCatalogInAnalyze(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	analysis := Analyze(catalog, apart(
		event("CREATE TABLE fresh (id bigint)"),
		event("ALTER TABLE fresh ADD COLUMN z int"),
	))

	require.Len(t, analysis.Statements, 2)
	assert.True(t, catalog.CreatedByMigration(relationName("fresh")))
	assert.Equal(t, pg_contract.LockAccessExclusive, analysis.Statements[1].LockOn(relationName("fresh")))
}

func TestAnalyzeWithoutEvents(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), nil)
	assert.Empty(t, analysis.Statements)
	assert.Empty(t, analysis.Envelopes)
}

func TestLevelGradingOnFindingWithoutRelation(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), []pg_contract.QueryEvent{event("CREATE TABLE fresh (id bigint)")})

	require.Len(t, analysis.Statements, 1)
	finding := findingOf(t, analysis.Statements[0], "R-TB-CREATE")
	require.Empty(t, finding.Targets)
	assert.Equal(t, pg_contract.LevelUnset, finding.Level)
}

func TestLevelGradingPerFinding(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	Analyze(catalog, []pg_contract.QueryEvent{event("CREATE TABLE fresh (id bigint)")})
	require.True(t, catalog.CreatedByMigration(relationName("fresh")))

	findings := []pg_contract.Finding{
		{Code: "fixed", Level: pg_contract.LevelHigh, Targets: []pg_contract.Target{{Relation: pg_contract.Relation{Name: relationName("fresh")}}}},
		{Code: "unset", Targets: []pg_contract.Target{{Relation: pg_contract.Relation{Name: relationName("fresh")}}}},
	}
	levelGrading(catalog, findings)

	assert.Equal(t, pg_contract.LevelHigh, findings[0].Level)
	assert.Equal(t, pg_contract.LevelLow, findings[1].Level)
}
