package pg_classify

import (
	"slices"
	"testing"
	"time"

	"apercu-cli/helper"
	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// event is one proxy event, written the way the proxy publishes them after a rewrite: one
// statement, timed on its own.
func event(sql string, ms int) pg_contract.QueryEvent {
	return pg_contract.QueryEvent{SQL: sql, Duration: time.Duration(ms) * time.Millisecond}
}

// cycle stamps one protocol cycle on a run of events, which is what one client message becomes.
// Several events in one cycle is a multi-statement Query the proxy rewrote: a statement each, timed
// apart, still one implicit transaction on the server.
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

// events is which proxy event each statement came from.
func events(analysis pg_contract.MigrationAnalysis) []int {
	out := make([]int, 0, len(analysis.Statements))
	for _, statement := range analysis.Statements {
		out = append(out, statement.Event)
	}
	return out
}

func durations(analysis pg_contract.MigrationAnalysis) []time.Duration {
	out := make([]time.Duration, 0, len(analysis.Statements))
	for _, statement := range analysis.Statements {
		out = append(out, statement.Duration)
	}
	return out
}

func TestMultipleLockLevel(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("BEGIN", 1),
		event("UPDATE orders SET status = 'done'", 10),
		event("ALTER TABLE orders ADD COLUMN z int", 100),
		event("ANALYZE orders", 5),
		event("COMMIT", 1),
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
	assert.Equal(t, 6*time.Millisecond, covered[0].BlockingWindow, "the ANALYZE and the COMMIT")

	write := envelopeOn(t, analysis, "orders", pg_contract.LockRowExclusive)
	assert.Equal(t, 1, write.OpenedBy)
	assert.Equal(t, 116*time.Millisecond, write.BlockingWindow)
	assert.Equal(t, 4, write.Statements)

	exclusive := envelopeOn(t, analysis, "orders", pg_contract.LockAccessExclusive)
	assert.Equal(t, 2, exclusive.OpenedBy)
	assert.Equal(t, 106*time.Millisecond, exclusive.BlockingWindow)
	assert.Equal(t, 3, exclusive.Statements)
}

func TestCoveredLock(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("BEGIN", 1),
		event("ALTER TABLE orders ADD COLUMN z int", 100),
		event("UPDATE orders SET status = 'done'", 10),
		event("ANALYZE orders", 5),
		event("COMMIT", 1),
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
	assert.Equal(t, 16*time.Millisecond, covered[0].BlockingWindow)
	assert.Equal(t, pg_contract.LockShareUpdateExclusive, covered[1].Lock)
	assert.Equal(t, 6*time.Millisecond, covered[1].BlockingWindow)
}

func TestCoveredEqualLocks(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("BEGIN", 1),
		event("UPDATE orders SET status = 'done'", 10),
		event("DELETE FROM orders WHERE status = 'stale'", 20),
		event("COMMIT", 1),
	))

	envelopes := pg_contract.EffectiveEnvelopes(analysis.EnvelopesOn(relationName("orders")))
	require.Len(t, envelopes, 1)
	assert.Equal(t, pg_contract.LockRowExclusive, envelopes[0].Lock)
	assert.Equal(t, 1, envelopes[0].OpenedBy)
	assert.Equal(t, 31*time.Millisecond, envelopes[0].BlockingWindow)

	covered := coveredOn(analysis, "orders")
	require.Len(t, covered, 1)
	assert.Equal(t, pg_contract.LockRowExclusive, covered[0].Lock)
	assert.Equal(t, 2, covered[0].OpenedBy)
	assert.Equal(t, 21*time.Millisecond, covered[0].BlockingWindow)
}

func TestNoEnvelope(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("ALTER TABLE orders ADD COLUMN z int", 100),
		event("ALTER TABLE users ADD COLUMN z int", 200),
	))

	require.Len(t, analysis.Statements, 2)
	assert.Empty(t, analysis.Envelopes, "neither statement holds a lock past its own end")

	assert.Equal(t, pg_contract.LockAccessExclusive, analysis.Statements[0].LockOn(relationName("orders")))
	assert.Equal(t, 100*time.Millisecond, analysis.Statements[0].Duration)
}

func TestImplicitRewritten(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), cycle(0,
		event("ALTER TABLE orders ADD COLUMN z int", 100),
		event("ALTER TABLE users ADD COLUMN z int", 200),
	))

	require.Len(t, analysis.Statements, 2)
	assert.Equal(t, []int{0, 1}, events(analysis), "one event each, timed apart")
	assert.Equal(t, []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}, durations(analysis))
	assert.Equal(t, 300*time.Millisecond, analysis.Elapsed())

	orders := envelopeOn(t, analysis, "orders", pg_contract.LockAccessExclusive)
	assert.Equal(t, 2, orders.Statements, "held while the second ALTER runs")
	assert.Equal(t, 300*time.Millisecond, orders.BlockingWindow)

	users := envelopeOn(t, analysis, "users", pg_contract.LockAccessExclusive)
	assert.Equal(t, 1, users.Statements)
	assert.Equal(t, 200*time.Millisecond, users.BlockingWindow)
}

func TestImplicitExtendedProtocol(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), slices.Concat(
		cycle(0, event("ALTER TABLE orders ADD COLUMN z int", 100), event("ANALYZE orders", 10)),
		cycle(1, event("ALTER TABLE users ADD COLUMN z int", 200)),
	))

	require.Len(t, analysis.Statements, 3)

	orders := envelopeOn(t, analysis, "orders", pg_contract.LockAccessExclusive)
	assert.Equal(t, 2, orders.Statements, "stops at the end of its own message")
	assert.Equal(t, 110*time.Millisecond, orders.BlockingWindow)

	assert.Empty(t, analysis.EnvelopesOn(relationName("users")), "the last message is one statement on its own")
}

func TestEnvelopeEnd(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("BEGIN", 0),
		event("ALTER TABLE orders ADD COLUMN z int", 100),
		event("COMMIT", 0),
		event("BEGIN", 0),
		event("ALTER TABLE users ADD COLUMN z int", 200),
		event("COMMIT", 0),
	))

	orders := envelopeOn(t, analysis, "orders", pg_contract.LockAccessExclusive)
	assert.Equal(t, 100*time.Millisecond, orders.BlockingWindow)
	assert.Equal(t, 2, orders.Statements, "the ALTER and the COMMIT")

	users := envelopeOn(t, analysis, "users", pg_contract.LockAccessExclusive)
	assert.Equal(t, 200*time.Millisecond, users.BlockingWindow)
	assert.NotEqual(t, orders.TxnGroup, users.TxnGroup)
}

func TestLevelGradingCreatedRelation(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("CREATE TABLE fresh (id bigint)", 1),
		event("ALTER TABLE fresh ADD COLUMN z int, ADD CONSTRAINT c CHECK (id >= 0)", 5),
		event("ALTER TABLE orders ADD COLUMN z int", 5),
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
		event("CREATE TABLE fresh (id bigint)", 1),
		event("ALTER TABLE fresh ADD COLUMN z int", 1),
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

	analysis := Analyze(testCatalog(t), []pg_contract.QueryEvent{event("CREATE TABLE fresh (id bigint)", 1)})

	require.Len(t, analysis.Statements, 1)
	finding := findingOf(t, analysis.Statements[0], "R-TB-CREATE")
	require.Empty(t, finding.Targets)
	assert.Equal(t, pg_contract.LevelUnset, finding.Level)
}

func TestLevelGradingPerFinding(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	Analyze(catalog, []pg_contract.QueryEvent{event("CREATE TABLE fresh (id bigint)", 1)})
	require.True(t, catalog.CreatedByMigration(relationName("fresh")))

	findings := []pg_contract.Finding{
		{Code: "fixed", Level: pg_contract.LevelHigh, Targets: []pg_contract.Target{{Relation: pg_contract.Relation{Name: relationName("fresh")}}}},
		{Code: "unset", Targets: []pg_contract.Target{{Relation: pg_contract.Relation{Name: relationName("fresh")}}}},
	}
	levelGrading(catalog, findings)

	assert.Equal(t, pg_contract.LevelHigh, findings[0].Level)
	assert.Equal(t, pg_contract.LevelLow, findings[1].Level)
}
