package pgclassify_test

import (
	"slices"
	"strings"
	"testing"

	"apercu-cli/internal/pgclassify"
	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultipleLockLevel(t *testing.T) {
	t.Parallel()

	analysis := pgclassify.Analyze(classifytest.Catalog(t), classifytest.Apart(
		classifytest.Event("BEGIN"),
		classifytest.Event("UPDATE orders SET status = 'done'"),
		classifytest.Event("ALTER TABLE orders ADD COLUMN z int"),
		classifytest.Event("ANALYZE orders"),
		classifytest.Event("COMMIT"),
	))

	require.Len(t, analysis.Statements, 5)
	for _, statement := range analysis.Statements {
		assert.Equal(t, pgcontract.TxnGroup(1), statement.TxnGroup, "%q", statement.RawSQL)
	}

	assert.Len(t, analysis.EnvelopesOn(relationName("orders")), 3)

	covered := coveredOn(analysis, "orders")
	require.Len(t, covered, 1)
	assert.Equal(t, pgcontract.LockShareUpdateExclusive, covered[0].Lock)
	assert.Equal(t, 3, covered[0].OpenedBy)
	assert.Equal(t, 2, covered[0].StatementsWindow, "the ANALYZE and the COMMIT")

	write := envelopeOn(t, analysis, "orders", pgcontract.LockRowExclusive)
	assert.Equal(t, 1, write.OpenedBy)
	assert.Equal(t, 4, write.StatementsWindow)

	exclusive := envelopeOn(t, analysis, "orders", pgcontract.LockAccessExclusive)
	assert.Equal(t, 2, exclusive.OpenedBy)
	assert.Equal(t, 3, exclusive.StatementsWindow)
}

func TestCoveredLock(t *testing.T) {
	t.Parallel()

	analysis := pgclassify.Analyze(classifytest.Catalog(t), classifytest.Apart(
		classifytest.Event("BEGIN"),
		classifytest.Event("ALTER TABLE orders ADD COLUMN z int"),
		classifytest.Event("UPDATE orders SET status = 'done'"),
		classifytest.Event("ANALYZE orders"),
		classifytest.Event("COMMIT"),
	))

	envelopes := analysis.EnvelopesOn(relationName("orders"))
	require.Len(t, envelopes, 3)

	effective := pgcontract.EffectiveEnvelopes(envelopes)
	require.Len(t, effective, 1, "only the ALTER raised anything, %v", envelopes)
	assert.Equal(t, pgcontract.LockAccessExclusive, effective[0].Lock)
	assert.Equal(t, 1, effective[0].OpenedBy)

	covered := coveredOn(analysis, "orders")
	require.Len(t, covered, 2)
	assert.Equal(t, pgcontract.LockRowExclusive, covered[0].Lock)
	assert.Equal(t, 3, covered[0].StatementsWindow)
	assert.Equal(t, pgcontract.LockShareUpdateExclusive, covered[1].Lock)
	assert.Equal(t, 2, covered[1].StatementsWindow)
}

func TestCoveredEqualLocks(t *testing.T) {
	t.Parallel()

	analysis := pgclassify.Analyze(classifytest.Catalog(t), classifytest.Apart(
		classifytest.Event("BEGIN"),
		classifytest.Event("UPDATE orders SET status = 'done'"),
		classifytest.Event("DELETE FROM orders WHERE status = 'stale'"),
		classifytest.Event("COMMIT"),
	))

	envelopes := pgcontract.EffectiveEnvelopes(analysis.EnvelopesOn(relationName("orders")))
	require.Len(t, envelopes, 1)
	assert.Equal(t, pgcontract.LockRowExclusive, envelopes[0].Lock)
	assert.Equal(t, 1, envelopes[0].OpenedBy)
	assert.Equal(t, 3, envelopes[0].StatementsWindow)

	covered := coveredOn(analysis, "orders")
	require.Len(t, covered, 1)
	assert.Equal(t, pgcontract.LockRowExclusive, covered[0].Lock)
	assert.Equal(t, 2, covered[0].OpenedBy)
	assert.Equal(t, 2, covered[0].StatementsWindow)
}

// TestShareDoesNotCoverLowerOrdinals validate that a SHARE lock does not cover RE and SUE.
func TestShareDoesNotCoverLowerOrdinals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sql  string
		lock pgcontract.Lock
	}{
		{sql: "UPDATE orders SET status = 'done'", lock: pgcontract.LockRowExclusive},
		{sql: "ANALYZE orders", lock: pgcontract.LockShareUpdateExclusive},
	}
	for _, test := range tests {
		t.Run(test.lock.String(), func(t *testing.T) {
			t.Parallel()

			analysis := pgclassify.Analyze(classifytest.Catalog(t), classifytest.Apart(
				classifytest.Event("BEGIN"),
				classifytest.Event("CREATE INDEX orders_total_idx ON orders (total)"),
				classifytest.Event(test.sql),
				classifytest.Event("COMMIT"),
			))

			assert.Empty(t, coveredOn(analysis, "orders"), "SHARE does not cover %s", test.lock.Short())
			envelopeOn(t, analysis, "orders", pgcontract.LockShare)
			raised := envelopeOn(t, analysis, "orders", test.lock)
			assert.Equal(t, 2, raised.OpenedBy)
			assert.Equal(t, 2, raised.StatementsWindow)
		})
	}
}

// TestLocksHeldTogetherCover is the other side of it: SHARE and ROW EXCLUSIVE held together block every mode SHARE ROW
// EXCLUSIVE does, though neither one does alone, so the trigger created after them adds nothing.
func TestLocksHeldTogetherCover(t *testing.T) {
	t.Parallel()

	analysis := pgclassify.Analyze(classifytest.Catalog(t), classifytest.Apart(
		classifytest.Event("BEGIN"),
		classifytest.Event("CREATE INDEX orders_total_idx ON orders (total)"),
		classifytest.Event("UPDATE orders SET status = 'done'"),
		classifytest.Event("CREATE TRIGGER orders_audit AFTER UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION bump()"),
		classifytest.Event("COMMIT"),
	))

	covered := coveredOn(analysis, "orders")
	require.Len(t, covered, 1)
	assert.Equal(t, pgcontract.LockShareRowExclusive, covered[0].Lock)
	assert.Equal(t, 3, covered[0].OpenedBy)
	assert.Equal(t, 2, covered[0].StatementsWindow)

	effective := pgcontract.EffectiveEnvelopes(analysis.EnvelopesOn(relationName("orders")))
	require.Len(t, effective, 2)
	assert.Equal(t, pgcontract.LockShare, effective[0].Lock)
	assert.Equal(t, pgcontract.LockRowExclusive, effective[1].Lock)
}

func TestNoEnvelope(t *testing.T) {
	t.Parallel()

	analysis := pgclassify.Analyze(classifytest.Catalog(t), classifytest.Apart(
		classifytest.Event("ALTER TABLE orders ADD COLUMN z int"),
		classifytest.Event("ALTER TABLE users ADD COLUMN z int"),
	))

	require.Len(t, analysis.Statements, 2)
	assert.Empty(t, analysis.Envelopes, "neither statement holds a lock past its own end")

	assert.Equal(t, pgcontract.LockAccessExclusive, analysis.Statements[0].LockOn(relationName("orders")))
}

func TestImplicitMultiStatementQuery(t *testing.T) {
	t.Parallel()

	analysis := pgclassify.Analyze(classifytest.Catalog(t), classifytest.Apart(
		classifytest.Event("ALTER TABLE orders ADD COLUMN z int; ALTER TABLE users ADD COLUMN z int"),
	))

	require.Len(t, analysis.Statements, 2, "one event, split into its statements")

	orders := envelopeOn(t, analysis, "orders", pgcontract.LockAccessExclusive)
	assert.Equal(t, 2, orders.StatementsWindow, "held while the second ALTER runs")

	users := envelopeOn(t, analysis, "users", pgcontract.LockAccessExclusive)
	assert.Equal(t, 1, users.StatementsWindow)
}

func TestImplicitExtendedProtocol(t *testing.T) {
	t.Parallel()

	analysis := pgclassify.Analyze(classifytest.Catalog(t), slices.Concat(
		classifytest.Cycle(0, classifytest.Event("ALTER TABLE orders ADD COLUMN z int"), classifytest.Event("ANALYZE orders")),
		classifytest.Cycle(1, classifytest.Event("ALTER TABLE users ADD COLUMN z int")),
	))

	require.Len(t, analysis.Statements, 3)

	orders := envelopeOn(t, analysis, "orders", pgcontract.LockAccessExclusive)
	assert.Equal(t, 2, orders.StatementsWindow, "stops at the end of its own message")

	assert.Empty(t, analysis.EnvelopesOn(relationName("users")), "the last message is one statement on its own")
}

// TestImplicitTransactionEndsPartWay is a COMMIT or a ROLLBACK inside a message: it ends the implicit transaction there,
// releasing its locks, and the statements after it run in another one.
func TestImplicitTransactionEndsPartWay(t *testing.T) {
	t.Parallel()

	for _, end := range []string{"COMMIT", "ROLLBACK"} {
		statements := []string{"ALTER TABLE orders ADD COLUMN z int", end, "ALTER TABLE users ADD COLUMN z int", "ANALYZE users"}
		pipeline := make([]pgcontract.QueryEvent, 0, len(statements))
		for _, sql := range statements {
			pipeline = append(pipeline, classifytest.Event(sql))
		}

		cases := []struct {
			name   string
			events []pgcontract.QueryEvent
		}{
			{name: end + " in a multi-statement query", events: classifytest.Apart(classifytest.Event(strings.Join(statements, "; ")))},
			{name: end + " in a pipeline", events: classifytest.Cycle(0, pipeline...)},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				analysis := pgclassify.Analyze(classifytest.Catalog(t), tc.events)
				require.Len(t, analysis.Statements, 4)

				orders := envelopeOn(t, analysis, "orders", pgcontract.LockAccessExclusive)
				assert.Equal(t, 2, orders.StatementsWindow, "the ALTER and the %s", end)

				users := envelopeOn(t, analysis, "users", pgcontract.LockAccessExclusive)
				assert.Equal(t, 2, users.OpenedBy)
				assert.Equal(t, 2, users.StatementsWindow, "the ALTER and the ANALYZE")
			})
		}
	}
}

func TestEnvelopeEnd(t *testing.T) {
	t.Parallel()

	analysis := pgclassify.Analyze(classifytest.Catalog(t), classifytest.Apart(
		classifytest.Event("BEGIN"),
		classifytest.Event("ALTER TABLE orders ADD COLUMN z int"),
		classifytest.Event("COMMIT"),
		classifytest.Event("BEGIN"),
		classifytest.Event("ALTER TABLE users ADD COLUMN z int"),
		classifytest.Event("COMMIT"),
	))

	orders := envelopeOn(t, analysis, "orders", pgcontract.LockAccessExclusive)
	assert.Equal(t, 2, orders.StatementsWindow, "the ALTER and the COMMIT")

	users := envelopeOn(t, analysis, "users", pgcontract.LockAccessExclusive)
	assert.Equal(t, 2, users.StatementsWindow)
	assert.NotEqual(t, orders.TxnGroup, users.TxnGroup)
}

// envelopeOn is the envelope that raised a relation to a lock, the covered ones aside.
func envelopeOn(t *testing.T, analysis pgcontract.MigrationAnalysis, table string, lock pgcontract.Lock) pgcontract.LockEnvelope {
	t.Helper()

	envelopes := analysis.EnvelopesOn(relationName(table))
	for _, envelope := range pgcontract.EffectiveEnvelopes(envelopes) {
		if envelope.Lock == lock {
			return envelope
		}
	}
	require.Failf(t, "missing envelope", "nothing raised %s on %s, only %v", lock.Short(), table, envelopes)
	return pgcontract.LockEnvelope{}
}

// coveredOn is the envelopes on a relation the transaction was already blocking at least as hard.
func coveredOn(analysis pgcontract.MigrationAnalysis, table string) []pgcontract.LockEnvelope {
	var out []pgcontract.LockEnvelope
	for _, envelope := range analysis.EnvelopesOn(relationName(table)) {
		if envelope.Covered {
			out = append(out, envelope)
		}
	}
	return out
}
