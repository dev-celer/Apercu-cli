package pg_classify

import (
	"testing"

	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var txnBlockMembers = []struct {
	sql     string
	subject string
}{
	{"CREATE INDEX CONCURRENTLY idx ON orders (status)", "CREATE INDEX CONCURRENTLY"},
	{"CREATE INDEX idx ON orders (status)", ""},
	{"DROP INDEX CONCURRENTLY orders_status_idx", "DROP INDEX CONCURRENTLY"},
	{"DROP INDEX orders_status_idx", ""},
	{"REINDEX INDEX CONCURRENTLY orders_status_idx", "REINDEX CONCURRENTLY"},
	{"REINDEX TABLE CONCURRENTLY orders", "REINDEX CONCURRENTLY"},
	{"REINDEX SCHEMA public", "REINDEX SCHEMA"},
	{"REINDEX DATABASE app", "REINDEX DATABASE"},
	{"REINDEX SYSTEM app", "REINDEX SYSTEM"},
	{"REINDEX TABLE events", "REINDEX TABLE"},
	{"REINDEX INDEX events_at_idx", "REINDEX INDEX"},
	{"REINDEX TABLE orders", ""},
	{"REINDEX INDEX orders_status_idx", ""},
	{"VACUUM", "VACUUM"},
	{"VACUUM orders", "VACUUM"},
	{"VACUUM FULL orders", "VACUUM"},
	{"ANALYZE orders", ""},
	{"CLUSTER", "CLUSTER"},
	{"CLUSTER events", "CLUSTER"},
	{"CLUSTER orders USING orders_status_idx", ""},
	{"ALTER TABLE events DETACH PARTITION events_2026 CONCURRENTLY", "ALTER TABLE ... DETACH CONCURRENTLY"},
	{"ALTER TABLE events DETACH PARTITION events_2026", ""},
	{"CREATE DATABASE d", "CREATE DATABASE"},
	{"DROP DATABASE d", "DROP DATABASE"},
	{"CREATE TABLESPACE ts LOCATION '/tmp/ts'", "CREATE TABLESPACE"},
	{"DROP TABLESPACE ts", "DROP TABLESPACE"},
}

func filterV01(analysis pg_contract.StatementAnalysis) []pg_contract.Error {
	var out []pg_contract.Error
	for _, err := range analysis.Errors {
		if err.Code == "V-01" {
			out = append(out, err)
		}
	}
	return out
}

func TestTxnBlockSafetyInAnExplicitBlock(t *testing.T) {
	t.Parallel()

	for _, member := range txnBlockMembers {
		t.Run(member.sql, func(t *testing.T) {
			t.Parallel()

			catalog := testCatalog(t)
			statements := analyze(t, catalog, "BEGIN; "+member.sql+"; COMMIT")
			require.Len(t, statements, 3)
			inBlock := statements[1]

			if member.subject == "" {
				assert.Empty(t, filterV01(inBlock), "the server runs this one inside a block")
				return
			}
			require.Len(t, filterV01(inBlock), 1)
			assert.Equal(t, member.subject+" cannot run inside a transaction block", filterV01(inBlock)[0].Message)
			assert.Empty(t, inBlock.Findings, "it fails before it reaches a relation, so it takes no lock")

			alone := single(t, testCatalog(t), member.sql)
			assert.Empty(t, filterV01(alone), "on its own it is what the server accepts")
		})
	}
}

func TestTxnBlockSafetyInAnImplicitTransaction(t *testing.T) {
	t.Parallel()

	shared := Analyze(testCatalog(t), cycle(0,
		event("VACUUM orders", 10),
		event("ANALYZE orders", 5),
	))
	require.Len(t, shared.Statements, 2)
	require.Len(t, filterV01(shared.Statements[0]), 1)
	assert.Equal(t, "VACUUM cannot run inside a transaction block", filterV01(shared.Statements[0])[0].Message)
	assert.Empty(t, filterV01(shared.Statements[1]), "ANALYZE is safe in a block")

	separate := Analyze(testCatalog(t), apart(
		event("VACUUM orders", 10),
		event("ANALYZE orders", 5),
	))
	require.Len(t, separate.Statements, 2)
	assert.Empty(t, filterV01(separate.Statements[0]), "a message of its own is what the server accepts")
	assert.Empty(t, filterV01(separate.Statements[1]))
}

func TestTxnBlockSafetyIgnorePosition(t *testing.T) {
	t.Parallel()

	first := Analyze(testCatalog(t), cycle(0, event("VACUUM orders", 10), event("ANALYZE orders", 5)))
	last := Analyze(testCatalog(t), cycle(0, event("ANALYZE orders", 5), event("VACUUM orders", 10)))

	require.Len(t, filterV01(first.Statements[0]), 1)
	require.Len(t, filterV01(last.Statements[1]), 1)
	assert.Equal(t, filterV01(first.Statements[0]), filterV01(last.Statements[1]))
}

// TestTxnBlockSafetyPartition assert that REINDEX and CLUSTER are refused on a partitioned relation only.
func TestTxnBlockSafetyPartition(t *testing.T) {
	t.Parallel()
	unsafe := map[string]bool{
		"REINDEX TABLE events":                   true,
		"REINDEX INDEX events_at_idx":            true,
		"CLUSTER events":                         true,
		"REINDEX TABLE orders":                   false,
		"REINDEX INDEX orders_status_idx":        false,
		"CLUSTER orders USING orders_status_idx": false,
	}
	for sql, expected := range unsafe {
		statements := analyze(t, testCatalog(t), "BEGIN; "+sql+"; COMMIT")
		require.Len(t, statements, 3)
		assert.Equalf(t, expected, len(filterV01(statements[1])) == 1, "%q: %v", sql, statements[1].Errors)
	}
}

func TestTxnBlockRefusalOpensNoEnvelope(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("BEGIN", 1),
		event("VACUUM orders", 100),
		event("ANALYZE orders", 5),
		event("COMMIT", 1),
	))

	require.Len(t, analysis.Statements, 4)
	require.Len(t, filterV01(analysis.Statements[1]), 1)

	envelopes := analysis.EnvelopesOn(relationName("orders"))
	require.Lenf(t, envelopes, 1, "only the ANALYZE took a lock: %v", envelopes)
	assert.Equal(t, pg_contract.LockShareUpdateExclusive, envelopes[0].Lock)
	assert.Equal(t, 2, envelopes[0].OpenedBy)
}

func TestOutsideOfTransactionIsIgnored(t *testing.T) {
	t.Parallel()

	assert.Empty(t, filterV01(single(t, testCatalog(t), "VACUUM orders")))
}
