package pg_classify

import (
	"path/filepath"
	"slices"
	"testing"

	"apercu-cli/helper/pg_catalog"
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

func filterV02(analysis pg_contract.StatementAnalysis) *pg_contract.Finding {
	for i, finding := range analysis.Findings {
		if finding.Code == queueRiskCode {
			return &analysis.Findings[i]
		}
	}
	return nil
}

func filterV02Relation(analysis pg_contract.StatementAnalysis) []string {
	finding := filterV02(analysis)
	if finding == nil {
		return nil
	}
	names := make([]string, 0, len(finding.Targets))
	for _, target := range finding.Targets {
		names = append(names, target.Relation.Name.String())
	}
	return names
}

func getCatalog(t *testing.T, value string) *pg_catalog.Catalog {
	t.Helper()

	pre, err := pg_catalog.LoadJSON(filepath.Join("..", "pg_catalog", "testdata", "snapshot_pg17_preview.json.gz"))
	require.NoError(t, err)
	pre.Settings = slices.DeleteFunc(pre.Settings, func(setting pg_catalog.Setting) bool { return setting.Name == "lock_timeout" })
	if value != "" {
		pre.Settings = append(pre.Settings, pg_catalog.Setting{Name: "lock_timeout", Value: value})
	}
	catalog, err := pg_catalog.NewCatalog(pg_catalog.CatalogOptions{Pre: pre})
	require.NoError(t, err)
	return catalog
}

func TestUnboundedQueueThreshold(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sql   string
		fires bool
	}{
		{"LOCK TABLE orders IN ACCESS SHARE MODE", false},
		{"LOCK TABLE orders IN ROW SHARE MODE", false},
		{"LOCK TABLE orders IN ROW EXCLUSIVE MODE", false},
		{"LOCK TABLE orders IN SHARE UPDATE EXCLUSIVE MODE", false},
		{"LOCK TABLE orders IN SHARE MODE", true},
		{"LOCK TABLE orders IN SHARE ROW EXCLUSIVE MODE", true},
		{"LOCK TABLE orders IN EXCLUSIVE MODE", true},
		{"LOCK TABLE orders IN ACCESS EXCLUSIVE MODE", true},
		{"SELECT * FROM orders FOR UPDATE", false},
		{"UPDATE orders SET status = 'done'", false},
		{"ANALYZE orders", false},
		{"CREATE INDEX CONCURRENTLY orders_total_idx ON orders (total)", false},
		{"CREATE INDEX orders_total_idx ON orders (total)", true},
		{"ALTER TABLE orders ADD CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES users (id) NOT VALID", true},
		{"REFRESH MATERIALIZED VIEW order_totals", true},
		{"ALTER TABLE orders ADD COLUMN z int", true},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.sql, func(t *testing.T) {
			analysis := single(t, catalog, testCase.sql)

			var blocking []string
			for _, locked := range analysis.LockedRelations() {
				if locked.Lock.IsWriteBlocking() {
					blocking = append(blocking, locked.Relation.Name.String())
				}
			}
			assert.ElementsMatch(t, blocking, filterV02Relation(analysis), "V-02 names every relation locked at SHARE or above")

			finding := filterV02(analysis)
			if !testCase.fires {
				assert.Nil(t, finding)
				return
			}
			require.NotNil(t, finding)
			assert.Equal(t, pg_contract.SeverityWarn, finding.Severity)
			assert.Equal(t, pg_contract.LevelHigh, finding.Level)
			assert.Contains(t, finding.Message, "lock_timeout is disabled")
			for _, target := range finding.Targets {
				assert.Equal(t, pg_contract.OpKindNone, target.OpKind, "V-02 does no work of its own on %s", target.Relation)
				assert.Equal(t, analysis.LockOn(target.Relation.Name), target.Lock, "V-02 reports the statement's lock on %s", target.Relation)
			}
		})
	}
}

func TestUnboundedQueueTargets(t *testing.T) {
	t.Parallel()

	t.Run("the table a new foreign key references keeps its role", func(t *testing.T) {
		analysis := single(t, testCatalog(t), "CREATE TABLE tb_fk (id bigint REFERENCES users (id))")
		require.Equal(t, []string{"apercu_snapshot_test.users"}, filterV02Relation(analysis))
		target := filterV02(analysis).Targets[0]
		assert.Equal(t, pg_contract.LockShareRowExclusive, target.Lock)
		assert.Equal(t, pg_contract.TargetRoleImplicit, target.Role)
	})

	t.Run("a recursing clause names every partition it locks", func(t *testing.T) {
		analysis := single(t, testCatalog(t), "ALTER TABLE events ADD COLUMN z int")
		assert.ElementsMatch(t, []string{
			"apercu_snapshot_test.events", "apercu_snapshot_test.events_2025", "apercu_snapshot_test.events_default",
		}, filterV02Relation(analysis))
	})

	t.Run("a relation the migration created has nobody to queue behind", func(t *testing.T) {
		analyses := analyze(t, testCatalog(t), "CREATE TABLE fresh (id bigint); "+
			"CREATE INDEX fresh_id_idx ON fresh (id); "+
			"ALTER TABLE fresh ADD CONSTRAINT fresh_fk FOREIGN KEY (id) REFERENCES users (id)")
		require.Len(t, analyses, 3)
		assert.Nil(t, filterV02(analyses[1]))
		assert.Equal(t, []string{"apercu_snapshot_test.users"}, filterV02Relation(analyses[2]), "only the table that was already there")
	})

	t.Run("an unknown relation fails safe", func(t *testing.T) {
		analysis := single(t, testCatalog(t), "ALTER TABLE no_such_table ADD COLUMN z int")
		assert.Equal(t, []string{"apercu_snapshot_test.no_such_table"}, filterV02Relation(analysis))
	})
}

func TestUnboundedQueueTimeoutInScope(t *testing.T) {
	t.Parallel()

	const alter = "ALTER TABLE orders ADD COLUMN z int"
	cases := []struct {
		name   string
		script string
		fires  []bool
		reason string
	}{
		{name: "nothing set", script: alter, fires: []bool{true}, reason: "lock_timeout is disabled"},
		{name: "a session timeout", script: "SET lock_timeout = '5s'; " + alter, fires: []bool{false}},
		{name: "a timeout of zero is disabled", script: "SET lock_timeout = 0; " + alter, fires: []bool{true}, reason: "lock_timeout is disabled"},
		{name: "a value the server rejects", script: "SET lock_timeout = 'soon'; " + alter, fires: []bool{true}, reason: "lock_timeout 'soon' is not a value the server accepts"},
		{name: "SET LOCAL outside a block applies nothing", script: "SET LOCAL lock_timeout = '5s'; " + alter, fires: []bool{true}},
		{
			name:   "SET LOCAL ends with its block",
			script: "BEGIN; SET LOCAL lock_timeout = '5s'; " + alter + "; COMMIT; " + alter,
			fires:  []bool{false, true},
		},
		{name: "a session SET survives the COMMIT", script: "BEGIN; SET lock_timeout = '5s'; COMMIT; " + alter, fires: []bool{false}},
		{name: "and not a ROLLBACK", script: "BEGIN; SET lock_timeout = '5s'; ROLLBACK; " + alter, fires: []bool{true}},
		{name: "RESET", script: "SET lock_timeout = '5s'; RESET lock_timeout; " + alter, fires: []bool{true}},
		{name: "RESET ALL", script: "SET lock_timeout = '5s'; RESET ALL; " + alter, fires: []bool{true}},
		{
			name:   "a savepoint unwinds the timeout",
			script: "BEGIN; SAVEPOINT s; SET LOCAL lock_timeout = '5s'; ROLLBACK TO SAVEPOINT s; " + alter + "; COMMIT",
			fires:  []bool{true},
		},
		{
			name:   "SET LOCAL to zero overrides a session timeout until the COMMIT",
			script: "SET lock_timeout = '5s'; BEGIN; SET LOCAL lock_timeout = 0; " + alter + "; COMMIT; " + alter,
			fires:  []bool{true, false},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var verdicts []bool
			for _, analysis := range analyze(t, testCatalog(t), testCase.script) {
				if analysis.Command != "ALTER TABLE" {
					continue
				}
				finding := filterV02(analysis)
				verdicts = append(verdicts, finding != nil)
				if finding != nil && testCase.reason != "" {
					assert.Contains(t, finding.Message, testCase.reason)
				}
			}
			assert.Equal(t, testCase.fires, verdicts)
		})
	}
}

func TestUnboundedQueueNowait(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	assert.NotNil(t, filterV02(single(t, catalog, "LOCK TABLE orders IN ACCESS EXCLUSIVE MODE")))
	assert.Nil(t, filterV02(single(t, catalog, "LOCK TABLE orders IN ACCESS EXCLUSIVE MODE NOWAIT")))
	assert.NotNil(t, filterV02(single(t, catalog, "ALTER TABLE ALL IN TABLESPACE pg_default SET TABLESPACE slow")))
	assert.Nil(t, filterV02(single(t, catalog, "ALTER TABLE ALL IN TABLESPACE pg_default SET TABLESPACE slow NOWAIT")))
}

func TestUnboundedQueueRefused(t *testing.T) {
	t.Parallel()

	inBlock := analyze(t, testCatalog(t), "BEGIN; VACUUM FULL orders; COMMIT")
	require.Len(t, filterV01(inBlock[1]), 1)
	assert.Nil(t, filterV02(inBlock[1]))

	const notEnforced = "ALTER TABLE orders ADD CONSTRAINT c CHECK (total > 0) NOT ENFORCED"
	tooOld := single(t, testCatalogOn(t, pg_contract.Version17), notEnforced)
	errorOf(t, tooOld, gateCode)
	assert.Nil(t, filterV02(tooOld))
	assert.NotNil(t, filterV02(single(t, testCatalogOn(t, pg_contract.Version18), notEnforced)), "the server that runs it queues for its lock")
}

func TestUnboundedQueueCovered(t *testing.T) {
	t.Parallel()

	const (
		lockAll     = "LOCK TABLE orders IN ACCESS EXCLUSIVE MODE"
		addColumn   = "ALTER TABLE orders ADD COLUMN z int"
		createIndex = "CREATE INDEX orders_total_idx ON orders (total)"
		addFK       = "ALTER TABLE orders ADD CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES users (id) NOT VALID"
	)
	fires := func(analysis pg_contract.MigrationAnalysis) []bool {
		out := make([]bool, 0, len(analysis.Statements))
		for _, statement := range analysis.Statements {
			out = append(out, filterV02(statement) != nil)
		}
		return out
	}

	t.Run("only the statement that raised the lock can wait for it", func(t *testing.T) {
		analysis := Analyze(testCatalog(t), apart(
			event("BEGIN", 1), event(lockAll, 1), event(addColumn, 1), event(createIndex, 1), event("COMMIT", 1),
		))
		assert.Equal(t, []bool{false, true, false, false, false}, fires(analysis))
	})

	t.Run("a lock upgrade waits again", func(t *testing.T) {
		analysis := Analyze(testCatalog(t), apart(
			event("BEGIN", 1), event(createIndex, 1), event(addColumn, 1), event("COMMIT", 1),
		))
		assert.Equal(t, []bool{false, true, true, false}, fires(analysis))
	})

	t.Run("an implicit transaction holds its locks the same way", func(t *testing.T) {
		analysis := Analyze(testCatalog(t), cycle(0, event(addColumn, 1), event(createIndex, 1)))
		assert.Equal(t, []bool{true, false}, fires(analysis))
	})

	t.Run("separate transactions each wait", func(t *testing.T) {
		analysis := Analyze(testCatalog(t), apart(event(addColumn, 1), event(createIndex, 1)))
		assert.Equal(t, []bool{true, true}, fires(analysis))
	})

	t.Run("only the relations already held are dropped", func(t *testing.T) {
		analysis := Analyze(testCatalog(t), apart(event("BEGIN", 1), event(lockAll, 1), event(addFK, 1), event("COMMIT", 1)))
		assert.Equal(t, []string{"apercu_snapshot_test.users"}, filterV02Relation(analysis.Statements[2]))
	})
}

func TestUnboundedQueueLeavesLocks(t *testing.T) {
	t.Parallel()

	script := func() []pg_contract.QueryEvent {
		return apart(
			event("BEGIN", 1),
			event("UPDATE orders SET status = 'done'", 10),
			event("ALTER TABLE orders ADD COLUMN z int", 100),
			event("CREATE INDEX users_email_idx ON users (email)", 20),
			event("COMMIT", 1),
			event("ALTER TABLE events ADD COLUMN y int", 50),
		)
	}
	unbounded := Analyze(testCatalog(t), script())
	bounded := Analyze(getCatalog(t, "5s"), script())

	assert.Equal(t, bounded.Envelopes, unbounded.Envelopes)
	require.Len(t, unbounded.Statements, len(bounded.Statements))
	for i := range unbounded.Statements {
		assert.Nil(t, filterV02(bounded.Statements[i]))
		assert.Equal(t, bounded.Statements[i].LockedRelations(), unbounded.Statements[i].LockedRelations())
		assert.Equal(t, bounded.Statements[i].MaxOpKind(), unbounded.Statements[i].MaxOpKind())
	}

	for _, i := range []int{2, 3, 5} {
		finding := filterV02(unbounded.Statements[i])
		require.NotNilf(t, finding, "%q", unbounded.Statements[i].RawSQL)
		assert.Equal(t, pg_contract.LevelHigh, finding.Level, "the level survives the grading pass")
	}
}
