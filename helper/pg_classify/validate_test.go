package pg_classify

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func filterV01(analysis pg_contract.StatementAnalysis) *pg_contract.Finding {
	for i, finding := range analysis.Findings {
		if finding.Code == queueRiskCode {
			return &analysis.Findings[i]
		}
	}
	return nil
}

func filterV01Relation(analysis pg_contract.StatementAnalysis) []string {
	return targetNames(filterV01(analysis))
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
			assert.ElementsMatch(t, blocking, filterV01Relation(analysis), "V-01 names every relation locked at SHARE or above")

			finding := filterV01(analysis)
			if !testCase.fires {
				assert.Nil(t, finding)
				return
			}
			require.NotNil(t, finding)
			assert.Equal(t, pg_contract.SeverityWarn, finding.Severity)
			assert.Equal(t, pg_contract.LevelHigh, finding.Level)
			assert.Contains(t, finding.Message, "lock_timeout is disabled")
			for _, target := range finding.Targets {
				assert.Equal(t, pg_contract.OpKindNone, target.OpKind, "V-01 does no work of its own on %s", target.Relation)
				assert.Equal(t, analysis.LockOn(target.Relation.Name), target.Lock, "V-01 reports the statement's lock on %s", target.Relation)
			}
		})
	}
}

func TestUnboundedQueueTargets(t *testing.T) {
	t.Parallel()

	t.Run("the table a new foreign key references keeps its role", func(t *testing.T) {
		analysis := single(t, testCatalog(t), "CREATE TABLE tb_fk (id bigint REFERENCES users (id))")
		require.Equal(t, []string{"apercu_snapshot_test.users"}, filterV01Relation(analysis))
		target := filterV01(analysis).Targets[0]
		assert.Equal(t, pg_contract.LockShareRowExclusive, target.Lock)
		assert.Equal(t, pg_contract.TargetRoleImplicit, target.Role)
	})

	t.Run("a recursing clause names every partition it locks", func(t *testing.T) {
		analysis := single(t, testCatalog(t), "ALTER TABLE events ADD COLUMN z int")
		assert.ElementsMatch(t, []string{
			"apercu_snapshot_test.events", "apercu_snapshot_test.events_2025", "apercu_snapshot_test.events_default",
		}, filterV01Relation(analysis))
	})

	t.Run("a relation the migration created has nobody to queue behind", func(t *testing.T) {
		analyses := analyze(t, testCatalog(t), "CREATE TABLE fresh (id bigint); "+
			"CREATE INDEX fresh_id_idx ON fresh (id); "+
			"ALTER TABLE fresh ADD CONSTRAINT fresh_fk FOREIGN KEY (id) REFERENCES users (id)")
		require.Len(t, analyses, 3)
		assert.Nil(t, filterV01(analyses[1]))
		assert.Equal(t, []string{"apercu_snapshot_test.users"}, filterV01Relation(analyses[2]), "only the table that was already there")
	})

	t.Run("an unknown relation fails safe", func(t *testing.T) {
		analysis := single(t, testCatalog(t), "ALTER TABLE no_such_table ADD COLUMN z int")
		assert.Equal(t, []string{"apercu_snapshot_test.no_such_table"}, filterV01Relation(analysis))
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
		{name: "SET LOCAL in a message of its own applies nothing", script: "SET LOCAL lock_timeout = '5s'; " + alter, fires: []bool{true}},
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
				finding := filterV01(analysis)
				verdicts = append(verdicts, finding != nil)
				if finding != nil && testCase.reason != "" {
					assert.Contains(t, finding.Message, testCase.reason)
				}
			}
			assert.Equal(t, testCase.fires, verdicts)
		})
	}
}

// TestUnboundedQueueSetLocalInImplicitTransaction: the statements of one protocol cycle share an
// implicit transaction, and a SET LOCAL lasts until that transaction ends.
func TestUnboundedQueueSetLocalInImplicitTransaction(t *testing.T) {
	t.Parallel()

	const (
		alter    = "ALTER TABLE orders ADD COLUMN z int"
		setLocal = "SET LOCAL lock_timeout = '5s'"
	)
	cases := []struct {
		name   string
		events []pg_contract.QueryEvent
		fires  []bool
	}{
		{name: "a multi-statement Query", events: apart(event(setLocal + "; " + alter)), fires: []bool{false}},
		{name: "a pipeline", events: cycle(0, event(setLocal), event(alter)), fires: []bool{false}},
		{name: "it ends with the Query", events: apart(event(setLocal+"; "+alter), event(alter)), fires: []bool{false, true}},
		{
			name:   "and with the pipeline",
			events: slices.Concat(cycle(0, event(setLocal), event(alter)), cycle(1, event(alter))),
			fires:  []bool{false, true},
		},
		{
			name:   "SET LOCAL to zero overrides a session timeout until the end of the message",
			events: apart(event("SET lock_timeout = '5s'"), event("SET LOCAL lock_timeout = 0; "+alter), event(alter)),
			fires:  []bool{true, false},
		},
		{
			// BEGIN turns the implicit transaction into the block, with what it already wrote.
			name:   "BEGIN carries it into the block it opens",
			events: apart(event(setLocal+"; BEGIN; "+alter), event("COMMIT"), event(alter)),
			fires:  []bool{false, true},
		},
		{
			name:   "a ROLLBACK in the message undoes a session SET too",
			events: apart(event("SET lock_timeout = '5s'; ROLLBACK; " + alter)),
			fires:  []bool{true},
		},
		{name: "a message of its own applies nothing", events: apart(event(setLocal), event(alter)), fires: []bool{true}},
		{
			name:   "nor does a SET LOCAL closing its message",
			events: slices.Concat(cycle(0, event("SELECT 1"), event(setLocal)), cycle(1, event(alter))),
			fires:  []bool{true},
		},
		{name: "a COMMIT in the message ends it", events: apart(event(setLocal + "; COMMIT; " + alter)), fires: []bool{true}},
		{
			name:   "an explicit block outlives its messages",
			events: apart(event("BEGIN"), event(setLocal), event(alter), event("COMMIT"), event(alter)),
			fires:  []bool{false, true},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var verdicts []bool
			for _, statement := range Analyze(testCatalog(t), testCase.events).Statements {
				if statement.Command == "ALTER TABLE" {
					verdicts = append(verdicts, filterV01(statement) != nil)
				}
			}
			assert.Equal(t, testCase.fires, verdicts)
		})
	}
}

func TestUnboundedQueueSetLocalSearchPath(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("SET LOCAL search_path = public; ALTER TABLE users ADD COLUMN x int"),
		event("ALTER TABLE users ADD COLUMN y int"),
	))
	require.Len(t, analysis.Statements, 3)
	assert.Equal(t, []string{"public.users"}, filterV01Relation(analysis.Statements[1]), "resolved through the SET LOCAL")
	assert.Equal(t, []string{"apercu_snapshot_test.users"}, filterV01Relation(analysis.Statements[2]), "back on the baseline after the message")
}

func TestUnboundedQueueNowait(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	assert.NotNil(t, filterV01(single(t, catalog, "LOCK TABLE orders IN ACCESS EXCLUSIVE MODE")))
	assert.Nil(t, filterV01(single(t, catalog, "LOCK TABLE orders IN ACCESS EXCLUSIVE MODE NOWAIT")))
	assert.NotNil(t, filterV01(single(t, catalog, "ALTER TABLE ALL IN TABLESPACE pg_default SET TABLESPACE slow")))
	assert.Nil(t, filterV01(single(t, catalog, "ALTER TABLE ALL IN TABLESPACE pg_default SET TABLESPACE slow NOWAIT")))
}

func TestUnboundedQueueRefused(t *testing.T) {
	t.Parallel()

	const notEnforced = "ALTER TABLE orders ADD CONSTRAINT c CHECK (total > 0) NOT ENFORCED"
	tooOld := single(t, testCatalogOn(t, pg_contract.Version17), notEnforced)
	errorOf(t, tooOld, gateCode)
	assert.Nil(t, filterV01(tooOld))
	assert.NotNil(t, filterV01(single(t, testCatalogOn(t, pg_contract.Version18), notEnforced)), "the server that runs it queues for its lock")
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
			out = append(out, filterV01(statement) != nil)
		}
		return out
	}

	t.Run("only the statement that raised the lock can wait for it", func(t *testing.T) {
		analysis := Analyze(testCatalog(t), apart(
			event("BEGIN"), event(lockAll), event(addColumn), event(createIndex), event("COMMIT"),
		))
		assert.Equal(t, []bool{false, true, false, false, false}, fires(analysis))
	})

	t.Run("a lock upgrade waits again", func(t *testing.T) {
		analysis := Analyze(testCatalog(t), apart(
			event("BEGIN"), event(createIndex), event(addColumn), event("COMMIT"),
		))
		assert.Equal(t, []bool{false, true, true, false}, fires(analysis))
	})

	t.Run("locks held together cover one neither covers alone", func(t *testing.T) {
		const (
			update        = "UPDATE orders SET status = 'done'"
			createTrigger = "CREATE TRIGGER orders_audit AFTER UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION bump()"
		)
		together := Analyze(testCatalog(t), apart(
			event("BEGIN"), event(createIndex), event(update), event(createTrigger), event("COMMIT"),
		))
		assert.Equal(t, []bool{false, true, false, false, false}, fires(together), "SHARE and ROW EXCLUSIVE cover SHARE ROW EXCLUSIVE")

		alone := Analyze(testCatalog(t), apart(
			event("BEGIN"), event(createIndex), event(createTrigger), event("COMMIT"),
		))
		assert.Equal(t, []bool{false, true, true, false}, fires(alone), "SHARE alone does not")
	})

	t.Run("an implicit transaction holds its locks the same way", func(t *testing.T) {
		analysis := Analyze(testCatalog(t), cycle(0, event(addColumn), event(createIndex)))
		assert.Equal(t, []bool{true, false}, fires(analysis))
	})

	t.Run("separate transactions each wait", func(t *testing.T) {
		analysis := Analyze(testCatalog(t), apart(event(addColumn), event(createIndex)))
		assert.Equal(t, []bool{true, true}, fires(analysis))
	})

	t.Run("a COMMIT or ROLLBACK part-way through a message releases the locks, so the next statement waits again", func(t *testing.T) {
		for _, end := range []string{"COMMIT", "ROLLBACK"} {
			query := Analyze(testCatalog(t), apart(event(addColumn+"; "+end+"; "+createIndex)))
			assert.Equal(t, []bool{true, false, true}, fires(query), "%s in a multi-statement query", end)

			pipeline := Analyze(testCatalog(t), cycle(0, event(addColumn), event(end), event(createIndex)))
			assert.Equal(t, []bool{true, false, true}, fires(pipeline), "%s in a pipeline", end)
		}
	})

	t.Run("only the relations already held are dropped", func(t *testing.T) {
		analysis := Analyze(testCatalog(t), apart(event("BEGIN"), event(lockAll), event(addFK), event("COMMIT")))
		assert.Equal(t, []string{"apercu_snapshot_test.users"}, filterV01Relation(analysis.Statements[2]))
	})
}

func TestUnboundedQueueLeavesLocks(t *testing.T) {
	t.Parallel()

	script := func() []pg_contract.QueryEvent {
		return apart(
			event("BEGIN"),
			event("UPDATE orders SET status = 'done'"),
			event("ALTER TABLE orders ADD COLUMN z int"),
			event("CREATE INDEX users_email_idx ON users (email)"),
			event("COMMIT"),
			event("ALTER TABLE events ADD COLUMN y int"),
		)
	}
	unbounded := Analyze(testCatalog(t), script())
	bounded := Analyze(getCatalog(t, "5s"), script())

	assert.Equal(t, bounded.Envelopes, unbounded.Envelopes)
	require.Len(t, unbounded.Statements, len(bounded.Statements))
	for i := range unbounded.Statements {
		assert.Nil(t, filterV01(bounded.Statements[i]))
		assert.Equal(t, bounded.Statements[i].LockedRelations(), unbounded.Statements[i].LockedRelations())
		assert.Equal(t, bounded.Statements[i].MaxOpKind(), unbounded.Statements[i].MaxOpKind())
	}

	for _, i := range []int{2, 3, 5} {
		finding := filterV01(unbounded.Statements[i])
		require.NotNilf(t, finding, "%q", unbounded.Statements[i].RawSQL)
		assert.Equal(t, pg_contract.LevelHigh, finding.Level, "the level survives the grading pass")
	}
}

func filterV04(analysis pg_contract.StatementAnalysis) *pg_contract.Finding {
	for i, finding := range analysis.Findings {
		if finding.Code == maintenanceStatementCode {
			return &analysis.Findings[i]
		}
	}
	return nil
}

func targetNames(finding *pg_contract.Finding) []string {
	if finding == nil {
		return nil
	}
	names := make([]string, 0, len(finding.Targets))
	for _, target := range finding.Targets {
		names = append(names, target.Relation.Name.String())
	}
	return names
}

func TestMaintenanceStatements(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sql     string
		rule    pg_contract.Code
		subject string
	}{
		{"VACUUM FULL orders", "R-MT-VACUUMFULL", "VACUUM FULL"},
		{"VACUUM (FULL, ANALYZE) orders", "R-MT-VACUUMFULL", "VACUUM FULL"},
		{"VACUUM (FULL 1) orders", "R-MT-VACUUMFULL", "VACUUM FULL"},
		{"VACUUM FULL", "R-MT-VACUUMFULL", "VACUUM FULL"},
		{"VACUUM FULL events", "R-MT-VACUUMFULL", "VACUUM FULL"},
		{"CLUSTER orders USING orders_pkey", "R-MT-CLUSTER", "CLUSTER"},
		{"CLUSTER events USING events_at_idx", "R-MT-CLUSTER", "CLUSTER"},
		{"CLUSTER", "R-MT-CLUSTER", "CLUSTER"},
		{"VACUUM orders", "", ""},
		{"VACUUM (FULL false) orders", "", ""},
		{"VACUUM (FULL 0) orders", "", ""},
		{"ANALYZE orders", "", ""},
		{"REINDEX TABLE orders", "", ""},
		{"ALTER TABLE orders CLUSTER ON orders_pkey", "", ""},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.sql, func(t *testing.T) {
			analysis := single(t, catalog, testCase.sql)
			finding := filterV04(analysis)
			if testCase.rule == "" {
				assert.Nil(t, finding)
				return
			}
			require.NotNil(t, finding)
			assert.Equal(t, pg_contract.SeverityWarn, finding.Severity)
			assert.Equal(t, pg_contract.LevelHigh, finding.Level)
			assert.True(t, strings.HasPrefix(finding.Message, testCase.subject+" rewrites"), finding.Message)

			rule := findingOf(t, analysis, testCase.rule)
			assert.ElementsMatch(t, targetNames(&rule), targetNames(finding), "V-04 names every relation %s rewrites", testCase.rule)
			for _, target := range finding.Targets {
				assert.Equal(t, pg_contract.OpKindNone, target.OpKind, "V-04 does no work of its own on %s", target.Relation)
				assert.Equal(t, analysis.LockOn(target.Relation.Name), target.Lock, "V-04 reports the statement's lock on %s", target.Relation)
			}
		})
	}
}

func TestMaintenanceStatementsRefused(t *testing.T) {
	t.Parallel()

	const vacuumOnly = "VACUUM (FULL) ONLY orders"
	tooOld := single(t, testCatalogOn(t, pg_contract.Version17), vacuumOnly)
	errorOf(t, tooOld, gateCode)
	assert.Nil(t, filterV04(tooOld), "a refused statement never rewrites anything")
	assert.NotNil(t, filterV04(single(t, testCatalogOn(t, pg_contract.Version18), vacuumOnly)), "the server that runs it rewrites the table")
}

func TestMaintenanceStatementsTargets(t *testing.T) {
	t.Parallel()

	t.Run("a table the migration created is empty", func(t *testing.T) {
		analyses := analyze(t, testCatalog(t), "CREATE TABLE fresh (id bigint); "+
			"CREATE INDEX fresh_id_idx ON fresh (id); "+
			"CLUSTER fresh USING fresh_id_idx")
		require.Len(t, analyses, 3)
		findingOf(t, analyses[2], "R-MT-CLUSTER")
		assert.Nil(t, filterV04(analyses[2]))
	})

	t.Run("only the tables that were already there are named", func(t *testing.T) {
		analyses := analyze(t, testCatalog(t), "CREATE TABLE fresh (id bigint); VACUUM FULL fresh, orders")
		require.Len(t, analyses, 2)
		assert.Equal(t, []string{"apercu_snapshot_test.orders"}, targetNames(filterV04(analyses[1])))
	})

	t.Run("an unknown relation fails safe", func(t *testing.T) {
		analysis := single(t, testCatalog(t), "VACUUM FULL no_such_table")
		assert.Equal(t, []string{"apercu_snapshot_test.no_such_table"}, targetNames(filterV04(analysis)))
	})
}

func TestMaintenanceStatementsLevelSurvivesGrading(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(event("VACUUM FULL orders"), event("CLUSTER orders USING orders_pkey")))
	require.Len(t, analysis.Statements, 2)
	for _, statement := range analysis.Statements {
		finding := filterV04(statement)
		require.NotNilf(t, finding, "%q", statement.RawSQL)
		assert.Equal(t, pg_contract.LevelHigh, finding.Level, "the level survives the grading pass")
	}
}

func filterV05(analysis pg_contract.StatementAnalysis) *pg_contract.Finding {
	for i, finding := range analysis.Findings {
		if finding.Code == indexRenameCode {
			return &analysis.Findings[i]
		}
	}
	return nil
}

func TestIndexRenamedAsTable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sql   string
		index string
		table string
	}{
		{"ALTER TABLE orders_open_idx RENAME TO i", "apercu_snapshot_test.orders_open_idx", "apercu_snapshot_test.orders"},
		{"ALTER TABLE IF EXISTS orders_open_idx RENAME TO i", "apercu_snapshot_test.orders_open_idx", "apercu_snapshot_test.orders"},
		{"ALTER TABLE apercu_snapshot_test.orders_open_idx RENAME TO i", "apercu_snapshot_test.orders_open_idx", "apercu_snapshot_test.orders"},
		{"ALTER TABLE orders_pkey RENAME TO i", "apercu_snapshot_test.orders_pkey", "apercu_snapshot_test.orders"},
		{"ALTER TABLE events_at_idx RENAME TO i", "apercu_snapshot_test.events_at_idx", "apercu_snapshot_test.events"},
		{"ALTER TABLE orders RENAME TO orders_old", "", ""},
		{"ALTER INDEX orders_open_idx RENAME TO i", "", ""},
		{"ALTER TABLE orders RENAME COLUMN status TO state", "", ""},
		{"ALTER TABLE order_totals RENAME TO t", "", ""},
		{"ALTER TABLE active_users RENAME TO t", "", ""},
		{"ALTER TABLE users_id_seq RENAME TO s", "", ""},
		{"ALTER TABLE no_such_idx RENAME TO i", "", ""},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.sql, func(t *testing.T) {
			analysis := single(t, catalog, testCase.sql)
			finding := filterV05(analysis)
			if testCase.index == "" {
				assert.Nil(t, finding)
				return
			}
			require.NotNil(t, finding)
			assert.Equal(t, pg_contract.SeverityWarn, finding.Severity)
			assert.Equal(t, pg_contract.LevelUnset, finding.Level, "graded from the table's size and traffic, not fixed")
			assert.Contains(t, finding.Message, "every query on "+testCase.table)
			assert.Contains(t, finding.Message, "ALTER INDEX "+testCase.index+" RENAME TO i")

			rule := findingOf(t, analysis, "R-AT-RENAME")
			assert.Equal(t, targetNames(&rule), targetNames(finding), "V-05 names the index R-AT-RENAME locks")
			assert.Equal(t, []string{testCase.index}, targetNames(finding))
			for _, target := range finding.Targets {
				assert.Equal(t, pg_contract.OpKindNone, target.OpKind, "V-05 does no work of its own on %s", target.Relation)
				assert.Equal(t, pg_contract.LockAccessExclusive, target.Lock)
				assert.Equal(t, analysis.LockOn(target.Relation.Name), target.Lock, "V-05 reports the statement's lock on %s", target.Relation)
			}
		})
	}
}

func TestIndexRenamedAsTableCreatedByMigration(t *testing.T) {
	t.Parallel()

	analyses := analyze(t, testCatalog(t), "CREATE INDEX fresh_idx ON orders (status); ALTER TABLE fresh_idx RENAME TO f")
	require.Len(t, analyses, 2)
	rule := findingOf(t, analyses[1], "R-AT-RENAME")
	require.Len(t, rule.Targets, 1)
	require.True(t, rule.Targets[0].Relation.Kind.IsIndex(), "the shadow catalog knows it is an index")
	assert.Nil(t, filterV05(analyses[1]), "an index the migration created is left out")
}

func TestIndexRenamedAsTableLevelLeftForGrading(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(event("ALTER TABLE orders_open_idx RENAME TO i")))
	require.Len(t, analysis.Statements, 1)
	finding := filterV05(analysis.Statements[0])
	require.NotNil(t, finding)
	assert.Equal(t, pg_contract.LevelUnset, finding.Level)
}

func filterV06(analysis pg_contract.StatementAnalysis) *pg_contract.Finding {
	for i, finding := range analysis.Findings {
		if finding.Code == partitionOfCode {
			return &analysis.Findings[i]
		}
	}
	return nil
}

func TestPartitionCreatedInPlace(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sql              string
		parent           string
		bound            string
		defaultPartition string
	}{
		{"CREATE TABLE tb_part PARTITION OF events FOR VALUES FROM ('2027-01-01') TO ('2028-01-01')",
			"apercu_snapshot_test.events", "FOR VALUES …", "apercu_snapshot_test.events_default"},
		{"CREATE TABLE tb_part PARTITION OF apercu_snapshot_test.events FOR VALUES FROM ('2027-01-01') TO ('2028-01-01')",
			"apercu_snapshot_test.events", "FOR VALUES …", "apercu_snapshot_test.events_default"},
		{"CREATE TABLE tb_part PARTITION OF events DEFAULT",
			"apercu_snapshot_test.events", "DEFAULT", "apercu_snapshot_test.events_default"},
		{"CREATE TABLE tb_part PARTITION OF no_such_parent FOR VALUES FROM (1) TO (2)",
			"apercu_snapshot_test.no_such_parent", "FOR VALUES …", ""},
		{"CREATE TABLE tb_part (id bigint)", "", "", ""},
		{"CREATE TABLE tb_part () INHERITS (legacy_parent)", "", "", ""},
		{"ALTER TABLE events ATTACH PARTITION tb_part FOR VALUES FROM ('2027-01-01') TO ('2028-01-01')", "", "", ""},
		{"CREATE TABLE tb_part PARTITION OF orders FOR VALUES FROM (1) TO (2)", "", "", ""},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.sql, func(t *testing.T) {
			analysis := single(t, catalog, testCase.sql)
			finding := filterV06(analysis)
			if testCase.parent == "" {
				assert.Nil(t, finding)
				return
			}
			require.NotNil(t, finding)
			assert.Equal(t, pg_contract.SeverityWarn, finding.Severity)
			assert.Equal(t, pg_contract.LevelUnset, finding.Level, "graded from the table's size and traffic, not fixed")
			assert.Contains(t, finding.Message, "ALTER TABLE "+testCase.parent+" ATTACH PARTITION apercu_snapshot_test.tb_part "+testCase.bound)
			if testCase.defaultPartition != "" {
				assert.Contains(t, finding.Message, "the default partition "+testCase.defaultPartition)
			} else {
				assert.NotContains(t, finding.Message, "the default partition")
			}

			rule := findingOf(t, analysis, "R-TB-CREATE-PARTOF")
			assert.Subset(t, targetNames(&rule), targetNames(finding), "V-06 names a relation R-TB-CREATE-PARTOF locks")
			assert.Equal(t, []string{testCase.parent}, targetNames(finding), "the default partition is left out")
			for _, target := range finding.Targets {
				assert.Equal(t, pg_contract.OpKindNone, target.OpKind, "V-06 does no work of its own on %s", target.Relation)
				assert.Equal(t, pg_contract.LockAccessExclusive, target.Lock)
				assert.Equal(t, pg_contract.TargetRoleDirect, target.Role)
				assert.Equal(t, analysis.LockOn(target.Relation.Name), target.Lock, "V-06 reports the statement's lock on %s", target.Relation)
			}
		})
	}
}

func TestPartitionCreatedInPlaceCreatedByMigration(t *testing.T) {
	t.Parallel()

	analyses := analyze(t, testCatalog(t), "CREATE TABLE fresh_parted (id int) PARTITION BY RANGE (id); "+
		"CREATE TABLE fresh_1 PARTITION OF fresh_parted FOR VALUES FROM (1) TO (10)")
	require.Len(t, analyses, 2)
	findingOf(t, analyses[1], "R-TB-CREATE-PARTOF")
	assert.Nil(t, filterV06(analyses[1]), "a parent the migration created is left out")
}

func filterV07(analysis pg_contract.StatementAnalysis) *pg_contract.Finding {
	for i, finding := range analysis.Findings {
		if finding.Code == extensionScriptCode {
			return &analysis.Findings[i]
		}
	}
	return nil
}

func TestExtensionScript(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sql string
		// message is what the finding has to say, empty when V-07 must stay silent.
		message string
	}{
		{"CREATE EXTENSION pgcrypto", "CREATE EXTENSION pgcrypto runs the extension's install script"},
		{"CREATE EXTENSION IF NOT EXISTS pgcrypto", "CREATE EXTENSION pgcrypto runs the extension's install script"},
		{"CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public CASCADE", "CASCADE also installs every extension it requires"},
		{"CREATE EXTENSION plpgsql", "CREATE EXTENSION plpgsql runs the extension's install script"},
		{"ALTER EXTENSION plpgsql UPDATE", "update scripts from version 1.0 to the default version"},
		{"ALTER EXTENSION plpgsql UPDATE TO '2.0'", "update scripts from version 1.0 to version 2.0"},
		{"ALTER EXTENSION pgcrypto UPDATE TO '1.3'", "update scripts to version 1.3"},
		{"CREATE EXTENSION IF NOT EXISTS plpgsql", ""},
		{"ALTER EXTENSION plpgsql UPDATE TO '1.0'", ""},
		{"ALTER EXTENSION plpgsql SET SCHEMA s", ""},
		{"CREATE SCHEMA s", ""},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.sql, func(t *testing.T) {
			analysis := single(t, catalog, testCase.sql)
			finding := filterV07(analysis)
			if testCase.message == "" {
				assert.Nil(t, finding)
				return
			}
			require.NotNil(t, finding)
			assert.Equal(t, pg_contract.SeverityWarn, finding.Severity)
			assert.Equal(t, pg_contract.LevelLow, finding.Level, "an opaque script is LOW whatever it touches")
			assert.Empty(t, finding.Targets, "the script names nothing the statement shows")
			assert.Contains(t, finding.Message, testCase.message)
			findingOf(t, analysis, "R-OB-EXTENSION")
		})
	}
}

// TestExtensionScriptAcrossTheMigration is V-07 in a migration: each statement that runs a script is flagged on its own,
// and the statements around them are left alone.
func TestExtensionScriptAcrossTheMigration(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("CREATE EXTENSION IF NOT EXISTS plpgsql"),
		event("CREATE EXTENSION pgcrypto"),
		event("CREATE TABLE secrets (id bigint, digest bytea)"),
		event("ALTER EXTENSION pgcrypto UPDATE"),
		// Accepted over-report: C-06 does not track extensions, so the second install still reads as one.
		event("CREATE EXTENSION IF NOT EXISTS pgcrypto"),
	))
	require.Len(t, analysis.Statements, 5)

	flagged := make([]bool, 0, len(analysis.Statements))
	for _, statement := range analysis.Statements {
		finding := filterV07(statement)
		flagged = append(flagged, finding != nil)
		if finding != nil {
			assert.Equal(t, pg_contract.LevelLow, finding.Level, "%q", statement.RawSQL)
		}
	}
	assert.Equal(t, []bool{false, true, false, true, true}, flagged)
}

func TestExtensionScriptSilentStillClassified(t *testing.T) {
	t.Parallel()

	analysis := single(t, testCatalog(t), "CREATE EXTENSION IF NOT EXISTS plpgsql")
	findingOf(t, analysis, "R-OB-EXTENSION")
	assert.Nil(t, filterV07(analysis), "the server runs no script for an extension already installed")
}

func TestExtensionScriptPrefersProduction(t *testing.T) {
	t.Parallel()

	pre, err := pg_catalog.LoadJSON(filepath.Join("..", "pg_catalog", "testdata", "snapshot_pg17_preview.json.gz"))
	require.NoError(t, err)
	prod := &pg_catalog.Snapshot{
		Source:     pg_catalog.SourceProd,
		PIT:        pg_catalog.PITPre,
		Header:     pg_catalog.Header{Version: pg_contract.Version17, ServerVersionNum: 170000},
		Extensions: []pg_catalog.Extension{{Name: "pgcrypto", Version: "1.3"}},
	}
	catalog, err := pg_catalog.NewCatalog(pg_catalog.CatalogOptions{Pre: pre, Prod: prod})
	require.NoError(t, err)

	assert.Nil(t, filterV07(single(t, catalog, "CREATE EXTENSION IF NOT EXISTS pgcrypto")), "production already has it")
	assert.NotNil(t, filterV07(single(t, catalog, "CREATE EXTENSION IF NOT EXISTS plpgsql")), "the preview's plpgsql does not count")
}

func filterV03(analysis pg_contract.StatementAnalysis) *pg_contract.Finding {
	for i, finding := range analysis.Findings {
		if finding.Code == nullableKeyCode {
			return &analysis.Findings[i]
		}
	}
	return nil
}

// nullableKeyCatalog adds to the fixture a range-partitioned readings whose key at can hold NULL, a list- and a
// hash-partitioned parent alike, and tables waiting to be attached, each with its own way of proving or not proving
// the bound. The fixture's own parents all have a NOT NULL key, where the partition has to be NOT NULL too.
func nullableKeyCatalog(t *testing.T) *pg_catalog.Catalog {
	t.Helper()

	pre, err := pg_catalog.LoadJSON(filepath.Join("..", "pg_catalog", "testdata", "snapshot_pg17_preview.json.gz"))
	require.NoError(t, err)

	oid := pg_catalog.OID(9_000_000)
	table := func(name, kind, key string, notNull bool, checks ...pg_catalog.Constraint) {
		oid++
		pre.Relations = append(pre.Relations, pg_catalog.Relation{OID: oid, Namespace: "apercu_snapshot_test", Name: name,
			Kind: kind, Persistence: "p", PartitionKey: key})
		pre.Columns = append(pre.Columns,
			pg_catalog.Column{RelID: oid, Num: 1, Name: "id", FormattedType: "bigint", NotNull: true},
			pg_catalog.Column{RelID: oid, Num: 2, Name: "at", FormattedType: "integer", NotNull: notNull})
		for i, check := range checks {
			check.OID, check.RelID, check.Type, check.Enforced = oid*10+pg_catalog.OID(i), oid, "c", true
			pre.Constraints = append(pre.Constraints, check)
		}
	}
	bound := func(name string, validated bool, def string) pg_catalog.Constraint {
		return pg_catalog.Constraint{Name: name, Validated: validated, Def: def}
	}
	positive := bound("readings_at_positive", true, "CHECK ((at > 0))")

	table("readings", "p", "RANGE (at)", false, positive)
	table("readings_list", "p", "LIST (at)", false)
	table("readings_hash", "p", "HASH (at)", false)
	table("rd_bound", "r", "", false, positive, bound("rd_bound_b", true, "CHECK (((at >= 100) AND (at < 200)))"))
	table("rd_proved", "r", "", false, positive, bound("rd_proved_b", true, "CHECK (((at IS NOT NULL) AND (at >= 100) AND (at < 200)))"))
	table("rd_split", "r", "", false, positive, bound("rd_split_b", true, "CHECK (((at >= 100) AND (at < 200)))"),
		bound("rd_split_nn", true, "CHECK ((at IS NOT NULL))"))
	table("rd_notnull", "r", "", true, positive, bound("rd_notnull_b", true, "CHECK (((at >= 100) AND (at < 200)))"))
	table("rd_notvalid", "r", "", false, positive, bound("rd_notvalid_b", false, "CHECK (((at >= 100) AND (at < 200))) NOT VALID"))
	table("rd_inherited", "r", "", false, positive)
	table("rd_plain", "r", "", false)
	table("rd_list", "r", "", false, bound("rd_list_b", true, "CHECK ((at = ANY (ARRAY[1, 2])))"))
	table("rd_list_null", "r", "", false, bound("rd_list_null_b", true, "CHECK (((at IS NULL) OR (at = ANY (ARRAY[1, 2]))))"))

	catalog, err := pg_catalog.NewCatalog(pg_catalog.CatalogOptions{Pre: pre})
	require.NoError(t, err)
	return catalog
}

func TestUnprovenNullableKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		sql      string
		fires    bool
		bounding string
	}{
		{name: "a range CHECK on a nullable key omits IS NOT NULL", fires: true, bounding: "rd_bound_b",
			sql: "ALTER TABLE readings ATTACH PARTITION rd_bound FOR VALUES FROM (100) TO (200)"},
		{name: "the CHECK says IS NOT NULL",
			sql: "ALTER TABLE readings ATTACH PARTITION rd_proved FOR VALUES FROM (100) TO (200)"},
		{name: "another CHECK says IS NOT NULL, and the server reads them together",
			sql: "ALTER TABLE readings ATTACH PARTITION rd_split FOR VALUES FROM (100) TO (200)"},
		{name: "the column is NOT NULL already",
			sql: "ALTER TABLE readings ATTACH PARTITION rd_notnull FOR VALUES FROM (100) TO (200)"},
		{name: "a NOT VALID CHECK proves nothing either way",
			sql: "ALTER TABLE readings ATTACH PARTITION rd_notvalid FOR VALUES FROM (100) TO (200)"},
		{name: "the parent's own CHECK is not a bound proof",
			sql: "ALTER TABLE readings ATTACH PARTITION rd_inherited FOR VALUES FROM (100) TO (200)"},
		{name: "no CHECK at all: nothing tried to prove the bound",
			sql: "ALTER TABLE readings ATTACH PARTITION rd_plain FOR VALUES FROM (100) TO (200)"},
		{name: "a list CHECK on a nullable key omits IS NOT NULL", fires: true, bounding: "rd_list_b",
			sql: "ALTER TABLE readings_list ATTACH PARTITION rd_list FOR VALUES IN (1, 2)"},
		{name: "a list CHECK that spells the NULL branch",
			sql: "ALTER TABLE readings_list ATTACH PARTITION rd_list_null FOR VALUES IN (NULL, 1, 2)"},
		{name: "a hash bound accepts NULL",
			sql: "ALTER TABLE readings_hash ATTACH PARTITION rd_bound FOR VALUES WITH (MODULUS 2, REMAINDER 0)"},
		{name: "a default partition has no bound of its own",
			sql: "ALTER TABLE readings ATTACH PARTITION rd_bound DEFAULT"},
		{name: "the fixture's parents have a NOT NULL key",
			sql: "ALTER TABLE events ATTACH PARTITION rd_bound FOR VALUES FROM ('2027-01-01') TO ('2028-01-01')"},
		{name: "an unknown partition",
			sql: "ALTER TABLE readings ATTACH PARTITION no_such_table FOR VALUES FROM (100) TO (200)"},
	}

	catalog := nullableKeyCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			analysis := single(t, catalog, testCase.sql)
			finding := filterV03(analysis)
			if !testCase.fires {
				assert.Nil(t, finding)
				return
			}
			require.NotNil(t, finding)
			assert.Equal(t, pg_contract.SeverityWarn, finding.Severity)
			assert.Equal(t, pg_contract.LevelUnset, finding.Level, "graded from the table's size and traffic, not fixed")
			assert.Contains(t, finding.Message, "at IS NOT NULL")
			assert.Contains(t, finding.Message, testCase.bounding)
			assert.NotContains(t, finding.Message, "readings_at_positive", "the parent's CHECK is not named as the bound proof")

			rule := findingOf(t, analysis, "R-AT-ATTACH")
			require.Len(t, finding.Targets, 1, "V-03 names the partition alone")
			target := finding.Targets[0]
			assert.Subset(t, targetNames(&rule), targetNames(finding), "V-03 names a relation R-AT-ATTACH locks")
			assert.Equal(t, pg_contract.OpKindNone, target.OpKind, "V-03 does no work of its own")
			assert.Equal(t, pg_contract.LockAccessExclusive, target.Lock)
			assert.Equal(t, analysis.LockOn(target.Relation.Name), target.Lock)
		})
	}
}

func TestUnprovenNullableKeyAcrossTheMigration(t *testing.T) {
	t.Parallel()

	const attach = "ALTER TABLE readings ATTACH PARTITION rd_plain FOR VALUES FROM (100) TO (200)"
	cases := []struct {
		name   string
		before string
		fires  bool
	}{
		{name: "a CHECK added earlier omits IS NOT NULL", fires: true,
			before: "ALTER TABLE rd_plain ADD CONSTRAINT rd_plain_b CHECK (at >= 100 AND at < 200)"},
		{name: "an unnamed CHECK added earlier omits IS NOT NULL", fires: true,
			before: "ALTER TABLE rd_plain ADD CHECK (at >= 100 AND at < 200)"},
		{name: "a CHECK added earlier says IS NOT NULL",
			before: "ALTER TABLE rd_plain ADD CONSTRAINT rd_plain_b CHECK (at IS NOT NULL AND at >= 100 AND at < 200)"},
		{name: "a CHECK added NOT VALID and never validated",
			before: "ALTER TABLE rd_plain ADD CONSTRAINT rd_plain_b CHECK (at >= 100 AND at < 200) NOT VALID"},
		{name: "a CHECK added NOT VALID, then validated", fires: true,
			before: "ALTER TABLE rd_plain ADD CONSTRAINT rd_plain_b CHECK (at >= 100 AND at < 200) NOT VALID; " +
				"ALTER TABLE rd_plain VALIDATE CONSTRAINT rd_plain_b"},
		{name: "a CHECK added, then dropped",
			before: "ALTER TABLE rd_plain ADD CONSTRAINT rd_plain_b CHECK (at >= 100 AND at < 200); " +
				"ALTER TABLE rd_plain DROP CONSTRAINT rd_plain_b"},
		{name: "the column made NOT NULL earlier",
			before: "ALTER TABLE rd_plain ADD CONSTRAINT rd_plain_b CHECK (at >= 100 AND at < 200); " +
				"ALTER TABLE rd_plain ALTER COLUMN at SET NOT NULL"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			analyses := analyze(t, nullableKeyCatalog(t), testCase.before+"; "+attach)
			last := analyses[len(analyses)-1]
			findingOf(t, last, "R-AT-ATTACH")
			if testCase.fires {
				assert.NotNil(t, filterV03(last))
			} else {
				assert.Nil(t, filterV03(last))
			}
		})
	}

	snapshot := []struct {
		name   string
		before string
		sql    string
		fires  bool
	}{
		{name: "a snapshot NOT VALID CHECK validated earlier", fires: true,
			before: "ALTER TABLE rd_notvalid VALIDATE CONSTRAINT rd_notvalid_b",
			sql:    "ALTER TABLE readings ATTACH PARTITION rd_notvalid FOR VALUES FROM (100) TO (200)"},
		{name: "a snapshot CHECK dropped earlier",
			before: "ALTER TABLE rd_bound DROP CONSTRAINT rd_bound_b",
			sql:    "ALTER TABLE readings ATTACH PARTITION rd_bound FOR VALUES FROM (100) TO (200)"},
		{name: "a snapshot IS NOT NULL CHECK dropped earlier", fires: true,
			before: "ALTER TABLE rd_split DROP CONSTRAINT rd_split_nn",
			sql:    "ALTER TABLE readings ATTACH PARTITION rd_split FOR VALUES FROM (100) TO (200)"},
	}
	for _, testCase := range snapshot {
		t.Run(testCase.name, func(t *testing.T) {
			analyses := analyze(t, nullableKeyCatalog(t), testCase.before+"; "+testCase.sql)
			last := analyses[len(analyses)-1]
			if testCase.fires {
				assert.NotNil(t, filterV03(last))
			} else {
				assert.Nil(t, filterV03(last))
			}
		})
	}
}

func TestUnprovenNullableKeyCreatedByMigration(t *testing.T) {
	t.Parallel()

	analyses := analyze(t, nullableKeyCatalog(t), "CREATE TABLE fresh (id bigint NOT NULL, at int, CHECK (at >= 100 AND at < 200)); "+
		"ALTER TABLE readings ATTACH PARTITION fresh FOR VALUES FROM (100) TO (200)")
	require.Len(t, analyses, 2)
	assert.Nil(t, filterV03(analyses[1]), "a partition the migration created is empty: its scan costs nothing")
}
