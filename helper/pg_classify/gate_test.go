package pg_classify

import (
	"slices"
	"testing"

	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errorOf is the one error carrying a rule id.
func errorOf(t *testing.T, analysis pg_contract.StatementAnalysis, code pg_contract.Code) pg_contract.Error {
	t.Helper()

	for _, err := range analysis.Errors {
		if err.Code == code {
			return err
		}
	}
	require.FailNowf(t, "no error", "%s is not among %v", code, analysis.Errors)
	return pg_contract.Error{}
}

func TestVersionGating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rule      string
		sql       string
		construct string
		floor     pg_contract.Version
	}{
		{"G-01", "ALTER TABLE orders ALTER COLUMN status SET EXPRESSION AS ('x')", "SET EXPRESSION AS", pg_contract.Version17},
		{"G-01", "ALTER TABLE orders SET ACCESS METHOD DEFAULT", "SET ACCESS METHOD DEFAULT", pg_contract.Version17},
		{"G-02", "ALTER TABLE orders ALTER COLUMN status SET STORAGE DEFAULT", "SET STORAGE DEFAULT", pg_contract.Version16},
		{"G-03", "ALTER TABLE orders ADD CONSTRAINT c CHECK (total > 0) NOT ENFORCED", "NOT ENFORCED", pg_contract.Version18},
		{"G-03", "VACUUM ONLY orders", "VACUUM ONLY", pg_contract.Version18},
		{"G-04", "ALTER TABLE orders ADD COLUMN g int GENERATED ALWAYS AS (1)", "GENERATED WITHOUT STORAGE KEYWORD", pg_contract.Version18},
	}
	for _, test := range tests {
		t.Run(test.rule+" "+test.construct, func(t *testing.T) {
			t.Parallel()

			below := single(t, testCatalogOn(t, test.floor-1), test.sql)
			gate := errorOf(t, below, "V-02")
			assert.Contains(t, gate.Message, test.construct)
			assert.Contains(t, gate.Message, "requires PostgreSQL "+test.floor.String())
			assert.Empty(t, below.Findings, "a statement the server refuses takes no lock")
			assert.Equal(t, pg_contract.LockNone, below.MaxLock())

			accepted := single(t, testCatalogOn(t, test.floor), test.sql)
			assert.Empty(t, accepted.Errors, "%s accepts it", test.floor)
		})
	}
}

func TestVersionGatingDisabledWhenNoKnownVersion(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	require.Equal(t, pg_contract.VersionUnknown, catalog.Version(), "the fixture has no prod snapshot")

	analysis := Analyze(catalog, apart(event("ALTER TABLE orders ADD CONSTRAINT c CHECK (total > 0) NOT ENFORCED")))
	require.Len(t, analysis.Statements, 1)
	assert.Empty(t, analysis.Statements[0].Errors)
	assert.NotEmpty(t, analysis.Statements[0].Findings, "the statement is read as the newest server reads it")
	assert.Equal(t, pg_contract.Exactly(pg_contract.Version18), analysis.Versions)
}

func TestVersionGatingIgnoreNotVersionDependent(t *testing.T) {
	t.Parallel()

	unfloored := pg_parse.Statement{Features: []pg_parse.Feature{{Name: "MADE UP", Since: pg_contract.VersionUnknown}}}
	errors := versionGate(pg_contract.Version15, unfloored)
	assert.Empty(t, errors)

	floored := pg_parse.Statement{Features: []pg_parse.Feature{{Name: "MADE UP", Since: pg_contract.Version18}}}
	errors = versionGate(pg_contract.Version15, floored)
	require.Len(t, errors, 1)
	assert.Equal(t, pg_contract.Code("V-02"), errors[0].Code)
}

func TestMigrationVersion(t *testing.T) {
	t.Parallel()

	plain := Analyze(testCatalog(t), apart(event("ALTER TABLE orders ADD COLUMN z int")))
	assert.Equal(t, pg_contract.Between(pg_contract.MinSupportedVersion, pg_contract.MaxSupportedVersion), plain.Versions,
		"nothing version-bound, so every supported server runs it")

	mixed := Analyze(testCatalog(t), apart(
		event("ALTER TABLE orders ALTER COLUMN status SET STORAGE DEFAULT"),
		event("VACUUM ONLY orders"),
	))
	assert.Equal(t, pg_contract.Exactly(pg_contract.Version18), mixed.Versions, "the highest floor wins")
	assert.False(t, mixed.Versions.IsEmpty())
}

func TestMigrationVersionImpossible(t *testing.T) {
	t.Parallel()

	analysis := Analyze(testCatalog(t), apart(
		event("VACUUM ONLY orders"),
		event("ALTER TABLE events SET UNLOGGED"),
	))

	require.Len(t, analysis.Statements, 2)
	capped := errorOf(t, analysis.Statements[1], "R-AT-LOGGED")
	require.Equal(t, pg_contract.AtMost(pg_contract.Version17), capped.Versions, "G-05 only errors from 18")
	assert.True(t, analysis.Versions.IsEmpty(), "18 for the VACUUM ONLY, 17 at most for the SET UNLOGGED: %s", analysis.Versions)
}

// TestVersionGateScenarios runs each refusal in a migration of several statements, one implicit transaction, once
// on a server that refuses it and once on one that runs it: the refused statement carries the error and no finding, and the statements
// around it are classified as if it were not there.
func TestVersionGateScenarios(t *testing.T) {
	t.Parallel()

	scenarios := []struct {
		rule       string
		statements []string
		// refused is the statement the refusing server rejects.
		refused    int
		refusedOn  pg_contract.Version
		acceptedOn pg_contract.Version
	}{
		{
			rule: "G-01",
			statements: []string{
				"ALTER TABLE orders ADD COLUMN z int",
				"ALTER TABLE orders ALTER COLUMN z SET STATISTICS DEFAULT",
				"CREATE INDEX orders_z_idx ON orders (z)",
			},
			refused: 1, refusedOn: pg_contract.Version16, acceptedOn: pg_contract.Version17,
		},
		{
			rule: "G-02",
			statements: []string{
				"ALTER TABLE orders ADD COLUMN z text",
				"ALTER TABLE orders ALTER COLUMN z SET STORAGE DEFAULT",
				"ALTER TABLE orders ALTER COLUMN code SET STORAGE EXTERNAL",
			},
			refused: 1, refusedOn: pg_contract.Version15, acceptedOn: pg_contract.Version16,
		},
		{
			rule: "G-03 refused outright",
			statements: []string{
				"ALTER TABLE orders ADD CONSTRAINT orders_code_nn NOT NULL code NOT VALID",
				"ALTER TABLE orders VALIDATE CONSTRAINT orders_code_nn",
			},
			refused: 0, refusedOn: pg_contract.Version17, acceptedOn: pg_contract.Version18,
		},
		{
			rule: "G-03 semantic",
			statements: []string{
				"ALTER TABLE ONLY events DROP CONSTRAINT c",
				"ALTER TABLE events_2025 DROP CONSTRAINT c",
			},
			refused: 0, refusedOn: pg_contract.Version17, acceptedOn: pg_contract.Version18,
		},
		{
			rule: "G-04",
			statements: []string{
				"ALTER TABLE orders ADD COLUMN g bigint GENERATED ALWAYS AS (id * 2)",
				"ALTER TABLE orders ADD COLUMN h int",
			},
			refused: 0, refusedOn: pg_contract.Version17, acceptedOn: pg_contract.Version18,
		},
		{
			rule: "G-05",
			statements: []string{
				"ALTER TABLE events SET UNLOGGED",
				"ALTER TABLE events ADD COLUMN z int",
			},
			refused: 0, refusedOn: pg_contract.Version18, acceptedOn: pg_contract.Version17,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.rule, func(t *testing.T) {
			t.Parallel()

			events := make([]pg_contract.QueryEvent, 0, len(scenario.statements))
			for _, sql := range scenario.statements {
				events = append(events, event(sql))
			}

			refusing := Analyze(testCatalogOn(t, scenario.refusedOn), cycle(0, slices.Clone(events)...))
			require.Len(t, refusing.Statements, len(scenario.statements))
			for i, statement := range refusing.Statements {
				if i == scenario.refused {
					assert.NotEmptyf(t, statement.Errors, "%s refuses %q", scenario.refusedOn, statement.RawSQL)
					assert.Emptyf(t, rulesOnly(statement).Findings, "%q never runs, so it takes no lock", statement.RawSQL)
					continue
				}
				assert.Emptyf(t, statement.Errors, "%q is not the refused one", statement.RawSQL)
				assert.NotEmptyf(t, rulesOnly(statement).Findings, "%q is classified as if the refused one were not there", statement.RawSQL)
			}
			for _, envelope := range refusing.Envelopes {
				assert.NotEqualf(t, scenario.refused, envelope.OpenedBy, "the refused statement opens no envelope: %v", envelope)
			}

			accepting := Analyze(testCatalogOn(t, scenario.acceptedOn), cycle(0, slices.Clone(events)...))
			assert.Empty(t, accepting.Errors(), "%s runs the whole migration", scenario.acceptedOn)
			assert.NotEmpty(t, rulesOnly(accepting.Statements[scenario.refused]).Findings)
		})
	}
}

// TestGeneratedWithoutStorageKeywordIsVirtualOn18 is G-04's other half: 18 reads the bare form as VIRTUAL,
// which stores nothing, so no existing row is touched.
func TestGeneratedWithoutStorageKeywordIsVirtualOn18(t *testing.T) {
	t.Parallel()

	catalog := testCatalogOn(t, pg_contract.Version18)
	for _, sql := range []string{
		"ALTER TABLE orders ADD COLUMN g bigint GENERATED ALWAYS AS (id * 2)",
		"ALTER TABLE orders ADD COLUMN g bigint GENERATED ALWAYS AS (id * 2) VIRTUAL",
	} {
		t.Run(sql, func(t *testing.T) {
			analysis := single(t, catalog, sql)
			require.Empty(t, analysis.Errors)
			finding := findingOf(t, analysis, "R-AT-ADDCOL")
			orders := targetOf(t, finding, "apercu_snapshot_test.orders")
			assert.Equal(t, pg_contract.OpKindMetadata, orders.OpKind)
			assert.Equal(t, pg_contract.LockAccessExclusive, orders.Lock)
			assert.Contains(t, finding.Message, "virtual generated column")
		})
	}
}

// TestSetUnloggedOnPartitionedTableBefore18 is G-05's other half: 15-17 accept the statement and change nothing.
func TestSetUnloggedOnPartitionedTableBefore18(t *testing.T) {
	t.Parallel()

	for _, version := range []pg_contract.Version{pg_contract.Version15, pg_contract.Version16, pg_contract.Version17} {
		for _, sql := range []string{"ALTER TABLE events SET UNLOGGED", "ALTER TABLE events SET LOGGED"} {
			t.Run(version.String()+" "+sql, func(t *testing.T) {
				analysis := single(t, testCatalogOn(t, version), sql)
				require.Empty(t, analysis.Errors, "%s accepts it", version)
				finding := findingOf(t, analysis, "R-AT-LOGGED")
				assert.Contains(t, finding.Message, "silently do nothing")
				for _, target := range finding.Targets {
					assert.Equalf(t, pg_contract.OpKindMetadata, target.OpKind, "%s is neither read nor rewritten", target.Relation.Name)
				}
			})
		}
	}
}

// TestInheritanceExpansionScenario is G-06 in a migration: from 18 on, VACUUM and ANALYZE work through a classic
// parent's children in their own right; before, VACUUM stops at the parent and ANALYZE only samples them.
func TestInheritanceExpansionScenario(t *testing.T) {
	t.Parallel()

	const child = "apercu_snapshot_test.legacy_child"
	cases := []struct {
		version pg_contract.Version
		// analyzed is the lock ANALYZE holds the child at, vacuumed whether VACUUM reaches it.
		analyzed pg_contract.Lock
		vacuumed bool
	}{
		{pg_contract.Version15, pg_contract.LockAccessShare, false},
		{pg_contract.Version17, pg_contract.LockAccessShare, false},
		{pg_contract.Version18, pg_contract.LockShareUpdateExclusive, true},
	}
	for _, testCase := range cases {
		t.Run(testCase.version.String(), func(t *testing.T) {
			analysis := Analyze(testCatalogOn(t, testCase.version), apart(
				event("ALTER TABLE legacy_parent ADD COLUMN z int"),
				event("ANALYZE legacy_parent"),
				event("VACUUM legacy_parent"),
			))
			require.Len(t, analysis.Statements, 3)
			require.Empty(t, analysis.Errors())

			analyzed := findingOf(t, analysis.Statements[1], "R-MT-ANALYZE")
			assert.Equal(t, testCase.analyzed, targetOf(t, analyzed, child).Lock)

			vacuumed := findingOf(t, analysis.Statements[2], "R-MT-VACUUM")
			reached := slices.ContainsFunc(vacuumed.Targets, func(target pg_contract.Target) bool {
				return target.Relation.Name.String() == child
			})
			assert.Equal(t, testCase.vacuumed, reached, "VACUUM targets %v", vacuumed.Targets)
		})
	}
}

// TestPrimaryKeyScenario is G-07 in a migration: a SET NOT NULL earlier in the file leaves every key column NOT NULL,
// so 15-17 hold the partitions at SHARE for the key, where 18 holds them at ACCESS EXCLUSIVE.
func TestPrimaryKeyScenario(t *testing.T) {
	t.Parallel()

	cases := []struct {
		version pg_contract.Version
		lock    pg_contract.Lock
	}{
		{pg_contract.Version15, pg_contract.LockShare},
		{pg_contract.Version17, pg_contract.LockShare},
		{pg_contract.Version18, pg_contract.LockAccessExclusive},
	}
	for _, testCase := range cases {
		t.Run(testCase.version.String(), func(t *testing.T) {
			analysis := Analyze(testCatalogOn(t, testCase.version), apart(
				event("ALTER TABLE events ALTER COLUMN id SET NOT NULL"),
				event("ALTER TABLE events ADD CONSTRAINT pk PRIMARY KEY (id, at)"),
			))
			require.Len(t, analysis.Statements, 2)
			require.Empty(t, analysis.Errors())

			finding := findingOf(t, analysis.Statements[1], "R-AT-ADDPK")
			partition := targetOf(t, finding, "apercu_snapshot_test.events_2025")
			assert.Equal(t, testCase.lock.Short(), partition.Lock.Short())
		})
	}
}

// TestLocksDoNotFollowTheVersion is G-08: outside the §8 list, a statement takes the same locks and does the same
// work on every supported server.
func TestLocksDoNotFollowTheVersion(t *testing.T) {
	t.Parallel()

	statements := []string{
		"ALTER TABLE orders ADD COLUMN z int",
		"ALTER TABLE orders ADD COLUMN z int DEFAULT 0",
		"ALTER TABLE orders ALTER COLUMN status TYPE varchar(20)",
		"ALTER TABLE orders ALTER COLUMN status SET NOT NULL",
		"ALTER TABLE orders ADD CONSTRAINT c CHECK (total > 0) NOT VALID",
		"ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users (id)",
		"ALTER TABLE orders SET LOGGED",
		"ALTER TABLE orders RENAME COLUMN code TO reference",
		"ALTER TABLE events ADD COLUMN z int",
		"ALTER TABLE events DETACH PARTITION events_2025",
		"ALTER TABLE legacy_parent ADD COLUMN z int",
		"CREATE INDEX orders_status_idx ON orders (status)",
		"CREATE INDEX CONCURRENTLY orders_status_idx ON orders (status)",
		"DROP INDEX orders_open_idx",
		"REINDEX TABLE orders",
		"TRUNCATE orders",
		"VACUUM FULL orders",
		"CLUSTER orders USING orders_pkey",
		"UPDATE orders SET status = 'done'",
		"CREATE TABLE fresh (id bigint PRIMARY KEY)",
		"DROP TABLE legacy_child",
	}

	type prediction struct {
		code   pg_contract.Code
		target string
		lock   string
		op     pg_contract.OpKind
	}
	predict := func(t *testing.T, version pg_contract.Version, sql string) []prediction {
		analysis := single(t, testCatalogOn(t, version), sql)
		require.Empty(t, analysis.Errors, "%s runs %q", version, sql)
		var out []prediction
		for _, finding := range analysis.Findings {
			out = append(out, prediction{code: finding.Code, lock: finding.MaxLock().Short(), op: finding.MaxOpKind()})
			for _, target := range finding.Targets {
				out = append(out, prediction{code: finding.Code, target: target.Relation.Name.String(), lock: target.Lock.Short(), op: target.OpKind})
			}
		}
		return out
	}

	for _, sql := range statements {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()

			reference := predict(t, pg_contract.MinSupportedVersion, sql)
			require.NotEmpty(t, reference)
			for version := pg_contract.MinSupportedVersion + 1; version <= pg_contract.MaxSupportedVersion; version++ {
				assert.Equalf(t, reference, predict(t, version, sql), "%q on %s against %s", sql, version, pg_contract.MinSupportedVersion)
			}
		})
	}
}
