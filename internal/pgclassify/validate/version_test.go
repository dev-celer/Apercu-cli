package validate_test

import (
	"slices"
	"testing"

	"apercu-cli/internal/pgclassify"
	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgclassify/validate"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errorOf is the one error carrying a rule id.
func errorOf(t *testing.T, analysis pgcontract.StatementAnalysis, code pgcontract.Code) pgcontract.Error {
	t.Helper()

	for _, err := range analysis.Errors {
		if err.Code == code {
			return err
		}
	}
	require.FailNowf(t, "no error", "%s is not among %v", code, analysis.Errors)
	return pgcontract.Error{}
}

func TestVersionGating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rule      string
		sql       string
		construct string
		floor     pgcontract.Version
	}{
		{"G-01", "ALTER TABLE orders ALTER COLUMN status SET EXPRESSION AS ('x')", "SET EXPRESSION AS", pgcontract.Version17},
		{"G-01", "ALTER TABLE orders SET ACCESS METHOD DEFAULT", "SET ACCESS METHOD DEFAULT", pgcontract.Version17},
		{"G-02", "ALTER TABLE orders ALTER COLUMN status SET STORAGE DEFAULT", "SET STORAGE DEFAULT", pgcontract.Version16},
		{"G-03", "ALTER TABLE orders ADD CONSTRAINT c CHECK (total > 0) NOT ENFORCED", "NOT ENFORCED", pgcontract.Version18},
		{"G-03", "VACUUM ONLY orders", "VACUUM ONLY", pgcontract.Version18},
		{"G-04", "ALTER TABLE orders ADD COLUMN g int GENERATED ALWAYS AS (1)", "GENERATED WITHOUT STORAGE KEYWORD", pgcontract.Version18},
	}
	for _, test := range tests {
		t.Run(test.rule+" "+test.construct, func(t *testing.T) {
			t.Parallel()

			below := classifytest.Single(t, classifytest.CatalogOn(t, test.floor-1), test.sql)
			gate := errorOf(t, below, "V-02")
			assert.Contains(t, gate.Message, test.construct)
			assert.Contains(t, gate.Message, "requires PostgreSQL "+test.floor.String())
			assert.Empty(t, below.Findings, "a statement the server refuses takes no lock")
			assert.Equal(t, pgcontract.LockNone, below.MaxLock())

			accepted := classifytest.Single(t, classifytest.CatalogOn(t, test.floor), test.sql)
			assert.Empty(t, accepted.Errors, "%s accepts it", test.floor)
		})
	}
}

func TestVersionGatingDisabledWhenNoKnownVersion(t *testing.T) {
	t.Parallel()

	catalog := classifytest.Catalog(t)
	require.Equal(t, pgcontract.VersionUnknown, catalog.Version(), "the fixture has no prod snapshot")

	analysis := pgclassify.Analyze(catalog, classifytest.Apart(classifytest.Event("ALTER TABLE orders ADD CONSTRAINT c CHECK (total > 0) NOT ENFORCED")))
	require.Len(t, analysis.Statements, 1)
	assert.Empty(t, analysis.Statements[0].Errors)
	assert.NotEmpty(t, analysis.Statements[0].Findings, "the statement is read as the newest server reads it")
	assert.Equal(t, pgcontract.Exactly(pgcontract.Version18), analysis.Versions)
}

func TestVersionGatingIgnoreNotVersionDependent(t *testing.T) {
	t.Parallel()

	unfloored := ir.Statement{Features: []ir.Feature{{Name: "MADE UP", Since: pgcontract.VersionUnknown}}}
	errors := validate.VersionGate(pgcontract.Version15, unfloored)
	assert.Empty(t, errors)

	floored := ir.Statement{Features: []ir.Feature{{Name: "MADE UP", Since: pgcontract.Version18}}}
	errors = validate.VersionGate(pgcontract.Version15, floored)
	require.Len(t, errors, 1)
	assert.Equal(t, pgcontract.Code("V-02"), errors[0].Code)
}

func TestMigrationVersion(t *testing.T) {
	t.Parallel()

	plain := pgclassify.Analyze(classifytest.Catalog(t), classifytest.Apart(classifytest.Event("ALTER TABLE orders ADD COLUMN z int")))
	assert.Equal(t, pgcontract.Between(pgcontract.MinSupportedVersion, pgcontract.MaxSupportedVersion), plain.Versions,
		"nothing version-bound, so every supported server runs it")

	mixed := pgclassify.Analyze(classifytest.Catalog(t), classifytest.Apart(
		classifytest.Event("ALTER TABLE orders ALTER COLUMN status SET STORAGE DEFAULT"),
		classifytest.Event("VACUUM ONLY orders"),
	))
	assert.Equal(t, pgcontract.Exactly(pgcontract.Version18), mixed.Versions, "the highest floor wins")
	assert.False(t, mixed.Versions.IsEmpty())
}

func TestMigrationVersionImpossible(t *testing.T) {
	t.Parallel()

	analysis := pgclassify.Analyze(classifytest.Catalog(t), classifytest.Apart(
		classifytest.Event("VACUUM ONLY orders"),
		classifytest.Event("ALTER TABLE events SET UNLOGGED"),
	))

	require.Len(t, analysis.Statements, 2)
	capped := errorOf(t, analysis.Statements[1], "R-AT-LOGGED")
	require.Equal(t, pgcontract.AtMost(pgcontract.Version17), capped.Versions, "G-05 only errors from 18")
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
		refusedOn  pgcontract.Version
		acceptedOn pgcontract.Version
	}{
		{
			rule: "G-01",
			statements: []string{
				"ALTER TABLE orders ADD COLUMN z int",
				"ALTER TABLE orders ALTER COLUMN z SET STATISTICS DEFAULT",
				"CREATE INDEX orders_z_idx ON orders (z)",
			},
			refused: 1, refusedOn: pgcontract.Version16, acceptedOn: pgcontract.Version17,
		},
		{
			rule: "G-02",
			statements: []string{
				"ALTER TABLE orders ADD COLUMN z text",
				"ALTER TABLE orders ALTER COLUMN z SET STORAGE DEFAULT",
				"ALTER TABLE orders ALTER COLUMN code SET STORAGE EXTERNAL",
			},
			refused: 1, refusedOn: pgcontract.Version15, acceptedOn: pgcontract.Version16,
		},
		{
			rule: "G-03 refused outright",
			statements: []string{
				"ALTER TABLE orders ADD CONSTRAINT orders_code_nn NOT NULL code NOT VALID",
				"ALTER TABLE orders VALIDATE CONSTRAINT orders_code_nn",
			},
			refused: 0, refusedOn: pgcontract.Version17, acceptedOn: pgcontract.Version18,
		},
		{
			rule: "G-03 semantic",
			statements: []string{
				"ALTER TABLE ONLY events DROP CONSTRAINT c",
				"ALTER TABLE events_2025 DROP CONSTRAINT c",
			},
			refused: 0, refusedOn: pgcontract.Version17, acceptedOn: pgcontract.Version18,
		},
		{
			rule: "G-04",
			statements: []string{
				"ALTER TABLE orders ADD COLUMN g bigint GENERATED ALWAYS AS (id * 2)",
				"ALTER TABLE orders ADD COLUMN h int",
			},
			refused: 0, refusedOn: pgcontract.Version17, acceptedOn: pgcontract.Version18,
		},
		{
			rule: "G-05",
			statements: []string{
				"ALTER TABLE events SET UNLOGGED",
				"ALTER TABLE events ADD COLUMN z int",
			},
			refused: 0, refusedOn: pgcontract.Version18, acceptedOn: pgcontract.Version17,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.rule, func(t *testing.T) {
			t.Parallel()

			events := make([]pgcontract.QueryEvent, 0, len(scenario.statements))
			for _, sql := range scenario.statements {
				events = append(events, classifytest.Event(sql))
			}

			refusing := pgclassify.Analyze(classifytest.CatalogOn(t, scenario.refusedOn), classifytest.Cycle(0, slices.Clone(events)...))
			require.Len(t, refusing.Statements, len(scenario.statements))
			for i, statement := range refusing.Statements {
				if i == scenario.refused {
					assert.NotEmptyf(t, statement.Errors, "%s refuses %q", scenario.refusedOn, statement.RawSQL)
					assert.Emptyf(t, classifytest.RulesOnly(statement).Findings, "%q never runs, so it takes no lock", statement.RawSQL)
					continue
				}
				assert.Emptyf(t, statement.Errors, "%q is not the refused one", statement.RawSQL)
				assert.NotEmptyf(t, classifytest.RulesOnly(statement).Findings, "%q is classified as if the refused one were not there", statement.RawSQL)
			}
			for _, envelope := range refusing.Envelopes {
				assert.NotEqualf(t, scenario.refused, envelope.OpenedBy, "the refused statement opens no envelope: %v", envelope)
			}

			accepting := pgclassify.Analyze(classifytest.CatalogOn(t, scenario.acceptedOn), classifytest.Cycle(0, slices.Clone(events)...))
			assert.Empty(t, accepting.Errors(), "%s runs the whole migration", scenario.acceptedOn)
			assert.NotEmpty(t, classifytest.RulesOnly(accepting.Statements[scenario.refused]).Findings)
		})
	}
}

// TestGeneratedWithoutStorageKeywordIsVirtualOn18 is G-04's other half: 18 reads the bare form as VIRTUAL,
// which stores nothing, so no existing row is touched.
func TestGeneratedWithoutStorageKeywordIsVirtualOn18(t *testing.T) {
	t.Parallel()

	catalog := classifytest.CatalogOn(t, pgcontract.Version18)
	for _, sql := range []string{
		"ALTER TABLE orders ADD COLUMN g bigint GENERATED ALWAYS AS (id * 2)",
		"ALTER TABLE orders ADD COLUMN g bigint GENERATED ALWAYS AS (id * 2) VIRTUAL",
	} {
		t.Run(sql, func(t *testing.T) {
			analysis := classifytest.Single(t, catalog, sql)
			require.Empty(t, analysis.Errors)
			finding := classifytest.FindingOf(t, analysis, "R-AT-ADDCOL")
			orders := classifytest.TargetOf(t, finding, "apercu_snapshot_test.orders")
			assert.Equal(t, pgcontract.OpKindMetadata, orders.OpKind)
			assert.Equal(t, pgcontract.LockAccessExclusive, orders.Lock)
			assert.Contains(t, finding.Message, "virtual generated column")
		})
	}
}

// TestSetUnloggedOnPartitionedTableBefore18 is G-05's other half: 15-17 accept the statement and change nothing.
func TestSetUnloggedOnPartitionedTableBefore18(t *testing.T) {
	t.Parallel()

	for _, version := range []pgcontract.Version{pgcontract.Version15, pgcontract.Version16, pgcontract.Version17} {
		for _, sql := range []string{"ALTER TABLE events SET UNLOGGED", "ALTER TABLE events SET LOGGED"} {
			t.Run(version.String()+" "+sql, func(t *testing.T) {
				analysis := classifytest.Single(t, classifytest.CatalogOn(t, version), sql)
				require.Empty(t, analysis.Errors, "%s accepts it", version)
				finding := classifytest.FindingOf(t, analysis, "R-AT-LOGGED")
				assert.Contains(t, finding.Message, "silently do nothing")
				for _, target := range finding.Targets {
					assert.Equalf(t, pgcontract.OpKindMetadata, target.OpKind, "%s is neither read nor rewritten", target.Relation.Name)
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
		version pgcontract.Version
		// analyzed is the lock ANALYZE holds the child at, vacuumed whether VACUUM reaches it.
		analyzed pgcontract.Lock
		vacuumed bool
	}{
		{pgcontract.Version15, pgcontract.LockAccessShare, false},
		{pgcontract.Version17, pgcontract.LockAccessShare, false},
		{pgcontract.Version18, pgcontract.LockShareUpdateExclusive, true},
	}
	for _, testCase := range cases {
		t.Run(testCase.version.String(), func(t *testing.T) {
			analysis := pgclassify.Analyze(classifytest.CatalogOn(t, testCase.version), classifytest.Apart(
				classifytest.Event("ALTER TABLE legacy_parent ADD COLUMN z int"),
				classifytest.Event("ANALYZE legacy_parent"),
				classifytest.Event("VACUUM legacy_parent"),
			))
			require.Len(t, analysis.Statements, 3)
			require.Empty(t, analysis.Errors())

			analyzed := classifytest.FindingOf(t, analysis.Statements[1], "R-MT-ANALYZE")
			assert.Equal(t, testCase.analyzed, classifytest.TargetOf(t, analyzed, child).Lock)

			vacuumed := classifytest.FindingOf(t, analysis.Statements[2], "R-MT-VACUUM")
			reached := slices.ContainsFunc(vacuumed.Targets, func(target pgcontract.Target) bool {
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
		version pgcontract.Version
		lock    pgcontract.Lock
	}{
		{pgcontract.Version15, pgcontract.LockShare},
		{pgcontract.Version17, pgcontract.LockShare},
		{pgcontract.Version18, pgcontract.LockAccessExclusive},
	}
	for _, testCase := range cases {
		t.Run(testCase.version.String(), func(t *testing.T) {
			analysis := pgclassify.Analyze(classifytest.CatalogOn(t, testCase.version), classifytest.Apart(
				classifytest.Event("ALTER TABLE events ALTER COLUMN id SET NOT NULL"),
				classifytest.Event("ALTER TABLE events ADD CONSTRAINT pk PRIMARY KEY (id, at)"),
			))
			require.Len(t, analysis.Statements, 2)
			require.Empty(t, analysis.Errors())

			finding := classifytest.FindingOf(t, analysis.Statements[1], "R-AT-ADDPK")
			partition := classifytest.TargetOf(t, finding, "apercu_snapshot_test.events_2025")
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
		code   pgcontract.Code
		target string
		lock   string
		op     pgcontract.OpKind
	}
	predict := func(t *testing.T, version pgcontract.Version, sql string) []prediction {
		analysis := classifytest.Single(t, classifytest.CatalogOn(t, version), sql)
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

			reference := predict(t, pgcontract.MinSupportedVersion, sql)
			require.NotEmpty(t, reference)
			for version := pgcontract.MinSupportedVersion + 1; version <= pgcontract.MaxSupportedVersion; version++ {
				assert.Equalf(t, reference, predict(t, version, sql), "%q on %s against %s", sql, version, pgcontract.MinSupportedVersion)
			}
		})
	}
}
