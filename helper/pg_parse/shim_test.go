package pg_parse

import (
	"testing"

	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShimRecognisesPg18Syntax(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		sql     string
		feature FeatureName
	}{
		{"a constraint that is never checked", "ALTER TABLE t ADD CONSTRAINT c CHECK (x > 0) NOT ENFORCED", FeatureNotEnforced},
		{"turning enforcement back on", "ALTER TABLE t ALTER CONSTRAINT c ENFORCED", FeatureNotEnforced},
		{"NOT NULL as a table constraint", "ALTER TABLE t ADD CONSTRAINT c NOT NULL a NOT VALID", FeatureNotNullConstraint},
		{"an unnamed NOT NULL table constraint", "ALTER TABLE t ADD NOT NULL a", FeatureNotNullConstraint},
		{"a constraint that starts inheriting", "ALTER TABLE t ALTER CONSTRAINT c INHERIT", FeatureConstraintInherit},
		{"a constraint that stops inheriting", "ALTER TABLE t ALTER CONSTRAINT c NO INHERIT", FeatureConstraintInherit},
		{"a virtual generated column", "ALTER TABLE t ADD COLUMN g int GENERATED ALWAYS AS (a + 1) VIRTUAL", FeatureVirtualGenerated},
		{"a generated column with no storage keyword", "ALTER TABLE t ADD COLUMN g int GENERATED ALWAYS AS (a + 1)", FeatureBareGenerated},
		{"a temporal primary key", "ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)", FeatureWithoutOverlaps},
		{"a temporal foreign key", "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (id, PERIOD v) REFERENCES r (id, PERIOD v)", FeatureForeignKeyPeriod},
		{"vacuum that does not recurse", "VACUUM ONLY t", FeatureVacuumOnly},
		{"analyze that does not recurse", "ANALYZE ONLY t", FeatureAnalyzeOnly},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			statement := ParseOne(tc.sql)
			require.Truef(t, statement.Parsed(), "did not parse: %s", statement.Unparsed)
			assert.Truef(t, statement.Uses(tc.feature), "expected feature %s, got %v", tc.feature, statement.Features)
			assert.Equal(t, pg_contract.Version18, statement.MinVersion())
		})
	}
}

func TestShimIgnoresLookalikes(t *testing.T) {
	t.Parallel()

	for _, sql := range []string{
		"ALTER TABLE t ADD COLUMN virtual int",
		"ALTER TABLE t ADD COLUMN period int",
		"ALTER TABLE t ADD COLUMN enforced int",
		"ALTER TABLE t INHERIT p",
		"ALTER TABLE t NO INHERIT p",
		"ALTER TABLE t ADD CONSTRAINT c CHECK (x > 0) NO INHERIT",
		"ALTER TABLE ONLY t ADD COLUMN a int",
		"ALTER TABLE t ADD COLUMN a int NOT NULL",
		"ALTER TABLE t ALTER COLUMN a SET NOT NULL",
		"TRUNCATE ONLY t",
		"SELECT * FROM ONLY t",
		"ALTER TABLE t ADD COLUMN g int GENERATED ALWAYS AS (a + 1) STORED",
		"ALTER TABLE t ADD COLUMN g int GENERATED ALWAYS AS IDENTITY",
		"INSERT INTO t VALUES ('NOT ENFORCED', 'WITHOUT OVERLAPS')",
	} {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()

			statement := ParseOne(sql)
			require.Truef(t, statement.Parsed(), "did not parse: %s", statement.Unparsed)
			assert.Emptyf(t, statement.Features, "shim fired on syntax every version accepts")
			assert.Equal(t, pg_contract.MinSupportedVersion, statement.MinVersion())
		})
	}
}

func TestShimAttachesToTheRightClause(t *testing.T) {
	t.Parallel()

	statement := ParseOne("ALTER TABLE t " +
		"ADD CONSTRAINT c1 CHECK (x > 0), " +
		"ADD CONSTRAINT c2 CHECK (y > 0) NOT ENFORCED, " +
		"ADD COLUMN g int GENERATED ALWAYS AS (a + 1) VIRTUAL, " +
		"ADD COLUMN h int GENERATED ALWAYS AS (a + 2) STORED")
	require.True(t, statement.Parsed())
	require.Len(t, statement.Subcommands, 4)

	assert.False(t, statement.Subcommands[0].Constraint.NotEnforced, "c1 was written without NOT ENFORCED")
	assert.True(t, statement.Subcommands[1].Constraint.NotEnforced)
	assert.Equal(t, GeneratedVirtual, statement.Subcommands[2].Column.Generated)
	assert.Equal(t, GeneratedStored, statement.Subcommands[3].Column.Generated, "h was written STORED")
}

// TestShimAttachesInsideACreateTable: a CREATE TABLE keeps its elements inside parenthesis,
// so the clause a construct belongs to is counted there rather than at the top level.
func TestShimAttachesInsideACreateTable(t *testing.T) {
	t.Parallel()

	statement := ParseOne("CREATE TABLE t (a int, g int GENERATED ALWAYS AS (a + 1) VIRTUAL, b int)")
	require.True(t, statement.Parsed())
	require.Len(t, statement.Subcommands, 3)

	assert.Equal(t, GeneratedNone, statement.Subcommands[0].Column.Generated)
	assert.Equal(t, GeneratedVirtual, statement.Subcommands[1].Column.Generated)
	assert.Equal(t, GeneratedNone, statement.Subcommands[2].Column.Generated)
}

// TestShimKeepsWhatItBlanksOut validate that the shim is capable of reconstructing the original statement intent.
func TestShimKeepsWhatItBlanksOut(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		feature FeatureName
		sql     string
		residue string
		check   func(t *testing.T, restored, residue Statement)
	}{
		{
			name:    "NOT ENFORCED constraint is not validated",
			feature: FeatureNotEnforced,
			sql:     "ALTER TABLE t ADD CONSTRAINT c CHECK (x > 0) NOT ENFORCED",
			residue: "ALTER TABLE t ADD CONSTRAINT c CHECK (x > 0)",
			check: func(t *testing.T, restored, residue Statement) {
				assert.True(t, constraintOf(t, restored).NotEnforced)
				assert.False(t, constraintOf(t, restored).Validated())
				assert.False(t, constraintOf(t, residue).NotEnforced)
				assert.True(t, constraintOf(t, residue).Validated())
			},
		},
		{
			name:    "NOT ENFORCED constraint",
			feature: FeatureNotEnforced,
			sql:     "ALTER TABLE t ALTER CONSTRAINT c NOT ENFORCED",
			residue: "ALTER TABLE t ALTER CONSTRAINT c",
			check: func(t *testing.T, restored, residue Statement) {
				assert.True(t, constraintOf(t, restored).EnforcementSet)
				assert.True(t, constraintOf(t, restored).NotEnforced)
				assert.False(t, constraintOf(t, restored).DeferralSet, "the clause named no deferral")
				assert.False(t, constraintOf(t, residue).EnforcementSet)
			},
		},
		{
			name:    "ENFORCED constraint",
			feature: FeatureNotEnforced,
			sql:     "ALTER TABLE t ALTER CONSTRAINT c ENFORCED",
			residue: "ALTER TABLE t ALTER CONSTRAINT c",
			check: func(t *testing.T, restored, residue Statement) {
				assert.True(t, constraintOf(t, restored).EnforcementSet)
				assert.False(t, constraintOf(t, restored).NotEnforced)
				assert.False(t, constraintOf(t, residue).EnforcementSet)
			},
		},
		{
			name:    "a NOT NULL constraint",
			feature: FeatureNotNullConstraint,
			sql:     "ALTER TABLE t ADD CONSTRAINT c NOT NULL a",
			residue: "ALTER TABLE t ADD CONSTRAINT c CHECK (a IS NOT NULL)",
			check: func(t *testing.T, restored, residue Statement) {
				assert.Equal(t, ConstraintNotNull, constraintOf(t, restored).Type)
				assert.Equal(t, []string{"a"}, constraintOf(t, restored).Columns)
				assert.Nil(t, constraintOf(t, restored).Expr, "the substituted CHECK body must not survive")
				assert.Equal(t, ConstraintCheck, constraintOf(t, residue).Type)
			},
		},
		{
			name:    "constraint inheriting",
			feature: FeatureConstraintInherit,
			sql:     "ALTER TABLE t ALTER CONSTRAINT c INHERIT",
			residue: "ALTER TABLE t ALTER CONSTRAINT c",
			check: func(t *testing.T, restored, residue Statement) {
				assert.True(t, constraintOf(t, restored).NoInheritSet)
				assert.True(t, constraintOf(t, restored).Inherit)
				assert.False(t, constraintOf(t, restored).DeferralSet, "the clause named no deferral")
				assert.False(t, constraintOf(t, residue).NoInheritSet)
			},
		},
		{
			name:    "constraint not inheriting",
			feature: FeatureConstraintInherit,
			sql:     "ALTER TABLE t ALTER CONSTRAINT c NO INHERIT",
			residue: "ALTER TABLE t ALTER CONSTRAINT c",
			check: func(t *testing.T, restored, residue Statement) {
				assert.True(t, constraintOf(t, restored).NoInheritSet, "the clause did name an inheritance")
				assert.False(t, constraintOf(t, restored).Inherit)
				assert.False(t, constraintOf(t, residue).NoInheritSet)
			},
		},
		{
			name:    "a virtual generated column is not the stored one it was rewritten to",
			feature: FeatureVirtualGenerated,
			sql:     "ALTER TABLE t ADD COLUMN g int GENERATED ALWAYS AS (a + 1) VIRTUAL",
			residue: "ALTER TABLE t ADD COLUMN g int GENERATED ALWAYS AS (a + 1) STORED",
			check: func(t *testing.T, restored, residue Statement) {
				assert.Equal(t, GeneratedVirtual, columnOf(t, restored).Generated)
				assert.Equal(t, GeneratedStored, columnOf(t, residue).Generated)
			},
		},
		{
			name:    "a generated column with no storage keyword means virtual",
			feature: FeatureBareGenerated,
			sql:     "ALTER TABLE t ADD COLUMN g int GENERATED ALWAYS AS (a + 1)",
			residue: "ALTER TABLE t ADD COLUMN g int GENERATED ALWAYS AS (a + 1) STORED",
			check: func(t *testing.T, restored, residue Statement) {
				assert.Equal(t, GeneratedVirtual, columnOf(t, restored).Generated)
				assert.Equal(t, GeneratedStored, columnOf(t, residue).Generated)
				require.Len(t, restored.Features, 1)
				assert.True(t, restored.Features[0].Ambiguous,
					"G-04 stays flagged even when production is known to be 18")
			},
		},
		{
			name:    "temporal primary key",
			feature: FeatureWithoutOverlaps,
			sql:     "ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)",
			residue: "ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY (id, valid_at)",
			check: func(t *testing.T, restored, residue Statement) {
				assert.True(t, constraintOf(t, restored).WithoutOverlaps)
				assert.Equal(t, []string{"id", "valid_at"}, constraintOf(t, restored).Columns,
					"blanking the keyword must not cost the column")
				assert.False(t, constraintOf(t, residue).WithoutOverlaps)
			},
		},
		{
			name:    "temporal foreign key",
			feature: FeatureForeignKeyPeriod,
			sql:     "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (id, PERIOD v) REFERENCES r (id, PERIOD v)",
			residue: "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (id, v) REFERENCES r (id, v)",
			check: func(t *testing.T, restored, residue Statement) {
				assert.True(t, constraintOf(t, restored).Period)
				assert.Equal(t, []string{"id", "v"}, constraintOf(t, restored).Columns)
				assert.Equal(t, []string{"id", "v"}, constraintOf(t, restored).ReferencedColumns)
				assert.False(t, constraintOf(t, residue).Period)
			},
		},
		{
			name:    "VACCUM ONLY",
			feature: FeatureVacuumOnly,
			sql:     "VACUUM ONLY t",
			residue: "VACUUM t",
			check: func(t *testing.T, restored, residue Statement) {
				require.Len(t, restored.Relations, 1)
				assert.True(t, restored.Relations[0].Only)
				assert.False(t, residue.Relations[0].Only)
			},
		},
		{
			name:    "ANALYZE ONLY",
			feature: FeatureAnalyzeOnly,
			sql:     "ANALYZE ONLY t",
			residue: "ANALYZE t",
			check: func(t *testing.T, restored, residue Statement) {
				require.Len(t, restored.Relations, 1)
				assert.True(t, restored.Relations[0].Only)
				assert.False(t, residue.Relations[0].Only)
			},
		},
	}

	covered := map[FeatureName]bool{}
	for _, tc := range cases {
		covered[tc.feature] = true
	}
	for _, name := range shimFeatures {
		assert.Truef(t, covered[name], "%s is rewritten by the shim but nothing checks it is restored", name)
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			restored := ParseOne(tc.sql)
			require.Truef(t, restored.Parsed(), "did not parse: %s", restored.Unparsed)
			assert.Truef(t, restored.Uses(tc.feature), "expected feature %s, got %v", tc.feature, restored.Features)
			assert.Equal(t, pg_contract.Version18, restored.MinVersion())

			residue := ParseOne(tc.residue)
			require.Truef(t, residue.Parsed(), "residue did not parse: %s", residue.Unparsed)
			assert.Empty(t, residue.Features, "the residue is the form every version accepts")
			assert.Equal(t, pg_contract.MinSupportedVersion, residue.MinVersion())

			tc.check(t, restored, residue)
		})
	}
}

func TestAlterConstraintKeepsBothHalvesOfItsClause(t *testing.T) {
	cases := []struct {
		name        string
		sql         string
		deferralSet bool
		deferrable  bool
		initially   bool
	}{
		{
			name: "an enforcement on its own names no deferral",
			sql:  "ALTER TABLE t ALTER CONSTRAINT c NOT ENFORCED",
		},
		{
			name:        "an enforcement before a deferral keeps the deferral",
			sql:         "ALTER TABLE t ALTER CONSTRAINT c NOT ENFORCED DEFERRABLE INITIALLY DEFERRED",
			deferralSet: true,
			deferrable:  true,
			initially:   true,
		},
		{
			name:        "an enforcement after a deferral keeps it too",
			sql:         "ALTER TABLE t ALTER CONSTRAINT c DEFERRABLE NOT ENFORCED",
			deferralSet: true,
			deferrable:  true,
		},
		{
			name:        "an explicit NOT DEFERRABLE is still a named deferral",
			sql:         "ALTER TABLE t ALTER CONSTRAINT c ENFORCED NOT DEFERRABLE",
			deferralSet: true,
		},
		{
			name:        "NO INHERIT combines with a deferral the same way",
			sql:         "ALTER TABLE t ALTER CONSTRAINT c NO INHERIT DEFERRABLE",
			deferralSet: true,
			deferrable:  true,
		},
		{
			name: "a bare INHERIT names no deferral",
			sql:  "ALTER TABLE t ALTER CONSTRAINT c INHERIT",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := constraintOf(t, ParseOne(tc.sql))
			assert.Equal(t, tc.deferralSet, c.DeferralSet)
			assert.Equal(t, tc.deferrable, c.Deferrable)
			assert.Equal(t, tc.initially, c.InitiallyDef)
		})
	}
}

func TestDeferralStaysOnItsOwnClause(t *testing.T) {
	s := ParseOne("ALTER TABLE t ALTER CONSTRAINT a NOT ENFORCED, ALTER CONSTRAINT b DEFERRABLE")
	require.Len(t, s.Subcommands, 2)

	assert.True(t, s.Subcommands[0].Constraint.EnforcementSet)
	assert.False(t, s.Subcommands[0].Constraint.DeferralSet, "the deferral belongs to the next clause")
	assert.True(t, s.Subcommands[1].Constraint.DeferralSet)
	assert.False(t, s.Subcommands[1].Constraint.EnforcementSet)
}

func TestShimOnlyAttachesToTheRightRelation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sql  string
		only []bool
	}{
		{"ANALYZE ONLY a, b", []bool{true, false}},
		{"ANALYZE a, ONLY b", []bool{false, true}},
		{"ANALYZE ONLY a, ONLY b", []bool{true, true}},
		{"VACUUM (ANALYZE, FULL) ONLY s.t (x, y)", []bool{true}},
	}

	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			t.Parallel()

			statement := ParseOne(tc.sql)
			require.Truef(t, statement.Parsed(), "did not parse: %s", statement.Unparsed)
			require.Len(t, statement.Relations, len(tc.only))
			for i, want := range tc.only {
				assert.Equalf(t, want, statement.Relations[i].Only, "relation %d", i)
			}
		})
	}
}

// constraintOf is the constraint of a single-clause statement.
func constraintOf(t *testing.T, s Statement) ConstraintDef {
	t.Helper()
	require.Len(t, s.Subcommands, 1)
	require.NotNil(t, s.Subcommands[0].Constraint)
	return *s.Subcommands[0].Constraint
}

// columnOf is the column of a single-clause statement.
func columnOf(t *testing.T, s Statement) ColumnDef {
	t.Helper()
	require.Len(t, s.Subcommands, 1)
	require.NotNil(t, s.Subcommands[0].Column)
	return *s.Subcommands[0].Column
}

// TestPeriodOnBothSidesStaysOneFeature: the two PERIODs of a temporal foreign key are one fact about the constraint,
// so they bound the version range once.
func TestPeriodOnBothSidesStaysOneFeature(t *testing.T) {
	t.Parallel()

	const sql = "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (id, PERIOD v) REFERENCES r (id, PERIOD v)"
	rewritten, features, ok := shimPg18(sql)
	require.True(t, ok)
	require.Len(t, features, 1, "one constraint, one version bound")
	assert.NotContains(t, rewritten, "PERIOD", "every occurrence has to be removed to re-parse")

	statement := ParseOne(sql)
	require.True(t, statement.Parsed())
	assert.True(t, statement.Subcommands[0].Constraint.Period)
	assert.Equal(t, []string{"id", "v"}, statement.Subcommands[0].Constraint.Columns)
	assert.Equal(t, []string{"id", "v"}, statement.Subcommands[0].Constraint.ReferencedColumns)
}

// TestShimGivesUpOnRealErrors: syntax that is wrong rather than new must still be reported as unparseable.
func TestShimGivesUpOnRealErrors(t *testing.T) {
	t.Parallel()

	for _, sql := range []string{
		"ALTER TABLE t ADD COLUMN",
		"ALTER TABLE t ADD CONSTRAINT c CHECK (",
		"SELECT FROM WHERE",
	} {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()

			statement := ParseOne(sql)
			assert.False(t, statement.Parsed())
			assert.NotEmpty(t, statement.Unparsed)
		})
	}
}

// TestShimPreservesOffsets: every rewrite but the NOT NULL one replaces a construct with the
// same number of bytes, so an error message still points at the statement the user wrote.
func TestShimPreservesOffsets(t *testing.T) {
	t.Parallel()

	const sql = "ALTER TABLE t ADD CONSTRAINT c CHECK (x > 0) NOT ENFORCED"
	rewritten, features, ok := shimPg18(sql)
	require.True(t, ok)
	require.Len(t, features, 1)
	assert.Len(t, rewritten, len(sql))
	assert.Equal(t, "ALTER TABLE t ADD CONSTRAINT c CHECK (x > 0)             ", rewritten)
}

// TestShimIsNotConsultedWhenTheGrammarAgrees guards the order of the pipeline:
// the shim is a fallback, so a statement libpg_query already understands is never rewritten.
func TestShimIsNotConsultedWhenTheGrammarAgrees(t *testing.T) {
	t.Parallel()

	statement := ParseOne("ALTER TABLE t ADD CONSTRAINT c CHECK (x > 0) NOT VALID")
	require.True(t, statement.Parsed())
	assert.Empty(t, statement.Features)
	assert.True(t, statement.Subcommands[0].Constraint.NotValid)
	assert.False(t, statement.Subcommands[0].Constraint.NotEnforced)
}
