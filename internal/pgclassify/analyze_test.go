package pgclassify_test

import (
	"testing"

	"apercu-cli/helper"
	"apercu-cli/internal/pgclassify"
	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func relationName(name string) helper.FullRelationName {
	return helper.FullRelationName{Schema: "apercu_snapshot_test", Table: name}
}

func TestLevelGradingCreatedRelation(t *testing.T) {
	t.Parallel()

	analysis := pgclassify.Analyze(classifytest.Catalog(t), classifytest.Apart(
		classifytest.Event("CREATE TABLE fresh (id bigint)"),
		classifytest.Event("ALTER TABLE fresh ADD COLUMN z int, ADD CONSTRAINT c CHECK (id >= 0)"),
		classifytest.Event("ALTER TABLE orders ADD COLUMN z int"),
	))
	require.Len(t, analysis.Statements, 3)

	for _, finding := range analysis.Statements[1].Findings {
		assert.Equal(t, pgcontract.LevelLow, finding.Level, "%s only touches a table this migration created", finding.Code)
	}

	// An existing table still waits for production's size and traffic to be graded.
	for _, finding := range classifytest.RulesOnly(analysis.Statements[2]).Findings {
		assert.Equal(t, pgcontract.LevelUnset, finding.Level, "%s touches a table that was already there", finding.Code)
	}
}

func TestShadowCatalogInAnalyze(t *testing.T) {
	t.Parallel()

	catalog := classifytest.Catalog(t)
	analysis := pgclassify.Analyze(catalog, classifytest.Apart(
		classifytest.Event("CREATE TABLE fresh (id bigint)"),
		classifytest.Event("ALTER TABLE fresh ADD COLUMN z int"),
	))

	require.Len(t, analysis.Statements, 2)
	assert.True(t, catalog.CreatedByMigration(relationName("fresh")))
	assert.Equal(t, pgcontract.LockAccessExclusive, analysis.Statements[1].LockOn(relationName("fresh")))
}

func TestAnalyzeWithoutEvents(t *testing.T) {
	t.Parallel()

	analysis := pgclassify.Analyze(classifytest.Catalog(t), nil)
	assert.Empty(t, analysis.Statements)
	assert.Empty(t, analysis.Envelopes)
}

func TestLevelGradingOnFindingWithoutRelation(t *testing.T) {
	t.Parallel()

	analysis := pgclassify.Analyze(classifytest.Catalog(t), []pgcontract.QueryEvent{classifytest.Event("CREATE TABLE fresh (id bigint)")})

	require.Len(t, analysis.Statements, 1)
	finding := classifytest.FindingOf(t, analysis.Statements[0], "R-TB-CREATE")
	require.Empty(t, finding.Targets)
	assert.Equal(t, pgcontract.LevelUnset, finding.Level)
}

func TestLevelGradingPerFinding(t *testing.T) {
	t.Parallel()

	catalog := classifytest.Catalog(t)
	pgclassify.Analyze(catalog, []pgcontract.QueryEvent{classifytest.Event("CREATE TABLE fresh (id bigint)")})
	require.True(t, catalog.CreatedByMigration(relationName("fresh")))

	findings := []pgcontract.Finding{
		{Code: "fixed", Level: pgcontract.LevelHigh, Targets: []pgcontract.Target{{Relation: pgcontract.Relation{Name: relationName("fresh")}}}},
		{Code: "unset", Targets: []pgcontract.Target{{Relation: pgcontract.Relation{Name: relationName("fresh")}}}},
	}
	pgclassify.LevelGrading(catalog, findings)

	assert.Equal(t, pgcontract.LevelHigh, findings[0].Level)
	assert.Equal(t, pgcontract.LevelLow, findings[1].Level)
}
