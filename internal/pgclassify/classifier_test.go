package pgclassify_test

import (
	"testing"

	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
)

// TestClassificationFollowsTheSessionContext is the pipeline's own, rather than any rule's: what the
// classifier carries from the session onto every statement.
func TestClassificationFollowsTheSessionContext(t *testing.T) {
	t.Parallel()

	t.Run("a table created earlier in the file is not an unknown one", func(t *testing.T) {
		analyses := classifytest.Analyze(t, classifytest.Catalog(t), "CREATE TABLE fresh (id int); ALTER TABLE fresh ADD COLUMN z int")
		finding := classifytest.FindingOf(t, analyses[1], "R-AT-ADDCOL")
		target := classifytest.TargetOf(t, finding, "apercu_snapshot_test.fresh")
		assert.Equal(t, pgcontract.RelationKindTable, target.Relation.Kind)
	})

	t.Run("the transaction group is carried onto every statement", func(t *testing.T) {
		analyses := classifytest.Analyze(t, classifytest.Catalog(t), "BEGIN; ALTER TABLE orders ADD COLUMN z int; COMMIT; ALTER TABLE orders ADD COLUMN y int")
		assert.Equal(t, []pgcontract.TxnGroup{1, 1, 1, 2}, groupsOf(analyses))
	})

	t.Run("an unparsed statement classifies to nothing", func(t *testing.T) {
		analyses := classifytest.Analyze(t, classifytest.Catalog(t), "NOT SQL AT ALL")
		assert.Empty(t, analyses[0].Findings)
		assert.Empty(t, analyses[0].Errors)
	})
}

func groupsOf(analyses []pgcontract.StatementAnalysis) []pgcontract.TxnGroup {
	out := make([]pgcontract.TxnGroup, 0, len(analyses))
	for _, analysis := range analyses {
		out = append(out, analysis.TxnGroup)
	}
	return out
}
