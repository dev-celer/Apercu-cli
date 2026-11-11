package rules_test

import (
	"testing"

	"apercu-cli/internal/pgclassify/classifytest"

	"github.com/stretchr/testify/assert"
)

func TestAlterSequenceNamesTheColumnItFeeds(t *testing.T) {
	t.Parallel()

	finding := classifytest.FindingOf(t, classifytest.Single(t, classifytest.Catalog(t), "ALTER SEQUENCE users_id_seq RESTART WITH 5"), "R-OB-ALTERSEQ")
	assert.Contains(t, finding.Message, "apercu_snapshot_test.users.id")
}
