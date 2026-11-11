package rules_test

import (
	"testing"

	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
)

func TestOwnerCarriesTheIndexes(t *testing.T) {
	t.Parallel()

	finding := classifytest.FindingOf(t, classifytest.Single(t, classifytest.Catalog(t), "ALTER TABLE orders OWNER TO app"), "R-AT-OWNER")
	for _, index := range []string{"apercu_snapshot_test.orders_pkey", "apercu_snapshot_test.orders_open_idx"} {
		target := classifytest.TargetOf(t, finding, index)
		assert.Equal(t, pgcontract.LockAccessExclusive, target.Lock)
		assert.Equal(t, pgcontract.TargetRoleImplicit, target.Role)
	}
}
