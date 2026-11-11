package rules_test

import (
	"testing"

	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionSubcommands(t *testing.T) {
	t.Parallel()

	cases := []ruleCase{
		{name: "attaching holds the parent at a lock that keeps it serving", sql: "ALTER TABLE events ATTACH PARTITION events_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')", code: "R-AT-ATTACH", lock: sue, op: meta, table: "apercu_snapshot_test.events"},
		{name: "detaching moves no row", sql: "ALTER TABLE events DETACH PARTITION events_2025", code: "R-AT-DETACH", lock: ael, op: meta, table: "apercu_snapshot_test.events"},
		{name: "finalising an interrupted detach is cheap", sql: "ALTER TABLE events DETACH PARTITION events_2025 FINALIZE", code: "R-AT-DETACH-FIN", lock: sue, op: meta, table: "apercu_snapshot_test.events"},
	}

	catalog := classifytest.Catalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

// TestAttachPartitionScansBothSides is R-AT-ATTACH.
func TestAttachPartitionScansBothSides(t *testing.T) {
	t.Parallel()

	finding := classifytest.FindingOf(t,
		classifytest.Single(t, classifytest.Catalog(t), "ALTER TABLE events ATTACH PARTITION events_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"),
		"R-AT-ATTACH")

	attached := classifytest.TargetOf(t, finding, "apercu_snapshot_test.events_2026")
	assert.Equal(t, pgcontract.LockAccessExclusive, attached.Lock)
	assert.Equal(t, pgcontract.OpKindScan, attached.OpKind)

	fallback := classifytest.TargetOf(t, finding, "apercu_snapshot_test.events_default")
	assert.Equal(t, pgcontract.OpKindScan, fallback.OpKind)
	assert.Equal(t, pgcontract.TargetRoleImplicit, fallback.Role)

	assert.Contains(t, finding.Message, "IS NOT NULL", "the implied partition constraint includes it, and a CHECK that omits it does not skip the scan")
}

// TestDetachConcurrentlyWithADefaultPartition test R-AT-DETACH-CONC on a parent with a default partition. The server
// refuses it and says so itself, so it is read as if it ran.
func TestDetachConcurrentlyWithADefaultPartition(t *testing.T) {
	t.Parallel()

	analysis := classifytest.Single(t, classifytest.Catalog(t), "ALTER TABLE events DETACH PARTITION events_2025 CONCURRENTLY")
	require.Empty(t, analysis.Errors)
	finding := classifytest.FindingOf(t, analysis, "R-AT-DETACH-CONC")
	assert.Equal(t, pgcontract.LockShareUpdateExclusive, classifytest.TargetOf(t, finding, "apercu_snapshot_test.events").Lock)
	assert.Equal(t, pgcontract.LockAccessExclusive, classifytest.TargetOf(t, finding, "apercu_snapshot_test.events_2025").Lock)
	assert.Equal(t, pgcontract.LockAccessExclusive, classifytest.TargetOf(t, finding, "apercu_snapshot_test.events_default").Lock)
}
