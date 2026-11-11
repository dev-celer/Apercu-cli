package rules_test

import (
	"testing"

	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
)

func TestTriggerAndSecuritySubcommands(t *testing.T) {
	t.Parallel()

	cases := []ruleCase{
		{name: "disabling a trigger waits for writers, not readers", sql: "ALTER TABLE orders DISABLE TRIGGER orders_bump", code: "R-AT-TRIG", lock: sre, op: meta},
		{name: "enabling every trigger is the same clause", sql: "ALTER TABLE orders ENABLE TRIGGER ALL", code: "R-AT-TRIG", lock: sre, op: meta},
		{name: "a replica trigger is its own rule id", sql: "ALTER TABLE orders ENABLE REPLICA TRIGGER orders_bump", code: "R-AT-TRIG-REP", lock: sre, op: meta},
		{name: "an always trigger is the same", sql: "ALTER TABLE orders ENABLE ALWAYS TRIGGER orders_bump", code: "R-AT-TRIG-REP", lock: sre, op: meta},
		{name: "a rule rewrites queries, so every plan is invalidated", sql: "ALTER TABLE orders DISABLE RULE r", code: "R-AT-RULE", lock: ael, op: meta},
		{name: "row level security is switched on under the strongest lock", sql: "ALTER TABLE orders ENABLE ROW LEVEL SECURITY", code: "R-AT-RLS", lock: ael, op: meta},
		{name: "and off the same way", sql: "ALTER TABLE orders DISABLE ROW LEVEL SECURITY", code: "R-AT-RLS", lock: ael, op: meta},
		{name: "forcing it on the owner is its own rule id", sql: "ALTER TABLE orders FORCE ROW LEVEL SECURITY", code: "R-AT-RLS-FORCE", lock: ael, op: meta},
		{name: "and releasing the owner too", sql: "ALTER TABLE orders NO FORCE ROW LEVEL SECURITY", code: "R-AT-RLS-FORCE", lock: ael, op: meta},
	}

	catalog := classifytest.Catalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

func TestTriggerReachesThePartitionsAndNotTheChildren(t *testing.T) {
	t.Parallel()

	catalog := classifytest.Catalog(t)
	partitioned := classifytest.FindingOf(t, classifytest.Single(t, catalog, "CREATE TRIGGER ob_trig BEFORE UPDATE ON events FOR EACH ROW EXECUTE FUNCTION bump()"), "R-OB-CREATETRIG")
	assert.Equal(t, pgcontract.LockShareRowExclusive, classifytest.TargetOf(t, partitioned, "apercu_snapshot_test.events_2025").Lock,
		"the trigger is cloned onto every partition")

	inherited := classifytest.FindingOf(t, classifytest.Single(t, catalog, "CREATE TRIGGER ob_trig BEFORE UPDATE ON legacy_parent FOR EACH ROW EXECUTE FUNCTION bump()"), "R-OB-CREATETRIG")
	assert.Len(t, inherited.Targets, 1, "an inheritance child inherits no trigger: %v", inherited.Targets)
}
