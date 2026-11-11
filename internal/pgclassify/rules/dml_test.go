package rules_test

import (
	"testing"

	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
)

func TestDataBackfills(t *testing.T) {
	t.Parallel()

	dml := pgcontract.OpKindDML
	re := pgcontract.LockRowExclusive

	cases := []ruleCase{
		{name: "an insert blocks no reader and no other writer", sql: "INSERT INTO orders (id) VALUES (1)", code: "R-DML-INSERT", lock: re, op: dml},
		{name: "an update rewrites the rows it matches", sql: "UPDATE orders SET status = 'done' WHERE id < 10", code: "R-DML-UPDATE", lock: re, op: dml},
		{name: "a delete leaves dead tuples behind", sql: "DELETE FROM orders WHERE id < 10", code: "R-DML-DELETE", lock: re, op: dml},
		{name: "a merge does all three", sql: "MERGE INTO orders t USING users s ON t.user_id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)", code: "R-DML-MERGE", lock: re, op: dml},
		{name: "a row lock barely touches the table", sql: "SELECT id FROM orders FOR UPDATE", code: "R-DML-SELECTFOR", lock: rs, op: noop},
		{name: "and FOR SHARE is the same clause", sql: "SELECT id FROM orders FOR SHARE", code: "R-DML-SELECTFOR", lock: rs, op: noop},
		{name: "a plain select is not a rule §5 carries", sql: "SELECT id FROM orders"},
	}

	catalog := classifytest.Catalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

func TestDMLNamesWhatItReads(t *testing.T) {
	t.Parallel()

	finding := classifytest.FindingOf(t, classifytest.Single(t, classifytest.Catalog(t), "INSERT INTO orders (id) SELECT id FROM users"), "R-DML-INSERT")
	source := classifytest.TargetOf(t, finding, "apercu_snapshot_test.users")
	assert.Equal(t, pgcontract.LockAccessShare, source.Lock, "the source is only read")
	assert.Equal(t, pgcontract.OpKindNone, source.OpKind)
}

func TestInsertRoutesAndTheOthersRecurse(t *testing.T) {
	t.Parallel()

	catalog := classifytest.Catalog(t)

	routed := classifytest.FindingOf(t, classifytest.Single(t, catalog, "INSERT INTO events (id) VALUES (1)"), "R-DML-INSERT")
	assert.Equal(t, pgcontract.LockRowExclusive, classifytest.TargetOf(t, routed, "apercu_snapshot_test.events_2025").Lock)

	inherited := classifytest.FindingOf(t, classifytest.Single(t, catalog, "INSERT INTO legacy_parent (id) VALUES (1)"), "R-DML-INSERT")
	assert.Len(t, inherited.Targets, 1, "an insert lands in the table it names: %v", inherited.Targets)

	updated := classifytest.FindingOf(t, classifytest.Single(t, catalog, "UPDATE legacy_parent SET id = id + 1"), "R-DML-UPDATE")
	assert.Equal(t, pgcontract.LockRowExclusive, classifytest.TargetOf(t, updated, "apercu_snapshot_test.legacy_child").Lock,
		"an update reads the whole inheritance tree")
}
