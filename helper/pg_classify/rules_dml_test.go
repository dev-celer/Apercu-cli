package pg_classify

import (
	"testing"

	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
)

func TestDataBackfills(t *testing.T) {
	t.Parallel()

	dml := pg_contract.OpKindDML
	re := pg_contract.LockRowExclusive

	cases := []ruleCase{
		{name: "an insert blocks no reader and no other writer", sql: "INSERT INTO orders (id) VALUES (1)", code: "R-DML-INSERT", lock: re, op: dml},
		{name: "an update rewrites the rows it matches", sql: "UPDATE orders SET status = 'done' WHERE id < 10", code: "R-DML-UPDATE", lock: re, op: dml},
		{name: "a delete leaves dead tuples behind", sql: "DELETE FROM orders WHERE id < 10", code: "R-DML-DELETE", lock: re, op: dml},
		{name: "a merge does all three", sql: "MERGE INTO orders t USING users s ON t.user_id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)", code: "R-DML-MERGE", lock: re, op: dml},
		{name: "a row lock barely touches the table", sql: "SELECT id FROM orders FOR UPDATE", code: "R-DML-SELECTFOR", lock: rs, op: noop},
		{name: "and FOR SHARE is the same clause", sql: "SELECT id FROM orders FOR SHARE", code: "R-DML-SELECTFOR", lock: rs, op: noop},
		{name: "a plain select is not a rule §5 carries", sql: "SELECT id FROM orders"},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

func TestDMLNamesWhatItReads(t *testing.T) {
	t.Parallel()

	finding := findingOf(t, single(t, testCatalog(t), "INSERT INTO orders (id) SELECT id FROM users"), "R-DML-INSERT")
	source := targetOf(t, finding, "apercu_snapshot_test.users")
	assert.Equal(t, pg_contract.LockAccessShare, source.Lock, "the source is only read")
	assert.Equal(t, pg_contract.OpKindNone, source.OpKind)
}

func TestInsertRoutesAndTheOthersRecurse(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)

	routed := findingOf(t, single(t, catalog, "INSERT INTO events (id) VALUES (1)"), "R-DML-INSERT")
	assert.Equal(t, pg_contract.LockRowExclusive, targetOf(t, routed, "apercu_snapshot_test.events_2025").Lock)

	inherited := findingOf(t, single(t, catalog, "INSERT INTO legacy_parent (id) VALUES (1)"), "R-DML-INSERT")
	assert.Len(t, inherited.Targets, 1, "an insert lands in the table it names: %v", inherited.Targets)

	updated := findingOf(t, single(t, catalog, "UPDATE legacy_parent SET id = id + 1"), "R-DML-UPDATE")
	assert.Equal(t, pg_contract.LockRowExclusive, targetOf(t, updated, "apercu_snapshot_test.legacy_child").Lock,
		"an update reads the whole inheritance tree")
}
