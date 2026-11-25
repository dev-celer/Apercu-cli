package pgclassify_test

import (
	"testing"

	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
)

// TestSetNotNullReadsTheMigrationsProofs checks that SET NOT NULL skips its scan on what the migration itself
// left standing: a CHECK it added or validated, a NOT NULL constraint it validated, a CHECK of the snapshot a DROP
// NOT NULL did not take away. orders.code is nullable and unproven in the snapshot; orders.status is proven by the
// snapshot's orders_status_not_null CHECK.
func TestSetNotNullReadsTheMigrationsProofs(t *testing.T) {
	t.Parallel()

	const (
		meta = pgcontract.OpKindMetadata
		scan = pgcontract.OpKindScan
	)
	cases := []struct {
		name    string
		version pgcontract.Version
		script  string
		op      pgcontract.OpKind
	}{
		{name: "a CHECK the migration added",
			script: "ALTER TABLE orders ADD CONSTRAINT code_nn CHECK (code IS NOT NULL); ALTER TABLE orders ALTER COLUMN code SET NOT NULL",
			op:     meta},
		{name: "a NOT VALID CHECK the migration validated",
			script: "ALTER TABLE orders ADD CONSTRAINT code_nn CHECK (code IS NOT NULL) NOT VALID; ALTER TABLE orders VALIDATE CONSTRAINT code_nn; " +
				"ALTER TABLE orders ALTER COLUMN code SET NOT NULL",
			op: meta},
		{name: "a CHECK spelled NOT (col IS NULL)",
			script: "ALTER TABLE orders ADD CONSTRAINT code_nn CHECK (NOT (code IS NULL)); ALTER TABLE orders ALTER COLUMN code SET NOT NULL",
			op:     meta},
		{name: "a CHECK with the test as one of its AND terms",
			script: "ALTER TABLE orders ADD CONSTRAINT code_nn CHECK (code IS NOT NULL AND total >= 0); ALTER TABLE orders ALTER COLUMN code SET NOT NULL",
			op:     meta},
		{name: "a CHECK that followed its column through a rename",
			script: "ALTER TABLE orders ADD CONSTRAINT code_nn CHECK (code IS NOT NULL); ALTER TABLE orders RENAME COLUMN code TO ref; " +
				"ALTER TABLE orders ALTER COLUMN ref SET NOT NULL",
			op: meta},
		{name: "a snapshot CHECK a DROP NOT NULL did not take away",
			script: "ALTER TABLE orders ALTER COLUMN status DROP NOT NULL; ALTER TABLE orders ALTER COLUMN status SET NOT NULL",
			op:     meta},
		{name: "a CHECK added after a DROP NOT NULL",
			script: "ALTER TABLE orders ALTER COLUMN code SET NOT NULL; ALTER TABLE orders ALTER COLUMN code DROP NOT NULL; " +
				"ALTER TABLE orders ADD CONSTRAINT code_nn CHECK (code IS NOT NULL); ALTER TABLE orders ALTER COLUMN code SET NOT NULL",
			op: meta},
		{name: "a NOT NULL constraint the migration validated", version: pgcontract.Version18,
			script: "ALTER TABLE orders ADD CONSTRAINT code_nn NOT NULL code NOT VALID; ALTER TABLE orders VALIDATE CONSTRAINT code_nn; " +
				"ALTER TABLE orders ALTER COLUMN code SET NOT NULL",
			op: meta},

		// What proves nothing.
		{name: "a CHECK still NOT VALID",
			script: "ALTER TABLE orders ADD CONSTRAINT code_nn CHECK (code IS NOT NULL) NOT VALID; ALTER TABLE orders ALTER COLUMN code SET NOT NULL",
			op:     scan},
		{name: "a CHECK with the test under an OR",
			script: "ALTER TABLE orders ADD CONSTRAINT code_nn CHECK (code IS NOT NULL OR total >= 0); ALTER TABLE orders ALTER COLUMN code SET NOT NULL",
			op:     scan},
		{name: "a CHECK on another column",
			script: "ALTER TABLE orders ADD CONSTRAINT total_nn CHECK (total IS NOT NULL); ALTER TABLE orders ALTER COLUMN code SET NOT NULL",
			op:     scan},
		{name: "a CHECK the migration dropped again",
			script: "ALTER TABLE orders ADD CONSTRAINT code_nn CHECK (code IS NOT NULL); ALTER TABLE orders DROP CONSTRAINT code_nn; " +
				"ALTER TABLE orders ALTER COLUMN code SET NOT NULL",
			op: scan},
		{name: "a snapshot CHECK the migration dropped",
			script: "ALTER TABLE orders DROP CONSTRAINT orders_status_not_null; ALTER TABLE orders ALTER COLUMN status SET NOT NULL",
			op:     scan},
		{name: "a NOT NULL constraint still NOT VALID", version: pgcontract.Version18,
			script: "ALTER TABLE orders ADD CONSTRAINT code_nn NOT NULL code NOT VALID; ALTER TABLE orders ALTER COLUMN code SET NOT NULL",
			op:     scan},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			catalog := classifytest.Catalog(t)
			if testCase.version != 0 {
				catalog = classifytest.CatalogOn(t, testCase.version)
			}
			analyses := classifytest.Analyze(t, catalog, testCase.script)
			last := analyses[len(analyses)-1]
			assert.Empty(t, last.Errors)
			finding := classifytest.FindingOf(t, last, "R-AT-SETNOTNULL")
			target := classifytest.TargetOf(t, finding, "apercu_snapshot_test.orders")
			assert.Equal(t, testCase.op, target.OpKind, finding.Message)
		})
	}
}
