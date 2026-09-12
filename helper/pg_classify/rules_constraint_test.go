package pg_classify

import (
	"testing"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConstraintSubcommands(t *testing.T) {
	t.Parallel()

	cases := []ruleCase{
		// R-AT-ADDCHECK.
		{name: "a CHECK is proved against every row", sql: "ALTER TABLE orders ADD CONSTRAINT c CHECK (total >= 0)", code: "R-AT-ADDCHECK", lock: ael, op: scan},
		{name: "NOT VALID defers the proof", sql: "ALTER TABLE orders ADD CONSTRAINT c CHECK (total >= 0) NOT VALID", code: "R-AT-ADDCHECK-NV", lock: ael, op: meta},
		{name: "NOT ENFORCED never proves it at all", sql: "ALTER TABLE orders ADD CONSTRAINT c CHECK (total >= 0) NOT ENFORCED", code: "R-AT-ADDCHECK-NE", lock: ael, op: meta},

		// R-AT-ADDPK, R-AT-ADDUNIQUE, R-AT-ADDUSINGIDX.
		{name: "a primary key builds its index under the lock", sql: "ALTER TABLE orders ADD CONSTRAINT pk PRIMARY KEY (id)", code: "R-AT-ADDPK", lock: ael, op: scan},
		{name: "a unique constraint builds its index under the lock", sql: "ALTER TABLE orders ADD CONSTRAINT u UNIQUE (code)", code: "R-AT-ADDUNIQUE", lock: ael, op: scan},
		{name: "an exclusion constraint is the same shape", sql: "ALTER TABLE orders ADD CONSTRAINT x EXCLUDE (code WITH =)", code: "R-AT-ADDUNIQUE", lock: ael, op: scan},
		{name: "adopting an existing index reads nothing", sql: "ALTER TABLE orders ADD CONSTRAINT u UNIQUE USING INDEX orders_open_idx", code: "R-AT-ADDUSINGIDX", lock: ael, op: meta},

		// R-AT-ADDFK and R-AT-ADDFK-NV.
		{name: "a foreign key is proved against every row", sql: "ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users (id)", code: "R-AT-ADDFK", lock: sre, op: scan},
		{name: "a NOT VALID foreign key proves nothing now", sql: "ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users (id) NOT VALID", code: "R-AT-ADDFK-NV", lock: sre, op: meta},

		// R-AT-ADDNN-NV and R-AT-SETNOTNULL.
		{name: "a NOT VALID NOT NULL defers its proof", sql: "ALTER TABLE orders ADD CONSTRAINT n NOT NULL code NOT VALID", code: "R-AT-ADDNN-NV", lock: ael, op: meta},
		{name: "an enforced one is a SET NOT NULL", sql: "ALTER TABLE orders ADD CONSTRAINT n NOT NULL code", code: "R-AT-SETNOTNULL", lock: ael, op: scan},

		// R-AT-VALIDATE.
		{name: "validating reads every row under a weak lock", sql: "ALTER TABLE orders VALIDATE CONSTRAINT orders_total_check", code: "R-AT-VALIDATE", lock: sue, op: scan},
		{name: "validating what is already valid is a no-op", sql: "ALTER TABLE orders VALIDATE CONSTRAINT orders_status_not_null", code: "R-AT-VALIDATE", lock: sue, op: meta},

		// R-AT-DROPCON.
		{name: "dropping a constraint is catalog only", sql: "ALTER TABLE orders DROP CONSTRAINT orders_total_check", code: "R-AT-DROPCON", lock: ael, op: meta},

		// R-AT-ALTERCON.
		{name: "deferrability is catalog only", sql: "ALTER TABLE orders ALTER CONSTRAINT orders_user_id_fkey DEFERRABLE", code: "R-AT-ALTERCON", lock: ael, op: meta},
		{name: "inheritance is catalog only", sql: "ALTER TABLE orders ALTER CONSTRAINT orders_total_check NO INHERIT", code: "R-AT-ALTERCON-INH", lock: ael, op: meta},
		{name: "turning enforcement off proves nothing", sql: "ALTER TABLE orders ALTER CONSTRAINT orders_user_id_fkey NOT ENFORCED", code: "R-AT-ALTERCON-ENF", lock: ael, op: meta},
		{name: "turning enforcement on reads every row", sql: "ALTER TABLE orders ALTER CONSTRAINT orders_user_id_fkey ENFORCED", code: "R-AT-ALTERCON-ENF", lock: ael, op: scan},

		// R-AT-RENAMECON.
		{name: "renaming a constraint is catalog only", sql: "ALTER TABLE orders RENAME CONSTRAINT orders_total_check TO c", code: "R-AT-RENAMECON", lock: ael, op: meta},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}

// TestForeignKeyLocksBothSides test the IMPLICIT lock R-AT-ADDFK take on the referenced table.
func TestForeignKeyLocksBothSides(t *testing.T) {
	t.Parallel()

	finding := findingOf(t,
		single(t, testCatalog(t), "ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users (id)"),
		"R-AT-ADDFK")

	referenced := targetOf(t, finding, "apercu_snapshot_test.users")
	assert.Equal(t, pg_contract.LockShareRowExclusive, referenced.Lock)
	assert.Equal(t, pg_contract.TargetRoleImplicit, referenced.Role)
	assert.False(t, referenced.Lock.IsReadBlocking(), "a foreign key never blocks reads on either side")
}

// TestConstraintClausesReachTheOtherSide covers the three clauses that lock a table the statement does not name.
func TestConstraintClausesReachTheOtherSide(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		code pg_contract.Code
		lock pg_contract.Lock
	}{
		{
			name: "validating a foreign key only reads the other side",
			sql:  "ALTER TABLE orders VALIDATE CONSTRAINT orders_user_id_fkey",
			code: "R-AT-VALIDATE",
			lock: rs,
		},
		{
			name: "dropping a foreign key locks the other side hard",
			sql:  "ALTER TABLE orders DROP CONSTRAINT orders_user_id_fkey",
			code: "R-AT-DROPCON",
			lock: ael,
		},
		{
			name: "re-enforcing a foreign key blocks writes on the other side",
			sql:  "ALTER TABLE orders ALTER CONSTRAINT orders_user_id_fkey ENFORCED",
			code: "R-AT-ALTERCON-ENF",
			lock: sre,
		},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			finding := findingOf(t, single(t, catalog, testCase.sql), testCase.code)
			referenced := targetOf(t, finding, "apercu_snapshot_test.users")
			assert.Equal(t, testCase.lock.Short(), referenced.Lock.Short())
			assert.Equal(t, pg_contract.TargetRoleImplicit, referenced.Role)
		})
	}
}

// TestAlterConstraintDoesNotLockTheOtherSide validate that ALTER CONSTRAINT on FK does not lock the target table.
func TestAlterConstraintDoesNotLockTheOtherSide(t *testing.T) {
	t.Parallel()

	finding := findingOf(t,
		single(t, testCatalog(t), "ALTER TABLE orders ALTER CONSTRAINT orders_user_id_fkey DEFERRABLE"),
		"R-AT-ALTERCON")

	require.Len(t, finding.Targets, 1)
	assert.Equal(t, "apercu_snapshot_test.orders", finding.Targets[0].Relation.Name.String())
}

// TestPrimaryKeyChildLockFollowsTheVersion test that the partitions are SHARE locked up to 17 and
// ACCESS EXCLUSIVE locked from 18, and an unknown production version takes the stronger reading.
func TestPrimaryKeyChildLockFollowsTheVersion(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		catalog func(*testing.T) *pg_catalog.Catalog
		lock    pg_contract.Lock
	}{
		{name: "15 holds the partitions at SHARE", catalog: func(t *testing.T) *pg_catalog.Catalog { return testCatalogOn(t, pg_contract.Version15) }, lock: sh},
		{name: "17 holds the partitions at SHARE", catalog: func(t *testing.T) *pg_catalog.Catalog { return testCatalogOn(t, pg_contract.Version17) }, lock: sh},
		{name: "18 holds them at ACCESS EXCLUSIVE", catalog: func(t *testing.T) *pg_catalog.Catalog { return testCatalogOn(t, pg_contract.Version18) }, lock: ael},
		{name: "an unknown version takes the stronger one", catalog: testCatalog, lock: ael},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			finding := findingOf(t,
				single(t, testCase.catalog(t), "ALTER TABLE events ADD CONSTRAINT pk PRIMARY KEY (id, at)"),
				"R-AT-ADDPK")
			partition := targetOf(t, finding, "apercu_snapshot_test.events_2025")
			assert.Equal(t, testCase.lock.Short(), partition.Lock.Short())
			assert.Equal(t, pg_contract.OpKindScan, partition.OpKind, "the partitions hold the rows the index is built from")
		})
	}
}
