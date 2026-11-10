//go:build integration

package pg_classify

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// constraintNamesDDL is the schema the migrations below run against.
const constraintNamesDDL = `
CREATE SCHEMA IF NOT EXISTS ` + oracleSchema + `;
CREATE SCHEMA IF NOT EXISTS cn_other;
SET search_path TO ` + oracleSchema + `;

CREATE TABLE cn_a (id bigint, k int, j int);
CREATE TABLE cn_ref (id bigint PRIMARY KEY);
CREATE TABLE cn_fk (id bigint, ref bigint REFERENCES cn_ref (id));
-- Holds, on another table, the names cn_a's first unnamed CHECK on k and cn_b's primary key would take.
CREATE TABLE cn_holder (k int, CONSTRAINT cn_a_k_check CHECK (k > 0));
CREATE TABLE cn_b (id bigint, k int);
CREATE INDEX cn_b_pkey ON cn_holder (k);
CREATE TABLE cn_c (id bigint PRIMARY KEY, k int, j int, CONSTRAINT cn_c_u UNIQUE (j), CHECK (k > 0), CHECK (k > j));
CREATE TABLE cn_parted (id bigint, k int) PARTITION BY RANGE (k);
CREATE TABLE cn_parted_1 PARTITION OF cn_parted FOR VALUES FROM (0) TO (10);
CREATE TABLE cn_ptree (k int PRIMARY KEY, CHECK (k > 0)) PARTITION BY RANGE (k);
CREATE TABLE cn_ptree_1 PARTITION OF cn_ptree FOR VALUES FROM (0) TO (10);
CREATE TABLE cn_ptwo (k int PRIMARY KEY) PARTITION BY RANGE (k);
CREATE TABLE cn_ptwo_1 PARTITION OF cn_ptwo FOR VALUES FROM (0) TO (10);
CREATE TABLE cn_ptwo_2 PARTITION OF cn_ptwo FOR VALUES FROM (10) TO (20);
CREATE TABLE cn_other.cn_away (k int, CHECK (k > 0));
`

var constraintNameScripts = []string{
	// CREATE TABLE.
	"CREATE TABLE cn_t (id int PRIMARY KEY, k int CHECK (k > 0), j int UNIQUE, r bigint REFERENCES cn_ref (id))",
	"CREATE TABLE cn_t (a int, b int, PRIMARY KEY (a, b), UNIQUE (b, a), CHECK (a > b), CHECK (a > 0), CHECK (a > 1), FOREIGN KEY (a) REFERENCES cn_ref (id))",
	"CREATE TABLE cn_t (a int CONSTRAINT c1 CHECK (a > 0), CONSTRAINT c2 UNIQUE (a), CONSTRAINT c3 FOREIGN KEY (a) REFERENCES cn_ref (id))",
	"CREATE TABLE cn_t (a int NOT NULL, b int NOT NULL)",
	"CREATE TABLE cn_t (a serial, b int GENERATED ALWAYS AS IDENTITY)",
	"CREATE TABLE cn_t (a int, b int, UNIQUE (a) INCLUDE (b))",
	"CREATE TABLE cn_t (a int, EXCLUDE USING btree (a WITH =))",
	"CREATE TABLE cn_t (a int, b int, EXCLUDE USING btree (a WITH =, (a + b) WITH =))",
	"CREATE TABLE cn_t (a int, UNIQUE (a), UNIQUE (a))",
	"CREATE TABLE cn_t (a text, EXCLUDE USING btree (lower(a) WITH =, a WITH =))",
	"CREATE TABLE " + strings.Repeat("t", 60) + " (a_long_column_name int UNIQUE CHECK (a_long_column_name > 0))",
	"CREATE TABLE cn_t (" + strings.Repeat("c", 62) + " int, " + strings.Repeat("d", 62) + " int, UNIQUE (" + strings.Repeat("c", 62) + ", " + strings.Repeat("d", 62) + "))",

	// A name already taken somewhere in the schema.
	"ALTER TABLE cn_a ADD CHECK (k > 0)",
	"ALTER TABLE cn_b ADD PRIMARY KEY (id)",
	"CREATE INDEX cn_t_pkey ON cn_a (k); CREATE TABLE cn_t (a int PRIMARY KEY)",
	"CREATE SEQUENCE cn_a_k_key; ALTER TABLE cn_a ADD UNIQUE (k)",
	"CREATE TABLE cn_a_k (a int, CONSTRAINT cn_a_k_key CHECK (a > 0)); ALTER TABLE cn_a ADD UNIQUE (k)",

	// ALTER TABLE adds.
	"ALTER TABLE cn_a ADD PRIMARY KEY (id), ADD UNIQUE (k, j), ADD CHECK (k > j), ADD CHECK (j > 0), ADD FOREIGN KEY (id) REFERENCES cn_ref (id)",
	"ALTER TABLE cn_a ADD COLUMN n int CHECK (n > 0) UNIQUE, ADD COLUMN m bigint REFERENCES cn_ref (id)",
	"ALTER TABLE cn_a ALTER COLUMN k SET NOT NULL",
	"ALTER TABLE cn_a ALTER COLUMN k SET NOT NULL; ALTER TABLE cn_a ALTER COLUMN k DROP NOT NULL",
	"ALTER TABLE cn_a ADD COLUMN n int NOT NULL DEFAULT 0",
	"ALTER TABLE cn_a ADD CONSTRAINT named_c CHECK (k > 0) NOT VALID",
	"CREATE UNIQUE INDEX cn_a_idx ON cn_a (id); ALTER TABLE cn_a ADD UNIQUE USING INDEX cn_a_idx",
	"CREATE UNIQUE INDEX cn_a_idx ON cn_a (id); ALTER TABLE cn_a ADD CONSTRAINT cn_x UNIQUE USING INDEX cn_a_idx",

	// Added, then taken away.
	"ALTER TABLE cn_a ADD CHECK (k > 0); ALTER TABLE cn_a ADD CHECK (k > 1); ALTER TABLE cn_a DROP CONSTRAINT cn_a_k_check1",
	"ALTER TABLE cn_a ADD UNIQUE (k); ALTER TABLE cn_a DROP CONSTRAINT cn_a_k_key; ALTER TABLE cn_a ADD UNIQUE (k)",
	"ALTER TABLE cn_a ADD UNIQUE (k), ADD UNIQUE (j); ALTER TABLE cn_a DROP COLUMN k",
	"CREATE TABLE cn_t (a int PRIMARY KEY); CREATE TABLE cn_u (a int REFERENCES cn_t (a)); DROP TABLE cn_t CASCADE",
	"CREATE TABLE cn_t (a int CHECK (a > 0)); DROP TABLE cn_t; CREATE TABLE cn_t (a int CHECK (a > 0))",

	// What the snapshot holds.
	"ALTER TABLE cn_c DROP CONSTRAINT cn_c_k_check",
	"ALTER TABLE cn_c DROP CONSTRAINT cn_c_u",
	"ALTER TABLE cn_c DROP CONSTRAINT cn_c_pkey",
	"ALTER TABLE cn_c DROP COLUMN k",
	"ALTER TABLE cn_c DROP COLUMN j",
	"DROP TABLE cn_c",
	"DROP TABLE cn_ref CASCADE",
	"ALTER TABLE cn_ref DROP COLUMN id CASCADE",
	"ALTER TABLE cn_c RENAME CONSTRAINT cn_c_u TO cn_c_w",
	"ALTER TABLE cn_c RENAME CONSTRAINT cn_c_u TO cn_c_w; ALTER TABLE cn_c DROP CONSTRAINT cn_c_w",
	"ALTER INDEX cn_c_u RENAME TO cn_c_v",
	"ALTER INDEX cn_c_pkey RENAME TO cn_c_p; ALTER TABLE cn_a ADD CHECK (j > 0)",
	"ALTER TABLE cn_c RENAME TO cn_d; ALTER TABLE cn_d ADD CHECK (j > 0)",
	"ALTER TABLE cn_c SET SCHEMA cn_other",
	"ALTER TABLE cn_other.cn_away SET SCHEMA " + oracleSchema,
	"ALTER TABLE cn_c RENAME COLUMN k TO l",

	// Partitions.
	"ALTER TABLE cn_parted ADD CHECK (k > 0)",
	"ALTER TABLE cn_parted ADD UNIQUE (k)",
	"ALTER TABLE cn_parted ADD PRIMARY KEY (k)",
	"ALTER TABLE cn_parted ADD FOREIGN KEY (id) REFERENCES cn_ref (id)",
	"ALTER TABLE cn_parted ALTER COLUMN k SET NOT NULL",
	"CREATE TABLE cn_ptree_2 PARTITION OF cn_ptree FOR VALUES FROM (10) TO (20)",
	"CREATE TABLE cn_t (k int NOT NULL, CONSTRAINT cn_ptree_k_check CHECK (k > 0)); ALTER TABLE cn_ptree ATTACH PARTITION cn_t FOR VALUES FROM (10) TO (20)",
	"ALTER TABLE cn_ptree DETACH PARTITION cn_ptree_1",
	"ALTER TABLE cn_ptree DROP CONSTRAINT cn_ptree_k_check",
	"ALTER TABLE cn_ptree DROP CONSTRAINT cn_ptree_pkey",
	"DROP TABLE cn_ptree_1",
	"DROP TABLE cn_ptree",
	"CREATE TABLE cn_t (a int REFERENCES cn_ptree (k))",
	"CREATE TABLE cn_t (a int REFERENCES cn_ptwo (k))",
	"CREATE TABLE cn_t (a int REFERENCES cn_ptwo (k)); ALTER TABLE cn_t DROP CONSTRAINT cn_t_a_fkey",
	"CREATE TABLE cn_t (a int REFERENCES cn_ptwo (k)); DROP TABLE cn_ptwo CASCADE",
	"CREATE TABLE cn_t (a int, UNIQUE (a), PRIMARY KEY (a))",
	"CREATE TABLE cn_t (a int, CONSTRAINT late UNIQUE (a), UNIQUE (a))",
	"CREATE TABLE cn_t (k int NOT NULL, UNIQUE (k)); ALTER TABLE cn_ptwo ATTACH PARTITION cn_t FOR VALUES FROM (20) TO (30)",
	"ALTER TABLE cn_parted ADD UNIQUE (k); CREATE TABLE cn_t (id bigint, k int PRIMARY KEY); ALTER TABLE cn_parted ATTACH PARTITION cn_t FOR VALUES FROM (20) TO (30)",
	"CREATE TABLE cn_t (k int PRIMARY KEY); ALTER TABLE cn_ptwo ATTACH PARTITION cn_t FOR VALUES FROM (20) TO (30)",
	"CREATE TABLE cn_t (k int CONSTRAINT own_pk PRIMARY KEY); ALTER TABLE cn_ptwo ATTACH PARTITION cn_t FOR VALUES FROM (20) TO (30)",
	"ALTER TABLE cn_ptwo DROP CONSTRAINT cn_ptwo_pkey; CREATE TABLE cn_ptwo_3 PARTITION OF cn_ptwo FOR VALUES FROM (20) TO (30)",

	// Inheritance and LIKE.
	"CREATE TABLE cn_kid () INHERITS (cn_c)",
	"CREATE TABLE cn_t (LIKE cn_c INCLUDING ALL)",
	"CREATE TABLE cn_t (LIKE cn_c)",
	"CREATE TABLE cn_t (LIKE cn_c); DROP TABLE cn_c",
	"CREATE TABLE cn_t (LIKE cn_c INCLUDING ALL); ALTER TABLE cn_t DROP CONSTRAINT cn_t_pkey",
}

// serverConstraintNames runs a migration on its own connection, reads back every constraint name the schema holds, and rolls it back.
func serverConstraintNames(t *testing.T, db *sql.DB, script string) []string {
	t.Helper()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.ExecContext(ctx, "SET search_path TO "+oracleSchema+"; BEGIN")
	require.NoError(t, err)
	defer func() { _, _ = conn.ExecContext(ctx, "ROLLBACK") }()
	_, err = conn.ExecContext(ctx, script)
	require.NoError(t, err, script)

	rows, err := conn.QueryContext(ctx, `SELECT DISTINCT k.conname FROM pg_constraint k JOIN pg_namespace n ON n.oid = k.connamespace WHERE n.nspname = $1 ORDER BY 1`, oracleSchema)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}

// TestConstraintNamesOracle compares the constraint names the classifier believes a schema holds after a migration with
// the ones a live 15/16/17/18 reports, so a generated name is known to be the server's.
func TestConstraintNamesOracle(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	for _, version := range integrationVersions {
		wg.Add(1)
		go func(version pg_contract.Version) {
			defer wg.Done()
			t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
				db := startPostgres(t, version)
				_, err := db.Exec(constraintNamesDDL)
				require.NoError(t, err)
				catalogs := oracleCatalogs(t, db)

				for _, script := range constraintNameScripts {
					t.Run(script, func(t *testing.T) {
						server := serverConstraintNames(t, db, script)

						catalog := catalogs()
						classifier := NewClassifier(catalog)
						classifier.Analyze(pg_parse.Parse("SET search_path TO " + oracleSchema + "; " + script))
						known := classifier.checks.namesInSchema(scope{catalog: catalog, version: catalog.Version(), checks: classifier.checks}, oracleSchema)

						var ours []string
						for name := range known {
							ours = append(ours, name)
						}
						slices.Sort(ours)
						assert.Equal(t, server, ours)
					})
				}
			})
		}(version)
	}
	wg.Wait()
}
