//go:build integration

package pg_classify

import "apercu-cli/helper/pg_contract"

// oracleTreeDDL is the partition half of the lock oracle's fixture: a two-level tree whose
// DEFAULT is itself partitioned, tables waiting to be attached, keys on partitioned tables,
// foreign keys out of and into a partitioned table, and a classic parent with two children.
// These are the shapes where a lock travels below a relation the statement never names.
const oracleTreeDDL = `
SET search_path TO ` + oracleSchema + `;

CREATE TABLE m (id bigint NOT NULL, k int NOT NULL, v text, uid bigint) PARTITION BY RANGE (k);
CREATE TABLE m_1 PARTITION OF m FOR VALUES FROM (0) TO (100) PARTITION BY HASH (id);
CREATE TABLE m_1a PARTITION OF m_1 FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE m_1b PARTITION OF m_1 FOR VALUES WITH (MODULUS 2, REMAINDER 1);
CREATE TABLE m_2 PARTITION OF m FOR VALUES FROM (100) TO (200);
CREATE TABLE m_def PARTITION OF m DEFAULT PARTITION BY LIST (v);
CREATE TABLE m_def_x PARTITION OF m_def FOR VALUES IN ('x');
CREATE TABLE m_def_d PARTITION OF m_def DEFAULT;
CREATE INDEX m_v_idx ON m (v);
ALTER TABLE m ADD CONSTRAINT m_k_nv CHECK (k >= 0) NOT VALID;
-- m_2 proves uid is never null on its own, which spares it the scan SET NOT NULL does everywhere else.
ALTER TABLE m_2 ADD CONSTRAINT m_2_uid_nn CHECK (uid IS NOT NULL);
CREATE TRIGGER m_row BEFORE UPDATE ON m FOR EACH ROW EXECUTE FUNCTION bump();
CREATE TRIGGER m_stmt AFTER UPDATE ON m FOR EACH STATEMENT EXECUTE FUNCTION bump();

-- Waiting to be attached: a plain table, one whose CHECK already proves its bound, a partitioned one.
CREATE TABLE m_new (id bigint NOT NULL, k int NOT NULL, v text, uid bigint, CONSTRAINT m_k_nv CHECK (k >= 0));
CREATE TABLE m_new2 (id bigint NOT NULL, k int NOT NULL, v text, uid bigint, CONSTRAINT m_k_nv CHECK (k >= 0),
    CONSTRAINT m_new2_bound CHECK (k IS NOT NULL AND k >= 500 AND k < 600));
-- m_new2 already has the index the parent's index tree asks for, so attaching it builds nothing.
CREATE INDEX m_new2_v_idx ON m_new2 (v);
CREATE TABLE m_new_p (id bigint NOT NULL, k int NOT NULL, v text, uid bigint, CONSTRAINT m_k_nv CHECK (k >= 0))
    PARTITION BY LIST (v);
CREATE TABLE m_new_p_a PARTITION OF m_new_p FOR VALUES IN ('a');

-- Keys on a partitioned table: pk_nn's key columns are NOT NULL already, pk_nl's id is not.
CREATE TABLE pk_nn (id bigint NOT NULL, k int NOT NULL) PARTITION BY RANGE (k);
CREATE TABLE pk_nn_1 PARTITION OF pk_nn FOR VALUES FROM (0) TO (100);
CREATE TABLE pk_nn_2 PARTITION OF pk_nn FOR VALUES FROM (100) TO (200) PARTITION BY RANGE (k);
CREATE TABLE pk_nn_2a PARTITION OF pk_nn_2 FOR VALUES FROM (100) TO (150);
CREATE TABLE pk_nl (id bigint, k int NOT NULL) PARTITION BY RANGE (k);
CREATE TABLE pk_nl_1 PARTITION OF pk_nl FOR VALUES FROM (0) TO (100);

-- A partitioned table holding a foreign key, and one a foreign key points into.
CREATE TABLE fkp (id bigint NOT NULL, uid bigint REFERENCES users(id)) PARTITION BY RANGE (id);
CREATE TABLE fkp_1 PARTITION OF fkp FOR VALUES FROM (0) TO (100);
CREATE TABLE fkp_2 PARTITION OF fkp FOR VALUES FROM (100) TO (200);
CREATE TABLE fkp_new (id bigint NOT NULL, uid bigint);
CREATE TABLE refd (id bigint NOT NULL PRIMARY KEY) PARTITION BY RANGE (id);
CREATE TABLE refd_1 PARTITION OF refd FOR VALUES FROM (0) TO (100);
CREATE TABLE refd_2 PARTITION OF refd FOR VALUES FROM (100) TO (200);
CREATE TABLE refing (id bigint PRIMARY KEY, rid bigint REFERENCES refd(id), rid2 bigint);
CREATE TABLE refd_new (id bigint NOT NULL PRIMARY KEY);
ALTER TABLE refing ADD CONSTRAINT refing_nv FOREIGN KEY (rid2) REFERENCES refd(id) NOT VALID;

-- Classic inheritance with two children.
CREATE TABLE inh (id bigint NOT NULL, uid bigint, v text);
CREATE TABLE inh_c1 () INHERITS (inh);
CREATE TABLE inh_c2 () INHERITS (inh);
CREATE TRIGGER inh_row BEFORE UPDATE ON inh FOR EACH ROW EXECUTE FUNCTION bump();
CREATE RULE inh_rule AS ON DELETE TO inh DO ALSO NOTHING;
CREATE PUBLICATION oracle_pub_empty;

-- The scenarios' own tables: each one is changed for good by its setup, so nothing else touches them.
CREATE TABLE sc_t1 (id bigint NOT NULL, k int NOT NULL, uid bigint);
CREATE TABLE sc_p2 (id bigint NOT NULL, k int NOT NULL, uid bigint) PARTITION BY RANGE (k);
CREATE TABLE sc_p2_1 PARTITION OF sc_p2 FOR VALUES FROM (0) TO (100);
CREATE TABLE sc_t2 (id bigint NOT NULL, k int NOT NULL, uid bigint);
CREATE TABLE sc_p3 (id bigint NOT NULL, k int NOT NULL, uid bigint) PARTITION BY RANGE (k);
CREATE TABLE sc_p3_1 PARTITION OF sc_p3 FOR VALUES FROM (0) TO (100);
CREATE TABLE sc_p3_2 PARTITION OF sc_p3 FOR VALUES FROM (100) TO (200);
CREATE TABLE sc_t4 (id bigint NOT NULL, k int NOT NULL, uid bigint);

INSERT INTO m SELECT i, i % 200, CASE WHEN i % 3 = 0 THEN 'x' ELSE 'y' END, (i % 200) + 1 FROM generate_series(1, 400) i;
-- The default partition holds rows outside every bound the scripts attach, so it is never in the way.
INSERT INTO m VALUES (1001, 700, 'x', 1), (1002, 701, 'y', 2);
INSERT INTO m_new VALUES (1, 250, 'zz', 1);
INSERT INTO m_new_p VALUES (1, 350, 'a', 1);
INSERT INTO m_new2 VALUES (1, 550, 'z', 1);
INSERT INTO pk_nn SELECT i, i % 150 FROM generate_series(1, 300) i;
INSERT INTO pk_nl SELECT i, i % 100 FROM generate_series(1, 300) i;
INSERT INTO fkp SELECT i, (i % 200) + 1 FROM generate_series(0, 199) i;
INSERT INTO refd SELECT generate_series(0, 199);
INSERT INTO refing SELECT i, 100 + (i % 100), 100 + (i % 100) FROM generate_series(0, 199) i;
INSERT INTO inh_c1 VALUES (1, 1, 'a');
INSERT INTO inh_c2 VALUES (2, 2, 'b');
INSERT INTO sc_t1 VALUES (1, 10, 1);
INSERT INTO sc_p2 VALUES (1, 10, 1);
INSERT INTO sc_t2 VALUES (1, 150, 1);
INSERT INTO sc_p3 VALUES (1, 10, 1), (2, 150, 2);
INSERT INTO sc_t4 VALUES (1, 10, 1);
ANALYZE;
`

// The gaps a script can still be tagged with. Every other gap the 2026-10-26 audit measured is closed,
// and its script now has to match the server.
const (
	gapInlinedDefault = "a VOLATILE SQL function inlined into a constant default does not rewrite the table"
)

var upTo16 = []pg_contract.Version{15, 16}

// partitionScripts are the 2026-10-26 audit probes, on the oracle's own tree.
var partitionScripts = []lockScript{
	// recursion through a two-level tree.
	{sql: "ALTER TABLE m ADD COLUMN z int"},
	{sql: "ALTER TABLE m ADD COLUMN z text DEFAULT gen_random_uuid()::text"},
	{sql: "ALTER TABLE m ALTER COLUMN uid TYPE int"},
	{sql: "ALTER TABLE m ALTER COLUMN uid SET NOT NULL"},
	{sql: "ALTER TABLE m ADD CONSTRAINT m_ck CHECK (k >= 0)"},
	{sql: "ALTER TABLE m VALIDATE CONSTRAINT m_k_nv"},
	{sql: "ALTER TABLE m ALTER COLUMN uid SET STATISTICS 100"},
	{sql: "ALTER TABLE m ALTER COLUMN uid SET (n_distinct = 5)"},
	{sql: "ALTER TABLE m DROP COLUMN uid"},
	{sql: "ALTER TABLE m RENAME COLUMN uid TO uid2"},
	{sql: "ALTER TABLE m REPLICA IDENTITY FULL"},
	{sql: "ALTER TABLE m OWNER TO apercu_lock_oracle_owner"},
	{sql: "ALTER TABLE m SET ACCESS METHOD heap", refusedOn: upTo16},
	{sql: "ALTER TABLE m ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY"},
	{sql: "ALTER TABLE inh ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY"},
	{sql: "ALTER TABLE m ALTER COLUMN k TYPE bigint", refusedOn: onEveryVersion, leftToServer: true},
	{sql: "ALTER TABLE m_2 ADD COLUMN z int", refusedOn: onEveryVersion, leftToServer: true},
	{sql: "ALTER TABLE m_2 ALTER COLUMN uid SET NOT NULL"},

	// keys on partitioned tables and on a classic parent.
	{sql: "ALTER TABLE pk_nn ADD PRIMARY KEY (id, k)"},
	{sql: "ALTER TABLE pk_nl ADD PRIMARY KEY (id, k)"},
	{sql: "ALTER TABLE pk_nn ADD CONSTRAINT pk_nn_u UNIQUE (id, k)"},
	{sql: "ALTER TABLE pk_nn ADD CONSTRAINT pk_nn_ex EXCLUDE USING btree (id WITH =, k WITH =)", refusedOn: upTo16},
	{sql: "ALTER TABLE inh ADD PRIMARY KEY (id)"},
	{sql: "ALTER TABLE inh ADD PRIMARY KEY (uid)"},
	{sql: "ALTER TABLE inh ADD CONSTRAINT inh_u UNIQUE (id)"},

	// foreign keys out of and into a partitioned table.
	{sql: "ALTER TABLE m ADD CONSTRAINT m_fk FOREIGN KEY (uid) REFERENCES users (id)"},
	{sql: "ALTER TABLE inh ADD CONSTRAINT inh_fk FOREIGN KEY (uid) REFERENCES users (id)"},
	{sql: "ALTER TABLE refing ADD CONSTRAINT refing_fk2 FOREIGN KEY (rid) REFERENCES refd (id)"},
	{sql: "ALTER TABLE refing VALIDATE CONSTRAINT refing_nv"},
	{sql: "ALTER TABLE refing DROP CONSTRAINT refing_rid_fkey"},
	{sql: "ALTER TABLE fkp DROP CONSTRAINT fkp_uid_fkey"},

	// triggers and rules.
	{sql: "ALTER TABLE m DISABLE TRIGGER m_row"},
	{sql: "ALTER TABLE m DISABLE TRIGGER m_stmt"},
	{sql: "ALTER TABLE m DISABLE TRIGGER ALL"},
	{sql: "ALTER TABLE inh DISABLE TRIGGER inh_row"},
	{sql: "ALTER TABLE inh DISABLE RULE inh_rule"},
	{sql: "CREATE TRIGGER m_stmt2 AFTER INSERT ON m FOR EACH STATEMENT EXECUTE FUNCTION bump()"},
	{sql: "CREATE TRIGGER m_row2 BEFORE INSERT ON m FOR EACH ROW EXECUTE FUNCTION bump()"},
	{sql: "DROP TRIGGER m_stmt ON m"},
	{sql: "DROP TRIGGER m_row ON m"},
	{sql: "ALTER TRIGGER m_row ON m RENAME TO m_row_renamed"},
	{sql: "ALTER TRIGGER m_stmt ON m RENAME TO m_stmt_renamed"},
	{sql: "ALTER TRIGGER inh_row ON inh RENAME TO inh_row_renamed"},

	// attach, detach, partition of.
	{sql: "ALTER TABLE m ATTACH PARTITION m_new FOR VALUES FROM (200) TO (300)"},
	{sql: "ALTER TABLE m ATTACH PARTITION m_new_p FOR VALUES FROM (300) TO (400)"},
	{sql: "ALTER TABLE m ATTACH PARTITION m_new2 FOR VALUES FROM (500) TO (600)"},
	{sql: "ALTER TABLE fkp ATTACH PARTITION fkp_new FOR VALUES FROM (200) TO (300)"},
	{sql: "ALTER TABLE refd ATTACH PARTITION refd_new FOR VALUES FROM (200) TO (300)"},
	{sql: "ALTER TABLE m_def ATTACH PARTITION m_new FOR VALUES IN ('zz')"},
	{sql: "ALTER TABLE m DETACH PARTITION m_2"},
	{sql: "ALTER TABLE m DETACH PARTITION m_1"},
	{sql: "ALTER TABLE m DETACH PARTITION m_def"},
	{sql: "ALTER TABLE m_1 DETACH PARTITION m_1a"},
	{sql: "ALTER TABLE refd DETACH PARTITION refd_1"},
	{sql: "ALTER TABLE fkp DETACH PARTITION fkp_1"},
	{sql: "CREATE TABLE m_3 PARTITION OF m FOR VALUES FROM (400) TO (500)"},
	{sql: "CREATE TABLE m_def_y PARTITION OF m_def FOR VALUES IN ('w')"},
	{sql: "CREATE TABLE fkp_3 PARTITION OF fkp FOR VALUES FROM (200) TO (300)"},
	{sql: "CREATE TABLE refd_3 PARTITION OF refd FOR VALUES FROM (200) TO (300)"},

	// drop and truncate.
	{sql: "DROP TABLE m_2"},
	{sql: "DROP TABLE m_1a"},
	{sql: "DROP TABLE m_def_x"},
	{sql: "DROP TABLE refd_1", refusedOn: onEveryVersion, leftToServer: true},
	{sql: "DROP TABLE fkp_1"},
	{sql: "DROP TABLE inh_c1"},
	{sql: "DROP TABLE m"},
	{sql: "DROP TABLE inh CASCADE"},
	{sql: "TRUNCATE m"},
	{sql: "TRUNCATE ONLY m", refusedOn: onEveryVersion, leftToServer: true},
	{sql: "TRUNCATE refd CASCADE"},
	{sql: "TRUNCATE inh"},

	// indexes.
	{sql: "CREATE INDEX oracle_m_uid_idx ON m (uid)"},
	{sql: "CREATE INDEX oracle_users_only_idx ON ONLY users (id)"},
	{sql: "CREATE INDEX oracle_inh_only_idx ON ONLY inh (id)"},
	{sql: "CREATE INDEX oracle_inh_v_idx ON inh (v)"},
	{sql: "ALTER INDEX m_v_idx RENAME TO m_v_idx2"},
	{sql: "DROP INDEX m_v_idx"},
	{sql: "REINDEX INDEX m_v_idx", outsideTransaction: true},

	// everything else that names a partitioned table.
	{sql: "ALTER PUBLICATION oracle_pub_empty ADD TABLE m"},
	{sql: "ALTER PUBLICATION oracle_pub_empty ADD TABLE inh"},
	{sql: "LOCK TABLE m IN SHARE MODE"},
	{sql: "SELECT * FROM m FOR UPDATE"},
	{sql: "UPDATE m SET v = v"},
	{sql: "INSERT INTO m SELECT * FROM m"},
	{sql: "DELETE FROM inh WHERE id = 1"},
	{sql: "COPY m TO PROGRAM 'cat > /dev/null'", refusedOn: onEveryVersion, leftToServer: true},
	{sql: "ANALYZE m"},
	{sql: "COMMENT ON TABLE m IS 'x'"},
	{sql: "GRANT SELECT ON m TO apercu_lock_oracle_owner"},
}

// lockScenario is a migration whose last statement depends on what the earlier ones did to the catalog.
type lockScenario struct {
	// setup runs and commits first, one statement at a time.
	setup []string
	// sql is the statement whose locks are compared.
	sql string
	gap string
}

var lockScenarios = []lockScenario{
	{
		// A partitioned parent is built around an existing table, then rewritten through the parent.
		setup: []string{
			"CREATE TABLE sc_np1 (id bigint NOT NULL, k int NOT NULL, uid bigint) PARTITION BY RANGE (k)",
			"ALTER TABLE sc_np1 ATTACH PARTITION sc_t1 FOR VALUES FROM (0) TO (1000)",
		},
		sql: "ALTER TABLE sc_np1 ADD COLUMN z text DEFAULT gen_random_uuid()::text",
	},
	{
		// An existing table attached to an existing parent, then rewritten through it.
		setup: []string{"ALTER TABLE sc_p2 ATTACH PARTITION sc_t2 FOR VALUES FROM (100) TO (200)"},
		sql:   "ALTER TABLE sc_p2 ADD COLUMN z text DEFAULT gen_random_uuid()::text",
	},
	{
		// A partition detached, then the parent rewritten: the detached table is no longer reached.
		setup: []string{"ALTER TABLE sc_p3 DETACH PARTITION sc_p3_2"},
		sql:   "ALTER TABLE sc_p3 ADD COLUMN z text DEFAULT gen_random_uuid()::text",
	},
	{
		// A new parent, an attached table, then an index built through the parent.
		setup: []string{
			"CREATE TABLE sc_np4 (id bigint NOT NULL, k int NOT NULL, uid bigint) PARTITION BY RANGE (k)",
			"ALTER TABLE sc_np4 ATTACH PARTITION sc_t4 FOR VALUES FROM (0) TO (1000)",
		},
		sql: "CREATE INDEX sc_np4_uid_idx ON sc_np4 (uid)",
	},
}
