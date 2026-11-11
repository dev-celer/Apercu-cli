//go:build integration

package pgclassify_test

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify"
	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// oracleDDL18 is the part of the fixture only PostgreSQL 18 can hold.
const oracleDDL18 = `
SET search_path TO ` + classifytest.OracleSchema + `;
ALTER TABLE computed ADD COLUMN virtual_total numeric(10,2) GENERATED ALWAYS AS (base * 3) VIRTUAL;
`

// oracleTablespace is created out of band: CREATE TABLESPACE needs a directory that already
// exists and belongs to the server's own user, and it cannot run inside a transaction block, so it
// is neither part of classifytest.OracleDDL nor something a plain fixture can carry.
const oracleTablespace = "apercu_lock_oracle_ts"

var oracleTablespaceSetup = []string{
	`COPY (SELECT 1) TO PROGRAM 'mkdir -p /tmp/` + oracleTablespace + ` && chmod 700 /tmp/` + oracleTablespace + `'`,
	`CREATE TABLESPACE ` + oracleTablespace + ` LOCATION '/tmp/` + oracleTablespace + `'`,
}

// oracleTablespaceDDL puts one relation of every relkind "ALL IN TABLESPACE" can select into that
// tablespace, plus a partitioned parent whose partition stays behind, so a rule that recursed or
// moved the wrong relkind would be caught.
const oracleTablespaceDDL = `
SET search_path TO ` + classifytest.OracleSchema + `;
CREATE TABLE ts_table (id bigint PRIMARY KEY) TABLESPACE ` + oracleTablespace + `;
CREATE INDEX ts_table_idx ON ts_table (id) TABLESPACE ` + oracleTablespace + `;
CREATE MATERIALIZED VIEW ts_matview TABLESPACE ` + oracleTablespace + ` AS SELECT id FROM ts_table;
CREATE TABLE ts_parted (id bigint) PARTITION BY RANGE (id) TABLESPACE ` + oracleTablespace + `;
CREATE TABLE ts_parted_1 PARTITION OF ts_parted FOR VALUES FROM (1) TO (100) TABLESPACE pg_default;
`

// lockScript is one statement replayed against a live server.
type lockScript struct {
	sql string
	// since is the oldest server that accepts the statement at all. Older ones never run it.
	since pgcontract.Version
	// refusedOn lists the versions the server is expected to reject the statement on, which is
	// what the classifier's REJECTED branch predicts. The two have to agree, version by version.
	refusedOn []pgcontract.Version
	// leftToServer marks a refusal the classifier does not predict: the server refuses the statement on
	// every version and says so itself, so the classifier reads it as if it ran.
	leftToServer bool
	// outsideTransaction runs the statement with no BEGIN, for the ones that cannot sit in a
	// block. No lock can be read back for those, so only the refusal is compared.
	outsideTransaction bool
	// gap names a known classifier gap: the script has to keep disagreeing with the server, on
	// gapOn or on every version when gapOn is empty, until the fix lands and the marker goes.
	gap   string
	gapOn []pgcontract.Version
}

// gapped reports whether the script is expected to disagree with the server on this version.
func (s lockScript) gapped(version pgcontract.Version) bool {
	return s.gap != "" && (len(s.gapOn) == 0 || slices.Contains(s.gapOn, version))
}

func (s lockScript) runsOn(version pgcontract.Version) bool {
	return s.since == pgcontract.VersionUnknown || version >= s.since
}

func (s lockScript) refused(version pgcontract.Version) bool {
	return slices.Contains(s.refusedOn, version)
}

// The version sets the refusals below are drawn from.
var (
	onEveryVersion = []pgcontract.Version{15, 16, 17, 18}
	upTo17         = []pgcontract.Version{15, 16, 17}
	from18         = []pgcontract.Version{18}
)

// lockScripts is one entry per §4 rule id whose lock the requirements assert rather than measure.
var lockScripts = []lockScript{
	// 4.1 columns.
	{sql: "ALTER TABLE orders ADD COLUMN z int"},
	// next_code() is a VOLATILE SQL function the planner inlines into a constant, so no rewrite happens.
	{sql: "ALTER TABLE orders ADD COLUMN z text DEFAULT next_code()", gap: gapInlinedDefault},
	{sql: "ALTER TABLE orders ADD COLUMN z positive_int"},
	{sql: "ALTER TABLE orders ADD COLUMN z text UNIQUE"},
	{sql: "ALTER TABLE events ADD COLUMN z int"},
	{sql: "ALTER TABLE legacy_parent ADD COLUMN z int"},
	{sql: "ALTER TABLE orders DROP COLUMN code"},
	{sql: "ALTER TABLE orders ALTER COLUMN total TYPE numeric(12,2)"},
	{sql: "ALTER TABLE orders ALTER COLUMN total TYPE numeric(10,4)"},
	{sql: "ALTER TABLE orders ALTER COLUMN status TYPE text USING upper(status)"},
	{sql: "ALTER TABLE users ALTER COLUMN created_at TYPE timestamp"},
	{sql: "ALTER TABLE orders ALTER COLUMN status SET DEFAULT 'open'"},
	{sql: "ALTER TABLE orders ALTER COLUMN status DROP DEFAULT"},
	{sql: "ALTER TABLE orders ALTER COLUMN status SET NOT NULL"},
	{sql: "ALTER TABLE orders ALTER COLUMN code SET NOT NULL"},
	{sql: "ALTER TABLE users ALTER COLUMN email DROP NOT NULL"},
	{sql: "ALTER TABLE orders ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY"},
	{sql: "ALTER TABLE orders ALTER COLUMN status SET STATISTICS 500"},
	{sql: "ALTER TABLE orders ALTER COLUMN status SET STATISTICS DEFAULT", since: pgcontract.Version17},
	{sql: "ALTER TABLE orders ALTER COLUMN status SET (n_distinct = 10)"},
	{sql: "ALTER TABLE orders ALTER COLUMN status RESET (n_distinct)"},
	{sql: "ALTER TABLE orders ALTER COLUMN status SET STORAGE EXTERNAL"},
	{sql: "ALTER TABLE orders ALTER COLUMN status SET STORAGE DEFAULT", since: pgcontract.Version16},
	{sql: "ALTER TABLE orders ALTER COLUMN status SET COMPRESSION pglz"},
	{sql: "ALTER TABLE orders RENAME COLUMN status TO state"},

	// SET EXPRESSION is 17, and a virtual generated column to point it at is 18.
	{sql: "ALTER TABLE computed ALTER COLUMN stored_total SET EXPRESSION AS (base * 4)", since: pgcontract.Version17},
	{sql: "ALTER TABLE computed ALTER COLUMN virtual_total SET EXPRESSION AS (base * 5)", since: pgcontract.Version18},
	{sql: "ALTER TABLE computed ALTER COLUMN stored_total DROP EXPRESSION"},
	{sql: "ALTER TABLE computed ALTER COLUMN virtual_total DROP EXPRESSION", since: pgcontract.Version18, refusedOn: from18, leftToServer: true},
	// G-04: GENERATED with no storage keyword is VIRTUAL on 18, stores nothing and reads no row; 15-17 cannot parse it.
	{sql: "ALTER TABLE computed ADD COLUMN bare_total numeric(10,2) GENERATED ALWAYS AS (base * 6)", refusedOn: upTo17},
	{sql: "ALTER TABLE computed ADD COLUMN new_virtual numeric(10,2) GENERATED ALWAYS AS (base * 7) VIRTUAL", refusedOn: upTo17},

	// 4.2 constraints.
	{sql: "ALTER TABLE orders ADD CONSTRAINT c CHECK (total >= 0)"},
	{sql: "ALTER TABLE orders ADD CONSTRAINT c CHECK (total >= 0) NOT VALID"},
	{sql: "ALTER TABLE orders ADD CONSTRAINT c CHECK (total >= 0) NOT ENFORCED", since: pgcontract.Version18},
	{sql: "ALTER TABLE users ADD CONSTRAINT u UNIQUE (email)"},
	{sql: "ALTER TABLE orders ADD CONSTRAINT u UNIQUE USING INDEX orders_code_uidx"},
	{sql: "ALTER TABLE events ADD CONSTRAINT pk PRIMARY KEY (id, at)"},
	{sql: "ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users (id)"},
	{sql: "ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users (id) NOT VALID"},
	{sql: "ALTER TABLE events ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES users (id) NOT VALID", refusedOn: upTo17},
	{sql: "ALTER TABLE orders ADD CONSTRAINT n NOT NULL code NOT VALID", since: pgcontract.Version18},
	{sql: "ALTER TABLE orders VALIDATE CONSTRAINT orders_total_nv"},
	{sql: "ALTER TABLE orders VALIDATE CONSTRAINT orders_status_nn"},
	{sql: "ALTER TABLE orders DROP CONSTRAINT orders_total_nv"},
	{sql: "ALTER TABLE orders DROP CONSTRAINT orders_user_id_fkey"},
	{sql: "ALTER TABLE orders ALTER CONSTRAINT orders_user_id_fkey DEFERRABLE"},
	{sql: "ALTER TABLE orders RENAME CONSTRAINT orders_total_nv TO c"},

	// 4.3 triggers and row level security.
	{sql: "ALTER TABLE orders DISABLE TRIGGER orders_bump"},
	{sql: "ALTER TABLE orders ENABLE REPLICA TRIGGER orders_bump"},
	{sql: "ALTER TABLE orders ENABLE ROW LEVEL SECURITY"},
	{sql: "ALTER TABLE orders FORCE ROW LEVEL SECURITY"},

	// 4.4 storage, layout, ownership.
	{sql: "ALTER TABLE orders SET (fillfactor = 70)"},
	{sql: "ALTER TABLE orders SET (autovacuum_vacuum_scale_factor = 0.1)"},
	{sql: "ALTER TABLE orders SET (fillfactor = 70, user_catalog_table = true)"},
	{sql: "ALTER TABLE orders RESET (fillfactor)"},
	{sql: "ALTER TABLE legacy_parent SET (fillfactor = 70)"},
	{sql: "ALTER TABLE events SET (fillfactor = 70)", refusedOn: onEveryVersion, leftToServer: true},
	{sql: "ALTER TABLE orders SET ACCESS METHOD heap"},
	{sql: "ALTER TABLE orders SET UNLOGGED"},
	{sql: "ALTER TABLE events SET UNLOGGED", refusedOn: from18},
	{sql: "ALTER TABLE orders CLUSTER ON orders_pkey"},
	{sql: "ALTER TABLE orders SET WITHOUT CLUSTER"},
	{sql: "ALTER TABLE legacy_parent SET WITHOUT CLUSTER"},
	{sql: "ALTER TABLE events CLUSTER ON events_pkey", refusedOn: onEveryVersion, leftToServer: true},
	{sql: "ALTER TABLE orders SET WITHOUT OIDS"},
	{sql: "ALTER TABLE orders OWNER TO apercu_lock_oracle_owner"},
	{sql: "ALTER TABLE orders REPLICA IDENTITY FULL"},
	{sql: "ALTER TABLE orders REPLICA IDENTITY USING INDEX orders_pkey"},
	{sql: "ALTER TABLE legacy_child NO INHERIT legacy_parent"},
	{sql: "ALTER TABLE orphan INHERIT legacy_parent"},
	{sql: "ALTER TABLE orders RENAME TO orders_old"},
	// V-05: ALTER TABLE renames an index under ACCESS EXCLUSIVE, where ALTER INDEX takes SHARE UPDATE EXCLUSIVE.
	{sql: "ALTER TABLE orders_status_idx RENAME TO orders_status_idx2"},
	{sql: "ALTER TABLE ixparted_only_idx RENAME TO ixparted_only_idx2"},
	{sql: "ALTER TABLE orders SET SCHEMA public"},
	{sql: "ALTER TABLE ts_table SET TABLESPACE pg_default"},

	// 4.4 "ALL IN TABLESPACE", where the target set is the snapshot's answer and not the
	// statement's: each object type moves its own relkinds, and none of them recurses.
	{sql: "ALTER TABLE ALL IN TABLESPACE " + oracleTablespace + " SET TABLESPACE pg_default"},
	{sql: "ALTER INDEX ALL IN TABLESPACE " + oracleTablespace + " SET TABLESPACE pg_default"},
	{sql: "ALTER MATERIALIZED VIEW ALL IN TABLESPACE " + oracleTablespace + " SET TABLESPACE pg_default"},
	{sql: "ALTER TABLE ALL IN TABLESPACE " + oracleTablespace + " OWNED BY apercu_lock_oracle_owner SET TABLESPACE pg_default"},
	{sql: "ALTER TABLE ALL IN TABLESPACE pg_default SET TABLESPACE " + oracleTablespace},

	// 4.5 partitions.
	{sql: "ALTER TABLE events ATTACH PARTITION events_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"},
	{sql: "ALTER TABLE parted ATTACH PARTITION parted_2 FOR VALUES FROM (100) TO (200)"},
	{sql: "ALTER TABLE parted DETACH PARTITION parted_1"},
	{sql: "ALTER TABLE ONLY events ADD CONSTRAINT c CHECK (id > 0)", refusedOn: onEveryVersion, leftToServer: true},
	{sql: "ALTER TABLE ONLY events ADD COLUMN z int", refusedOn: onEveryVersion, leftToServer: true},
	{sql: "ALTER TABLE ONLY events DROP COLUMN id", refusedOn: onEveryVersion, leftToServer: true},
	{sql: "ALTER TABLE ONLY events ALTER COLUMN id SET NOT NULL"},
	{sql: "ALTER TABLE ONLY events ALTER COLUMN id TYPE int", refusedOn: onEveryVersion, leftToServer: true},
	{sql: "ALTER TABLE ONLY events ALTER COLUMN id DROP NOT NULL", refusedOn: upTo17},
	{sql: "ALTER TABLE ONLY events DROP CONSTRAINT events_id_check", refusedOn: upTo17},
	{sql: "ALTER TABLE events DROP CONSTRAINT events_id_check"},
	{sql: "ALTER TABLE ONLY legacy_parent DROP CONSTRAINT legacy_id_check"},
	{sql: "ALTER TABLE ONLY events RENAME COLUMN id TO ident", refusedOn: onEveryVersion, leftToServer: true},
	{sql: "ALTER TABLE ONLY events ALTER COLUMN id SET DEFAULT 1"},
	{sql: "ALTER TABLE ONLY legacy_parent ADD COLUMN z int", refusedOn: onEveryVersion, leftToServer: true},
	{sql: "ALTER TABLE ONLY legacy_parent DROP COLUMN id"},
	{sql: "ALTER TABLE ONLY legacy_parent ADD CONSTRAINT c CHECK (id > 0)", refusedOn: onEveryVersion, leftToServer: true},
	{sql: "ALTER TABLE ONLY legacy_parent ALTER COLUMN id SET NOT NULL"},
	{sql: "ALTER TABLE ONLY legacy_parent ALTER COLUMN id SET DEFAULT 1"},
	{sql: "ALTER TABLE ONLY legacy_parent ALTER COLUMN id SET STORAGE PLAIN"},
	{sql: "ALTER TABLE ONLY legacy_parent ALTER COLUMN id DROP NOT NULL"},
	{sql: "ALTER TABLE ONLY events ALTER COLUMN id SET STORAGE PLAIN"},
	{sql: "ALTER TABLE ONLY events ENABLE TRIGGER ALL"},
	{sql: "ALTER TABLE ONLY events ALTER COLUMN id SET STATISTICS 100"},
	{sql: "ALTER TABLE ONLY events OWNER TO apercu_lock_oracle_owner"},
	{sql: "ALTER TABLE parted DETACH PARTITION parted_1 CONCURRENTLY", outsideTransaction: true, refusedOn: onEveryVersion, leftToServer: true},

	// 5.1 index DDL.
	{sql: "CREATE INDEX oracle_new_idx ON orders (code)"},
	{sql: "CREATE UNIQUE INDEX oracle_new_uidx ON users (email)"},
	{sql: "CREATE INDEX oracle_part_idx ON parted (id)"},
	{sql: "CREATE INDEX oracle_only_idx ON ONLY parted (id)"},
	{sql: "CREATE INDEX CONCURRENTLY oracle_conc_idx ON spare (id)", outsideTransaction: true},
	{sql: "CREATE INDEX CONCURRENTLY oracle_conc_part_idx ON parted (id)", outsideTransaction: true, refusedOn: onEveryVersion},
	{sql: "DROP INDEX spare_spare_idx"},
	{sql: "DROP INDEX CONCURRENTLY oracle_conc_idx", outsideTransaction: true},
	{sql: "ALTER INDEX orders_status_idx RENAME TO orders_status_idx2"},
	{sql: "ALTER INDEX orders_status_idx SET (fillfactor = 70)"},
	{sql: "ALTER INDEX orders_status_idx SET TABLESPACE " + oracleTablespace},
	{sql: "ALTER INDEX ixparted_only_idx ATTACH PARTITION ixparted_2_id_idx"},
	{sql: "REINDEX TABLE spare"},
	{sql: "REINDEX INDEX spare_id_idx"},
	{sql: "REINDEX TABLE CONCURRENTLY spare", outsideTransaction: true},
	{sql: "REINDEX SCHEMA " + classifytest.OracleSchema, outsideTransaction: true},

	// 5.2 creation and destruction.
	{sql: "CREATE TABLE oracle_new (id bigint PRIMARY KEY, user_id bigint REFERENCES users (id))"},
	{sql: "CREATE TABLE oracle_like (LIKE orders)"},
	{sql: "CREATE TABLE oracle_inherits () INHERITS (spare)"},
	// V-06: PARTITION OF holds the parent at ACCESS EXCLUSIVE, where ATTACH PARTITION (4.5) takes SHARE UPDATE EXCLUSIVE.
	{sql: "CREATE TABLE oracle_partof PARTITION OF parted FOR VALUES FROM (200) TO (300)"},
	{sql: "CREATE TABLE oracle_ctas AS SELECT id, status FROM orders"},
	{sql: "DROP TABLE ticket_line"},
	{sql: "DROP TABLE ticket CASCADE"},
	{sql: "DROP TABLE parted_1"},

	// 5.3 views, matviews, sequences.
	{sql: "CREATE VIEW oracle_view AS SELECT id FROM orders"},
	{sql: "CREATE OR REPLACE VIEW active_users AS SELECT id, email FROM users WHERE id > 0"},
	{sql: "DROP VIEW active_users"},
	{sql: "CREATE MATERIALIZED VIEW oracle_mv AS SELECT id FROM orders"},
	{sql: "CREATE MATERIALIZED VIEW oracle_mv_empty AS SELECT id FROM orders WITH NO DATA"},
	{sql: "REFRESH MATERIALIZED VIEW order_totals"},
	{sql: "REFRESH MATERIALIZED VIEW CONCURRENTLY order_totals"},
	{sql: "ALTER SEQUENCE standalone_seq RESTART WITH 5"},
	{sql: "CREATE SEQUENCE oracle_seq"},
	{sql: "DROP SEQUENCE standalone_seq"},

	// 5.4 the objects that hang off a table.
	{sql: "CREATE TRIGGER oracle_trig BEFORE UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION bump()"},
	{sql: "CREATE TRIGGER oracle_trig BEFORE UPDATE ON parted FOR EACH ROW EXECUTE FUNCTION bump()"},
	{sql: "DROP TRIGGER orders_bump ON orders"},
	{sql: "ALTER TRIGGER orders_bump ON orders RENAME TO orders_bump2"},
	{sql: "CREATE POLICY oracle_policy ON orders USING (true)"},
	{sql: "DROP POLICY policy_tbl_p ON policy_tbl"},
	{sql: "CREATE RULE oracle_rule AS ON DELETE TO orders DO INSTEAD NOTHING"},
	{sql: "DROP RULE policy_tbl_r ON policy_tbl"},
	{sql: "CREATE STATISTICS oracle_stats ON id, status FROM orders"},
	{sql: "DROP STATISTICS stats_tbl_stats"},
	{sql: "ALTER STATISTICS stats_tbl_stats SET STATISTICS 100"},
	{sql: "COMMENT ON TABLE orders IS 'the orders'"},
	{sql: "COMMENT ON COLUMN orders.status IS 'the status'"},
	{sql: "GRANT SELECT ON orders TO apercu_lock_oracle_owner"},
	{sql: "REVOKE SELECT ON orders FROM apercu_lock_oracle_owner"},
	{sql: "ALTER DEFAULT PRIVILEGES IN SCHEMA " + classifytest.OracleSchema + " GRANT SELECT ON TABLES TO apercu_lock_oracle_owner"},

	// 5.5 types and domains.
	{sql: "ALTER TYPE mood ADD VALUE 'meh'"},
	{sql: "ALTER TYPE mood RENAME VALUE 'ok' TO 'fine'"},
	{sql: "ALTER TYPE plain_comp ADD ATTRIBUTE zip text"},
	{sql: "ALTER TYPE plain_comp DROP ATTRIBUTE b"},
	{sql: "ALTER TYPE plain_comp ALTER ATTRIBUTE a TYPE varchar(80)"},
	{sql: "ALTER TYPE address ADD ATTRIBUTE zip text", refusedOn: onEveryVersion},
	{sql: "ALTER TYPE address ADD ATTRIBUTE zip text CASCADE"},
	{sql: "ALTER TYPE address DROP ATTRIBUTE city CASCADE"},
	{sql: "ALTER TYPE address ALTER ATTRIBUTE street TYPE varchar(80) CASCADE"},
	{sql: "ALTER TYPE address ALTER ATTRIBUTE street TYPE varchar(80)", refusedOn: onEveryVersion},
	{sql: "ALTER TYPE col_comp ADD ATTRIBUTE zip text"},
	{sql: "ALTER TYPE col_comp ADD ATTRIBUTE zip text CASCADE"},
	{sql: "ALTER TYPE col_comp ALTER ATTRIBUTE a TYPE varchar(80) CASCADE", refusedOn: onEveryVersion},
	{sql: "ALTER DOMAIN positive_int ADD CONSTRAINT d CHECK (VALUE > -1)"},
	{sql: "ALTER DOMAIN positive_int ADD CONSTRAINT d CHECK (VALUE > -1) NOT VALID"},
	{sql: "DROP TYPE mood", refusedOn: onEveryVersion},
	{sql: "DROP TYPE mood CASCADE"},
	{sql: "DROP DOMAIN loose_text"},

	// 5.6 publications, subscriptions, extensions, schemas.
	{sql: "ALTER PUBLICATION oracle_pub ADD TABLE orders"},
	{sql: "ALTER PUBLICATION oracle_pub DROP TABLE pub_tbl"},
	{sql: "CREATE SCHEMA oracle_schema"},

	// 5.7 maintenance.
	{sql: "ANALYZE orders"},
	{sql: "ANALYZE parted"},
	{sql: "ANALYZE spare"},
	{sql: "ANALYZE ONLY spare", since: pgcontract.Version18},
	{sql: "VACUUM spare", outsideTransaction: true},
	{sql: "VACUUM FULL spare", outsideTransaction: true},
	{sql: "CLUSTER orders USING orders_pkey"},
	{sql: "TRUNCATE spare"},
	{sql: "TRUNCATE users", refusedOn: onEveryVersion},
	{sql: "TRUNCATE users CASCADE"},
	{sql: "TRUNCATE ticket RESTART IDENTITY", refusedOn: onEveryVersion},
	{sql: "TRUNCATE ticket, ticket_line RESTART IDENTITY"},
	{sql: "LOCK TABLE orders"},
	{sql: "LOCK TABLE orders IN SHARE MODE"},
	{sql: "LOCK TABLE parted IN ACCESS SHARE MODE"},
	{sql: "COPY spare TO PROGRAM 'cat > /dev/null'"},
	{sql: "COPY spare FROM PROGRAM 'echo 1'"},
	// The rows cover every partition on purpose: routing is data-dependent, so the rule names
	// them all and only a load that reaches them all can confirm the prediction.
	{sql: "COPY parted FROM PROGRAM 'echo 50; echo 150'"},

	// 5.8 data backfills.
	{sql: "INSERT INTO spare (id) VALUES (1)"},
	{sql: "INSERT INTO parted (id) SELECT id FROM orders"},
	{sql: "UPDATE orders SET status = 'done' WHERE id < 10"},
	{sql: "UPDATE spare SET id = id + 1"},
	{sql: "DELETE FROM spare WHERE id < 10"},
	{sql: "MERGE INTO spare t USING orders s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)"},
	{sql: "SELECT id FROM orders FOR UPDATE"},
	{sql: "SELECT id FROM orders FOR SHARE"},
	{sql: "SELECT id FROM spare FOR UPDATE"},
}

// TestPredictedLocksMatchTheServer is the lock oracle. §4 asserts a lock level for every rule id;
// this runs each one on 15, 16, 17 and 18 and reads back what the server took, from a second
// session, while the transaction is still open.
func TestPredictedLocksMatchTheServer(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	for _, version := range classifytest.Versions {
		wg.Add(1)
		go func(version pgcontract.Version) {
			defer wg.Done()
			t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
				db := classifytest.StartPostgres(t, version)
				_, err := db.Exec(classifytest.OracleDDL)
				require.NoError(t, err)
				for _, statement := range oracleTablespaceSetup {
					_, err = db.Exec(statement)
					require.NoErrorf(t, err, "%s", statement)
				}
				_, err = db.Exec(oracleTablespaceDDL)
				require.NoError(t, err)
				if version >= pgcontract.Version18 {
					_, err = db.Exec(oracleDDL18)
					require.NoError(t, err)
				}
				_, err = db.Exec(oracleTreeDDL)
				require.NoError(t, err)

				catalogs := classifytest.OracleCatalogs(t, db)
				require.Equal(t, version, catalogs().Version(), "P-18 has to see the version the rules are gated on")

				for _, script := range append(slices.Clone(lockScripts), partitionScripts...) {
					if !script.runsOn(version) {
						continue
					}
					t.Run(script.sql, func(t *testing.T) { replayLocks(t, db, catalogs(), script, version) })
				}
				// The scenarios commit their setup, so they run last.
				for _, scenario := range lockScenarios {
					t.Run(scenario.sql, func(t *testing.T) { replayScenario(t, db, catalogs(), scenario) })
				}
			})
		}(version)
	}
	wg.Wait()
}

// replayLocks runs one statement and compares what the classifier said against what the server did.
func replayLocks(t *testing.T, db *sql.DB, catalog *pgcatalog.Catalog, script lockScript, version pgcontract.Version) {
	t.Helper()

	statements := pgparse.Parse(script.sql)
	require.Len(t, statements, 1)
	analysis := pgclassify.NewClassifier(catalog).Next(statements[0])

	observed, execErr := classifytest.RunObserved(t, db, script.sql, script.outsideTransaction)

	// The script's own expectation about the server is not a classifier claim: it has to hold.
	assert.Equalf(t, script.refused(version), execErr != nil,
		"the server %s %q, the script says it %s (server said: %v)",
		classifytest.Accepted(execErr), script.sql, classifytest.Expected(script.refused(version)), execErr)

	var mismatches []string
	if (script.refused(version) && !script.leftToServer) != analysis.HasErrors() {
		mismatches = append(mismatches, fmt.Sprintf("the classifier %s %q, the server %s it", classifierVerdict(analysis), script.sql, classifytest.Accepted(execErr)))
	}
	if execErr == nil && !script.outsideTransaction {
		mismatches = append(mismatches, compareLocks(catalog, analysis, observed)...)
	}
	reportMismatches(t, script.sql, script.gap, script.gapped(version), mismatches)
}

// reportMismatches fails on every disagreement, except on a script tagged with a gap, which has to keep disagreeing.
func reportMismatches(t *testing.T, sql, gap string, gapped bool, mismatches []string) {
	t.Helper()
	if !gapped {
		for _, mismatch := range mismatches {
			t.Error(mismatch)
		}
		return
	}
	if len(mismatches) == 0 {
		t.Errorf("%q now matches the server: gap fixed (%s), remove the marker", sql, gap)
		return
	}
	t.Logf("known gap (%s): %s", gap, strings.Join(mismatches, "; "))
}

// workExempt are the commands whose reads are their own business: a query's scans are its result,
// not a cost the classifier grades, and ANALYZE samples without counting a scan.
var workExempt = map[pgcontract.Command]bool{
	"INSERT": true, "UPDATE": true, "DELETE": true, "MERGE": true, "SELECT FOR": true,
	"COPY FROM": true, "COPY TO": true, "CREATE TABLE AS": true, "CREATE VIEW": true,
	"CREATE MATERIALIZED VIEW": true, "REFRESH MATERIALIZED VIEW": true, "ANALYZE": true,
	// TRUNCATE swaps in a new file without reading the old one: METADATA by design.
	"TRUNCATE": true,
}

// compareLocks lists every disagreement between the classifier's targets and what the server did.
func compareLocks(catalog *pgcatalog.Catalog, analysis pgcontract.StatementAnalysis, observed classifytest.Observation) []string {
	var mismatches []string
	predicted := predictedTargets(analysis)

	for _, held := range observed.Locks {
		if catalog.Resolve(held.Name, nil).Origin != pgcatalog.OriginExisting {
			// A relation the statement creates is locked at ACCESS EXCLUSIVE under a name no
			// other session can reach yet, so it blocks nobody and no rule names it.
			continue
		}
		target, named := predicted[held.Name]
		kind := pgcontract.RelationKindFromRelkind(held.Kind)
		if !named {
			// An index is locked with the table it belongs to whether or not a rule names it
			if kind.IsTable() || kind == pgcontract.RelationKindMaterializedView {
				mismatches = append(mismatches, fmt.Sprintf("%q locks %s at %s and the classifier never named it", analysis.RawSQL, held.Name, held.Lock.Short()))
			}
			continue
		}
		if held.Lock != target.Lock {
			mismatches = append(mismatches, fmt.Sprintf("%q on %s: the server holds %s, the classifier predicts %s", analysis.RawSQL, held.Name, held.Lock.Short(), target.Lock.Short()))
		}
	}

	for name, target := range predicted {
		if _, held := observed.Locks[name]; !held {
			mismatches = append(mismatches, fmt.Sprintf("%q predicts %s on %s and the server locked nothing there", analysis.RawSQL, target.Lock.Short(), name))
		}
	}

	if workExempt[analysis.Command] {
		return mismatches
	}
	for name, target := range predicted {
		// Only a relation with rows of its own can be read or rewritten.
		if target.Relation.Kind != pgcontract.RelationKindTable && target.Relation.Kind != pgcontract.RelationKindMaterializedView {
			continue
		}
		if _, held := observed.Locks[name]; !held {
			continue
		}
		worked := observed.Work[name]
		switch {
		case target.OpKind.ScalesWithTableSize() && !worked:
			mismatches = append(mismatches, fmt.Sprintf("%q predicts %s on %s and the server never read it", analysis.RawSQL, target.OpKind, name))
		case !target.OpKind.ScalesWithTableSize() && worked &&
			(target.Role == pgcontract.TargetRoleDirect || target.Role == pgcontract.TargetRoleExpanded):
			mismatches = append(mismatches, fmt.Sprintf("%q reads %s, the classifier says %s", analysis.RawSQL, name, target.OpKind))
		}
	}
	return mismatches
}

// replayScenario commits the setup, then compares the last statement's locks: the classifier
// reads the whole migration from the catalog taken before it, as the CLI would.
func replayScenario(t *testing.T, db *sql.DB, catalog *pgcatalog.Catalog, scenario lockScenario) {
	t.Helper()

	statements := pgparse.Parse(strings.Join(append(slices.Clone(scenario.setup), scenario.sql), ";\n"))
	require.Len(t, statements, len(scenario.setup)+1)
	analyses := pgclassify.NewClassifier(catalog).Analyze(statements)

	for _, statement := range scenario.setup {
		_, err := db.Exec("SET search_path TO " + classifytest.OracleSchema + "; " + statement)
		require.NoErrorf(t, err, "%s", statement)
	}
	observed, err := classifytest.RunObserved(t, db, scenario.sql, false)
	require.NoError(t, err)

	last := analyses[len(analyses)-1]
	require.False(t, last.HasErrors(), "%v", last.Errors)
	reportMismatches(t, scenario.sql, scenario.gap, scenario.gap != "", compareLocks(catalog, last, observed))
}

func classifierVerdict(analysis pgcontract.StatementAnalysis) string {
	if analysis.HasErrors() {
		return "rejected"
	}
	return "accepted"
}

// predictedTargets is, for each relation, the strongest lock and the most severe work the
// classifier says the statement takes there, under the role it was first named with.
func predictedTargets(analysis pgcontract.StatementAnalysis) map[helper.FullRelationName]pgcontract.Target {
	out := map[helper.FullRelationName]pgcontract.Target{}
	for _, finding := range analysis.Findings {
		for _, target := range finding.Targets {
			if !strings.EqualFold(target.Relation.Name.Schema, classifytest.OracleSchema) {
				continue
			}
			if target.Lock == pgcontract.LockNone {
				continue
			}
			merged, seen := out[target.Relation.Name]
			if !seen {
				out[target.Relation.Name] = target
				continue
			}
			merged.Lock = pgcontract.MaxLock(merged.Lock, target.Lock)
			merged.OpKind = pgcontract.MaxOpKind(merged.OpKind, target.OpKind)
			out[target.Relation.Name] = merged
		}
	}
	return out
}
