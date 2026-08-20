package sql_parsing

import (
	"testing"

	"apercu-cli/helper"
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
)

// classify runs ClassifyOperation for a single statement and reports the
// resulting operation type together
func classify(sql string) metricshelper.EventOperationType {
	query := &metricshelper.QueryEventAnalysis{
		Event:          &metricshelper.QueryEvent{SQL: sql},
		AffectedTables: []helper.FullRelationName{{Schema: "public", Table: "t"}},
	}
	store := warning.NewWarningStore()
	prodStats := &metricshelper.DatabaseMetrics{}

	ClassifyOperation(query, store, prodStats)
	return query.Type
}

func TestClassifyOperation(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want metricshelper.EventOperationType
	}{
		// --- ADD COLUMN ---
		{"add col nullable", "ALTER TABLE t ADD COLUMN a int", metricshelper.EventOperationTypeMetadataOnly},
		{"add col default const", "ALTER TABLE t ADD COLUMN a int DEFAULT 5", metricshelper.EventOperationTypeMetadataOnly},
		{"add col not null default const", "ALTER TABLE t ADD COLUMN a int NOT NULL DEFAULT 5", metricshelper.EventOperationTypeMetadataOnly},
		{"add col default now()", "ALTER TABLE t ADD COLUMN a timestamptz DEFAULT now()", metricshelper.EventOperationTypeRewriteUnderLock},
		{"add col default gen_random_uuid", "ALTER TABLE t ADD COLUMN a uuid DEFAULT gen_random_uuid()", metricshelper.EventOperationTypeRewriteUnderLock},
		{"add col generated stored", "ALTER TABLE t ADD COLUMN a int GENERATED ALWAYS AS (b + 1) STORED", metricshelper.EventOperationTypeRewriteUnderLock},
		{"add col generated identity", "ALTER TABLE t ADD COLUMN a int GENERATED ALWAYS AS IDENTITY", metricshelper.EventOperationTypeRewriteUnderLock},

		// --- ALTER COLUMN ---
		{"set default", "ALTER TABLE t ALTER COLUMN a SET DEFAULT 1", metricshelper.EventOperationTypeMetadataOnly},
		{"drop default", "ALTER TABLE t ALTER COLUMN a DROP DEFAULT", metricshelper.EventOperationTypeMetadataOnly},
		{"drop not null", "ALTER TABLE t ALTER COLUMN a DROP NOT NULL", metricshelper.EventOperationTypeMetadataOnly},
		{"set not null", "ALTER TABLE t ALTER COLUMN a SET NOT NULL", metricshelper.EventOperationTypeScanUnderLock},
		{"type varchar to text", "ALTER TABLE t ALTER COLUMN a TYPE text", metricshelper.EventOperationTypeRewriteUnderLock},
		{"type varchar widen to text", "ALTER TABLE t ALTER COLUMN a TYPE text USING a::text", metricshelper.EventOperationTypeRewriteUnderLock},
		{"type to int rewrite", "ALTER TABLE t ALTER COLUMN a TYPE bigint", metricshelper.EventOperationTypeRewriteUnderLock},
		{"set storage", "ALTER TABLE t ALTER COLUMN a SET STORAGE EXTERNAL", metricshelper.EventOperationTypeMetadataOnly},

		// A bare ALTER COLUMN TYPE cannot see the source column length, so it is
		// conservatively treated as a rewrite even for varchar → text.
		{"varchar to text coercible", "ALTER TABLE t ALTER COLUMN a TYPE text /* from varchar */", metricshelper.EventOperationTypeRewriteUnderLock},

		// --- Constraints ---
		{"add check", "ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0)", metricshelper.EventOperationTypeScanUnderLock},
		{"add check not valid", "ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0) NOT VALID", metricshelper.EventOperationTypeMetadataOnly},
		{"add fk", "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (a) REFERENCES u (id)", metricshelper.EventOperationTypeScanUnderLock},
		{"add fk not valid", "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (a) REFERENCES u (id) NOT VALID", metricshelper.EventOperationTypeMetadataOnly},
		{"add unique", "ALTER TABLE t ADD CONSTRAINT c UNIQUE (a)", metricshelper.EventOperationTypeScanUnderLock},
		{"add primary key", "ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY (a)", metricshelper.EventOperationTypeScanUnderLock},
		{"add exclude", "ALTER TABLE t ADD CONSTRAINT c EXCLUDE USING gist (a WITH =)", metricshelper.EventOperationTypeScanUnderLock},
		{"drop constraint", "ALTER TABLE t DROP CONSTRAINT c", metricshelper.EventOperationTypeMetadataOnly},

		// --- Indexes ---
		{"create index", "CREATE INDEX idx ON t (a)", metricshelper.EventOperationTypeScanUnderLock},
		{"create index concurrently", "CREATE INDEX CONCURRENTLY idx ON t (a)", metricshelper.EventOperationTypeNonBlocking},
		{"drop index", "DROP INDEX idx", metricshelper.EventOperationTypeMetadataOnly},
		{"drop index concurrently", "DROP INDEX CONCURRENTLY idx", metricshelper.EventOperationTypeNonBlocking},
		{"reindex", "REINDEX INDEX idx", metricshelper.EventOperationTypeRewriteUnderLock},
		{"reindex concurrently", "REINDEX INDEX CONCURRENTLY idx", metricshelper.EventOperationTypeNonBlocking},

		// --- Table-level ---
		{"set logged", "ALTER TABLE t SET LOGGED", metricshelper.EventOperationTypeRewriteUnderLock},
		{"set unlogged", "ALTER TABLE t SET UNLOGGED", metricshelper.EventOperationTypeRewriteUnderLock},
		{"set tablespace", "ALTER TABLE t SET TABLESPACE ts", metricshelper.EventOperationTypeRewriteUnderLock},
		{"cluster", "CLUSTER t USING idx", metricshelper.EventOperationTypeRewriteUnderLock},
		{"vacuum full", "VACUUM FULL t", metricshelper.EventOperationTypeRewriteUnderLock},
		{"truncate", "TRUNCATE t", metricshelper.EventOperationTypeMetadataOnly},
		{"set fillfactor", "ALTER TABLE t SET (fillfactor = 70)", metricshelper.EventOperationTypeMetadataOnly},

		// --- Other DDL ---
		{"create trigger", "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()", metricshelper.EventOperationTypeMetadataOnly},
		{"enable trigger", "ALTER TABLE t ENABLE TRIGGER trg", metricshelper.EventOperationTypeMetadataOnly},
		{"disable trigger", "ALTER TABLE t DISABLE TRIGGER trg", metricshelper.EventOperationTypeMetadataOnly},
		{"refresh matview", "REFRESH MATERIALIZED VIEW mv", metricshelper.EventOperationTypeRewriteUnderLock},
		{"refresh matview concurrently", "REFRESH MATERIALIZED VIEW CONCURRENTLY mv", metricshelper.EventOperationTypeNonBlocking},
		{"alter type add value", "ALTER TYPE color ADD VALUE 'blue'", metricshelper.EventOperationTypeMetadataOnly},
		{"rename column", "ALTER TABLE t RENAME COLUMN a TO b", metricshelper.EventOperationTypeMetadataOnly},

		// --- Multi-subcommand ---
		{"add col + set not null", "ALTER TABLE t ADD COLUMN a int, ALTER COLUMN b SET NOT NULL", metricshelper.EventOperationTypeScanUnderLock},
		{"set default + type change", "ALTER TABLE t ALTER COLUMN a SET DEFAULT 1, ALTER COLUMN b TYPE bigint", metricshelper.EventOperationTypeRewriteUnderLock},
		{"with trailing semicolon", "ALTER TABLE t ADD COLUMN a int, ALTER COLUMN b SET NOT NULL;", metricshelper.EventOperationTypeScanUnderLock},

		// --- Formatting ---
		{"trailing semicolon", "ALTER TABLE t SET LOGGED;", metricshelper.EventOperationTypeRewriteUnderLock},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classify(tc.sql)
			if got != tc.want {
				t.Errorf("kind = %q, want %q (sql: %s)", got, tc.want, tc.sql)
			}
		})
	}
}
