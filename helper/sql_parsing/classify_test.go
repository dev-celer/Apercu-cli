package sql_parsing

import (
	"strings"
	"testing"

	"apercu-cli/helper"
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
)

// classify runs ClassifyOperation for a single statement and reports the
// resulting operation type together with whether any emitted warning carries a
// remediation. Remediation text lives on the (unexported) warning, so we detect
// it through the public GetTextLong output.
func classify(sql string, version float32) (metricshelper.EventOperationType, bool) {
	query := &metricshelper.QueryEventAnalysis{
		Event:          &metricshelper.QueryEvent{SQL: sql},
		AffectedTables: []helper.FullTableName{{Schema: "public", Table: "t"}},
	}
	store := warning.NewWarningStore()
	prodStats := &metricshelper.DatabaseMetrics{}

	ClassifyOperation(query, version, store, prodStats)

	hasRemediation := false
	for _, w := range query.Warnings {
		if strings.Contains(w.GetTextLong(), "remediation:") {
			hasRemediation = true
			break
		}
	}
	return query.Type, hasRemediation
}

func TestClassifyOperation(t *testing.T) {
	const pg15 = float32(15)

	cases := []struct {
		name    string
		sql     string
		version float32
		want    metricshelper.EventOperationType
		// wantRem: "" means no warning must carry a remediation; "any" means at
		// least one emitted warning must carry a remediation.
		wantRem string
	}{
		// --- ADD COLUMN ---
		{"add col nullable", "ALTER TABLE t ADD COLUMN a int", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},
		{"add col default const", "ALTER TABLE t ADD COLUMN a int DEFAULT 5", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},
		{"add col not null default const", "ALTER TABLE t ADD COLUMN a int NOT NULL DEFAULT 5", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},
		// Version is not currently factored into ADD COLUMN DEFAULT <const> — it is
		// treated as metadata-only regardless of server version.
		{"add col default const pre-11", "ALTER TABLE t ADD COLUMN a int DEFAULT 5", 10, metricshelper.EventOperationTypeMetadataOnly, ""},
		{"add col default now()", "ALTER TABLE t ADD COLUMN a timestamptz DEFAULT now()", pg15, metricshelper.EventOperationTypeRewriteUnderLock, "any"},
		{"add col default gen_random_uuid", "ALTER TABLE t ADD COLUMN a uuid DEFAULT gen_random_uuid()", pg15, metricshelper.EventOperationTypeRewriteUnderLock, "any"},
		{"add col generated stored", "ALTER TABLE t ADD COLUMN a int GENERATED ALWAYS AS (b + 1) STORED", pg15, metricshelper.EventOperationTypeRewriteUnderLock, "any"},
		{"add col generated identity", "ALTER TABLE t ADD COLUMN a int GENERATED ALWAYS AS IDENTITY", pg15, metricshelper.EventOperationTypeRewriteUnderLock, "any"},

		// --- ALTER COLUMN ---
		{"set default", "ALTER TABLE t ALTER COLUMN a SET DEFAULT 1", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},
		{"drop default", "ALTER TABLE t ALTER COLUMN a DROP DEFAULT", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},
		{"drop not null", "ALTER TABLE t ALTER COLUMN a DROP NOT NULL", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},
		{"set not null", "ALTER TABLE t ALTER COLUMN a SET NOT NULL", pg15, metricshelper.EventOperationTypeScanUnderLock, ""},
		{"type varchar to text", "ALTER TABLE t ALTER COLUMN a TYPE text", pg15, metricshelper.EventOperationTypeRewriteUnderLock, "any"},
		{"type varchar widen to text", "ALTER TABLE t ALTER COLUMN a TYPE text USING a::text", pg15, metricshelper.EventOperationTypeRewriteUnderLock, "any"},
		{"type to int rewrite", "ALTER TABLE t ALTER COLUMN a TYPE bigint", pg15, metricshelper.EventOperationTypeRewriteUnderLock, "any"},
		{"set storage", "ALTER TABLE t ALTER COLUMN a SET STORAGE EXTERNAL", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},

		// A bare ALTER COLUMN TYPE cannot see the source column length, so it is
		// conservatively treated as a rewrite even for varchar → text.
		{"varchar to text coercible", "ALTER TABLE t ALTER COLUMN a TYPE text /* from varchar */", pg15, metricshelper.EventOperationTypeRewriteUnderLock, "any"},

		// --- Constraints ---
		{"add check", "ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0)", pg15, metricshelper.EventOperationTypeScanUnderLock, "any"},
		{"add check not valid", "ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0) NOT VALID", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},
		{"add fk", "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (a) REFERENCES u (id)", pg15, metricshelper.EventOperationTypeScanUnderLock, "any"},
		{"add fk not valid", "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (a) REFERENCES u (id) NOT VALID", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},
		{"add unique", "ALTER TABLE t ADD CONSTRAINT c UNIQUE (a)", pg15, metricshelper.EventOperationTypeScanUnderLock, "any"},
		{"add primary key", "ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY (a)", pg15, metricshelper.EventOperationTypeScanUnderLock, "any"},
		{"add exclude", "ALTER TABLE t ADD CONSTRAINT c EXCLUDE USING gist (a WITH =)", pg15, metricshelper.EventOperationTypeScanUnderLock, ""},
		{"drop constraint", "ALTER TABLE t DROP CONSTRAINT c", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},

		// --- Indexes ---
		{"create index", "CREATE INDEX idx ON t (a)", pg15, metricshelper.EventOperationTypeScanUnderLock, "any"},
		{"create index concurrently", "CREATE INDEX CONCURRENTLY idx ON t (a)", pg15, metricshelper.EventOperationTypeNonBlocking, ""},
		{"drop index", "DROP INDEX idx", pg15, metricshelper.EventOperationTypeMetadataOnly, "any"},
		{"drop index concurrently", "DROP INDEX CONCURRENTLY idx", pg15, metricshelper.EventOperationTypeNonBlocking, ""},
		{"reindex", "REINDEX INDEX idx", pg15, metricshelper.EventOperationTypeRewriteUnderLock, "any"},
		{"reindex concurrently", "REINDEX INDEX CONCURRENTLY idx", pg15, metricshelper.EventOperationTypeNonBlocking, ""},

		// --- Table-level ---
		{"set logged", "ALTER TABLE t SET LOGGED", pg15, metricshelper.EventOperationTypeRewriteUnderLock, ""},
		{"set unlogged", "ALTER TABLE t SET UNLOGGED", pg15, metricshelper.EventOperationTypeRewriteUnderLock, ""},
		{"set tablespace", "ALTER TABLE t SET TABLESPACE ts", pg15, metricshelper.EventOperationTypeRewriteUnderLock, ""},
		{"cluster", "CLUSTER t USING idx", pg15, metricshelper.EventOperationTypeRewriteUnderLock, ""},
		{"vacuum full", "VACUUM FULL t", pg15, metricshelper.EventOperationTypeRewriteUnderLock, ""},
		{"truncate", "TRUNCATE t", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},
		{"set fillfactor", "ALTER TABLE t SET (fillfactor = 70)", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},

		// --- Other DDL ---
		{"create trigger", "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},
		{"enable trigger", "ALTER TABLE t ENABLE TRIGGER trg", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},
		{"disable trigger", "ALTER TABLE t DISABLE TRIGGER trg", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},
		{"refresh matview", "REFRESH MATERIALIZED VIEW mv", pg15, metricshelper.EventOperationTypeRewriteUnderLock, "any"},
		{"refresh matview concurrently", "REFRESH MATERIALIZED VIEW CONCURRENTLY mv", pg15, metricshelper.EventOperationTypeNonBlocking, ""},
		{"alter type add value", "ALTER TYPE color ADD VALUE 'blue'", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},
		{"rename column", "ALTER TABLE t RENAME COLUMN a TO b", pg15, metricshelper.EventOperationTypeMetadataOnly, ""},

		// --- Multi-subcommand: most severe wins ---
		{"add col + set not null", "ALTER TABLE t ADD COLUMN a int, ALTER COLUMN b SET NOT NULL", pg15, metricshelper.EventOperationTypeScanUnderLock, ""},
		{"set default + type change", "ALTER TABLE t ALTER COLUMN a SET DEFAULT 1, ALTER COLUMN b TYPE bigint", pg15, metricshelper.EventOperationTypeRewriteUnderLock, "any"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, hasRem := classify(tc.sql, tc.version)
			if got != tc.want {
				t.Errorf("kind = %q, want %q (sql: %s)", got, tc.want, tc.sql)
			}
			switch tc.wantRem {
			case "":
				if hasRem {
					t.Errorf("remediation present, want none (sql: %s)", tc.sql)
				}
			case "any":
				if !hasRem {
					t.Errorf("remediation missing, want one (sql: %s)", tc.sql)
				}
			}
		})
	}
}
