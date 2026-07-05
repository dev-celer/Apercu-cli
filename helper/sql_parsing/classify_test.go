package sql_parsing

import "testing"

func TestClassifyOperation(t *testing.T) {
	const pg15 = float32(15)

	cases := []struct {
		name    string
		sql     string
		version float32
		want    EventOperationType
		// wantRem: "" means we only assert the kind; "any" means remediation
		// must be non-empty; otherwise an exact match is asserted-as-contains.
		wantRem string
	}{
		// --- ADD COLUMN ---
		{"add col nullable", "ALTER TABLE t ADD COLUMN a int", pg15, EventOperationTypeMetadataOnly, ""},
		{"add col default const", "ALTER TABLE t ADD COLUMN a int DEFAULT 5", pg15, EventOperationTypeMetadataOnly, ""},
		{"add col not null default const", "ALTER TABLE t ADD COLUMN a int NOT NULL DEFAULT 5", pg15, EventOperationTypeMetadataOnly, ""},
		{"add col default const pre-11", "ALTER TABLE t ADD COLUMN a int DEFAULT 5", 10, EventOperationTypeRewriteUnderLock, "any"},
		{"add col default now()", "ALTER TABLE t ADD COLUMN a timestamptz DEFAULT now()", pg15, EventOperationTypeRewriteUnderLock, "any"},
		{"add col default gen_random_uuid", "ALTER TABLE t ADD COLUMN a uuid DEFAULT gen_random_uuid()", pg15, EventOperationTypeRewriteUnderLock, "any"},
		{"add col generated stored", "ALTER TABLE t ADD COLUMN a int GENERATED ALWAYS AS (b + 1) STORED", pg15, EventOperationTypeRewriteUnderLock, "any"},
		{"add col generated identity", "ALTER TABLE t ADD COLUMN a int GENERATED ALWAYS AS IDENTITY", pg15, EventOperationTypeRewriteUnderLock, "any"},

		// --- ALTER COLUMN ---
		{"set default", "ALTER TABLE t ALTER COLUMN a SET DEFAULT 1", pg15, EventOperationTypeMetadataOnly, ""},
		{"drop default", "ALTER TABLE t ALTER COLUMN a DROP DEFAULT", pg15, EventOperationTypeMetadataOnly, ""},
		{"drop not null", "ALTER TABLE t ALTER COLUMN a DROP NOT NULL", pg15, EventOperationTypeMetadataOnly, ""},
		{"set not null", "ALTER TABLE t ALTER COLUMN a SET NOT NULL", pg15, EventOperationTypeScanUnderLock, ""},
		{"type varchar to text", "ALTER TABLE t ALTER COLUMN a TYPE text", pg15, EventOperationTypeRewriteUnderLock, "any"},
		{"type varchar widen to text", "ALTER TABLE t ALTER COLUMN a TYPE text USING a::text", pg15, EventOperationTypeRewriteUnderLock, "any"},
		{"type to int rewrite", "ALTER TABLE t ALTER COLUMN a TYPE bigint", pg15, EventOperationTypeRewriteUnderLock, "any"},
		{"set storage", "ALTER TABLE t ALTER COLUMN a SET STORAGE EXTERNAL", pg15, EventOperationTypeMetadataOnly, ""},

		// varchar(n) -> text is the canonical binary-coercible widening (source in SQL).
		{"varchar to text coercible", "ALTER TABLE t ALTER COLUMN a TYPE text /* from varchar */", pg15, EventOperationTypeRewriteUnderLock, "any"},

		// --- Constraints ---
		{"add check", "ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0)", pg15, EventOperationTypeScanUnderLock, "any"},
		{"add check not valid", "ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0) NOT VALID", pg15, EventOperationTypeMetadataOnly, ""},
		{"add fk", "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (a) REFERENCES u (id)", pg15, EventOperationTypeScanUnderLock, "any"},
		{"add fk not valid", "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (a) REFERENCES u (id) NOT VALID", pg15, EventOperationTypeMetadataOnly, ""},
		{"add unique", "ALTER TABLE t ADD CONSTRAINT c UNIQUE (a)", pg15, EventOperationTypeScanUnderLock, "any"},
		{"add primary key", "ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY (a)", pg15, EventOperationTypeScanUnderLock, "any"},
		{"add exclude", "ALTER TABLE t ADD CONSTRAINT c EXCLUDE USING gist (a WITH =)", pg15, EventOperationTypeScanUnderLock, ""},
		{"drop constraint", "ALTER TABLE t DROP CONSTRAINT c", pg15, EventOperationTypeMetadataOnly, ""},

		// --- Indexes ---
		{"create index", "CREATE INDEX idx ON t (a)", pg15, EventOperationTypeScanUnderLock, "any"},
		{"create index concurrently", "CREATE INDEX CONCURRENTLY idx ON t (a)", pg15, EventOperationTypeNonBlocking, ""},
		{"drop index", "DROP INDEX idx", pg15, EventOperationTypeMetadataOnly, "any"},
		{"drop index concurrently", "DROP INDEX CONCURRENTLY idx", pg15, EventOperationTypeNonBlocking, ""},
		{"reindex", "REINDEX INDEX idx", pg15, EventOperationTypeRewriteUnderLock, "any"},
		{"reindex concurrently", "REINDEX INDEX CONCURRENTLY idx", pg15, EventOperationTypeNonBlocking, ""},

		// --- Table-level ---
		{"set logged", "ALTER TABLE t SET LOGGED", pg15, EventOperationTypeRewriteUnderLock, ""},
		{"set unlogged", "ALTER TABLE t SET UNLOGGED", pg15, EventOperationTypeRewriteUnderLock, ""},
		{"set tablespace", "ALTER TABLE t SET TABLESPACE ts", pg15, EventOperationTypeRewriteUnderLock, ""},
		{"cluster", "CLUSTER t USING idx", pg15, EventOperationTypeRewriteUnderLock, ""},
		{"vacuum full", "VACUUM FULL t", pg15, EventOperationTypeRewriteUnderLock, ""},
		{"truncate", "TRUNCATE t", pg15, EventOperationTypeMetadataOnly, ""},
		{"set fillfactor", "ALTER TABLE t SET (fillfactor = 70)", pg15, EventOperationTypeMetadataOnly, ""},

		// --- Other DDL ---
		{"create trigger", "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()", pg15, EventOperationTypeMetadataOnly, ""},
		{"enable trigger", "ALTER TABLE t ENABLE TRIGGER trg", pg15, EventOperationTypeMetadataOnly, ""},
		{"disable trigger", "ALTER TABLE t DISABLE TRIGGER trg", pg15, EventOperationTypeMetadataOnly, ""},
		{"refresh matview", "REFRESH MATERIALIZED VIEW mv", pg15, EventOperationTypeRewriteUnderLock, "any"},
		{"refresh matview concurrently", "REFRESH MATERIALIZED VIEW CONCURRENTLY mv", pg15, EventOperationTypeNonBlocking, ""},
		{"alter type add value", "ALTER TYPE color ADD VALUE 'blue'", pg15, EventOperationTypeMetadataOnly, ""},
		{"rename column", "ALTER TABLE t RENAME COLUMN a TO b", pg15, EventOperationTypeMetadataOnly, ""},

		// --- Multi-subcommand: most severe wins ---
		{"add col + set not null", "ALTER TABLE t ADD COLUMN a int, ALTER COLUMN b SET NOT NULL", pg15, EventOperationTypeScanUnderLock, ""},
		{"set default + type change", "ALTER TABLE t ALTER COLUMN a SET DEFAULT 1, ALTER COLUMN b TYPE bigint", pg15, EventOperationTypeRewriteUnderLock, "any"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, rem := ClassifyOperation(tc.sql, tc.version)
			if got != tc.want {
				t.Errorf("kind = %q, want %q (sql: %s)", got, tc.want, tc.sql)
			}
			switch tc.wantRem {
			case "":
				if rem != "" {
					t.Errorf("remediation = %q, want empty (sql: %s)", rem, tc.sql)
				}
			case "any":
				if rem == "" {
					t.Errorf("remediation empty, want non-empty (sql: %s)", tc.sql)
				}
			}
		})
	}
}
