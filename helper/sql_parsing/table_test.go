package sql_parsing

import (
	"reflect"
	"testing"

	"apercu-cli/helper"
)

func tbl(schema, table string) helper.FullTableName {
	return helper.FullTableName{Schema: schema, Table: table}
}

func TestParseFullTableName(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want helper.FullTableName
	}{
		{"bare table defaults to public", "users", tbl("public", "users")},
		{"schema qualified", "app.users", tbl("app", "users")},
		{"empty string", "", tbl("public", "")},
		{"multiple dots split on first", "a.b.c", tbl("a", "b.c")},
		{"trailing dot", "app.", tbl("public", "app.")},
		{"leading dot", ".users", tbl("public", "users")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseFullTableName(tc.in); got != tc.want {
				t.Errorf("parseFullTableName(%q) = %+v, want %+v", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseTables(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []helper.FullTableName
	}{
		// --- CREATE INDEX ---
		{"create index", "CREATE INDEX idx ON t (a)", []helper.FullTableName{tbl("public", "t")}},
		{"create index schema qualified", "CREATE INDEX idx ON app.t (a)", []helper.FullTableName{tbl("app", "t")}},
		{"create unique index", "CREATE UNIQUE INDEX idx ON t (a)", []helper.FullTableName{tbl("public", "t")}},
		{"create index concurrently", "CREATE INDEX CONCURRENTLY idx ON t (a)", []helper.FullTableName{tbl("public", "t")}},
		{"create index if not exists", "CREATE INDEX IF NOT EXISTS idx ON t (a)", []helper.FullTableName{tbl("public", "t")}},
		{"create index on only", "CREATE INDEX idx ON ONLY t (a)", []helper.FullTableName{tbl("public", "t")}},
		{"create index lowercase", "create index idx on t (a)", []helper.FullTableName{tbl("public", "t")}},
		{"leading whitespace trimmed", "  CREATE INDEX idx ON t (a)", []helper.FullTableName{tbl("public", "t")}},

		// --- CREATE TRIGGER ---
		{"create trigger", "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()", []helper.FullTableName{tbl("public", "t")}},
		{"create trigger with 'ON' keywork inside the function", "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f( ON xxx )", []helper.FullTableName{tbl("public", "t")}},

		// --- REFRESH MATERIALIZED VIEW ---
		{"refresh matview", "REFRESH MATERIALIZED VIEW mv", []helper.FullTableName{tbl("public", "mv")}},
		{"refresh matview concurrently", "REFRESH MATERIALIZED VIEW CONCURRENTLY app.mv", []helper.FullTableName{tbl("app", "mv")}},

		// --- CLUSTER ---
		{"cluster using index", "CLUSTER t USING idx", []helper.FullTableName{tbl("public", "t")}},
		{"cluster schema qualified", "CLUSTER app.t USING idx", []helper.FullTableName{tbl("app", "t")}},
		{"cluster verbose", "CLUSTER VERBOSE t USING idx", []helper.FullTableName{tbl("public", "t")}},

		// --- ALTER TABLE ---
		{"alter table add column", "ALTER TABLE t ADD COLUMN a int", []helper.FullTableName{tbl("public", "t")}},
		{"alter table if exists", "ALTER TABLE IF EXISTS t ADD COLUMN a int", []helper.FullTableName{tbl("public", "t")}},
		{"alter table only", "ALTER TABLE ONLY app.t ADD COLUMN a int", []helper.FullTableName{tbl("app", "t")}},
		{"alter table if exists only", "ALTER TABLE IF EXISTS ONLY app.t DROP COLUMN a", []helper.FullTableName{tbl("app", "t")}},

		// --- Non-matching / unknown ---
		{"select returns nil", "SELECT * FROM t", nil},
		{"empty string returns nil", "", nil},
		{"drop table returns nil", "DROP TABLE t", nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseTables(tc.sql)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ParseTables(%q) = %+v, want %+v", tc.sql, got, tc.want)
			}
		})
	}
}

func TestParseVacuum(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []helper.FullTableName
	}{
		{"single table", "VACUUM t", []helper.FullTableName{tbl("public", "t")}},
		{"schema qualified", "VACUUM app.t", []helper.FullTableName{tbl("app", "t")}},
		{"multiple tables", "VACUUM a, b", []helper.FullTableName{tbl("public", "a"), tbl("public", "b")}},
		{"with parenthesized options", "VACUUM (VERBOSE, ANALYZE) t", []helper.FullTableName{tbl("public", "t")}},
		{"only keyword stripped", "VACUUM ONLY t", []helper.FullTableName{tbl("public", "t")}},
		{"table with column list", "VACUUM t (a, b)", []helper.FullTableName{tbl("public", "t")}},
		{"vacuum full legacy", "VACUUM FULL t", []helper.FullTableName{tbl("public", "t")}},
		{"vacuum full all tables", "VACUUM (FULL)", []helper.FullTableName{}},
		{"vacuum full legacy all tables", "VACUUM FULL", []helper.FullTableName{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseTables(tc.sql)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ParseTables(%q) = %+v, want %+v", tc.sql, got, tc.want)
			}
		})
	}
}
