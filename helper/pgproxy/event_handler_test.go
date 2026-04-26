package main

import (
	"reflect"
	"testing"
)

func ptrLock(l QueryLock) *QueryLock { return &l }

func TestStripLeadingComments(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"only whitespace", "   \t\n", ""},
		{"plain sql untouched", "SELECT 1", "SELECT 1"},
		{"leading whitespace trimmed", "   SELECT 1", "SELECT 1"},
		{"single line comment", "-- hi\nSELECT 1", "SELECT 1"},
		{"multiple line comments", "-- hi\n-- ho\nSELECT 1", "SELECT 1"},
		{"block comment", "/* hello */ SELECT 1", "SELECT 1"},
		{"line then block", "-- one\n/* two */ SELECT 1", "SELECT 1"},
		{"unterminated line comment yields empty", "-- forever", ""},
		{"unterminated block comment yields empty", "/* forever", ""},
		{"comment after sql is preserved", "SELECT /* mid */ 1", "SELECT /* mid */ 1"},
		{"line comment with crlf-ish", "-- hi\nSELECT 1\n", "SELECT 1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := stripLeadingComments(tc.in); got != tc.want {
				t.Errorf("stripLeadingComments(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestCollapseSpaces(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"single token", "SELECT", "SELECT"},
		{"runs of spaces collapse", "SELECT   *", "SELECT *"},
		{"tab becomes space", "SELECT\t*", "SELECT *"},
		{"newline becomes space", "SELECT\n*", "SELECT *"},
		{"mixed whitespace collapses", "SELECT \t\n*", "SELECT *"},
		{"leading and trailing not trimmed", "  SELECT  ", " SELECT "},
		{"whitespace inside double quotes preserved", `SELECT * FROM "my  table"`, `SELECT * FROM "my  table"`},
		{"newline inside double quotes preserved", "\"a\nb\"", "\"a\nb\""},
		{"whitespace inside single quotes preserved", `SELECT 'a   b'`, `SELECT 'a   b'`},
		{"escaped doubled double quote", `"foo""bar"`, `"foo""bar"`},
		{"escaped doubled single quote", `'a''b'`, `'a''b'`},
		{"mixed inside and outside literals", `SELECT   "x  y" ,   'a  b'`, `SELECT "x  y" , 'a  b'`},
		{"empty quoted", `""`, `""`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := collapseSpaces(tc.in); got != tc.want {
				t.Errorf("collapseSpaces(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestExtractIdentifier(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"unquoted", "users", "users"},
		{"unquoted with trailing space", "users ", "users"},
		{"unquoted stops at paren", "users(id)", "users"},
		{"unquoted stops at comma", "users, other", "users"},
		{"unquoted schema-qualified", "public.users", "public.users"},
		{"unquoted schema with trailing", "public.users ", "public.users"},
		{"quoted no schema", `"users"`, `"users"`},
		{"quoted stops at paren", `"users"(id)`, `"users"`},
		{"both segments quoted", `"public"."users"`, `"public"."users"`},
		{"quoted schema unquoted table", `"public".users`, `"public".users`},
		{"unquoted schema quoted table", `public."users"`, `public."users"`},
		{"escaped doubled quote inside identifier", `"a""b"`, `"a""b"`},
		{"identifier with digits and underscores", "my_table_42", "my_table_42"},
		{"case preserved", "Users", "Users"},
		{"unterminated quote returns empty", `"foo`, ""},
		{"only dot returns empty", ".", ""},
		{"trailing dot returns empty", "foo.", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractIdentifier(tc.in); got != tc.want {
				t.Errorf("extractIdentifier(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestGetAffectedTable(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"select returns empty", "SELECT * FROM users", ""},
		{"begin returns empty", "BEGIN", ""},
		{"insert into", "INSERT INTO users (id) VALUES (1)", "users"},
		{"insert into schema-qualified", "INSERT INTO public.users (id) VALUES (1)", "public.users"},
		{"insert into quoted", `INSERT INTO "users" (id) VALUES (1)`, `"users"`},
		{"insert into both quoted", `INSERT INTO "public"."users" (id) VALUES (1)`, `"public"."users"`},
		{"update", "UPDATE users SET name = 'a'", "users"},
		{"delete from", "DELETE FROM users WHERE id = 1", "users"},
		{"merge into", "MERGE INTO users USING src ON ...", "users"},
		{"truncate table", "TRUNCATE TABLE users", "users"},
		{"truncate plain", "TRUNCATE users", "users"},
		{"alter table", "ALTER TABLE users ADD COLUMN x int", "users"},
		{"alter table only", "ALTER TABLE ONLY users ADD COLUMN x int", "users"},
		{"alter table if exists", "ALTER TABLE IF EXISTS users ADD COLUMN x int", "users"},
		{"drop table", "DROP TABLE users", "users"},
		{"drop table if exists", "DROP TABLE IF EXISTS users", "users"},
		{"create table", "CREATE TABLE users (id int)", "users"},
		{"create table if not exists", "CREATE TABLE IF NOT EXISTS users (id int)", "users"},
		{"vacuum", "VACUUM users", "users"},
		{"analyze", "ANALYZE users", "users"},
		{"cluster", "CLUSTER users", "users"},
		{"lock table", "LOCK TABLE users", "users"},
		{"copy from", "COPY users FROM stdin", "users"},
		{"refresh mat view", "REFRESH MATERIALIZED VIEW v", "v"},
		{"refresh mat view concurrently", "REFRESH MATERIALIZED VIEW CONCURRENTLY v", "v"},
		{"lowercase keyword", "insert into users (id) values (1)", "users"},

		{"create index", "CREATE INDEX i ON users (id)", "users"},
		{"create unique index", "CREATE UNIQUE INDEX i ON users (id)", "users"},
		{"create index concurrently", "CREATE INDEX CONCURRENTLY i ON users (id)", "users"},
		{"create unique index concurrently", "CREATE UNIQUE INDEX CONCURRENTLY i ON users (id)", "users"},
		{"create index without name", "CREATE INDEX ON users (id)", "users"},
		{"create index if not exists", "CREATE INDEX IF NOT EXISTS i ON users (id)", "users"},
		{"create index schema-qualified table", "CREATE INDEX i ON public.users (id)", "public.users"},
		{"create index quoted table", `CREATE INDEX i ON "users" (id)`, `"users"`},
		{"create index quoted schema and table", `CREATE INDEX i ON "public"."users" (id)`, `"public"."users"`},
		{"create index on only", "CREATE INDEX i ON ONLY users (id)", "users"},
		{"create index lowercase", "create index i on users (id)", "users"},
		{"create index multi-column", "CREATE INDEX i ON users (id, name)", "users"},
		{"create index using btree", "CREATE INDEX i ON users USING btree (id)", "users"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev := &QueryEvent{SQL: tc.sql}
			if got := getAffectedTable(ev); got != tc.want {
				t.Errorf("getAffectedTable(%q) = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}

func TestGetLockType(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want *QueryLock
	}{
		{"empty returns nil", "", nil},
		{"begin returns nil", "BEGIN", nil},
		{"commit returns nil", "COMMIT", nil},
		{"set returns nil", "SET search_path = public", nil},
		{"explain returns nil", "EXPLAIN SELECT 1", nil},

		{"select", "SELECT * FROM users", ptrLock(QueryLockAccessShare)},
		{"select for update", "SELECT * FROM users FOR UPDATE", ptrLock(QueryLockRowShare)},
		{"select for share", "SELECT * FROM users FOR SHARE", ptrLock(QueryLockRowShare)},
		{"select for no key update", "SELECT * FROM users FOR NO KEY UPDATE", ptrLock(QueryLockRowShare)},
		{"select for key share", "SELECT * FROM users FOR KEY SHARE", ptrLock(QueryLockRowShare)},
		{"with cte select", "WITH x AS (SELECT 1) SELECT * FROM x", ptrLock(QueryLockAccessShare)},
		{"values", "VALUES (1), (2)", ptrLock(QueryLockAccessShare)},
		{"table form", "TABLE users", ptrLock(QueryLockAccessShare)},
		{"lowercase select", "select * from users", ptrLock(QueryLockAccessShare)},

		{"insert", "INSERT INTO users (id) VALUES (1)", ptrLock(QueryLockRowExclusive)},
		{"update", "UPDATE users SET x = 1", ptrLock(QueryLockRowExclusive)},
		{"delete", "DELETE FROM users", ptrLock(QueryLockRowExclusive)},
		{"merge", "MERGE INTO users USING src ON ...", ptrLock(QueryLockRowExclusive)},
		{"copy from", "COPY users FROM stdin", ptrLock(QueryLockRowExclusive)},
		{"copy to", "COPY users TO stdout", ptrLock(QueryLockAccessShare)},

		{"truncate", "TRUNCATE users", ptrLock(QueryLockAccessExclusive)},
		{"cluster", "CLUSTER users", ptrLock(QueryLockAccessExclusive)},
		{"vacuum non-full", "VACUUM users", ptrLock(QueryLockShareUpdateExclusive)},
		{"vacuum full", "VACUUM FULL users", ptrLock(QueryLockAccessExclusive)},
		{"analyze", "ANALYZE users", ptrLock(QueryLockShareUpdateExclusive)},
		{"create statistics", "CREATE STATISTICS s ON x FROM users", ptrLock(QueryLockShareUpdateExclusive)},
		{"comment on", "COMMENT ON TABLE users IS 'x'", ptrLock(QueryLockShareUpdateExclusive)},

		{"reindex", "REINDEX TABLE users", ptrLock(QueryLockAccessExclusive)},
		{"reindex concurrently", "REINDEX TABLE CONCURRENTLY users", ptrLock(QueryLockShareUpdateExclusive)},
		{"refresh mv", "REFRESH MATERIALIZED VIEW v", ptrLock(QueryLockAccessExclusive)},
		{"refresh mv concurrently", "REFRESH MATERIALIZED VIEW CONCURRENTLY v", ptrLock(QueryLockExclusive)},

		{"create index", "CREATE INDEX i ON users (id)", ptrLock(QueryLockShare)},
		{"create unique index", "CREATE UNIQUE INDEX i ON users (id)", ptrLock(QueryLockShare)},
		{"create index concurrently", "CREATE INDEX CONCURRENTLY i ON users (id)", ptrLock(QueryLockShareUpdateExclusive)},
		{"create trigger", "CREATE TRIGGER t BEFORE UPDATE ON users EXECUTE FUNCTION f()", ptrLock(QueryLockShareRowExclusive)},

		{"drop index", "DROP INDEX i", ptrLock(QueryLockAccessExclusive)},
		{"drop index concurrently", "DROP INDEX CONCURRENTLY i", ptrLock(QueryLockShareUpdateExclusive)},
		{"drop table", "DROP TABLE users", ptrLock(QueryLockAccessExclusive)},
		{"drop view", "DROP VIEW v", ptrLock(QueryLockAccessExclusive)},
		{"drop materialized view", "DROP MATERIALIZED VIEW v", ptrLock(QueryLockAccessExclusive)},
		{"drop sequence", "DROP SEQUENCE s", ptrLock(QueryLockAccessExclusive)},
		{"drop schema", "DROP SCHEMA s", ptrLock(QueryLockAccessExclusive)},

		{"lock default", "LOCK TABLE users", ptrLock(QueryLockAccessExclusive)},
		{"lock access exclusive", "LOCK TABLE users IN ACCESS EXCLUSIVE MODE", ptrLock(QueryLockAccessExclusive)},
		{"lock access share", "LOCK TABLE users IN ACCESS SHARE MODE", ptrLock(QueryLockAccessShare)},
		{"lock row share", "LOCK TABLE users IN ROW SHARE MODE", ptrLock(QueryLockRowShare)},
		{"lock row exclusive", "LOCK TABLE users IN ROW EXCLUSIVE MODE", ptrLock(QueryLockRowExclusive)},
		{"lock share update exclusive", "LOCK TABLE users IN SHARE UPDATE EXCLUSIVE MODE", ptrLock(QueryLockShareUpdateExclusive)},
		{"lock share row exclusive", "LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE", ptrLock(QueryLockShareRowExclusive)},
		{"lock share", "LOCK TABLE users IN SHARE MODE", ptrLock(QueryLockShare)},
		{"lock exclusive", "LOCK TABLE users IN EXCLUSIVE MODE", ptrLock(QueryLockExclusive)},

		{"alter table default", "ALTER TABLE users ADD COLUMN x int", ptrLock(QueryLockAccessExclusive)},
		{"alter table validate constraint", "ALTER TABLE users VALIDATE CONSTRAINT c", ptrLock(QueryLockShareUpdateExclusive)},
		{"alter table set statistics", "ALTER TABLE users ALTER COLUMN x SET STATISTICS 100", ptrLock(QueryLockShareUpdateExclusive)},
		{"alter table cluster on", "ALTER TABLE users CLUSTER ON i", ptrLock(QueryLockShareUpdateExclusive)},
		{"alter table set without cluster", "ALTER TABLE users SET WITHOUT CLUSTER", ptrLock(QueryLockShareUpdateExclusive)},
		{"alter table attach partition", "ALTER TABLE users ATTACH PARTITION p FOR VALUES IN (1)", ptrLock(QueryLockShareUpdateExclusive)},
		{"alter table detach partition", "ALTER TABLE users DETACH PARTITION p", ptrLock(QueryLockAccessExclusive)},
		{"alter table detach partition concurrently", "ALTER TABLE users DETACH PARTITION p CONCURRENTLY", ptrLock(QueryLockShareUpdateExclusive)},
		{"alter table enable trigger", "ALTER TABLE users ENABLE TRIGGER t", ptrLock(QueryLockShareRowExclusive)},
		{"alter table disable trigger", "ALTER TABLE users DISABLE TRIGGER t", ptrLock(QueryLockShareRowExclusive)},

		{"alter index rename", "ALTER INDEX i RENAME TO j", ptrLock(QueryLockShareUpdateExclusive)},
		{"alter index set statistics", "ALTER INDEX i ALTER COLUMN x SET STATISTICS 100", ptrLock(QueryLockShareUpdateExclusive)},
		{"alter index set tablespace", "ALTER INDEX i SET TABLESPACE foo", ptrLock(QueryLockAccessExclusive)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev := &QueryEvent{SQL: tc.sql}
			got := getLockType(ev)
			if !reflect.DeepEqual(got, tc.want) {
				gotStr, wantStr := "<nil>", "<nil>"
				if got != nil {
					gotStr = string(*got)
				}
				if tc.want != nil {
					wantStr = string(*tc.want)
				}
				t.Errorf("getLockType(%q) = %s, want %s", tc.sql, gotStr, wantStr)
			}
		})
	}
}
