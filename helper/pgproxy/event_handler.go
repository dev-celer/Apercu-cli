package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type QueryEvent struct {
	SQL          string          `json:"sql"`
	StartedAt    time.Time       `json:"started_at"`
	Duration     time.Duration   `json:"duration"`
	CommandTag   string          `json:"command_tag"`
	RowsAffected int64           `json:"rows_affected"`
	Error        string          `json:"error,omitempty"`
	Stats        QueryEventStats `json:"stats,omitempty"`
}

type QueryEventStats struct {
	Lock  *QueryLock `json:"lock,omitempty"`
	Table string     `json:"table,omitempty"`
}

type QueryLock string

const (
	QueryLockAccessExclusive      QueryLock = "ACCESS_EXCLUSIVE"
	QueryLockExclusive            QueryLock = "EXCLUSIVE"
	QueryLockShareRowExclusive    QueryLock = "SHARE_ROW_EXCLUSIVE"
	QueryLockShare                QueryLock = "SHARE"
	QueryLockShareUpdateExclusive QueryLock = "SHARE_UPDATE_EXCLUSIVE"
	QueryLockRowExclusive         QueryLock = "ROW_EXCLUSIVE"
	QueryLockRowShare             QueryLock = "ROW_SHARE"
	QueryLockAccessShare          QueryLock = "ACCESS_SHARE"
)

func handleEvent(ev QueryEvent) {
	ev.SQL = strings.ReplaceAll(ev.SQL, "\n", " ")
	ev.SQL = stripLeadingComments(ev.SQL)
	ev.SQL = collapseSpaces(ev.SQL)

	lock := getLockType(&ev)
	table := getAffectedTable(&ev)

	ev.Stats = QueryEventStats{
		Lock:  lock,
		Table: table,
	}

	data, err := json.Marshal(ev)
	if err != nil {
		return
	}

	_, _ = fmt.Println(string(data))
}

func collapseSpaces(sql string) string {
	var b strings.Builder
	b.Grow(len(sql))

	inDouble, inSingle, prevSpace := false, false, false

	for i := 0; i < len(sql); i++ {
		c := sql[i]

		switch {
		case inDouble:
			b.WriteByte(c)
			if c == '"' {
				if i+1 < len(sql) && sql[i+1] == '"' {
					b.WriteByte('"')
					i++
					continue
				}
				inDouble = false
			}
			prevSpace = false
		case inSingle:
			b.WriteByte(c)
			if c == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					b.WriteByte('\'')
					i++
					continue
				}
				inSingle = false
			}
			prevSpace = false
		case c == '"':
			b.WriteByte(c)
			inDouble = true
			prevSpace = false
		case c == '\'':
			b.WriteByte(c)
			inSingle = true
			prevSpace = false
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			if !prevSpace {
				b.WriteByte(' ')
				prevSpace = true
			}
		default:
			b.WriteByte(c)
			prevSpace = false
		}
	}

	return b.String()
}

func getLockType(ev *QueryEvent) *QueryLock {
	upper := strings.ToUpper(ev.SQL)
	if upper == "" {
		return nil
	}

	hasPrefix := func(p string) bool { return strings.HasPrefix(upper, p) }
	contains := func(s string) bool { return strings.Contains(upper, s) }

	switch {
	case hasPrefix("SELECT"), hasPrefix("WITH"), hasPrefix("VALUES"), hasPrefix("TABLE "):
		if contains(" FOR UPDATE") || contains(" FOR NO KEY UPDATE") || contains(" FOR SHARE") || contains(" FOR KEY SHARE") {
			return new(QueryLockRowShare)
		}
		return new(QueryLockAccessShare)

	case hasPrefix("INSERT"), hasPrefix("UPDATE"), hasPrefix("DELETE"), hasPrefix("MERGE"):
		return new(QueryLockRowExclusive)

	case hasPrefix("COPY"):
		// COPY ... TO reads (ACCESS SHARE), COPY ... FROM writes (ROW EXCLUSIVE).
		if contains(" TO ") && !contains(" FROM ") {
			return new(QueryLockAccessShare)
		}
		return new(QueryLockRowExclusive)

	case hasPrefix("TRUNCATE"), hasPrefix("CLUSTER"):
		return new(QueryLockAccessExclusive)

	case hasPrefix("VACUUM"):
		if contains(" FULL") {
			return new(QueryLockAccessExclusive)
		}
		return new(QueryLockShareUpdateExclusive)

	case hasPrefix("ANALYZE"), hasPrefix("CREATE STATISTICS"), hasPrefix("COMMENT ON"):
		return new(QueryLockShareUpdateExclusive)

	case hasPrefix("REINDEX"):
		if contains(" CONCURRENTLY") {
			return new(QueryLockShareUpdateExclusive)
		}
		return new(QueryLockAccessExclusive)

	case hasPrefix("REFRESH MATERIALIZED VIEW"):
		if contains(" CONCURRENTLY") {
			return new(QueryLockExclusive)
		}
		return new(QueryLockAccessExclusive)

	case hasPrefix("CREATE INDEX"), hasPrefix("CREATE UNIQUE INDEX"):
		if contains(" CONCURRENTLY") {
			return new(QueryLockShareUpdateExclusive)
		}
		return new(QueryLockShare)

	case hasPrefix("CREATE TRIGGER"):
		return new(QueryLockShareRowExclusive)

	case hasPrefix("DROP INDEX"):
		if contains(" CONCURRENTLY") {
			return new(QueryLockShareUpdateExclusive)
		}
		return new(QueryLockAccessExclusive)

	case hasPrefix("DROP TABLE"),
		hasPrefix("DROP MATERIALIZED VIEW"),
		hasPrefix("DROP VIEW"),
		hasPrefix("DROP SEQUENCE"),
		hasPrefix("DROP TRIGGER"),
		hasPrefix("DROP TYPE"),
		hasPrefix("DROP FUNCTION"),
		hasPrefix("DROP DOMAIN"),
		hasPrefix("DROP SCHEMA"):
		return new(QueryLockAccessExclusive)

	case hasPrefix("LOCK"):
		return lockTableMode(upper)

	case hasPrefix("ALTER TABLE"):
		return alterTableLock(upper)

	case hasPrefix("ALTER INDEX"):
		// RENAME and SET STATISTICS take SHARE UPDATE EXCLUSIVE; everything else is ACCESS EXCLUSIVE.
		if contains(" RENAME ") || contains(" SET STATISTICS") {
			return new(QueryLockShareUpdateExclusive)
		}
		return new(QueryLockAccessExclusive)
	}

	return nil
}

func alterTableLock(upper string) *QueryLock {
	contains := func(s string) bool { return strings.Contains(upper, s) }

	switch {
	case contains(" VALIDATE CONSTRAINT"),
		contains(" SET STATISTICS"),
		contains(" CLUSTER ON"),
		contains(" SET WITHOUT CLUSTER"),
		contains(" ATTACH PARTITION"):
		return new(QueryLockShareUpdateExclusive)
	case contains(" DETACH PARTITION") && contains(" CONCURRENTLY"):
		return new(QueryLockShareUpdateExclusive)
	case contains(" ENABLE TRIGGER"),
		contains(" DISABLE TRIGGER"),
		contains(" ENABLE REPLICA TRIGGER"),
		contains(" ENABLE ALWAYS TRIGGER"):
		return new(QueryLockShareRowExclusive)
	}
	return new(QueryLockAccessExclusive)
}

func lockTableMode(upper string) *QueryLock {
	// Order matters: longer / more specific modes first.
	switch {
	case strings.Contains(upper, "ACCESS EXCLUSIVE"):
		return new(QueryLockAccessExclusive)
	case strings.Contains(upper, "ACCESS SHARE"):
		return new(QueryLockAccessShare)
	case strings.Contains(upper, "SHARE UPDATE EXCLUSIVE"):
		return new(QueryLockShareUpdateExclusive)
	case strings.Contains(upper, "SHARE ROW EXCLUSIVE"):
		return new(QueryLockShareRowExclusive)
	case strings.Contains(upper, "ROW EXCLUSIVE"):
		return new(QueryLockRowExclusive)
	case strings.Contains(upper, "ROW SHARE"):
		return new(QueryLockRowShare)
	case strings.Contains(upper, "EXCLUSIVE"):
		return new(QueryLockExclusive)
	case strings.Contains(upper, "SHARE"):
		return new(QueryLockShare)
	}
	// `LOCK TABLE foo` with no mode defaults to ACCESS EXCLUSIVE.
	return new(QueryLockAccessExclusive)
}

func stripLeadingComments(sql string) string {
	sql = strings.TrimSpace(sql)
	for {
		switch {
		case strings.HasPrefix(sql, "--"):
			idx := strings.IndexByte(sql, '\n')
			if idx < 0 {
				return ""
			}
			sql = strings.TrimSpace(sql[idx+1:])
		case strings.HasPrefix(sql, "/*"):
			idx := strings.Index(sql, "*/")
			if idx < 0 {
				return ""
			}
			sql = strings.TrimSpace(sql[idx+2:])
		default:
			return sql
		}
	}
}

func getAffectedTable(ev *QueryEvent) string {
	upper := strings.ToUpper(ev.SQL)

	prefixes := []string{
		"INSERT INTO ", "UPDATE ", "DELETE FROM ", "MERGE INTO ",
		"TRUNCATE TABLE ", "TRUNCATE ",
		"ALTER TABLE ", "DROP TABLE ", "CREATE TABLE ",
		"VACUUM ", "ANALYZE ", "CLUSTER ",
		"LOCK TABLE ", "LOCK ", "COPY ",
		"REFRESH MATERIALIZED VIEW ",
	}

	var rest string
	for _, p := range prefixes {
		if strings.HasPrefix(upper, p) {
			rest = ev.SQL[len(p):]
			break
		}
	}

	// CREATE INDEX prefix implementation
	if rest == "" {
		prefixes := []string{
			"CREATE INDEX ", "CREATE UNIQUE INDEX ",
		}

		for _, p := range prefixes {
			if strings.HasPrefix(upper, p) {
				idx := strings.Index(upper, " ON ")
				if idx != -1 {
					rest = ev.SQL[idx+4:]
					break
				}
			}
		}
	}

	if rest == "" {
		return ""
	}

	for {
		rest = strings.TrimSpace(rest)
		up := strings.ToUpper(rest)
		switch {
		case strings.HasPrefix(up, "IF EXISTS "):
			rest = rest[len("IF EXISTS "):]
		case strings.HasPrefix(up, "IF NOT EXISTS "):
			rest = rest[len("IF NOT EXISTS "):]
		case strings.HasPrefix(up, "ONLY "):
			rest = rest[len("ONLY "):]
		case strings.HasPrefix(up, "CONCURRENTLY "):
			rest = rest[len("CONCURRENTLY "):]
		default:
			return extractIdentifier(rest)
		}
	}
}

func extractIdentifier(s string) string {
	if s == "" {
		return ""
	}

	idx := 0
	for {
		if idx >= len(s) {
			return ""
		}

		// In quote text
		if s[idx] == '"' {
			foundDot := false

			for i := idx + 1; i < len(s); i++ {
				if s[i] == '"' {
					if i+1 < len(s) {
						if s[i+1] == '"' {
							i++
							continue
						}
						if s[i+1] == '.' {
							idx = i + 2
							foundDot = true
							break
						}
					}

					return s[:i+1]
				}
			}

			if foundDot {
				continue
			}

			return ""
		}

		// Non quote parsing
		end := idx
		foundDot := false
		for end < len(s) {
			c := s[end]
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
				end++
				continue
			}
			if c == '.' {
				end++
				idx = end
				foundDot = true
				break
			}
			break
		}
		if !foundDot {
			return s[:end]
		}
	}
}
