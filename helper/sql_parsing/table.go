package sql_parsing

import (
	"apercu-cli/helper"
	"regexp"
	"strings"
)

func parseFullTableName(table string) helper.FullTableName {
	before, after, found := strings.Cut(table, ".")
	if !found || after == "" {
		return helper.FullTableName{
			Schema: "public",
			Table:  table,
		}
	}
	if before == "" {
		before = "public"
	}
	return helper.FullTableName{
		Schema: before,
		Table:  after,
	}
}

var createIndexRegex = regexp.MustCompile(`(?i)CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF NOT EXISTS\s+)?(\S+)\s+ON\s+(?:ONLY\s+)?(\S+)`)
var createTriggerRegex = regexp.MustCompile(`(?i)CREATE\s+(?:OR REPLACE\s+)?(?:CONSTRAINT\s+)?TRIGGER\s+(\S+).*ON\s+(\S+).*EXECUTE`)
var refreshViewRegex = regexp.MustCompile(`(?i)REFRESH MATERIALIZED VIEW\s+(?:CONCURRENTLY\s+)?(\S+)`)
var clusterRegex = regexp.MustCompile(`(?i)CLUSTER\s+(?:VERBOSE\s+(?:TRUE\s+|YES\s+|ON\s+|1\s+|FALSE\s+|NO\s+|OFF\s+|0\s+)?)?(\S+)`)
var alterTableRegex = regexp.MustCompile(`(?i)ALTER TABLE\s+(?:IF EXISTS\s+)?(?:ONLY\s+)?(\S+)`)

func ParseTables(sql string) []helper.FullTableName {
	sql = strings.TrimSpace(sql)

	upper := strings.ToUpper(sql)
	prefix := func(p string) bool {
		return strings.HasPrefix(upper, p)
	}

	switch {
	case prefix("CREATE INDEX"), prefix("CREATE UNIQUE INDEX"):
		m := createIndexRegex.FindStringSubmatch(sql)
		if len(m) != 3 {
			return nil
		}
		return []helper.FullTableName{
			parseFullTableName(m[2]),
		}
	case prefix("CREATE TRIGGER"):
		m := createTriggerRegex.FindStringSubmatch(sql)
		if len(m) != 3 {
			return nil
		}
		return []helper.FullTableName{
			parseFullTableName(m[2]),
		}
	case prefix("REFRESH MATERIALIZED VIEW"):
		m := refreshViewRegex.FindStringSubmatch(sql)
		if len(m) != 2 {
			return nil
		}
		return []helper.FullTableName{
			parseFullTableName(m[1]),
		}
	case prefix("CLUSTER"):
		m := clusterRegex.FindStringSubmatch(sql)
		if len(m) != 2 {
			return nil
		}
		return []helper.FullTableName{
			parseFullTableName(m[1]),
		}
	case prefix("VACUUM"):
		return parseVacuum(sql)
	case prefix("ALTER TABLE"):
		m := alterTableRegex.FindStringSubmatch(sql)
		if len(m) != 2 {
			return nil
		}
		return []helper.FullTableName{
			parseFullTableName(m[1]),
		}
	}

	return nil
}

var vacuumFirstPartRegex = regexp.MustCompile(`(?i)VACUUM\s*(?:\([^)]*\)\s*)?(.*)`)
var legacyVacuumRegex = regexp.MustCompile(`(?i)VACUUM\s+(?:FULL\s*)?(?:FREEZE\s*)?(?:VERBOSE\s*)?(?:ANALYZE\s*)?(.*)`)
var vacuumTableRegex = regexp.MustCompile(`(?i)(?:ONLY\s+)?([^\s,]+)\s*(?:\*\s*)?(?:\([^)]*\)\s*)?(?:,\s*)?`)

func parseVacuum(sql string) []helper.FullTableName {
	// Remove the first part of the command to leave only the tables, support legacy formatting
	m := vacuumFirstPartRegex.FindStringSubmatch(sql)
	if len(m) != 2 {
		return nil
	}
	m2 := legacyVacuumRegex.FindStringSubmatch(sql)
	if len(m2) != 2 {
		return nil
	}

	// choose between the legacy and current format by selecting the on that leave the least element on the table side
	if len(m2[1]) > len(m[1]) {
		sql = m[1]
	} else {
		sql = m2[1]
	}

	// Extract all table names
	m3 := vacuumTableRegex.FindAllStringSubmatch(sql, -1)
	tables := make([]helper.FullTableName, 0, len(m))
	for _, i := range m3 {
		if len(i) != 2 {
			continue
		}
		tables = append(tables, parseFullTableName(i[1]))
	}
	return tables
}
