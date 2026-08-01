package sql_parsing

import (
	"strings"
)

func splitAlterTableQuery(query string) []string {
	query = strings.ToUpper(query)
	body := strings.TrimSpace(strings.TrimPrefix(query, "ALTER TABLE"))

	subQueries := splitAlterTableTopLevel(body)
	for i, s := range subQueries {
		subQueries[i] = strings.TrimSpace(s)
	}
	return subQueries
}

// normalizeSubcommand collapses runs of whitespace (newlines and tabs included)
// into single spaces and strips trailing statement punctuation, so keyword
// matching works the same on a one-liner and on a formatted multi-line migration.
func normalizeSubcommand(s string) string {
	return strings.Trim(strings.Join(strings.Fields(s), " "), "; ")
}

// splitAlterTableTopLevel splits on commas that are not nested inside parentheses or
// single/double-quoted strings.
func splitAlterTableTopLevel(s string) []string {
	var parts []string
	depth := 0
	inSingle, inDouble := false, false
	start := 0

	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inSingle:
			if c == '\'' {
				inSingle = false
			}
		case inDouble:
			if c == '"' {
				inDouble = false
			}
		case c == '\'':
			inSingle = true
		case c == '"':
			inDouble = true
		case c == '(':
			depth++
		case c == ')':
			if depth > 0 {
				depth--
			}
		case c == ',' && depth == 0:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}
