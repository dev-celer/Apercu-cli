package main

import (
	"apercu-cli/helper/metrics"
	"encoding/json"
	"fmt"
	"strings"
)

func handleEvent(ev metrics.QueryEvent) {
	ev.SQL = strings.ReplaceAll(ev.SQL, "\n", " ")
	ev.SQL = stripLeadingComments(ev.SQL)
	ev.SQL = collapseSpaces(ev.SQL)

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
