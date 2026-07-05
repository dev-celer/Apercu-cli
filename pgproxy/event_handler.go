package main

import (
	"apercu-cli/helper/metrics"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
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

func handleSetLocksTimeoutEvent(ev *metrics.QueryEvent) {
	ev.SQL = strings.ReplaceAll(ev.SQL, "\n", " ")
	ev.SQL = stripLeadingComments(ev.SQL)
	ev.SQL = collapseSpaces(ev.SQL)

	isLockEvent, l := getLockTimeoutValue(ev)
	if isLockEvent {
		ev.LocksTimeout = l
	}
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

// getLockTimeoutValue return (isSetLockTimeout, lockTimeoutValue (nil is DEFAULT))
func getLockTimeoutValue(ev *metrics.QueryEvent) (bool, *int64) {
	upper := strings.ToUpper(ev.SQL)

	// check for reset
	if strings.HasPrefix(upper, "RESET LOCK_TIMEOUT") || strings.HasPrefix(upper, "RESET ALL") {
		return true, nil
	}

	prefixes := []string{
		"SET SESSION ", "SET LOCAL ", "SET ", "ALTER SYSTEM SET ",
	}

	var rest, upperRest string
	for _, p := range prefixes {
		if strings.HasPrefix(upper, p) {
			rest = ev.SQL[len(p):]
			upperRest = upper[len(p):]
			break
		}
	}
	if rest == "" {
		return false, nil
	}

	prefixes = []string{
		"LOCK_TIMEOUT = ", "LOCK_TIMEOUT TO ", "LOCK_TIMEOUT =", "LOCK_TIMEOUT= ", "LOCK_TIMEOUT=",
	}

	hasPrefix := false
	for _, p := range prefixes {
		if strings.HasPrefix(upperRest, p) {
			hasPrefix = true
			rest = rest[len(p):]
			upperRest = upperRest[len(p):]
			break
		}
	}
	if rest == "" || !hasPrefix {
		return false, nil
	}

	// Strip trailing semicolon / space
	rest = strings.TrimRight(rest, "; ")
	upperRest = strings.TrimRight(upperRest, "; ")

	// Handle default value
	if upperRest == "DEFAULT" {
		return true, nil
	}

	// strip quote
	rest = strings.ReplaceAll(rest, "'", "")
	upperRest = strings.ReplaceAll(upperRest, "'", "")
	// Handle int value
	i, err := strconv.ParseInt(rest, 10, 64)
	if err == nil {
		return true, &i
	}

	// handle unsupported duration value
	if strings.HasSuffix(upperRest, "D") {
		// 1d -> ms
		v := rest[:len(rest)-1]
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return true, new(i * 24 * 60 * 60 * 1000)
		}
	}
	if strings.HasSuffix(upperRest, "MIN") {
		// 1min -> 1m
		rest = rest[:len(rest)-3] + "m"
		upperRest = upperRest[:len(upperRest)-3] + "M"
	}

	// Handle duration value
	d, err := time.ParseDuration(rest)
	if err == nil {
		return true, new(d.Milliseconds())
	}
	return false, nil
}
