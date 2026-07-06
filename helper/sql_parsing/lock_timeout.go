package sql_parsing

import (
	"strconv"
	"strings"
	"time"
)

// GetLockTimeoutValue return (isSetLockTimeout, lockTimeoutValue (nil is DEFAULT))
func GetLockTimeoutValue(sql string) (bool, *int64) {
	upper := strings.ToUpper(sql)

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
			rest = sql[len(p):]
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
