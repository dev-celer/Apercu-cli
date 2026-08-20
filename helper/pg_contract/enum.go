// Package pg_contract holds the vocabulary shared by every stage of the migration parser.
package pg_contract

import (
	"fmt"
	"strings"
)

// normalizeEnumKey folds a spelling into a lookup key. Case, separators and the
// trailing "Lock" of a pg_locks mode name are all irrelevant, so "ACCESS_EXCLUSIVE",
// "access exclusive" and "AccessExclusiveLock" collapse to the same key.
func normalizeEnumKey(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(s)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return strings.TrimSuffix(b.String(), "lock")
}

// marshalEnum renders an enum value through its canonical-name table.
func marshalEnum[T comparable](v T, names map[T]string) ([]byte, error) {
	if s, ok := names[v]; ok {
		return []byte(s), nil
	}
	return nil, fmt.Errorf("pg_contract: unknown %T value %v", v, v)
}

// parseEnum resolves any accepted spelling of an enum value.
func parseEnum[T comparable](s string, aliases map[string]T) (T, error) {
	var zero T
	if v, ok := aliases[normalizeEnumKey(s)]; ok {
		return v, nil
	}
	return zero, fmt.Errorf("pg_contract: unknown %T %q", zero, s)
}

// buildAliases indexes every canonical name, plus any extra spellings, by
// normalized key.
func buildAliases[T comparable](names map[T]string, extra map[string]T) map[string]T {
	aliases := make(map[string]T, len(names)+len(extra))
	for v, name := range names {
		aliases[normalizeEnumKey(name)] = v
	}
	for name, v := range extra {
		aliases[normalizeEnumKey(name)] = v
	}
	return aliases
}
