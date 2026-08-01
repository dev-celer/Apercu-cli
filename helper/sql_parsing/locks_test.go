package sql_parsing

import (
	"testing"

	metricshelper "apercu-cli/helper/metrics"
)

func TestGetLockTypeAlterTable(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want metricshelper.QueryLock
	}{
		// --- Single subcommand ---
		{"validate constraint", "ALTER TABLE t VALIDATE CONSTRAINT c", metricshelper.QueryLockShareUpdateExclusive},
		{"set statistics", "ALTER TABLE t ALTER COLUMN a SET STATISTICS 100", metricshelper.QueryLockShareUpdateExclusive},
		{"cluster on", "ALTER TABLE t CLUSTER ON idx", metricshelper.QueryLockShareUpdateExclusive},
		{"set without cluster", "ALTER TABLE t SET WITHOUT CLUSTER", metricshelper.QueryLockShareUpdateExclusive},
		{"attach partition", "ALTER TABLE t ATTACH PARTITION p FOR VALUES FROM (1) TO (2)", metricshelper.QueryLockShareUpdateExclusive},
		{"detach partition concurrently", "ALTER TABLE t DETACH PARTITION p CONCURRENTLY", metricshelper.QueryLockShareUpdateExclusive},
		{"detach partition", "ALTER TABLE t DETACH PARTITION p", metricshelper.QueryLockAccessExclusive},
		{"disable trigger", "ALTER TABLE t DISABLE TRIGGER trg", metricshelper.QueryLockShareRowExclusive},
		{"enable always trigger", "ALTER TABLE t ENABLE ALWAYS TRIGGER trg", metricshelper.QueryLockShareRowExclusive},
		{"add column", "ALTER TABLE t ADD COLUMN a int", metricshelper.QueryLockAccessExclusive},
		{"drop column", "ALTER TABLE t DROP COLUMN a", metricshelper.QueryLockAccessExclusive},

		// --- Chained subcommands ---
		{
			"validate then add column",
			"ALTER TABLE t VALIDATE CONSTRAINT c, ADD COLUMN a int",
			metricshelper.QueryLockAccessExclusive,
		},
		{
			"add column then validate",
			"ALTER TABLE t ADD COLUMN a int, VALIDATE CONSTRAINT c",
			metricshelper.QueryLockAccessExclusive,
		},
		{
			"validate then disable trigger",
			"ALTER TABLE t VALIDATE CONSTRAINT c, DISABLE TRIGGER trg",
			metricshelper.QueryLockShareRowExclusive,
		},
		{
			"all weak subcommands stay weak",
			"ALTER TABLE t VALIDATE CONSTRAINT c, CLUSTER ON idx, ALTER COLUMN a SET STATISTICS 10",
			metricshelper.QueryLockShareUpdateExclusive,
		},
		{
			"trailing semicolon",
			"ALTER TABLE t VALIDATE CONSTRAINT c, SET WITHOUT CLUSTER;",
			metricshelper.QueryLockShareUpdateExclusive,
		},

		// CONCURRENTLY belongs to the DETACH subcommand only; it must not make a
		// sibling DETACH look concurrent.
		{
			"concurrently does not leak across subcommands",
			"ALTER TABLE t DETACH PARTITION p1, DETACH PARTITION p2 CONCURRENTLY",
			metricshelper.QueryLockAccessExclusive,
		},

		// --- Commas that are not subcommand separators ---
		{
			"comma inside parentheses",
			"ALTER TABLE t ALTER COLUMN a SET STATISTICS 100, ADD CONSTRAINT c UNIQUE (a, b)",
			metricshelper.QueryLockAccessExclusive,
		},
		{
			"comma inside a quoted default",
			"ALTER TABLE t VALIDATE CONSTRAINT c, ADD COLUMN a text DEFAULT 'x,y'",
			metricshelper.QueryLockAccessExclusive,
		},

		// --- Formatting ---
		{
			"multi-line statement",
			"ALTER TABLE t\n    VALIDATE CONSTRAINT c,\n    ALTER COLUMN a SET STATISTICS 100",
			metricshelper.QueryLockShareUpdateExclusive,
		},
		{
			"lowercase input",
			"alter table t validate constraint c, add column a int",
			metricshelper.QueryLockAccessExclusive,
		},
		{
			"schema qualified with only",
			"ALTER TABLE ONLY app.t VALIDATE CONSTRAINT c",
			metricshelper.QueryLockShareUpdateExclusive,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := GetLockType(tc.sql)
			if got == nil {
				t.Fatalf("GetLockType(%q) = nil, want %v", tc.sql, tc.want)
			}
			if *got != tc.want {
				t.Errorf("GetLockType(%q) = %v, want %v", tc.sql, *got, tc.want)
			}
		})
	}
}

func TestQueryLockStrengthOrdering(t *testing.T) {
	// Weakest to strongest, per the Postgres lock-mode table.
	ordered := []metricshelper.QueryLock{
		metricshelper.QueryLockAccessShare,
		metricshelper.QueryLockRowShare,
		metricshelper.QueryLockRowExclusive,
		metricshelper.QueryLockShareUpdateExclusive,
		metricshelper.QueryLockShare,
		metricshelper.QueryLockShareRowExclusive,
		metricshelper.QueryLockExclusive,
		metricshelper.QueryLockAccessExclusive,
	}

	if s := metricshelper.QueryLock("").Strength(); s != 0 {
		t.Errorf("empty lock Strength() = %d, want 0", s)
	}
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1].Strength() >= ordered[i].Strength() {
			t.Errorf("Strength(%v) = %d is not weaker than Strength(%v) = %d",
				ordered[i-1], ordered[i-1].Strength(), ordered[i], ordered[i].Strength())
		}
	}
}
