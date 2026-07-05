package sql_parsing

import (
	"apercu-cli/helper/metrics"
	"strings"
)

func GetLockType(sql string) *metrics.QueryLock {
	upper := strings.ToUpper(sql)
	if upper == "" {
		return nil
	}

	hasPrefix := func(p string) bool { return strings.HasPrefix(upper, p) }
	contains := func(s string) bool { return strings.Contains(upper, s) }

	switch {
	case hasPrefix("SELECT"), hasPrefix("WITH"), hasPrefix("VALUES"), hasPrefix("TABLE "):
		if contains(" FOR UPDATE") || contains(" FOR NO KEY UPDATE") || contains(" FOR SHARE") || contains(" FOR KEY SHARE") {
			return new(metrics.QueryLockRowShare)
		}
		return new(metrics.QueryLockAccessShare)

	case hasPrefix("INSERT"), hasPrefix("UPDATE"), hasPrefix("DELETE"), hasPrefix("MERGE"):
		return new(metrics.QueryLockRowExclusive)

	case hasPrefix("COPY"):
		// COPY ... TO reads (ACCESS SHARE), COPY ... FROM writes (ROW EXCLUSIVE).
		if contains(" TO ") && !contains(" FROM ") {
			return new(metrics.QueryLockAccessShare)
		}
		return new(metrics.QueryLockRowExclusive)

	case hasPrefix("TRUNCATE"), hasPrefix("CLUSTER"):
		return new(metrics.QueryLockAccessExclusive)

	case hasPrefix("VACUUM"):
		if contains(" FULL") {
			return new(metrics.QueryLockAccessExclusive)
		}
		return new(metrics.QueryLockShareUpdateExclusive)

	case hasPrefix("ANALYZE"), hasPrefix("CREATE STATISTICS"), hasPrefix("COMMENT ON"):
		return new(metrics.QueryLockShareUpdateExclusive)

	case hasPrefix("REINDEX"):
		if contains(" CONCURRENTLY") {
			return new(metrics.QueryLockShareUpdateExclusive)
		}
		return new(metrics.QueryLockAccessExclusive)

	case hasPrefix("REFRESH MATERIALIZED VIEW"):
		if contains(" CONCURRENTLY") {
			return new(metrics.QueryLockExclusive)
		}
		return new(metrics.QueryLockAccessExclusive)

	case hasPrefix("CREATE INDEX"), hasPrefix("CREATE UNIQUE INDEX"):
		if contains(" CONCURRENTLY") {
			return new(metrics.QueryLockShareUpdateExclusive)
		}
		return new(metrics.QueryLockShare)

	case hasPrefix("CREATE TRIGGER"):
		return new(metrics.QueryLockShareRowExclusive)

	case hasPrefix("DROP INDEX"):
		if contains(" CONCURRENTLY") {
			return new(metrics.QueryLockShareUpdateExclusive)
		}
		return new(metrics.QueryLockAccessExclusive)

	case hasPrefix("DROP TABLE"),
		hasPrefix("DROP MATERIALIZED VIEW"),
		hasPrefix("DROP VIEW"),
		hasPrefix("DROP SEQUENCE"),
		hasPrefix("DROP TRIGGER"),
		hasPrefix("DROP TYPE"),
		hasPrefix("DROP FUNCTION"),
		hasPrefix("DROP DOMAIN"),
		hasPrefix("DROP SCHEMA"):
		return new(metrics.QueryLockAccessExclusive)

	case hasPrefix("LOCK"):
		return new(lockTableMode(upper))

	case hasPrefix("ALTER TABLE"):
		return new(alterTableLock(upper))

	case hasPrefix("ALTER INDEX"):
		// RENAME and SET STATISTICS take SHARE UPDATE EXCLUSIVE; everything else is ACCESS EXCLUSIVE.
		if contains(" RENAME ") || contains(" SET STATISTICS") {
			return new(metrics.QueryLockShareUpdateExclusive)
		}
		return new(metrics.QueryLockAccessExclusive)
	}

	return nil
}

func alterTableLock(upper string) metrics.QueryLock {
	contains := func(s string) bool { return strings.Contains(upper, s) }

	switch {
	case contains(" VALIDATE CONSTRAINT"),
		contains(" SET STATISTICS"),
		contains(" CLUSTER ON"),
		contains(" SET WITHOUT CLUSTER"),
		contains(" ATTACH PARTITION"):
		return metrics.QueryLockShareUpdateExclusive
	case contains(" DETACH PARTITION") && contains(" CONCURRENTLY"):
		return metrics.QueryLockShareUpdateExclusive
	case contains(" ENABLE TRIGGER"),
		contains(" DISABLE TRIGGER"),
		contains(" ENABLE REPLICA TRIGGER"),
		contains(" ENABLE ALWAYS TRIGGER"):
		return metrics.QueryLockShareRowExclusive
	}
	return metrics.QueryLockAccessExclusive
}

func lockTableMode(upper string) metrics.QueryLock {
	// Order matters: longer / more specific modes first.
	switch {
	case strings.Contains(upper, "ACCESS EXCLUSIVE"):
		return metrics.QueryLockAccessExclusive
	case strings.Contains(upper, "ACCESS SHARE"):
		return metrics.QueryLockAccessShare
	case strings.Contains(upper, "SHARE UPDATE EXCLUSIVE"):
		return metrics.QueryLockShareUpdateExclusive
	case strings.Contains(upper, "SHARE ROW EXCLUSIVE"):
		return metrics.QueryLockShareRowExclusive
	case strings.Contains(upper, "ROW EXCLUSIVE"):
		return metrics.QueryLockRowExclusive
	case strings.Contains(upper, "ROW SHARE"):
		return metrics.QueryLockRowShare
	case strings.Contains(upper, "EXCLUSIVE"):
		return metrics.QueryLockExclusive
	case strings.Contains(upper, "SHARE"):
		return metrics.QueryLockShare
	}
	// `LOCK TABLE foo` with no mode defaults to ACCESS EXCLUSIVE.
	return metrics.QueryLockAccessExclusive
}
