package sql_parsing

import (
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"strings"
)

func appendWarnings(w []*warning.LockWarning, warningStore *warning.WarningStore, query *metricshelper.QueryEventAnalysis) {
	if warningStore != nil {
		for _, i := range w {
			warningStore.AddWarning(i)
			query.Warnings = append(query.Warnings, i)
		}
	}
}

// ClassifyOperation inject the operation type and warning if necessary, the warning is also created in the warningStore.
func ClassifyOperation(query *metricshelper.QueryEventAnalysis, serverVersion float32, warningStore *warning.WarningStore, prodStats *metricshelper.DatabaseMetrics) {
	upper := strings.ToUpper(strings.TrimSpace(query.Event.SQL))
	if upper == "" {
		query.Type = metricshelper.EventOperationTypeNonBlocking
		return
	}

	hasPrefix := func(p string) bool { return strings.HasPrefix(upper, p) }
	contains := func(s string) bool { return strings.Contains(upper, s) }

	switch {
	case hasPrefix("ALTER TABLE"):
		classifyAlterTable(upper, query, serverVersion, warningStore, prodStats)
		return

	case hasPrefix("CREATE UNIQUE INDEX"), hasPrefix("CREATE INDEX"):
		if contains(" CONCURRENTLY") {
			query.Type = metricshelper.EventOperationTypeNonBlocking
			return
		}
		query.Type = metricshelper.EventOperationTypeScanUnderLock
		w := warning.NewLockWarnings(query, warning.CodeCreateIndexWithoutConcurrently, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return

	case hasPrefix("DROP INDEX"):
		if contains(" CONCURRENTLY") {
			query.Type = metricshelper.EventOperationTypeNonBlocking
			return
		}
		if contains(" CASCADE") {
			query.Type = metricshelper.EventOperationTypeMetadataOnly
			w := warning.NewLockWarnings(query, warning.CodeDropIndexCascadeWithoutConcurrently, serverVersion, prodStats)
			appendWarnings(w, warningStore, query)
			return
		}
		// TODO Detect if the index is UNIQUE / PRIMARY KEY before recommending using CONCURRENTLY
		query.Type = metricshelper.EventOperationTypeMetadataOnly
		w := warning.NewLockWarnings(query, warning.CodeDropIndexWithoutConcurrently, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return

	case hasPrefix("REINDEX"):
		if contains(" CONCURRENTLY") {
			query.Type = metricshelper.EventOperationTypeNonBlocking
			return
		}
		query.Type = metricshelper.EventOperationTypeRewriteUnderLock
		w := warning.NewLockWarnings(query, warning.CodeReindexWithoutConcurrently, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return

	case hasPrefix("REFRESH MATERIALIZED VIEW"):
		if contains(" CONCURRENTLY") {
			query.Type = metricshelper.EventOperationTypeNonBlocking
			return
		}
		query.Type = metricshelper.EventOperationTypeRewriteUnderLock
		w := warning.NewLockWarnings(query, warning.CodeRefreshMaterializedViewWithoutConcurrently, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return

	case hasPrefix("CLUSTER"):
		query.Type = metricshelper.EventOperationTypeRewriteUnderLock
		w := warning.NewLockWarnings(query, warning.CodeCluster, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return

	case hasPrefix("VACUUM"):
		if contains(" FULL") {
			query.Type = metricshelper.EventOperationTypeRewriteUnderLock
			w := warning.NewLockWarnings(query, warning.CodeVacuumFull, serverVersion, prodStats)
			appendWarnings(w, warningStore, query)
			return
		}
		query.Type = metricshelper.EventOperationTypeNonBlocking
		return

	case hasPrefix("TRUNCATE"):
		query.Type = metricshelper.EventOperationTypeMetadataOnly
		return

	case hasPrefix("CREATE TRIGGER"):
		query.Type = metricshelper.EventOperationTypeMetadataOnly
		w := warning.NewLockWarnings(query, warning.CodeCreateTrigger, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return
	case hasPrefix("CREATE RULE"):
		query.Type = metricshelper.EventOperationTypeMetadataOnly
		w := warning.NewLockWarnings(query, warning.CodeCreateRule, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return
	case hasPrefix("ALTER RULE"):
		query.Type = metricshelper.EventOperationTypeMetadataOnly
		w := warning.NewLockWarnings(query, warning.CodeAlterRule, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return

	case hasPrefix("ALTER TYPE"):
		if contains(" ADD VALUE") {
			query.Type = metricshelper.EventOperationTypeMetadataOnly
			return
		}
		// TODO Store a list of tables using the type to include in warning
		query.Type = metricshelper.EventOperationTypeRewriteUnderLock
		w := warning.NewLockWarnings(query, warning.CodeAlterType, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return
	}

	query.Type = metricshelper.EventOperationTypeNonBlocking
}

// classifyAlterTable splits an ALTER TABLE statement into its comma-separated
// subcommands and returns the most severe classification across them.
func classifyAlterTable(upper string, query *metricshelper.QueryEventAnalysis, serverVersion float32, warningStore *warning.WarningStore, prodStats *metricshelper.DatabaseMetrics) {
	body := strings.TrimSpace(strings.TrimPrefix(upper, "ALTER TABLE"))

	for _, sub := range splitAlterTableTopLevel(body) {
		sub = strings.TrimSpace(sub)
		classifyAlterSubcommand(sub, query, serverVersion, warningStore, prodStats)
	}
}

func classifyAlterSubcommand(sub string, query *metricshelper.QueryEventAnalysis, serverVersion float32, warningStore *warning.WarningStore, prodStats *metricshelper.DatabaseMetrics) {
	contains := func(s string) bool { return strings.Contains(sub, s) }

	switch {
	case contains("ADD CONSTRAINT"), contains("ADD PRIMARY KEY"),
		contains("ADD UNIQUE"), contains("ADD FOREIGN KEY"),
		contains("ADD CHECK"), contains("ADD EXCLUDE"):
		classifyAddConstraint(sub, query, serverVersion, warningStore, prodStats)
		return

	case contains("VALIDATE CONSTRAINT"):
		return

	case contains("DROP CONSTRAINT"):
		if query.Type.Severity() < metricshelper.EventOperationTypeMetadataOnly.Severity() {
			query.Type = metricshelper.EventOperationTypeMetadataOnly
		}
		w := warning.NewLockWarnings(query, warning.CodeAlterTableDropConstraint, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return

	// --- Column type/null/default/storage changes ---
	case contains("ALTER COLUMN"), contains("ALTER "):
		if ok := classifyAlterColumn(sub, query, serverVersion, warningStore, prodStats); ok {
			return
		}

	// --- Add / drop column ---
	default:
		switch {
		case contains("DROP COLUMN"):
			if query.Type.Severity() < metricshelper.EventOperationTypeMetadataOnly.Severity() {
				query.Type = metricshelper.EventOperationTypeMetadataOnly
			}
			w := warning.NewLockWarnings(query, warning.CodeAlterTableDropColumn, serverVersion, prodStats)
			appendWarnings(w, warningStore, query)
			return
		case contains("ADD COLUMN"), strings.HasPrefix(sub, "ADD ") || strings.Contains(sub, " ADD "):
			classifyAddColumn(sub, query, serverVersion, warningStore, prodStats)
			return

		// --- Table-level rewrites ---
		case contains("SET LOGGED"), contains("SET UNLOGGED"):
			if query.Type.Severity() < metricshelper.EventOperationTypeRewriteUnderLock.Severity() {
				query.Type = metricshelper.EventOperationTypeRewriteUnderLock
			}
			w := warning.NewLockWarnings(query, warning.CodeAlterTableLogged, serverVersion, prodStats)
			appendWarnings(w, warningStore, query)
			return
		case contains("SET TABLESPACE"):
			if query.Type.Severity() < metricshelper.EventOperationTypeRewriteUnderLock.Severity() {
				query.Type = metricshelper.EventOperationTypeRewriteUnderLock
			}
			w := warning.NewLockWarnings(query, warning.CodeAlterTableTablespace, serverVersion, prodStats)
			appendWarnings(w, warningStore, query)
			return

		// --- Cheap metadata changes ---
		case contains("SET (FILLFACTOR"):
			if query.Type.Severity() < metricshelper.EventOperationTypeMetadataOnly.Severity() {
				query.Type = metricshelper.EventOperationTypeMetadataOnly
			}
			w := warning.NewLockWarnings(query, warning.CodeAlterTableFillFactor, serverVersion, prodStats)
			appendWarnings(w, warningStore, query)
			return
		case contains("RESET ("):
			if query.Type.Severity() < metricshelper.EventOperationTypeMetadataOnly.Severity() {
				query.Type = metricshelper.EventOperationTypeMetadataOnly
			}
			w := warning.NewLockWarnings(query, warning.CodeAlterTableReset, serverVersion, prodStats)
			appendWarnings(w, warningStore, query)
			return
		case contains("ENABLE TRIGGER"), contains("DISABLE TRIGGER"),
			contains("ENABLE REPLICA TRIGGER"), contains("ENABLE ALWAYS TRIGGER"):
			if query.Type.Severity() < metricshelper.EventOperationTypeMetadataOnly.Severity() {
				query.Type = metricshelper.EventOperationTypeMetadataOnly
			}
			w := warning.NewLockWarnings(query, warning.CodeAlterTableSwitchTrigger, serverVersion, prodStats)
			appendWarnings(w, warningStore, query)
			return
		case contains("RENAME"):
			if query.Type.Severity() < metricshelper.EventOperationTypeMetadataOnly.Severity() {
				query.Type = metricshelper.EventOperationTypeMetadataOnly
			}
			w := warning.NewLockWarnings(query, warning.CodeAlterTableRename, serverVersion, prodStats)
			appendWarnings(w, warningStore, query)
			return
		}
	}

	return
}

func classifyAddColumn(sub string, query *metricshelper.QueryEventAnalysis, serverVersion float32, warningStore *warning.WarningStore, prodStats *metricshelper.DatabaseMetrics) {
	contains := func(s string) bool { return strings.Contains(sub, s) }

	switch {
	case contains("GENERATED") && contains("STORED"):
		if query.Type.Severity() < metricshelper.EventOperationTypeRewriteUnderLock.Severity() {
			query.Type = metricshelper.EventOperationTypeRewriteUnderLock
		}
		w := warning.NewLockWarnings(query, warning.CodeAddColumnGeneratedAlwaysAsStored, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return
	case contains("GENERATED") && contains("AS IDENTITY"):
		if query.Type.Severity() < metricshelper.EventOperationTypeRewriteUnderLock.Severity() {
			query.Type = metricshelper.EventOperationTypeRewriteUnderLock
		}
		w := warning.NewLockWarnings(query, warning.CodeAddColumnGeneratedAlwaysAsIdentity, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return
	case contains("DEFAULT") && hasVolatileDefault(sub):
		if query.Type.Severity() < metricshelper.EventOperationTypeRewriteUnderLock.Severity() {
			query.Type = metricshelper.EventOperationTypeRewriteUnderLock
		}
		w := warning.NewLockWarnings(query, warning.CodeAddColumnVolatileDefault, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return
	default:
		if query.Type.Severity() < metricshelper.EventOperationTypeMetadataOnly.Severity() {
			query.Type = metricshelper.EventOperationTypeMetadataOnly
		}
		w := warning.NewLockWarnings(query, warning.CodeAddColumn, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return
	}
}

// classifyAlterColumn returns ok=false when the subcommand is an ALTER COLUMN
// shape it does not recognize, so the caller can fall through.
func classifyAlterColumn(sub string, query *metricshelper.QueryEventAnalysis, serverVersion float32, warningStore *warning.WarningStore, prodStats *metricshelper.DatabaseMetrics) bool {
	contains := func(s string) bool { return strings.Contains(sub, s) }

	switch {
	case contains("SET DEFAULT"), contains("DROP DEFAULT"):
		if query.Type.Severity() < metricshelper.EventOperationTypeMetadataOnly.Severity() {
			query.Type = metricshelper.EventOperationTypeMetadataOnly
		}
		w := warning.NewLockWarnings(query, warning.CodeAlterColumnDefault, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return true
	case contains("DROP NOT NULL"):
		if query.Type.Severity() < metricshelper.EventOperationTypeMetadataOnly.Severity() {
			query.Type = metricshelper.EventOperationTypeMetadataOnly
		}
		w := warning.NewLockWarnings(query, warning.CodeAlterColumnDropNotNull, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return true
	case contains("SET NOT NULL"):
		// TODO : Check if a check not null constraint exist, if not, recommend creating is first then set not null will reuse the constraint and be metadata only
		if query.Type.Severity() < metricshelper.EventOperationTypeScanUnderLock.Severity() {
			query.Type = metricshelper.EventOperationTypeScanUnderLock
		}
		w := warning.NewLockWarnings(query, warning.CodeAlterColumnSetNotNull, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return true
	case contains("SET STORAGE"):
		if query.Type.Severity() < metricshelper.EventOperationTypeMetadataOnly.Severity() {
			query.Type = metricshelper.EventOperationTypeMetadataOnly
		}
		w := warning.NewLockWarnings(query, warning.CodeAlterColumnSetStorage, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return true
	case contains("SET STATISTICS"):
		if query.Type.Severity() < metricshelper.EventOperationTypeMetadataOnly.Severity() {
			query.Type = metricshelper.EventOperationTypeMetadataOnly
		}
		w := warning.NewLockWarnings(query, warning.CodeAlterColumnSetStatistics, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return true
	case contains(" TYPE "), contains(" SET DATA TYPE "):
		if isBinaryCoercibleWidening(sub) {
			if query.Type.Severity() < metricshelper.EventOperationTypeMetadataOnly.Severity() {
				query.Type = metricshelper.EventOperationTypeMetadataOnly
			}
			w := warning.NewLockWarnings(query, warning.CodeAlterColumnSetTypeWidening, serverVersion, prodStats)
			appendWarnings(w, warningStore, query)
			return true
		}
		if query.Type.Severity() < metricshelper.EventOperationTypeRewriteUnderLock.Severity() {
			query.Type = metricshelper.EventOperationTypeRewriteUnderLock
		}
		w := warning.NewLockWarnings(query, warning.CodeAlterColumnSetTypeNotWidening, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return true
	}

	return false
}

func classifyAddConstraint(sub string, query *metricshelper.QueryEventAnalysis, serverVersion float32, warningStore *warning.WarningStore, prodStats *metricshelper.DatabaseMetrics) {
	contains := func(s string) bool { return strings.Contains(sub, s) }

	notValid := contains("NOT VALID")

	switch {
	case contains("CHECK"), contains("FOREIGN KEY"), contains("REFERENCES"):
		if notValid {
			if query.Type.Severity() < metricshelper.EventOperationTypeMetadataOnly.Severity() {
				query.Type = metricshelper.EventOperationTypeMetadataOnly
			}
			w := warning.NewLockWarnings(query, warning.CodeAlterTableAddConstraintNotValid, serverVersion, prodStats)
			appendWarnings(w, warningStore, query)
			return
		}
		if query.Type.Severity() < metricshelper.EventOperationTypeScanUnderLock.Severity() {
			query.Type = metricshelper.EventOperationTypeScanUnderLock
		}
		w := warning.NewLockWarnings(query, warning.CodeAlterTableAddConstraint, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return
	case contains("PRIMARY KEY"), contains("UNIQUE"):
		// TODO Should check if column is NOT NULL else recommend to enforce NOT NULL before adding PRIMARY KEY
		if contains("USING INDEX") {
			if query.Type.Severity() < metricshelper.EventOperationTypeMetadataOnly.Severity() {
				query.Type = metricshelper.EventOperationTypeMetadataOnly
			}
			w := warning.NewLockWarnings(query, warning.CodeAlterTableAddUniqueWithIndex, serverVersion, prodStats)
			appendWarnings(w, warningStore, query)
			return
		}
		if query.Type.Severity() < metricshelper.EventOperationTypeScanUnderLock.Severity() {
			query.Type = metricshelper.EventOperationTypeScanUnderLock
		}
		w := warning.NewLockWarnings(query, warning.CodeAlterTableAddUniqueWithoutIndex, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return
	case contains("EXCLUDE"):
		if query.Type.Severity() < metricshelper.EventOperationTypeScanUnderLock.Severity() {
			query.Type = metricshelper.EventOperationTypeScanUnderLock
		}
		w := warning.NewLockWarnings(query, warning.CodeAlterTableAddConstraintExclude, serverVersion, prodStats)
		appendWarnings(w, warningStore, query)
		return
	}

	return
}

// hasVolatileDefault reports whether the subcommand's DEFAULT clause references a
// known volatile builtin. This is a denylist: user-defined volatile functions or
// unusual expressions are not detected.
func hasVolatileDefault(sub string) bool {
	volatile := []string{
		"NOW(", "RANDOM(", "GEN_RANDOM_UUID(", "NEXTVAL(",
		"CLOCK_TIMESTAMP(", "STATEMENT_TIMESTAMP(", "TIMEOFDAY(",
		"UUID_GENERATE_V4(", "CURRENT_TIMESTAMP",
	}
	for _, v := range volatile {
		if strings.Contains(sub, v) {
			return true
		}
	}
	return false
}

// isBinaryCoercibleWidening recognizes the well-known no-rewrite type changes:
// varchar(n) → text, and varchar(n) → varchar(m) with m >= n.
func isBinaryCoercibleWidening(sub string) bool {
	// Source must be a VARCHAR / CHARACTER VARYING column for the safe pairs.
	if !strings.Contains(sub, "VARCHAR") && !strings.Contains(sub, "CHARACTER VARYING") {
		return false
	}

	target := typeTarget(sub)
	switch {
	case target == "TEXT":
		return true
	case strings.HasPrefix(target, "VARCHAR"), strings.HasPrefix(target, "CHARACTER VARYING"):
		// Unbounded target (no length) is always a widening.
		newLen, hasNew := typeLength(target)
		if !hasNew {
			return true
		}
		oldLen, hasOld := typeLength(sourceVarcharLength(sub))
		// Unknown source length: be conservative and treat as rewrite.
		return hasOld && newLen >= oldLen
	}
	return false
}

// typeTarget extracts the target type token following "TYPE" / "SET DATA TYPE".
func typeTarget(sub string) string {
	idx := strings.Index(sub, " SET DATA TYPE ")
	skip := len(" SET DATA TYPE ")
	if idx < 0 {
		idx = strings.Index(sub, " TYPE ")
		skip = len(" TYPE ")
	}
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(sub[idx+skip:])
	// Cut at USING / COLLATE / comma / end.
	for _, stop := range []string{" USING ", " COLLATE ", ","} {
		if i := strings.Index(rest, stop); i >= 0 {
			rest = rest[:i]
		}
	}
	return strings.TrimSpace(rest)
}

// sourceVarcharLength returns the source-type token (the column declaration is
// not in the statement, so we only have the target). Source length is unknown
// from a bare ALTER COLUMN TYPE, so this returns "" — callers treat unknown as
// non-widening (conservative).
func sourceVarcharLength(string) string { return "" }

// typeLength parses the (n) length suffix of a varchar token, e.g. VARCHAR(255).
func typeLength(typ string) (int, bool) {
	open := strings.Index(typ, "(")
	if open < 0 {
		return 0, false
	}
	close := strings.Index(typ, ")")
	if close < 0 || close <= open+1 {
		return 0, false
	}
	n := 0
	for _, c := range typ[open+1 : close] {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
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
