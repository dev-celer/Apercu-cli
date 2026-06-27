package metrics

import "strings"

// ClassifyOperation infers the operation kind and a remediation hint for a DDL statement.
func ClassifyOperation(sql string, serverVersion float32) (EventOperationType, string) {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	if upper == "" {
		return EventOperationTypeNonBlocking, ""
	}

	hasPrefix := func(p string) bool { return strings.HasPrefix(upper, p) }
	contains := func(s string) bool { return strings.Contains(upper, s) }

	switch {
	case hasPrefix("ALTER TABLE"):
		return classifyAlterTable(upper, serverVersion)

	case hasPrefix("CREATE UNIQUE INDEX"), hasPrefix("CREATE INDEX"):
		if contains(" CONCURRENTLY") {
			return EventOperationTypeNonBlocking, ""
		}
		return EventOperationTypeScanUnderLock, "We recommand to use 'CREATE INDEX' with the 'CONCURRENTLY' keyword for production environment"

	case hasPrefix("DROP INDEX"):
		if contains(" CONCURRENTLY") {
			return EventOperationTypeNonBlocking, ""
		}
		if contains(" CASCADE") {
			// CONCURRENTLY does not support the CASCADE parameter, so we should not recommend it as remediation
			return EventOperationTypeMetadataOnly, ""
		}
		return EventOperationTypeMetadataOnly, "We recommand to use 'DROP INDEX' with the 'CONCURRENTLY' keyword for production environment if the index does not enforce 'UNIQUE' or 'PRIMARY KEY'"

	case hasPrefix("REINDEX"):
		if contains(" CONCURRENTLY") {
			return EventOperationTypeNonBlocking, ""
		}
		if serverVersion >= 12 || serverVersion == 0 {
			return EventOperationTypeRewriteUnderLock, "We recommend to use 'REINDEX' with the 'CONCURRENTLY' keyword for production environment if the postgres major version is 12+"
		}
		return EventOperationTypeRewriteUnderLock, ""

	case hasPrefix("REFRESH MATERIALIZED VIEW"):
		if contains(" CONCURRENTLY") {
			// Blocks only writes to the matview — acceptable.
			return EventOperationTypeNonBlocking, ""
		}
		return EventOperationTypeRewriteUnderLock, "We recommend to use 'REFRESH MATERIALIZED VIEW' with the 'CONCURRENTLY' keyword for production environment if the view include a 'UNIQUE' index that includes all rows"

	case hasPrefix("CLUSTER"):
		return EventOperationTypeRewriteUnderLock, ""

	case hasPrefix("VACUUM"):
		if contains(" FULL") {
			return EventOperationTypeRewriteUnderLock, ""
		}
		return EventOperationTypeNonBlocking, ""

	case hasPrefix("TRUNCATE"):
		return EventOperationTypeMetadataOnly, ""

	case hasPrefix("CREATE TRIGGER"),
		hasPrefix("CREATE RULE"),
		hasPrefix("ALTER RULE"):
		return EventOperationTypeMetadataOnly, ""

	case hasPrefix("ALTER TYPE"):
		return EventOperationTypeMetadataOnly, ""
	}

	return EventOperationTypeNonBlocking, ""
}

// classifyAlterTable splits an ALTER TABLE statement into its comma-separated
// subcommands and returns the most severe classification across them.
func classifyAlterTable(upper string, version float32) (EventOperationType, string) {
	kind := EventOperationTypeNonBlocking
	remediation := ""

	// Drop the leading "ALTER TABLE" keyword so the table-level prefix's "ALTER "
	// token isn't mistaken for an "ALTER COLUMN" subcommand during classification.
	body := strings.TrimSpace(strings.TrimPrefix(upper, "ALTER TABLE"))

	for _, sub := range splitAlterTableTopLevel(body) {
		sub = strings.TrimSpace(sub)
		k, r := classifyAlterSubcommand(sub, version)
		if k.severity() > kind.severity() {
			kind, remediation = k, r
		}
	}

	return kind, remediation
}

func classifyAlterSubcommand(sub string, version float32) (EventOperationType, string) {
	contains := func(s string) bool { return strings.Contains(sub, s) }

	switch {
	case contains("ADD CONSTRAINT"), contains("ADD PRIMARY KEY"),
		contains("ADD UNIQUE"), contains("ADD FOREIGN KEY"),
		contains("ADD CHECK"), contains("ADD EXCLUDE"):
		return classifyAddConstraint(sub)

	case contains("VALIDATE CONSTRAINT"):
		return EventOperationTypeNonBlocking, ""

	case contains("DROP CONSTRAINT"):
		return EventOperationTypeMetadataOnly, ""

	// --- Column type/null/default/storage changes ---
	case contains("ALTER COLUMN"), contains("ALTER "):
		if k, r, ok := classifyAlterColumn(sub, version); ok {
			return k, r
		}

	// --- Add / drop column ---
	default:
		switch {
		case contains("DROP COLUMN"):
			return EventOperationTypeMetadataOnly, ""
		case contains("ADD COLUMN"), strings.HasPrefix(sub, "ADD ") || strings.Contains(sub, " ADD "):
			return classifyAddColumn(sub, version)

		// --- Table-level rewrites ---
		case contains("SET LOGGED"), contains("SET UNLOGGED"):
			return EventOperationTypeRewriteUnderLock, ""
		case contains("SET TABLESPACE"):
			return EventOperationTypeRewriteUnderLock, ""

		// --- Cheap metadata changes ---
		case contains("SET (FILLFACTOR"), contains("RESET ("):
			return EventOperationTypeMetadataOnly, ""
		case contains("ENABLE TRIGGER"), contains("DISABLE TRIGGER"),
			contains("ENABLE REPLICA TRIGGER"), contains("ENABLE ALWAYS TRIGGER"):
			return EventOperationTypeMetadataOnly, ""
		case contains("RENAME"):
			return EventOperationTypeMetadataOnly, ""
		}
	}

	return EventOperationTypeNonBlocking, ""
}

func classifyAddColumn(sub string, version float32) (EventOperationType, string) {
	contains := func(s string) bool { return strings.Contains(sub, s) }

	switch {
	case contains("GENERATED") && contains("STORED"):
		return EventOperationTypeRewriteUnderLock, "we recommend to avoid adding a new column with 'GENERATED ALWAYS AS ... STORED' where possible, instead you can create a simple nullable column, create a trigger 'BEFORE INSERT OR UPDATE' with your default, backfill the rows than set to NOT NULL"
	case contains("GENERATED") && contains("AS IDENTITY"):
		return EventOperationTypeRewriteUnderLock, "when adding a new column with 'GENERATED ALWAYS AS IDENTITY', we recommend to first create a nullable column, backfill the rows, set 'NOT NULL', than attach the 'GENERATED ALWAYS AS IDENTITY' property"
	case contains("DEFAULT") && hasVolatileDefault(sub):
		return EventOperationTypeRewriteUnderLock, "when adding a new column with volatile default, we recommend to first add a nullable column without default, set default for new rows then backfill existing rows in batches"
	case contains("DEFAULT"):
		// Non-volatile constant default. Metadata-only on PG 11+.
		if version != 0 && version < 11 {
			return EventOperationTypeRewriteUnderLock, "when adding a new column with default in postgres version pre-11, we recommend to first add a nullable column without default, set default for new rows then backfill existing rows in batches"
		}
		return EventOperationTypeMetadataOnly, ""
	default:
		return EventOperationTypeMetadataOnly, ""
	}
}

// classifyAlterColumn returns ok=false when the subcommand is an ALTER COLUMN
// shape it does not recognize, so the caller can fall through.
func classifyAlterColumn(sub string, version float32) (EventOperationType, string, bool) {
	contains := func(s string) bool { return strings.Contains(sub, s) }

	switch {
	case contains("SET DEFAULT"), contains("DROP DEFAULT"):
		return EventOperationTypeMetadataOnly, "", true
	case contains("DROP NOT NULL"):
		return EventOperationTypeMetadataOnly, "", true
	case contains("SET NOT NULL"):
		// TODO PG12+ : Check if a check not null constraint exist, if not, recommend creating is first then set not null will reuse the constraint and be metadata only
		return EventOperationTypeScanUnderLock, "", true
	case contains("SET STORAGE"):
		return EventOperationTypeMetadataOnly, "", true
	case contains("SET STATISTICS"):
		return EventOperationTypeMetadataOnly, "", true
	case contains(" TYPE "), contains(" SET DATA TYPE "):
		if isBinaryCoercibleWidening(sub) {
			return EventOperationTypeMetadataOnly, "", true
		}
		return EventOperationTypeRewriteUnderLock, "We recommend adding a new column of the target type, backfill it, swap via RENAME then dropping the old column", true
	}

	return EventOperationTypeNonBlocking, "", false
}

func classifyAddConstraint(sub string) (EventOperationType, string) {
	contains := func(s string) bool { return strings.Contains(sub, s) }

	notValid := contains("NOT VALID")

	switch {
	case contains("CHECK"):
		if notValid {
			return EventOperationTypeMetadataOnly, ""
		}
		return EventOperationTypeScanUnderLock, "we recommend to use 'ADD CONSTRAINT ... CHECK' with 'NOT VALID', then VALIDATE CONSTRAINT in a separate statement"
	case contains("FOREIGN KEY"), contains("REFERENCES"):
		if notValid {
			return EventOperationTypeMetadataOnly, ""
		}
		return EventOperationTypeScanUnderLock, "we recommend to use 'ADD FOREIGN KEY' with 'NOT VALID', then VALIDATE CONSTRAINT in a separate statement"
	case contains("PRIMARY KEY"):
		// TODO Should check if column is NOT NULL else recommend to enforce NOT NULL before adding PRIMARY KEY
		if contains("USING INDEX") {
			return EventOperationTypeMetadataOnly, ""
		}
		return EventOperationTypeScanUnderLock, "we recommend creating a unique index concurrently first then enforcing the primary key with 'USING INDEX <idx>'"
	case contains("UNIQUE"):
		if contains("USING INDEX") {
			return EventOperationTypeMetadataOnly, ""
		}
		return EventOperationTypeScanUnderLock, "we recommend creating a unique index concurrently first then add the unique constraint with 'USING INDEX <idx>'"
	case contains("EXCLUDE"):
		return EventOperationTypeScanUnderLock, ""
	}

	return EventOperationTypeNonBlocking, ""
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
