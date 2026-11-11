// Package validate holds the checks V-01 to V-07, which run after the rules and add warnings from
// what the rules found.
package validate

import (
	"fmt"
	"slices"
	"strings"

	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgclassify/session"
	"apercu-cli/internal/pgclassify/stmt"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// QueueRiskCode is a statement that can wait for a lock blocking writers or readers with nothing to bound the wait.
const QueueRiskCode pgcontract.Code = "V-01"

// UnboundedQueue create warn findings on a statement that take a blocking lock without lock_timeout set.
// it ignores a relation that was created during the migration.
func UnboundedQueue(s engine.Scope, findings []pgcontract.Finding) []pgcontract.Finding {
	if s.Context.LockTimeout.Set() || s.Statement.Flags.Nowait {
		return nil
	}

	var targets []pgcontract.Target
	for _, finding := range findings {
		for _, target := range finding.Targets {
			if !target.Lock.IsWriteBlocking() || s.Catalog.CreatedByMigration(target.Relation.Name) {
				continue
			}
			targets = append(targets, pgcontract.Target{
				Relation: target.Relation,
				Lock:     target.Lock,
				OpKind:   pgcontract.OpKindNone,
				Role:     target.Role,
			})
		}
	}
	if len(targets) == 0 {
		return nil
	}

	return []pgcontract.Finding{{
		Code:     QueueRiskCode,
		Severity: pgcontract.SeverityWarn,
		Level:    pgcontract.LevelHigh,
		Message: fmt.Sprintf("%s, so this statement can wait indefinitely for its locks; while it waits, the writers "+
			"arriving after it queue behind it, and the readers too where the lock is ACCESS EXCLUSIVE", unboundedReason(s.Context.LockTimeout)),
		Targets: engine.DedupeTargets(targets),
	}}
}

func unboundedReason(timeout session.Timeout) string {
	switch {
	case timeout.Raw == "":
		return "no lock_timeout is set"
	case !timeout.Valid:
		return fmt.Sprintf("lock_timeout '%s' is not a value the server accepts", strings.Trim(timeout.Raw, `'"`))
	}
	return "lock_timeout is disabled"
}

// GateCode is V-02, the statement is not supported in this server version.
const GateCode pgcontract.Code = "V-02"

// VersionGate checks that a statement is compatible with the production version, and emit errors if it isn't the case.
func VersionGate(version pgcontract.Version, statement ir.Statement) []pgcontract.Error {
	var errors []pgcontract.Error

	for _, feature := range statement.Features {
		if version != pgcontract.VersionUnknown && version < feature.Since {
			errors = append(errors, pgcontract.Error{
				Code:    GateCode,
				Message: fmt.Sprintf("%s requires PostgreSQL %s, production runs %s", feature.Name, feature.Since, version),
			})
		}
	}

	return errors
}

// MigrationVersions narrows a versionRange from the statements, used only when the production version isn't available.
func MigrationVersions(versions pgcontract.VersionRange, statement ir.Statement, errors []pgcontract.Error) pgcontract.VersionRange {
	versions = versions.Intersect(pgcontract.AtLeast(statement.MinVersion()))
	for _, err := range errors {
		versions = versions.Intersect(err.Versions)
	}
	return versions
}

// NullableKeyCode is an ATTACH PARTITION whose CHECK bounds a nullable partition key without ruling NULL out, so it proves nothing.
const NullableKeyCode pgcontract.Code = "V-03"

// UnprovenNullableKey create a warn finding on an ATTACH PARTITION whose partition carries a CHECK on a nullable partition key
// column and nothing that says the column IS NOT NULL. The implied partition constraint includes it, so the CHECK cannot prove
// the bound and the partition is read in full.
// it ignores a partition that was created during the migration, a DEFAULT partition, a HASH key, a key column the parent
// holds NOT NULL, and a key column no CHECK of the partition mentions, since nothing there was meant to prove the bound.
func UnprovenNullableKey(s engine.Scope, findings []pgcontract.Finding) []pgcontract.Finding {
	if s.Statement.Command != "ALTER TABLE" {
		return nil
	}
	var attach *ir.Subcommand
	for i, sub := range s.Statement.Subcommands {
		if sub.Kind == ir.SubAttachPartition && sub.Value != "DEFAULT" && len(sub.Relations) > 0 {
			attach = &s.Statement.Subcommands[i]
		}
	}
	if attach == nil {
		return nil
	}

	parent := s.Relation()
	strategy, keys := parent.Relation.PartitionKeyColumns()
	// A hash bound is a satisfies_hash_partition call that accepts NULL: there is no IS NOT NULL to omit.
	if strategy == "HASH" || len(keys) == 0 {
		return nil
	}
	partition := s.Resolve(attach.Relations[0])
	if !partition.Exists() || s.Catalog.CreatedByMigration(partition.Name) {
		return nil
	}

	checks := s.ChecksOn(partition)
	inherited := s.CheckNamesOn(parent)
	var columns, bounding []string
	for _, key := range keys {
		key = s.Checks.CurrentColumn(parent.Key(), key)
		// A key the parent holds NOT NULL has to be NOT NULL on the partition too, or the server refuses the attach.
		if s.NeverNull(parent, key) || s.NeverNull(partition, key) {
			continue
		}
		var mentioned []string
		tested := false
		for _, check := range checks {
			tested = tested || stmt.TestsNull(check.Expr, key)
			if !inherited[check.Name] && stmt.Mentions(check.Expr, key) {
				mentioned = append(mentioned, check.Name)
			}
		}
		if len(mentioned) > 0 && !tested {
			columns = append(columns, key)
			bounding = append(bounding, mentioned...)
		}
	}
	if len(columns) == 0 {
		return nil
	}

	var targets []pgcontract.Target
	for _, finding := range findings {
		for _, target := range finding.Targets {
			if target.Relation.Name != partition.Name {
				continue
			}
			targets = append(targets, pgcontract.Target{
				Relation: target.Relation,
				Lock:     target.Lock,
				OpKind:   pgcontract.OpKindNone,
				Role:     target.Role,
			})
		}
	}
	if len(targets) == 0 {
		return nil
	}

	slices.Sort(bounding)
	notNull := make([]string, len(columns))
	for i, column := range columns {
		notNull[i] = column + " IS NOT NULL"
	}
	return []pgcontract.Finding{{
		Code:     NullableKeyCode,
		Severity: pgcontract.SeverityWarn,
		Message: fmt.Sprintf("%s can hold NULL in %s, and the partition constraint ATTACH has to prove includes %s, which no CHECK on %s "+
			"says: %s cannot prove the bound on its own, so %s is still read in full under ACCESS EXCLUSIVE. Add %s to the CHECK so the "+
			"scan is skipped", partition.Name, strings.Join(columns, ", "), strings.Join(notNull, " AND "), partition.Name,
			strings.Join(slices.Compact(bounding), ", "), partition.Name, strings.Join(notNull, " AND ")),
		Targets: engine.DedupeTargets(targets),
	}}
}

// MaintenanceStatementCode is a VACUUM FULL or a CLUSTER, maintenance statement that should not be found inside a migration file.
const MaintenanceStatementCode pgcontract.Code = "V-04"

var maintenanceStatementsList = map[pgcontract.Code]string{
	"R-MT-VACUUMFULL": "VACUUM FULL",
	"R-MT-CLUSTER":    "CLUSTER",
}

// MaintenanceStatements create a warn finding on a VACUUM FULL or a CLUSTER, naming every relation it rewrites.
// it ignores a relation that was created during the migration.
func MaintenanceStatements(s engine.Scope, findings []pgcontract.Finding) []pgcontract.Finding {
	var out []pgcontract.Finding
	for _, finding := range findings {
		subject, rewrites := maintenanceStatementsList[finding.Code]
		if !rewrites {
			continue
		}

		var targets []pgcontract.Target
		for _, target := range finding.Targets {
			if s.Catalog.CreatedByMigration(target.Relation.Name) {
				continue
			}
			targets = append(targets, pgcontract.Target{
				Relation: target.Relation,
				Lock:     target.Lock,
				OpKind:   pgcontract.OpKindNone,
				Role:     target.Role,
			})
		}
		if len(targets) == 0 {
			continue
		}

		out = append(out, pgcontract.Finding{
			Code:     MaintenanceStatementCode,
			Severity: pgcontract.SeverityWarn,
			Level:    pgcontract.LevelHigh,
			Message: fmt.Sprintf("%s rewrites the table into a new file under ACCESS EXCLUSIVE: nothing reads or writes it until it finishes, "+
				"the outage grows with the table's size, and it needs room for a second copy; it is maintenance rather than a schema change, "+
				"so run it outside the migration", subject),
			Targets: engine.DedupeTargets(targets),
		})
	}
	return out
}

// IndexRenameCode is an ALTER TABLE ... RENAME TO whose target is an index, which ALTER INDEX renames under a weaker lock.
const IndexRenameCode pgcontract.Code = "V-05"

// IndexRenamedAsTable create a warn finding on an ALTER TABLE ... RENAME TO that names an index.
// it ignores an index that was created during the migration, and a relation whose kind is unknown.
func IndexRenamedAsTable(s engine.Scope, findings []pgcontract.Finding) []pgcontract.Finding {
	var targets []pgcontract.Target
	for _, finding := range findings {
		if finding.Code != "R-AT-RENAME" {
			continue
		}
		for _, target := range finding.Targets {
			if !target.Relation.Kind.IsIndex() || s.Catalog.CreatedByMigration(target.Relation.Name) {
				continue
			}
			targets = append(targets, pgcontract.Target{
				Relation: target.Relation,
				Lock:     target.Lock,
				OpKind:   pgcontract.OpKindNone,
				Role:     target.Role,
			})
		}
	}
	if len(targets) == 0 {
		return nil
	}

	table := "its table"
	if resolved, ok := s.TableOfIndex(s.Relation()); ok {
		table = resolved.Name.String()
	}
	newName := ""
	if len(s.Statement.Subcommands) > 0 {
		newName = s.Statement.Subcommands[0].NewName
	}
	index := targets[0].Relation.Name

	return []pgcontract.Finding{{
		Code:     IndexRenameCode,
		Severity: pgcontract.SeverityWarn,
		Message: fmt.Sprintf("ALTER TABLE renames index %s under ACCESS EXCLUSIVE; every query on %s opens its indexes when it is planned "+
			"and every write updates them, so all of them wait behind it. ALTER INDEX %s RENAME TO %s does the same under "+
			"SHARE UPDATE EXCLUSIVE, which blocks neither reads nor writes", index, table, index, newName),
		Targets: engine.DedupeTargets(targets),
	}}
}

// PartitionOfCode is a CREATE TABLE ... PARTITION OF, which ATTACH PARTITION does under a weaker lock on the parent.
const PartitionOfCode pgcontract.Code = "V-06"

// PartitionCreatedInPlace create a warn finding on a CREATE TABLE ... PARTITION OF, naming the parent it holds at ACCESS EXCLUSIVE.
// it ignores a parent that was created during the migration.
func PartitionCreatedInPlace(s engine.Scope, findings []pgcontract.Finding) []pgcontract.Finding {
	var targets []pgcontract.Target
	defaultPartition := ""
	for _, finding := range findings {
		if finding.Code != "R-TB-CREATE-PARTOF" {
			continue
		}
		for _, target := range finding.Targets {
			if target.Role == pgcontract.TargetRoleImplicit && defaultPartition == "" {
				defaultPartition = target.Relation.Name.String()
			}
			if target.Role != pgcontract.TargetRoleDirect || s.Catalog.CreatedByMigration(target.Relation.Name) {
				continue
			}
			if kind := target.Relation.Kind; kind != pgcontract.RelationKindUnknown && !kind.IsPartitioned() {
				continue
			}
			targets = append(targets, pgcontract.Target{
				Relation: target.Relation,
				Lock:     target.Lock,
				OpKind:   pgcontract.OpKindNone,
				Role:     target.Role,
			})
		}
	}
	if len(targets) == 0 {
		return nil
	}

	bound := "FOR VALUES …"
	for _, sub := range s.Statement.Subcommands {
		if sub.Kind == ir.SubAttachPartition && sub.Value == "DEFAULT" {
			bound = "DEFAULT"
		}
	}
	partition := s.Relation().Name
	parent := targets[0].Relation.Name

	message := fmt.Sprintf("CREATE TABLE … PARTITION OF holds %s at ACCESS EXCLUSIVE while the partition is added, so every query on it "+
		"waits behind the statement. CREATE TABLE %s with the same columns, followed by ALTER TABLE %s ATTACH PARTITION %s %s, does the "+
		"same under SHARE UPDATE EXCLUSIVE, which blocks neither reads nor writes; the new table is empty, so the scan ATTACH makes of it "+
		"is instant", parent, partition, parent, partition, bound)
	if defaultPartition != "" {
		message += fmt.Sprintf("; the default partition %s is held at ACCESS EXCLUSIVE and scanned either way", defaultPartition)
	}

	return []pgcontract.Finding{{
		Code:     PartitionOfCode,
		Severity: pgcontract.SeverityWarn,
		Message:  message,
		Targets:  engine.DedupeTargets(targets),
	}}
}

// ExtensionScriptCode is a CREATE EXTENSION or an ALTER EXTENSION ... UPDATE, which runs a script the statement does not show.
const ExtensionScriptCode pgcontract.Code = "V-07"

// ExtensionScript create a warn finding on a statement that runs an extension's install or update script.
// it ignores a statement for which the server runs no script: IF NOT EXISTS on an installed extension, or an UPDATE TO the installed version.
func ExtensionScript(s engine.Scope, findings []pgcontract.Finding) []pgcontract.Finding {
	if !slices.ContainsFunc(findings, func(finding pgcontract.Finding) bool { return finding.Code == "R-OB-EXTENSION" }) {
		return nil
	}
	if len(s.Statement.Subcommands) == 0 {
		return nil
	}
	sub := s.Statement.Subcommands[0]
	installed, isInstalled := s.Catalog.Extension(sub.Name)

	var message string
	if s.Statement.Command == "ALTER EXTENSION" {
		if isInstalled && sub.Value != "" && installed.Version == sub.Value {
			return nil
		}
		path := "to the default version"
		if sub.Value != "" {
			path = fmt.Sprintf("to version %s", sub.Value)
		}
		if isInstalled {
			path = fmt.Sprintf("from version %s %s", installed.Version, path)
		}
		message = fmt.Sprintf("ALTER EXTENSION %s UPDATE runs the extension's update scripts %s, which the migration does not show: they can "+
			"create, alter and fill objects under locks of their own, and none of them are reported here. Read the scripts before approving, "+
			"or update the extension ahead of the migration", sub.Name, path)
	} else {
		if isInstalled && s.Statement.Flags.IfNotExists {
			return nil
		}
		message = fmt.Sprintf("CREATE EXTENSION %s runs the extension's install script, which the migration does not show: it can create, "+
			"alter and fill objects under locks of its own, and none of them are reported here", sub.Name)
		if s.Statement.Flags.Cascade {
			message += "; CASCADE also installs every extension it requires that is missing, each through its own script"
		}
		message += ". Read the script before approving, or install the extension ahead of the migration"
	}

	return []pgcontract.Finding{{
		Code:     ExtensionScriptCode,
		Severity: pgcontract.SeverityWarn,
		Level:    pgcontract.LevelLow,
		Message:  message,
	}}
}
