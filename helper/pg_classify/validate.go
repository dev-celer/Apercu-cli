package pg_classify

import (
	"fmt"
	"slices"
	"strings"

	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// txnBlockCode is a statement the server refuses to run inside a transaction block.
const txnBlockCode pg_contract.Code = "V-01"

// queueRiskCode is a statement that can wait for a lock blocking writers or readers with nothing to bound the wait.
const queueRiskCode pg_contract.Code = "V-02"

// maintenanceStatementCode is a VACUUM FULL or a CLUSTER, maintenance statement that should not be found inside a migration file.
const maintenanceStatementCode pg_contract.Code = "V-06"

var maintenanceStatementsList = map[pg_contract.Code]string{
	"R-MT-VACUUMFULL": "VACUUM FULL",
	"R-MT-CLUSTER":    "CLUSTER",
}

// indexRenameCode is an ALTER TABLE ... RENAME TO whose target is an index, which ALTER INDEX renames under a weaker lock.
const indexRenameCode pg_contract.Code = "V-07"

// partitionOfCode is a CREATE TABLE ... PARTITION OF, which ATTACH PARTITION does under a weaker lock on the parent.
const partitionOfCode pg_contract.Code = "V-08"

// extensionScriptCode is a CREATE EXTENSION or an ALTER EXTENSION ... UPDATE, which runs a script the statement does not show.
const extensionScriptCode pg_contract.Code = "V-09"

// txnUnsafeSubject convert the command name from a parsed statement to the same way the server names it when it refuses to run
// inside a transaction block. return an empty string if it is safe to run inside transaction.
func txnUnsafeSubject(s scope) string {
	statement := s.statement
	switch statement.Command {
	case "CREATE INDEX", "DROP INDEX":
		if statement.Flags.Concurrently {
			return string(statement.Command) + " CONCURRENTLY"
		}
	case "REINDEX INDEX", "REINDEX TABLE":
		if statement.Flags.Concurrently {
			return "REINDEX CONCURRENTLY"
		}
		if s.partitioned() {
			return string(statement.Command)
		}
	case "REINDEX SCHEMA", "REINDEX DATABASE", "REINDEX SYSTEM":
		return string(statement.Command)
	case "VACUUM":
		return "VACUUM"
	case "CLUSTER":
		if len(statement.Relations) == 0 || s.partitioned() {
			return "CLUSTER"
		}
	case "CREATEDB":
		return "CREATE DATABASE"
	case "DROPDB":
		return "DROP DATABASE"
	case "CREATE TABLE SPACE":
		return "CREATE TABLESPACE"
	case "DROP TABLE SPACE":
		return "DROP TABLESPACE"
	case "ALTER TABLE":
		for _, sub := range statement.Subcommands {
			if sub.Kind == pg_parse.SubDetachPartition && sub.Flags.Concurrently {
				return "ALTER TABLE ... DETACH CONCURRENTLY"
			}
		}
	}
	return ""
}

// txnBlockSafety return errors for statement that cannot run inside transaction but are in one.
func txnBlockSafety(s scope, inTransaction bool) []pg_contract.Error {
	if !inTransaction {
		return nil
	}
	subject := txnUnsafeSubject(s)
	if subject == "" {
		return nil
	}
	return []pg_contract.Error{{
		Code:    txnBlockCode,
		Message: fmt.Sprintf("%s cannot run inside a transaction block", subject),
	}}
}

// unboundedQueue create warn findings on a statement that take a blocking lock without lock_timeout set.
// it ignores a relation that was created during the migration.
func unboundedQueue(s scope, findings []pg_contract.Finding) []pg_contract.Finding {
	if s.context.LockTimeout.Set() || s.statement.Flags.Nowait {
		return nil
	}

	var targets []pg_contract.Target
	for _, finding := range findings {
		for _, target := range finding.Targets {
			if !target.Lock.IsWriteBlocking() || s.catalog.CreatedByMigration(target.Relation.Name) {
				continue
			}
			targets = append(targets, pg_contract.Target{
				Relation: target.Relation,
				Lock:     target.Lock,
				OpKind:   pg_contract.OpKindNone,
				Role:     target.Role,
			})
		}
	}
	if len(targets) == 0 {
		return nil
	}

	return []pg_contract.Finding{{
		Code:     queueRiskCode,
		Severity: pg_contract.SeverityWarn,
		Level:    pg_contract.LevelHigh,
		Message: fmt.Sprintf("%s, so this statement can wait indefinitely for its locks; while it waits, the writers "+
			"arriving after it queue behind it, and the readers too where the lock is ACCESS EXCLUSIVE", unboundedReason(s.context.LockTimeout)),
		Targets: dedupeTargets(targets),
	}}
}

// maintenanceStatements create a warn finding on a VACUUM FULL or a CLUSTER, naming every relation it rewrites.
// it ignores a relation that was created during the migration.
func maintenanceStatements(s scope, findings []pg_contract.Finding) []pg_contract.Finding {
	var out []pg_contract.Finding
	for _, finding := range findings {
		subject, rewrites := maintenanceStatementsList[finding.Code]
		if !rewrites {
			continue
		}

		var targets []pg_contract.Target
		for _, target := range finding.Targets {
			if s.catalog.CreatedByMigration(target.Relation.Name) {
				continue
			}
			targets = append(targets, pg_contract.Target{
				Relation: target.Relation,
				Lock:     target.Lock,
				OpKind:   pg_contract.OpKindNone,
				Role:     target.Role,
			})
		}
		if len(targets) == 0 {
			continue
		}

		out = append(out, pg_contract.Finding{
			Code:     maintenanceStatementCode,
			Severity: pg_contract.SeverityWarn,
			Level:    pg_contract.LevelHigh,
			Message: fmt.Sprintf("%s rewrites the table into a new file under ACCESS EXCLUSIVE: nothing reads or writes it until it finishes, "+
				"the outage grows with the table's size, and it needs room for a second copy; it is maintenance rather than a schema change, "+
				"so run it outside the migration", subject),
			Targets: dedupeTargets(targets),
		})
	}
	return out
}

// indexRenamedAsTable create a warn finding on an ALTER TABLE ... RENAME TO that names an index.
// it ignores an index that was created during the migration, and a relation whose kind is unknown.
func indexRenamedAsTable(s scope, findings []pg_contract.Finding) []pg_contract.Finding {
	var targets []pg_contract.Target
	for _, finding := range findings {
		if finding.Code != "R-AT-RENAME" {
			continue
		}
		for _, target := range finding.Targets {
			if !target.Relation.Kind.IsIndex() || s.catalog.CreatedByMigration(target.Relation.Name) {
				continue
			}
			targets = append(targets, pg_contract.Target{
				Relation: target.Relation,
				Lock:     target.Lock,
				OpKind:   pg_contract.OpKindNone,
				Role:     target.Role,
			})
		}
	}
	if len(targets) == 0 {
		return nil
	}

	table := "its table"
	if resolved, ok := s.tableOfIndex(s.relation()); ok {
		table = resolved.Name.String()
	}
	newName := ""
	if len(s.statement.Subcommands) > 0 {
		newName = s.statement.Subcommands[0].NewName
	}
	index := targets[0].Relation.Name

	return []pg_contract.Finding{{
		Code:     indexRenameCode,
		Severity: pg_contract.SeverityWarn,
		Message: fmt.Sprintf("ALTER TABLE renames index %s under ACCESS EXCLUSIVE; every query on %s opens its indexes when it is planned "+
			"and every write updates them, so all of them wait behind it. ALTER INDEX %s RENAME TO %s does the same under "+
			"SHARE UPDATE EXCLUSIVE, which blocks neither reads nor writes", index, table, index, newName),
		Targets: dedupeTargets(targets),
	}}
}

// partitionCreatedInPlace create a warn finding on a CREATE TABLE ... PARTITION OF, naming the parent it holds at ACCESS EXCLUSIVE.
// it ignores a parent that was created during the migration.
func partitionCreatedInPlace(s scope, findings []pg_contract.Finding) []pg_contract.Finding {
	var targets []pg_contract.Target
	defaultPartition := ""
	for _, finding := range findings {
		if finding.Code != "R-TB-CREATE-PARTOF" {
			continue
		}
		for _, target := range finding.Targets {
			if target.Role == pg_contract.TargetRoleImplicit && defaultPartition == "" {
				defaultPartition = target.Relation.Name.String()
			}
			if target.Role != pg_contract.TargetRoleDirect || s.catalog.CreatedByMigration(target.Relation.Name) {
				continue
			}
			if kind := target.Relation.Kind; kind != pg_contract.RelationKindUnknown && !kind.IsPartitioned() {
				continue
			}
			targets = append(targets, pg_contract.Target{
				Relation: target.Relation,
				Lock:     target.Lock,
				OpKind:   pg_contract.OpKindNone,
				Role:     target.Role,
			})
		}
	}
	if len(targets) == 0 {
		return nil
	}

	bound := "FOR VALUES …"
	for _, sub := range s.statement.Subcommands {
		if sub.Kind == pg_parse.SubAttachPartition && sub.Value == "DEFAULT" {
			bound = "DEFAULT"
		}
	}
	partition := s.relation().Name
	parent := targets[0].Relation.Name

	message := fmt.Sprintf("CREATE TABLE … PARTITION OF holds %s at ACCESS EXCLUSIVE while the partition is added, so every query on it "+
		"waits behind the statement. CREATE TABLE %s with the same columns, followed by ALTER TABLE %s ATTACH PARTITION %s %s, does the "+
		"same under SHARE UPDATE EXCLUSIVE, which blocks neither reads nor writes; the new table is empty, so the scan ATTACH makes of it "+
		"is instant", parent, partition, parent, partition, bound)
	if defaultPartition != "" {
		message += fmt.Sprintf("; the default partition %s is held at ACCESS EXCLUSIVE and scanned either way", defaultPartition)
	}

	return []pg_contract.Finding{{
		Code:     partitionOfCode,
		Severity: pg_contract.SeverityWarn,
		Message:  message,
		Targets:  dedupeTargets(targets),
	}}
}

// extensionScript create a warn finding on a statement that runs an extension's install or update script.
// it ignores a statement for which the server runs no script: IF NOT EXISTS on an installed extension, or an UPDATE TO the installed version.
func extensionScript(s scope, findings []pg_contract.Finding) []pg_contract.Finding {
	if !slices.ContainsFunc(findings, func(finding pg_contract.Finding) bool { return finding.Code == "R-OB-EXTENSION" }) {
		return nil
	}
	if len(s.statement.Subcommands) == 0 {
		return nil
	}
	sub := s.statement.Subcommands[0]
	installed, isInstalled := s.catalog.Extension(sub.Name)

	var message string
	if s.statement.Command == "ALTER EXTENSION" {
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
		if isInstalled && s.statement.Flags.IfNotExists {
			return nil
		}
		message = fmt.Sprintf("CREATE EXTENSION %s runs the extension's install script, which the migration does not show: it can create, "+
			"alter and fill objects under locks of its own, and none of them are reported here", sub.Name)
		if s.statement.Flags.Cascade {
			message += "; CASCADE also installs every extension it requires that is missing, each through its own script"
		}
		message += ". Read the script before approving, or install the extension ahead of the migration"
	}

	return []pg_contract.Finding{{
		Code:     extensionScriptCode,
		Severity: pg_contract.SeverityWarn,
		Level:    pg_contract.LevelLow,
		Message:  message,
	}}
}

func unboundedReason(timeout Timeout) string {
	switch {
	case timeout.Raw == "":
		return "no lock_timeout is set"
	case !timeout.Valid:
		return fmt.Sprintf("lock_timeout '%s' is not a value the server accepts", strings.Trim(timeout.Raw, `'"`))
	}
	return "lock_timeout is disabled"
}
