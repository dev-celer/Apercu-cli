package pg_classify

import (
	"fmt"
	"strings"

	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// txnBlockCode is a statement the server refuses to run inside a transaction block.
const txnBlockCode pg_contract.Code = "V-01"

// queueRiskCode is a statement that can wait for a lock blocking writers or readers with nothing to bound the wait.
const queueRiskCode pg_contract.Code = "V-02"

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

func unboundedReason(timeout Timeout) string {
	switch {
	case timeout.Raw == "":
		return "no lock_timeout is set"
	case !timeout.Valid:
		return fmt.Sprintf("lock_timeout '%s' is not a value the server accepts", strings.Trim(timeout.Raw, `'"`))
	}
	return "lock_timeout is disabled"
}
