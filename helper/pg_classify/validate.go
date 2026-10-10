package pg_classify

import (
	"fmt"

	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// txnBlockCode is a statement the server refuses to run inside a transaction block.
const txnBlockCode pg_contract.Code = "V-01"

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
