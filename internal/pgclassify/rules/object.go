package rules

import (
	"fmt"
	"strings"

	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgcontract"
)

// ruleStatistics is R-OB-STATS.
func ruleStatistics(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-OB-STATS", "the extended statistics object changes; nothing is sampled until the next ANALYZE")
	if len(s.Statement.Relations) > 0 {
		// CREATE STATISTICS names its tables; the other two spell only the statistics object.
		return e.OnEvery(s)
	}

	e = e.OnNone()
	if s.Statement.Command == "ALTER STATISTICS" {
		e.Message = "the statistics target is recorded on the statistics object; the table behind it is not locked at all"
		return e
	}
	for _, sub := range s.Statement.Subcommands {
		table, ok := s.Catalog.ExtStatTable(sub.Object.Schema, sub.Object.Table, s.Context.SearchPath)
		if !ok {
			e.Message += fmt.Sprintf("; the snapshot holds no statistics object named %s, so the table behind it is unknown", sub.ObjectName())
			continue
		}
		e.Touches = append(e.Touches, engine.Touch{
			Info: s.InfoOf(table),
			Lock: pgcontract.LockShareUpdateExclusive,
			Op:   pgcontract.OpKindMetadata,
			Role: pgcontract.TargetRoleResolved,
		})
	}
	return e
}

// ruleComment is R-OB-COMMENT.
func ruleComment(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-OB-COMMENT", "the comment is written to the catalog and nothing else changes")
	if len(s.Statement.Relations) == 0 {
		return e.OnNone()
	}
	return e
}

// ruleGrant is R-OB-GRANT.
func ruleGrant(s engine.Scope) engine.Effect {
	verb := "granted"
	if s.Statement.Command == "REVOKE" {
		verb = "revoked"
	}
	privileges := make([]string, 0, len(s.Statement.Options))
	for _, option := range s.Statement.Options {
		privileges = append(privileges, option.Name)
	}
	subject := "every privilege"
	if len(privileges) > 0 {
		subject = strings.Join(privileges, ", ")
	}

	// PG18 opens the relation at ACCESS SHARE, 15 to 17 take no lock at all. An unknown production version reads as 18, the stronger of the two.
	e := engine.NewEffect(s, "R-OB-GRANT", fmt.Sprintf("%s is %s on the relation; the access control list is a catalog row and no data is touched", subject, verb))
	if s.AtLeast18() {
		e.Lock = pgcontract.LockAccessShare
	}
	if len(s.Statement.Relations) == 0 {
		// GRANT … ON ALL TABLES IN SCHEMA and the role-level forms name no relation.
		e.Message += "; the statement names no relation directly"
		return e.OnNone()
	}
	return e.OnEvery(s)
}

// ruleDefaultPrivileges is R-OB-DEFACL.
func ruleDefaultPrivileges(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-OB-DEFACL", "only the privileges future objects will be created with change; nothing that exists is touched")
	return e.OnNone()
}

// rulePublication is R-OB-PUB.
func rulePublication(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-OB-PUB", "the table joins or leaves the publication; logical replication picks the change up and no row is read")
	return e.OnEvery(s)
}

// ruleSubscription is R-OB-SUB.
func ruleSubscription(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-OB-SUB", "nothing in this database is locked, but the subscriber may start a full copy of every table in the publication")
	return e.OnNone()
}

// ruleExtension is R-OB-EXTENSION, for CREATE EXTENSION and ALTER EXTENSION … UPDATE.
func ruleExtension(s engine.Scope) engine.Effect {
	script := "install"
	if s.Statement.Command == "ALTER EXTENSION" {
		script = "update"
	}
	e := engine.NewEffect(s, "R-OB-EXTENSION", fmt.Sprintf("the extension's %s script is opaque: it can create, alter and populate anything, so neither the locks it takes nor the work it does can be read from the statement", script))
	return e.OnNone()
}

// ruleSchema is R-OB-SCHEMA.
func ruleSchema(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-OB-SCHEMA", "a schema is a namespace; creating or dropping an empty one locks nothing")
	if s.Statement.Command == "DROP SCHEMA" && s.Statement.Flags.Cascade {
		e.Message = "DROP SCHEMA … CASCADE drops every object in the schema, each under its own ACCESS EXCLUSIVE lock; the statement names none of them, so none can be listed here"
	}
	return e.OnNone()
}
