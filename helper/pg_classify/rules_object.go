package pg_classify

import (
	"fmt"
	"strings"

	"apercu-cli/helper/pg_contract"
)

// ruleCreateTrigger is R-OB-CREATETRIG.
func ruleCreateTrigger(s scope) effect {
	e := newEffect(s, "R-OB-CREATETRIG", "the trigger is recorded on the table; writes wait for it, reads do not")
	e.lock = pg_contract.LockShareRowExclusive
	e.recursion = partitionsOnly
	return e.onEvery(s)
}

// ruleDropTrigger is R-OB-DROPTRIG.
func ruleDropTrigger(s scope) effect {
	name := ""
	if len(s.statement.Subcommands) > 0 {
		name = s.statement.Subcommands[0].Name
	}
	e := newEffect(s, "R-OB-DROPTRIG", fmt.Sprintf("trigger %q is removed, under a stronger lock than the one that created it", name))
	e.recursion = partitionsOnly
	return e
}

// ruleAlterTrigger is R-OB-ALTERTRIG.
func ruleAlterTrigger(s scope) effect {
	sub := s.statement.Subcommands
	rename := ""
	if len(sub) > 0 {
		rename = fmt.Sprintf("trigger %q is renamed to %q", sub[0].Name, sub[0].NewName)
	}
	return newEffect(s, "R-OB-ALTERTRIG", rename+"; the clone on every inheritor is renamed with it, so they are all locked")
}

// rulePolicy is R-OB-POLICY.
func rulePolicy(s scope) effect {
	e := newEffect(s, "R-OB-POLICY", "the row-level security policy changes in the catalog, which invalidates every plan built against the table")
	e.recursion = parentOnly
	return e
}

// ruleObjectRule is R-OB-RULE.
func ruleObjectRule(s scope) effect {
	e := newEffect(s, "R-OB-RULE", "the rewrite rule changes in the catalog, which invalidates every plan built against the table")
	e.recursion = parentOnly
	return e
}

// ruleStatistics is R-OB-STATS.
func ruleStatistics(s scope) effect {
	e := newEffect(s, "R-OB-STATS", "the extended statistics object changes; nothing is sampled until the next ANALYZE")
	e.lock = pg_contract.LockShareUpdateExclusive
	e.recursion = parentOnly
	if len(s.statement.Relations) > 0 {
		// CREATE STATISTICS names its tables; the other two spell only the statistics object.
		return e.onEvery(s)
	}

	e = e.onNone()
	if s.statement.Command == "ALTER STATISTICS" {
		e.message = "the statistics target is recorded on the statistics object; the table behind it is not locked at all"
		return e
	}
	for _, sub := range s.statement.Subcommands {
		table, ok := s.catalog.ExtStatTable(sub.Object.Schema, sub.Object.Table, s.context.SearchPath)
		if !ok {
			e.message += fmt.Sprintf("; the snapshot holds no statistics object named %s, so the table behind it is unknown", sub.ObjectName())
			continue
		}
		e.extra = append(e.extra, pg_contract.Target{
			Relation: table.Contract(),
			Lock:     pg_contract.LockShareUpdateExclusive,
			OpKind:   pg_contract.OpKindMetadata,
			Role:     pg_contract.TargetRoleResolved,
		})
	}
	return e
}

// ruleComment is R-OB-COMMENT.
func ruleComment(s scope) effect {
	e := newEffect(s, "R-OB-COMMENT", "the comment is written to the catalog and nothing else changes")
	e.lock = pg_contract.LockShareUpdateExclusive
	e.recursion = parentOnly
	if len(s.statement.Relations) == 0 {
		return e.onNone()
	}
	return e
}

// ruleGrant is R-OB-GRANT.
func ruleGrant(s scope) effect {
	verb := "granted"
	if s.statement.Command == "REVOKE" {
		verb = "revoked"
	}
	privileges := make([]string, 0, len(s.statement.Options))
	for _, option := range s.statement.Options {
		privileges = append(privileges, option.Name)
	}
	subject := "every privilege"
	if len(privileges) > 0 {
		subject = strings.Join(privileges, ", ")
	}

	// PG18 opens the relation at ACCESS SHARE, 15 to 17 take no lock at all. An unknown production version reads as 18, the stronger of the two.
	e := newEffect(s, "R-OB-GRANT", fmt.Sprintf("%s is %s on the relation; the access control list is a catalog row and no data is touched", subject, verb))
	e.lock = pg_contract.LockNone
	if s.atLeast18() {
		e.lock = pg_contract.LockAccessShare
	}
	e.recursion = parentOnly
	if len(s.statement.Relations) == 0 {
		// GRANT … ON ALL TABLES IN SCHEMA and the role-level forms name no relation.
		e.message += "; the statement names no relation directly"
		return e.onNone()
	}
	return e.onEvery(s)
}

// ruleDefaultPrivileges is R-OB-DEFACL.
func ruleDefaultPrivileges(s scope) effect {
	e := newEffect(s, "R-OB-DEFACL", "only the privileges future objects will be created with change; nothing that exists is touched")
	e.lock = pg_contract.LockNone
	e.op = pg_contract.OpKindNone
	e.recursion = parentOnly
	return e.onNone()
}

// rulePublication is R-OB-PUB.
func rulePublication(s scope) effect {
	e := newEffect(s, "R-OB-PUB", "the table joins or leaves the publication; logical replication picks the change up and no row is read")
	e.lock = pg_contract.LockShareUpdateExclusive
	return e.onEvery(s)
}

// ruleSubscription is R-OB-SUB.
func ruleSubscription(s scope) effect {
	e := newEffect(s, "R-OB-SUB", "nothing in this database is locked, but the subscriber may start a full copy of every table in the publication")
	e.lock = pg_contract.LockNone
	e.op = pg_contract.OpKindNone
	e.recursion = parentOnly
	return e.onNone()
}

// ruleExtension is R-OB-EXTENSION.
func ruleExtension(s scope) effect {
	e := newEffect(s, "R-OB-EXTENSION", "the extension's install script is opaque: it can create, alter and populate anything, so neither the locks it takes nor the work it does can be read from the statement")
	e.lock = pg_contract.LockNone
	e.op = pg_contract.OpKindNone
	e.recursion = parentOnly
	return e.onNone()
}

// ruleSchema is R-OB-SCHEMA.
func ruleSchema(s scope) effect {
	e := newEffect(s, "R-OB-SCHEMA", "a schema is a namespace; creating or dropping an empty one locks nothing")
	e.lock = pg_contract.LockNone
	e.op = pg_contract.OpKindNone
	e.recursion = parentOnly
	if s.statement.Command == "DROP SCHEMA" && s.statement.Flags.Cascade {
		e.message = "DROP SCHEMA … CASCADE drops every object in the schema, each under its own ACCESS EXCLUSIVE lock; the statement names none of them, so none can be listed here"
	}
	return e.onNone()
}
