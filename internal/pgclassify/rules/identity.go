package rules

import (
	"fmt"

	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// ruleOwner is R-AT-OWNER.
func ruleOwner(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-OWNER", fmt.Sprintf("the table and its indexes change owner to %s", sub.Value))
	if !s.Relation().Exists() {
		return e
	}
	for _, index := range s.Catalog.IndexesOf(s.Relation().Relation.OID) {
		rel, ok := s.Catalog.ByOID(index.IndexRelID)
		if !ok {
			continue
		}
		e.Touches = append(e.Touches, engine.Touch{
			Info: s.InfoOf(rel),
			Lock: pgcontract.LockAccessExclusive,
			Op:   pgcontract.OpKindMetadata,
			Role: pgcontract.TargetRoleImplicit,
		})
	}
	return e
}

// ruleReplicaIdentity is R-AT-REPLIDENT.
func ruleReplicaIdentity(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-REPLIDENT", fmt.Sprintf("the replica identity becomes %s", sub.Value))
	if sub.Value == "FULL" {
		e.Message += "; every later UPDATE and DELETE will log the whole old row, which is a replication cost and not a lock cost"
	}
	if sub.Name != "" {
		e.Message += fmt.Sprintf(", carried by index %q", sub.Name)
		e.Touches = append(e.Touches, s.IndexTarget(sub.Name, pgcontract.LockShare, pgcontract.OpKindMetadata))
	}
	return e
}

// ruleRenameRelation is R-AT-RENAME.
func ruleRenameRelation(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-RENAME", fmt.Sprintf("the relation is renamed to %q in the catalog", sub.NewName))
	return e
}

// ruleSetSchema is R-AT-SETSCHEMA.
func ruleSetSchema(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-SETSCHEMA", fmt.Sprintf("the relation moves to schema %q in the catalog", sub.Value))
	return e
}

// ruleInherit is R-AT-INHERIT.
func ruleInherit(s engine.Scope, sub ir.Subcommand) engine.Effect {
	verb, parentLock := "starts inheriting from", pgcontract.LockShareUpdateExclusive
	if sub.Kind == ir.SubDropInherit {
		verb, parentLock = "stops inheriting from", pgcontract.LockAccessShare
	}

	e := engine.NewEffect(s, "R-AT-INHERIT", "")
	parent := "an unnamed parent"
	if len(sub.Relations) > 0 {
		resolved := s.Resolve(sub.Relations[0])
		parent = resolved.Name.String()
		e.Touches = append(e.Touches, engine.Touch{
			Info: resolved,
			Lock: parentLock,
			Op:   pgcontract.OpKindMetadata,
			Role: pgcontract.TargetRoleDirect,
		})
	}
	e.Message = fmt.Sprintf("the table %s %s; only the catalog edge changes", verb, parent)
	return e
}

// ruleOf is R-AT-OF.
func ruleOf(s engine.Scope, sub ir.Subcommand) engine.Effect {
	if sub.Kind == ir.SubDropOf {
		return engine.NewEffect(s, "R-AT-OF", "the table stops being a typed table; its columns are unchanged")
	}
	return engine.NewEffect(s, "R-AT-OF", fmt.Sprintf("the table becomes a typed table over %s; its columns already match", sub.Value))
}
