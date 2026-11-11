package rules

import (
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgcontract"
)

// ruleCreateView is R-OB-CREATEVIEW and R-OB-REPLACEVIEW.
func ruleCreateView(s engine.Scope) engine.Effect {
	if s.Statement.Flags.OrReplace && s.Relation().Exists() {
		e := engine.NewEffect(s, "R-OB-REPLACEVIEW", "the view definition is replaced, which blocks every query already reading through it until the statement commits")
		for _, info := range s.Sources() {
			e.Touches = append(e.Touches, engine.Touch{
				Info: info,
				Lock: pgcontract.LockAccessShare,
				Op:   pgcontract.OpKindNone,
				Role: pgcontract.TargetRoleDirect,
			})
		}
		return e
	}

	e := engine.NewEffect(s, "R-OB-CREATEVIEW", "the view is only a stored query; the relations it reads are opened to resolve their columns and nothing else")
	return onSources(e, s)
}

// ruleDropView is R-OB-DROPVIEW, which covers a materialized view the same way.
func ruleDropView(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-OB-DROPVIEW", "the view is removed from the catalog; every query still reading through it waits and then fails")
	e = e.OnEvery(s)
	if s.Statement.Flags.Cascade {
		for _, info := range s.Relations {
			e.Touches = append(e.Touches, viewDependents(s, info)...)
		}
		e.Message += "; CASCADE takes every view built on top of it as well"
	}
	return e
}

// viewDependents is the views and materialized views a CASCADE takes with it.
func viewDependents(s engine.Scope, info pgcatalog.RelationInfo) []engine.Touch {
	if !info.Exists() {
		return nil
	}
	var targets []engine.Touch
	for _, dep := range s.Catalog.ViewDependents(info.Relation.OID) {
		rel, ok := s.Catalog.ByOID(dep.DependentRelID)
		if !ok {
			continue
		}
		targets = append(targets, engine.Touch{
			Info: s.InfoOf(rel),
			Lock: pgcontract.LockAccessExclusive,
			Op:   pgcontract.OpKindMetadata,
			Role: pgcontract.TargetRoleImplicit,
		})
	}
	return targets
}

// ruleCreateMatView is R-OB-CREATEMV.
func ruleCreateMatView(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-OB-CREATEMV", "the query is run once and its result is written to disk; the sources are only held at ACCESS SHARE, but the statement lasts as long as the query does")
	if !s.Statement.Flags.WithData {
		e.Op = pgcontract.OpKindNone
		e.Message = "WITH NO DATA records the query and reads nothing; the matview is unscannable until a REFRESH fills it"
	}
	return onSources(e, s)
}

// ruleRefreshMatView is R-OB-REFRESHMV and R-OB-REFRESHMV-CONC.
func ruleRefreshMatView(s engine.Scope) engine.Effect {
	var e engine.Effect
	if s.Statement.Flags.Concurrently {
		e = engine.NewEffect(s, "R-OB-REFRESHMV-CONC",
			"the new contents are computed into a temporary table and merged row by row, so reads keep working while writes wait; it needs a non-partial, non-expression UNIQUE index and an already-populated matview, and it is slower than the plain form")
	} else {
		e = engine.NewEffect(s, "R-OB-REFRESHMV", "the query is re-run and the matview's file is replaced, so nothing can read it until the statement commits")
	}
	e.Touches = append(e.Touches, matViewSources(s, s.Relation())...)

	if s.Statement.Flags.Concurrently && !s.Statement.Flags.WithData {
		return e.Reject("REFRESH MATERIALIZED VIEW CONCURRENTLY … WITH NO DATA is not a valid combination", pgcontract.AnyVersion)
	}
	return e
}

// matViewSources is the relations the stored query reads when it runs again.
func matViewSources(s engine.Scope, info pgcatalog.RelationInfo) []engine.Touch {
	if !info.Exists() {
		return nil
	}
	var targets []engine.Touch
	for _, dep := range s.Catalog.ViewSources(info.Relation.OID) {
		rel, ok := s.Catalog.ByOID(dep.ReferencedRelID)
		if !ok {
			continue
		}
		targets = append(targets, engine.Touch{
			Info: s.InfoOf(rel),
			Lock: pgcontract.LockAccessShare,
			Op:   pgcontract.OpKindNone,
			Role: pgcontract.TargetRoleResolved,
		})
	}
	return targets
}
