package rules

import (
	"fmt"

	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// ruleCreateTrigger is R-OB-CREATETRIG.
func ruleCreateTrigger(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-OB-CREATETRIG", "the trigger is recorded on the table; writes wait for it, reads do not")
	if !s.Statement.Flags.Row {
		e.Below = engine.Nowhere
	}
	return e.OnEvery(s)
}

// ruleDropTrigger is R-OB-DROPTRIG.
func ruleDropTrigger(s engine.Scope) engine.Effect {
	name := ""
	if len(s.Statement.Subcommands) > 0 {
		name = s.Statement.Subcommands[0].Name
	}
	e := engine.NewEffect(s, "R-OB-DROPTRIG", fmt.Sprintf("trigger %q is removed, under a stronger lock than the one that created it", name))
	if !s.RowTrigger(name) {
		e.Below = engine.Nowhere
	}
	return e
}

// ruleAlterTrigger is R-OB-ALTERTRIG.
func ruleAlterTrigger(s engine.Scope) engine.Effect {
	sub := s.Statement.Subcommands
	rename := ""
	if len(sub) > 0 {
		rename = fmt.Sprintf("trigger %q is renamed to %q", sub[0].Name, sub[0].NewName)
	}
	// The server locks every partition before it even looks the trigger up, so a statement trigger reaches them too.
	return engine.NewEffect(s, "R-OB-ALTERTRIG", rename+"; every partition is locked with it, and a row trigger's clone on each one is renamed too")
}

// ruleTrigger is R-AT-TRIG and R-AT-TRIG-REP.
func ruleTrigger(s engine.Scope, sub ir.Subcommand) engine.Effect {
	code := pgcontract.Code("R-AT-TRIG")
	if sub.Value == "REPLICA" || sub.Value == "ALWAYS" {
		code = "R-AT-TRIG-REP"
	}
	e := engine.NewEffect(s, code, "the trigger's firing mode changes in the catalog; writes on the table wait, reads do not")
	if !s.RowTrigger(sub.Name) {
		e.Below = engine.Nowhere
	}
	return e
}

// ruleObjectRule is R-OB-RULE.
func ruleObjectRule(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-OB-RULE", "the rewrite rule changes in the catalog, which invalidates every plan built against the table")
	return e
}

// ruleRule is R-AT-RULE.
func ruleRule(s engine.Scope, sub ir.Subcommand) engine.Effect {
	return engine.NewEffect(s, "R-AT-RULE", fmt.Sprintf("rule %q changes its firing mode in the catalog", sub.Name))
}

// rulePolicy is R-OB-POLICY.
func rulePolicy(s engine.Scope) engine.Effect {
	e := engine.NewEffect(s, "R-OB-POLICY", "the row-level security policy changes in the catalog, which invalidates every plan built against the table")
	return e
}

// ruleRowSecurity is R-AT-RLS.
func ruleRowSecurity(s engine.Scope, _ ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-RLS", "row level security is switched on or off for the table; the policies themselves are untouched")
	return e
}

// ruleForceRowSecurity is R-AT-RLS-FORCE.
func ruleForceRowSecurity(s engine.Scope, _ ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-RLS-FORCE", "row level security starts or stops applying to the table's owner as well")
	return e
}
