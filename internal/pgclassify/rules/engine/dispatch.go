package engine

import (
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// One is a whole command that classifies to a single effect.
func One(rule func(Scope) Effect) func(Scope) ([]pgcontract.Finding, []pgcontract.Error) {
	return func(s Scope) ([]pgcontract.Finding, []pgcontract.Error) {
		finding, errors := rule(s).Report(s)
		if finding == nil {
			return nil, errors
		}
		return []pgcontract.Finding{*finding}, errors
	}
}

// ClassifyClauses run every rule subcommands through their associated rule to retrieve a list of findings and errors
func ClassifyClauses(s Scope, rules map[ir.SubKind]func(Scope, ir.Subcommand) Effect) ([]pgcontract.Finding, []pgcontract.Error) {
	var findings []pgcontract.Finding
	var errors []pgcontract.Error

	for _, sub := range s.Statement.Subcommands {
		// select the rule for this subcommands
		rule := rules[sub.Kind]
		if rule == nil {
			continue
		}
		// run the rule
		result := rule(s, sub)

		finding, clauseErrors := result.Report(s)
		errors = append(errors, clauseErrors...)
		if finding != nil {
			findings = append(findings, *finding)
		}
	}
	return findings, errors
}
