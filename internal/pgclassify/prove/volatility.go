package prove

import (
	"strings"

	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgclassify/shadow"
	"apercu-cli/internal/pgparse/ir"
)

// VolatilityOf grades an expression by the least predictable thing it can invoke.
//
//  1. (schema, name, argument count) against the catalog
//  2. the dependencies the post-migration snapshot recorded for the column, user functions only
//  3. fail safe to the most volatile candidate
func VolatilityOf(s shadow.Scope, expr *ir.Expr, column string) pgcatalog.Volatility {
	walk := walkVolatility(s, expr)
	if len(walk.ambiguous) == 0 {
		return walk.settled
	}
	if resolved, ok := resolveFromPostSnapshot(s, column, walk.ambiguous); ok {
		return walk.settled.Worst(resolved)
	}
	return walk.failSafe
}

// walk is what one pass over an expression found.
type walk struct {
	// settled is the worst volatility of the calls the name alone pinned down.
	settled pgcatalog.Volatility
	// failSafe is settled joined with the worst reading of the calls it did not.
	failSafe pgcatalog.Volatility
	// ambiguous are the names whose overloads did not agree on a volatility.
	ambiguous []call
}

// walkVolatility is the first step of volatilityOf.
func walkVolatility(s shadow.Scope, expr *ir.Expr) walk {
	found := walk{settled: pgcatalog.VolatilityImmutable, failSafe: pgcatalog.VolatilityImmutable}

	expr.Walk(func(node *ir.Expr) bool {
		switch node.Kind {
		case ir.ExprFuncCall, ir.ExprOperator, ir.ExprCast:
			invoked := callOf(node)
			volatility, undecided := candidateVolatility(s, invoked)
			found.failSafe = found.failSafe.Worst(volatility)
			if undecided {
				found.ambiguous = append(found.ambiguous, invoked)
				return true
			}
			found.settled = found.settled.Worst(volatility)
		case ir.ExprSubquery:
			// Subquery failsafe to volatile.
			found.settled = found.settled.Worst(pgcatalog.VolatilityVolatile)
			found.failSafe = found.failSafe.Worst(pgcatalog.VolatilityVolatile)
		}
		return true
	})
	return found
}

// call is one invocation as the statement wrote it.
type call struct {
	schema string
	name   string
	kind   ir.ExprKind
	args   int
}

func callOf(node *ir.Expr) call {
	parts := node.Name
	invoked := call{kind: node.Kind, args: len(node.Args)}
	if len(parts) == 0 {
		return invoked
	}
	invoked.name = parts[len(parts)-1]
	invoked.schema = strings.Join(parts[:len(parts)-1], ".")
	return invoked
}

// candidateVolatility is the catalog lookup. It reports the most volatile candidate and whether the name left more than one option.
func candidateVolatility(s shadow.Scope, invoked call) (pgcatalog.Volatility, bool) {
	if invoked.name == "" {
		return pgcatalog.VolatilityUnknown, false
	}
	switch invoked.kind {
	case ir.ExprCast:
		return castVolatility(s, invoked), false
	case ir.ExprOperator:
		operators := s.Catalog.OperatorsByName(invoked.schema, invoked.name, s.Context.SearchPath)
		if len(operators) == 0 {
			return pgcatalog.VolatilityUnknown, false
		}
		worst, agree := pgcatalog.VolatilityImmutable, true
		for _, operator := range operators {
			volatility := s.Catalog.OperatorVolatility(operator.OID)
			agree = agree && (len(operators) == 1 || volatility == s.Catalog.OperatorVolatility(operators[0].OID))
			worst = worst.Worst(volatility)
		}
		return worst, !agree
	default:
		procs := s.Catalog.ProcsByName(invoked.schema, invoked.name, invoked.args, s.Context.SearchPath)
		if len(procs) == 0 {
			return pgcatalog.VolatilityUnknown, false
		}
		worst, agree := pgcatalog.VolatilityImmutable, true
		for _, proc := range procs {
			volatility := s.Catalog.ProcVolatility(proc.OID)
			agree = agree && volatility == s.Catalog.ProcVolatility(procs[0].OID)
			worst = worst.Worst(volatility)
		}
		return worst, !agree
	}
}

// castVolatility return the volatility of a cast proc.
func castVolatility(s shadow.Scope, invoked call) pgcatalog.Volatility {
	target, ok := s.Catalog.TypeByName(invoked.schema, invoked.name, s.Context.SearchPath)
	if !ok {
		return pgcatalog.VolatilityUnknown
	}
	worst := s.Catalog.TypeInputVolatility(target.OID)
	for _, cast := range s.Catalog.CastsTo(target.OID) {
		worst = worst.Worst(s.Catalog.CastVolatility(cast.Source, cast.Target))
	}
	return worst
}

// walkVolatility is the second step of volatilityOf.
func resolveFromPostSnapshot(s shadow.Scope, column string, ambiguous []call) (pgcatalog.Volatility, bool) {
	if column == "" {
		return pgcatalog.VolatilityUnknown, false
	}
	def, ok := s.Catalog.PostDefault(s.Relation().Name, column)
	if !ok || (len(def.ReferencedProcs) == 0 && len(def.ReferencedOperators) == 0) {
		return pgcatalog.VolatilityUnknown, false
	}

	recorded := map[snapshot.OID]bool{}
	for _, oid := range def.ReferencedProcs {
		recorded[oid] = true
	}

	worst := pgcatalog.VolatilityImmutable
	settled := false
	for _, invoked := range ambiguous {
		for _, proc := range s.Catalog.ProcsByName(invoked.schema, invoked.name, invoked.args, s.Context.SearchPath) {
			if recorded[proc.OID] {
				worst = worst.Worst(s.Catalog.ProcVolatility(proc.OID))
				settled = true
			}
		}
	}
	for _, oid := range def.ReferencedOperators {
		worst = worst.Worst(s.Catalog.OperatorVolatility(oid))
		settled = true
	}
	return worst, settled
}
