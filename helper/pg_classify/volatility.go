package pg_classify

import (
	"strings"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_parse"
)

// volatilityOf grades an expression by the least predictable thing it can invoke.
//
//  1. (schema, name, argument count) against the catalog
//  2. the dependencies the post-migration snapshot recorded for the column, user functions only
//  3. fail safe to the most volatile candidate
func (s scope) volatilityOf(expr *pg_parse.Expr, column string) pg_catalog.Volatility {
	walk := s.walkVolatility(expr)
	if len(walk.ambiguous) == 0 {
		return walk.settled
	}
	if resolved, ok := s.resolveFromPostSnapshot(column, walk.ambiguous); ok {
		return walk.settled.Worst(resolved)
	}
	return walk.failSafe
}

// walk is what one pass over an expression found.
type walk struct {
	// settled is the worst volatility of the calls the name alone pinned down.
	settled pg_catalog.Volatility
	// failSafe is settled joined with the worst reading of the calls it did not.
	failSafe pg_catalog.Volatility
	// ambiguous are the names whose overloads did not agree on a volatility.
	ambiguous []call
}

// walkVolatility is the first step of volatilityOf.
func (s scope) walkVolatility(expr *pg_parse.Expr) walk {
	found := walk{settled: pg_catalog.VolatilityImmutable, failSafe: pg_catalog.VolatilityImmutable}

	expr.Walk(func(node *pg_parse.Expr) bool {
		switch node.Kind {
		case pg_parse.ExprFuncCall, pg_parse.ExprOperator, pg_parse.ExprCast:
			invoked := callOf(node)
			volatility, undecided := s.candidateVolatility(invoked)
			found.failSafe = found.failSafe.Worst(volatility)
			if undecided {
				found.ambiguous = append(found.ambiguous, invoked)
				return true
			}
			found.settled = found.settled.Worst(volatility)
		case pg_parse.ExprSubquery:
			// Subquery failsafe to volatile.
			found.settled = found.settled.Worst(pg_catalog.VolatilityVolatile)
			found.failSafe = found.failSafe.Worst(pg_catalog.VolatilityVolatile)
		}
		return true
	})
	return found
}

// call is one invocation as the statement wrote it.
type call struct {
	schema string
	name   string
	kind   pg_parse.ExprKind
	args   int
}

func callOf(node *pg_parse.Expr) call {
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
func (s scope) candidateVolatility(invoked call) (pg_catalog.Volatility, bool) {
	if invoked.name == "" {
		return pg_catalog.VolatilityUnknown, false
	}
	switch invoked.kind {
	case pg_parse.ExprCast:
		return s.castVolatility(invoked), false
	case pg_parse.ExprOperator:
		operators := s.catalog.OperatorsByName(invoked.schema, invoked.name, s.context.SearchPath)
		if len(operators) == 0 {
			return pg_catalog.VolatilityUnknown, false
		}
		worst, agree := pg_catalog.VolatilityImmutable, true
		for _, operator := range operators {
			volatility := s.catalog.OperatorVolatility(operator.OID)
			agree = agree && (len(operators) == 1 || volatility == s.catalog.OperatorVolatility(operators[0].OID))
			worst = worst.Worst(volatility)
		}
		return worst, !agree
	default:
		procs := s.catalog.ProcsByName(invoked.schema, invoked.name, invoked.args, s.context.SearchPath)
		if len(procs) == 0 {
			return pg_catalog.VolatilityUnknown, false
		}
		worst, agree := pg_catalog.VolatilityImmutable, true
		for _, proc := range procs {
			volatility := s.catalog.ProcVolatility(proc.OID)
			agree = agree && volatility == s.catalog.ProcVolatility(procs[0].OID)
			worst = worst.Worst(volatility)
		}
		return worst, !agree
	}
}

// castVolatility return the volatility of a cast proc.
func (s scope) castVolatility(invoked call) pg_catalog.Volatility {
	target, ok := s.catalog.TypeByName(invoked.schema, invoked.name, s.context.SearchPath)
	if !ok {
		return pg_catalog.VolatilityUnknown
	}
	worst := s.catalog.TypeInputVolatility(target.OID)
	for _, cast := range s.catalog.CastsTo(target.OID) {
		worst = worst.Worst(s.catalog.CastVolatility(cast.Source, cast.Target))
	}
	return worst
}

// walkVolatility is the second step of volatilityOf.
func (s scope) resolveFromPostSnapshot(column string, ambiguous []call) (pg_catalog.Volatility, bool) {
	if column == "" {
		return pg_catalog.VolatilityUnknown, false
	}
	def, ok := s.catalog.PostDefault(s.relation().Name, column)
	if !ok || (len(def.ReferencedProcs) == 0 && len(def.ReferencedOperators) == 0) {
		return pg_catalog.VolatilityUnknown, false
	}

	recorded := map[pg_catalog.OID]bool{}
	for _, oid := range def.ReferencedProcs {
		recorded[oid] = true
	}

	worst := pg_catalog.VolatilityImmutable
	settled := false
	for _, invoked := range ambiguous {
		for _, proc := range s.catalog.ProcsByName(invoked.schema, invoked.name, invoked.args, s.context.SearchPath) {
			if recorded[proc.OID] {
				worst = worst.Worst(s.catalog.ProcVolatility(proc.OID))
				settled = true
			}
		}
	}
	for _, oid := range def.ReferencedOperators {
		worst = worst.Worst(s.catalog.OperatorVolatility(oid))
		settled = true
	}
	return worst, settled
}
