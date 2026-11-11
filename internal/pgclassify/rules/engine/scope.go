package engine

import (
	"apercu-cli/internal/pgclassify/shadow"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// Scope is what a rule runs against: a checked view, plus the effect machinery's own methods.
type Scope struct {
	shadow.Scope
}

// IndexTarget resolves an index a clause names by bare name.
func (s Scope) IndexTarget(name string, lock pgcontract.Lock, op pgcontract.OpKind) Touch {
	ref := ir.RelationRef{Name: s.Relation().Name}
	ref.Name.Table = name
	return Touch{
		Info: s.Resolve(ref),
		Lock: lock,
		Op:   op,
		Role: pgcontract.TargetRoleResolved,
	}
}
