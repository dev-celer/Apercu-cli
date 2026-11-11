// Package engine is the machinery every rule is written with: the clause table, the effect a rule
// builds, the touches it takes, and their expansion into targets across partitions and children.
package engine

import (
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify/stmt"
	"apercu-cli/internal/pgcontract"
)

// RelationTarget is one relation a rule acts on directly.
type RelationTarget struct {
	Info pgcatalog.RelationInfo
	// only suppresses the expansion below this relation, as the ONLY keyword does.
	only bool
	Role pgcontract.TargetRole
}

// Effect is what one subcommand does.
type Effect struct {
	Code    pgcontract.Code
	Message string
	// lock, op, below, underOnly and tableLock are what the named relations get; newEffect seeds them from the clause table.
	Lock      pgcontract.Lock
	Op        pgcontract.OpKind
	Below     Below
	underOnly Below
	TableLock pgcontract.Lock
	// leafOp, when set, decides the work on each leaf below a named relation.
	LeafOp func(leaf pgcatalog.RelationInfo) pgcontract.OpKind
	// on lists the relations the rule acts on.
	On []RelationTarget
	// touches are the relations the clause reaches beyond the named ones.
	Touches []Touch
	// errors are what the server would refuse.
	errors []pgcontract.Error
	// silent drops the clause from the findings: it errored and never runs.
	silent bool
	// row is the clause table's row the effect started from, which holds what the server refuses.
	row clause
}

// NewEffect is where every rule starts: the clause table gives the static half, the rule then refines it.
func NewEffect(s Scope, code pgcontract.Code, message string) Effect {
	row := clauseFor(code)
	reach, underOnly := row.reachAt(s.Version)
	return Effect{
		Code:      code,
		Message:   message,
		Lock:      row.lock,
		Op:        row.op,
		Below:     reach,
		underOnly: underOnly,
		TableLock: row.tableLock,
		On:        []RelationTarget{{Info: s.Relation(), only: s.Only(), Role: pgcontract.TargetRoleDirect}},
		row:       row,
	}
}

// refusals turns what the clause table says the server refuses into errors: the clause on a
// partitioned table on a version that does not support it, or ONLY where the server wants the partitions too.
func (e Effect) refusals(s Scope) Effect {
	if e.silent {
		return e
	}
	for _, on := range e.On {
		if !stmt.PartitionedInfo(on.Info) {
			continue
		}
		if !e.row.accepts(s.Version) {
			return e.Reject(e.row.refusal, s.VersionsWhen(e.row.partitioned))
		}
		if on.only && e.underOnly.Partitions == Refused && s.hasPartitions(on.Info) {
			return e.Reject(e.row.onlyRefusal, s.VersionsWhen(e.row.onlyAcceptedFrom()))
		}
	}
	return e
}

// OnNone says the statement locks nothing it names.
func (e Effect) OnNone() Effect {
	e.On = nil
	return e
}

// OnEvery set relations a statement lists as equals, each acting as its own named relations.
func (e Effect) OnEvery(s Scope) Effect {
	e.On = make([]RelationTarget, 0, len(s.Relations))
	for i, info := range s.Relations {
		e.On = append(e.On, RelationTarget{Info: info, only: s.Statement.Relations[i].Only, Role: pgcontract.TargetRoleDirect})
	}
	return e
}

// Reject says that the statement errored.
func (e Effect) Reject(reason string, versions pgcontract.VersionRange) Effect {
	e.silent = true
	e.errors = append(e.errors, pgcontract.Error{Code: e.Code, Message: reason, Versions: versions})
	return e
}

// Report turns an effect into the finding it produces, nil when it never runs.
func (e Effect) Report(s Scope) (*pgcontract.Finding, []pgcontract.Error) {
	e = e.refusals(s)
	if e.silent || e.Code == "" {
		return nil, e.errors
	}
	return &pgcontract.Finding{
		Code:     e.Code,
		Severity: pgcontract.SeverityInfo,
		Message:  e.Message,
		Targets:  DedupeTargets(e.targets(s)),
	}, e.errors
}

// targets turns the effect into what its finding carries: the named relations and the touches,
// then everything the server reaches below them.
func (e Effect) targets(s Scope) []pgcontract.Target {
	touches := make([]Touch, 0, len(e.On)+len(e.Touches))
	for _, on := range e.On {
		reach := e.Below
		if on.only {
			reach = e.underOnly
		}
		touches = append(touches, Touch{
			Info: on.Info, Lock: e.Lock, Op: e.Op, Role: on.Role,
			Below: reach, TableLock: e.TableLock, LeafOp: e.LeafOp,
		})
	}
	touches = append(touches, e.Touches...)

	return s.Expand(touches)
}

// OpOn downgrades work that is measured in rows to metadata on a partitioned parent.
func OpOn(op pgcontract.OpKind, partitioned bool) pgcontract.OpKind {
	if partitioned && op.ScalesWithTableSize() {
		return pgcontract.OpKindMetadata
	}
	return op
}

// DedupeTargets collapse relation, carrying the strongest lock and the most severe operation.
func DedupeTargets(targets []pgcontract.Target) []pgcontract.Target {
	out := make([]pgcontract.Target, 0, len(targets))
	at := map[pgcontract.Relation]int{}
	for _, target := range targets {
		index, seen := at[target.Relation]
		if !seen {
			at[target.Relation] = len(out)
			out = append(out, target)
			continue
		}
		out[index].Lock = pgcontract.MaxLock(out[index].Lock, target.Lock)
		out[index].OpKind = pgcontract.MaxOpKind(out[index].OpKind, target.OpKind)
	}
	return out
}
