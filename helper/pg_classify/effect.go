package pg_classify

import (
	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// relationTarget is one relation a rule acts on directly.
type relationTarget struct {
	info pg_catalog.RelationInfo
	// only suppresses the expansion below this relation, as the ONLY keyword does.
	only bool
	role pg_contract.TargetRole
}

// effect is what one subcommand does.
type effect struct {
	code    pg_contract.Code
	message string
	// lock, op, below, underOnly and tableLock are what the named relations get; newEffect seeds them from the clause table.
	lock      pg_contract.Lock
	op        pg_contract.OpKind
	below     below
	underOnly below
	tableLock pg_contract.Lock
	// leafOp, when set, decides the work on each leaf below a named relation.
	leafOp func(leaf pg_catalog.RelationInfo) pg_contract.OpKind
	// on lists the relations the rule acts on.
	on []relationTarget
	// touches are the relations the clause reaches beyond the named ones.
	touches []touch
	// errors are what the server would refuse.
	errors []pg_contract.Error
	// silent drops the clause from the findings: it errored and never runs.
	silent bool
	// row is the clause table's row the effect started from, which holds what the server refuses.
	row clause
}

// newEffect is where every rule starts: the clause table gives the static half, the rule then refines it.
func newEffect(s scope, code pg_contract.Code, message string) effect {
	row := clauseFor(code)
	reach, underOnly := row.reachAt(s.version)
	return effect{
		code:      code,
		message:   message,
		lock:      row.lock,
		op:        row.op,
		below:     reach,
		underOnly: underOnly,
		tableLock: row.tableLock,
		on:        []relationTarget{{info: s.relation(), only: s.only(), role: pg_contract.TargetRoleDirect}},
		row:       row,
	}
}

// refusals turns what the clause table says the server refuses into errors: the clause on a
// partitioned table on a version that does not support it, or ONLY where the server wants the partitions too.
func (e effect) refusals(s scope) effect {
	if e.silent {
		return e
	}
	for _, on := range e.on {
		if !partitionedInfo(on.info) {
			continue
		}
		if !e.row.accepts(s.version) {
			return e.reject(e.row.refusal, s.versionsWhen(e.row.partitioned))
		}
		if on.only && e.underOnly.partitions == refused && s.hasPartitions(on.info) {
			return e.reject(e.row.onlyRefusal, s.versionsWhen(e.row.onlyAcceptedFrom()))
		}
	}
	return e
}

// onNone says the statement locks nothing it names.
func (e effect) onNone() effect {
	e.on = nil
	return e
}

// onEvery set relations a statement lists as equals, each acting as its own named relations.
func (e effect) onEvery(s scope) effect {
	e.on = make([]relationTarget, 0, len(s.relations))
	for i, info := range s.relations {
		e.on = append(e.on, relationTarget{info: info, only: s.statement.Relations[i].Only, role: pg_contract.TargetRoleDirect})
	}
	return e
}

// reject says that the statement errored.
func (e effect) reject(reason string, versions pg_contract.VersionRange) effect {
	e.silent = true
	e.errors = append(e.errors, pg_contract.Error{Code: e.code, Message: reason, Versions: versions})
	return e
}

// classifyClauses run every rule subcommands through their associated rule to retrieve a list of findings and errors
func classifyClauses(s scope, rules map[pg_parse.SubKind]func(scope, pg_parse.Subcommand) effect) ([]pg_contract.Finding, []pg_contract.Error) {
	var findings []pg_contract.Finding
	var errors []pg_contract.Error

	for _, sub := range s.statement.Subcommands {
		// select the rule for this subcommands
		rule := rules[sub.Kind]
		if rule == nil {
			continue
		}
		// run the rule
		result := rule(s, sub)

		finding, clauseErrors := result.report(s)
		errors = append(errors, clauseErrors...)
		if finding != nil {
			findings = append(findings, *finding)
		}
	}
	return findings, errors
}

// one is a whole command that classifies to a single effect.
func one(rule func(scope) effect) func(scope) ([]pg_contract.Finding, []pg_contract.Error) {
	return func(s scope) ([]pg_contract.Finding, []pg_contract.Error) {
		finding, errors := rule(s).report(s)
		if finding == nil {
			return nil, errors
		}
		return []pg_contract.Finding{*finding}, errors
	}
}

// report turns an effect into the finding it produces, nil when it never runs.
func (e effect) report(s scope) (*pg_contract.Finding, []pg_contract.Error) {
	e = e.refusals(s)
	if e.silent || e.code == "" {
		return nil, e.errors
	}
	return &pg_contract.Finding{
		Code:     e.code,
		Severity: pg_contract.SeverityInfo,
		Message:  e.message,
		Targets:  dedupeTargets(e.targets(s)),
	}, e.errors
}

// targets turns the effect into what its finding carries: the named relations and the touches,
// then everything the server reaches below them.
func (e effect) targets(s scope) []pg_contract.Target {
	touches := make([]touch, 0, len(e.on)+len(e.touches))
	for _, on := range e.on {
		reach := e.below
		if on.only {
			reach = e.underOnly
		}
		touches = append(touches, touch{
			info: on.info, lock: e.lock, op: e.op, role: on.role,
			below: reach, tableLock: e.tableLock, leafOp: e.leafOp,
		})
	}
	touches = append(touches, e.touches...)

	return s.expand(touches)
}

// opOn downgrades work that is measured in rows to metadata on a partitioned parent.
func opOn(op pg_contract.OpKind, partitioned bool) pg_contract.OpKind {
	if partitioned && op.ScalesWithTableSize() {
		return pg_contract.OpKindMetadata
	}
	return op
}

// dedupeTargets collapse relation, carrying the strongest lock and the most severe operation.
func dedupeTargets(targets []pg_contract.Target) []pg_contract.Target {
	out := make([]pg_contract.Target, 0, len(targets))
	at := map[pg_contract.Relation]int{}
	for _, target := range targets {
		index, seen := at[target.Relation]
		if !seen {
			at[target.Relation] = len(out)
			out = append(out, target)
			continue
		}
		out[index].Lock = pg_contract.MaxLock(out[index].Lock, target.Lock)
		out[index].OpKind = pg_contract.MaxOpKind(out[index].OpKind, target.OpKind)
	}
	return out
}
