package pg_classify

import (
	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

type recursion uint8

const (
	// recurses reaches the partitions and the inheritance children.
	recurses recursion = iota
	// partitionsOnly reaches the partitions and leaves the inheritance children alone.
	partitionsOnly
	// parentOnly stops at the parent table.
	parentOnly
	// rejected is a clause the server refuses on a partitioned parent.
	rejected
)

// expands reports whether the class reaches anything below the table.
func (r recursion) expands() bool { return r == recurses || r == partitionsOnly }

// relationTarget is one relation a rule acts on directly.
type relationTarget struct {
	info pg_catalog.RelationInfo
	// only suppresses the expansion below this relation, as the ONLY keyword does.
	only bool
	role pg_contract.TargetRole
}

// effect is what one subcommand does.
type effect struct {
	code      pg_contract.Code
	message   string
	lock      pg_contract.Lock
	op        pg_contract.OpKind
	recursion recursion
	// on lists the relations the rule acts on.
	on []relationTarget
	// partitionLock is the lock the partitions take instead of the table's.
	partitionLock pg_contract.Lock
	// partitionLockSet indicate if the partitionLock override value should be used
	partitionLockSet bool
	// childrenUnderOnly marks a clause that stops at the named table under ONLY but still opens the children.
	// They are locked exactly as hard as they would have been without the ONLY.
	childrenUnderOnly bool
	// extra are the relations the clause names or implies beyond the primary targets.
	extra []pg_contract.Target
	// errors are what the server would refuse.
	errors []pg_contract.Error
	// silent drops the clause from the findings: it errored and never runs.
	silent bool
}

// newEffect is where every rule starts, the rule analysis will then refine attribute of the effect.
func newEffect(s scope, code pg_contract.Code, message string) effect {
	return effect{
		code:      code,
		message:   message,
		lock:      pg_contract.LockAccessExclusive,
		op:        pg_contract.OpKindMetadata,
		recursion: recurses,
		on:        []relationTarget{{info: s.relation(), only: s.only(), role: pg_contract.TargetRoleDirect}},
	}
}

// onPartitions override the lock taken on the main table for its partitions.
func (e effect) onPartitions(lock pg_contract.Lock) effect {
	e.partitionLock, e.partitionLockSet = lock, true
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
	e.recursion = rejected
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

		if reason, versions, refused := s.onlyRefusal(sub); refused {
			result = result.reject(reason, versions)
		}
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

// targets convert the effect's on values to targets object.
func (e effect) targets(s scope) []pg_contract.Target {
	var targets []pg_contract.Target
	for _, on := range e.on {
		targets = append(targets, pg_contract.Target{
			Relation: contract(on.info),
			Lock:     e.lock,
			OpKind:   opOn(e.op, partitionedInfo(on.info)),
			Role:     on.role,
		})
	}
	targets = append(targets, e.extra...)

	if !e.recursion.expands() {
		return targets
	}
	for _, on := range e.on {
		if on.only && !e.childrenUnderOnly {
			continue
		}
		for _, child := range s.descendantsOf(on.info) {
			if e.recursion == partitionsOnly && !child.partition {
				continue
			}
			lock := e.lock
			if e.partitionLockSet && child.partition {
				lock = e.partitionLock
			}
			targets = append(targets, pg_contract.Target{
				Relation: child.relation,
				Lock:     lock,
				OpKind:   opOn(e.op, child.relation.Kind.IsPartitioned()),
				Role:     pg_contract.TargetRoleExpanded,
			})
		}
	}
	return targets
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
