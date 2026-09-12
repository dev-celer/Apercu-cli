package pg_classify

import (
	"fmt"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// ruleAddConstraint handle every ADD CONSTRAINT.
func ruleAddConstraint(s scope, sub pg_parse.Subcommand) effect {
	constraint := sub.Constraint
	if constraint == nil {
		e := newEffect("R-AT-ADDCON", "the constraint definition did not reach the IR, so a full scan is assumed")
		e.op = pg_contract.OpKindScan
		return e
	}

	var e effect
	switch constraint.Type {
	case pg_parse.ConstraintCheck:
		e = addCheck(*constraint)
	case pg_parse.ConstraintNotNull:
		e = addNotNull(s, sub, *constraint)
	case pg_parse.ConstraintPrimaryKey, pg_parse.ConstraintUnique, pg_parse.ConstraintExclusion:
		e = addKey(s, *constraint)
	case pg_parse.ConstraintForeignKey:
		e = addForeignKey(s, *constraint)
	default:
		e = newEffect("R-AT-ADDCON", "an unmodelled constraint; a full scan is assumed")
		e.op = pg_contract.OpKindScan
	}

	return e
}

// addCheck is R-AT-ADDCHECK.
func addCheck(constraint pg_parse.ConstraintDef) effect {
	switch {
	case constraint.NotEnforced:
		return newEffect("R-AT-ADDCHECK-NE", "NOT ENFORCED is a permanent opt-out: the constraint is recorded and never checked, now or later")
	case constraint.NotValid:
		return newEffect("R-AT-ADDCHECK-NV", "NOT VALID records the constraint and enforces it for new rows; the existing ones are read by a later VALIDATE CONSTRAINT")
	}
	e := newEffect("R-AT-ADDCHECK", "every existing row is read to prove the predicate holds")
	e.op = pg_contract.OpKindScan
	return e
}

// addNotNull is the PG18 spelling of a NOT NULL.
func addNotNull(s scope, sub pg_parse.Subcommand, constraint pg_parse.ConstraintDef) effect {
	if constraint.NotValid {
		return newEffect("R-AT-ADDNN-NV", "NOT VALID records the constraint and enforces it for new rows; the existing ones are read by a later VALIDATE CONSTRAINT")
	}
	column := sub.Name
	if len(constraint.Columns) > 0 {
		column = constraint.Columns[0]
	}
	return ruleSetNotNull(s, pg_parse.Subcommand{Name: column})
}

// addKey is R-AT-ADDPK, R-AT-ADDUNIQUE and R-AT-ADDUSINGIDX.
func addKey(s scope, constraint pg_parse.ConstraintDef) effect {
	if constraint.UsingIndex != "" {
		e := newEffect("R-AT-ADDUSINGIDX", fmt.Sprintf("the constraint adopts the existing index %q instead of building one, so no row is read", constraint.UsingIndex))
		e.extra = append(e.extra, s.indexTarget(constraint.UsingIndex, pg_contract.LockShareUpdateExclusive, pg_contract.OpKindMetadata))
		return e
	}

	if constraint.Type != pg_parse.ConstraintPrimaryKey {
		e := newEffect("R-AT-ADDUNIQUE", "the index behind the constraint is built under ACCESS EXCLUSIVE, reading every row; CREATE INDEX CONCURRENTLY followed by ADD CONSTRAINT … USING INDEX does the same work without the lock")
		e.op = pg_contract.OpKindScan
		return e
	}

	e := newEffect("R-AT-ADDPK", "the index behind the primary key is built under ACCESS EXCLUSIVE, reading every row, and the key columns are made NOT NULL with it")
	e.op = pg_contract.OpKindScan
	// 15-17 hold SHARE on each partition while the parent's index is built, 18 holds
	// ACCESS EXCLUSIVE. An unknown production version takes the stronger reading.
	if s.partitioned() && !s.atLeast18() {
		e.partitionLock = pg_contract.LockShare
		e.message += "; on PostgreSQL 15-17 each partition is held at SHARE rather than ACCESS EXCLUSIVE"
	}
	return e
}

// addForeignKey is R-AT-ADDFK and R-AT-ADDFK-NV.
func addForeignKey(s scope, constraint pg_parse.ConstraintDef) effect {
	var e effect
	if constraint.NotValid {
		e = newEffect("R-AT-ADDFK-NV", "NOT VALID records the key and enforces it for new rows; no existing row is read")
	} else {
		e = newEffect("R-AT-ADDFK", "every existing row is read and matched against the referenced table; writes are blocked on both tables, reads on neither")
		e.op = pg_contract.OpKindScan
	}
	e.lock = pg_contract.LockShareRowExclusive

	referenced := s.resolve(constraint.References)
	e.extra = append(e.extra, pg_contract.Target{
		Relation: contract(referenced),
		Lock:     pg_contract.LockShareRowExclusive,
		OpKind:   pg_contract.OpKindMetadata,
		Role:     pg_contract.TargetRoleImplicit,
	})

	// a partitioned table only accepts a NOT VALID foreign key from 18 on.
	if constraint.NotValid && s.partitioned() && !s.atLeast18() {
		e = e.reject("a NOT VALID foreign key on a partitioned table requires PostgreSQL 18", s.versionsWhen(pg_contract.AtLeast(pg_contract.Version18)))
	}
	return e
}

// indexTarget resolves an index a clause names by bare name.
func (s scope) indexTarget(name string, lock pg_contract.Lock, op pg_contract.OpKind) pg_contract.Target {
	ref := pg_parse.RelationRef{Name: s.relation().Name}
	ref.Name.Table = name
	return pg_contract.Target{
		Relation: contract(s.resolve(ref)),
		Lock:     lock,
		OpKind:   op,
		Role:     pg_contract.TargetRoleResolved,
	}
}

// ruleValidateConstraint is R-AT-VALIDATE.
func ruleValidateConstraint(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect("R-AT-VALIDATE", "every existing row is read to prove the constraint holds, under a lock that blocks neither reads nor writes")
	e.lock = pg_contract.LockShareUpdateExclusive
	e.op = pg_contract.OpKindScan

	constraint, ok := s.constraint(sub.Name)
	if !ok {
		return e
	}
	switch {
	case !constraint.Enforced:
		return e.reject(fmt.Sprintf("constraint %q is NOT ENFORCED and cannot be validated", sub.Name), pg_contract.AnyVersion)
	case constraint.Validated:
		e.op = pg_contract.OpKindMetadata
		e.message = fmt.Sprintf("constraint %q is already validated, so the statement is a no-op", sub.Name)
	}
	if constraint.Type == "f" {
		if referenced, ok := s.catalog.ByOID(constraint.ForeignRelID); ok {
			e.extra = append(e.extra, pg_contract.Target{
				Relation: referenced.Contract(),
				Lock:     pg_contract.LockRowShare,
				OpKind:   pg_contract.OpKindMetadata,
				Role:     pg_contract.TargetRoleImplicit,
			})
		}
	}
	return e
}

// ruleDropConstraint is R-AT-DROPCON.
func ruleDropConstraint(s scope, sub pg_parse.Subcommand) effect {
	e := newEffect("R-AT-DROPCON", fmt.Sprintf("constraint %q is removed from the catalog", sub.Name))
	// ONLY stops the constraint being dropped from the children,
	// not the children being locked: their inheritance count still has to come down.
	e.childrenUnderOnly = true

	if constraint, ok := s.constraint(sub.Name); ok && constraint.Type == "f" {
		if referenced, ok := s.catalog.ByOID(constraint.ForeignRelID); ok {
			e.extra = append(e.extra, pg_contract.Target{
				Relation: referenced.Contract(),
				Lock:     pg_contract.LockAccessExclusive,
				OpKind:   pg_contract.OpKindMetadata,
				Role:     pg_contract.TargetRoleImplicit,
			})
			e.message += "; the referenced table is locked with it, because the key's trigger lives on that side too"
		}
	}

	return e
}

// ruleAlterConstraint is R-AT-ALTERCON.
func ruleAlterConstraint(s scope, sub pg_parse.Subcommand) effect {
	constraint := sub.Constraint
	if constraint == nil {
		return newEffect("R-AT-ALTERCON", "the constraint's attributes change; no row is read and the referenced table is not locked")
	}

	switch {
	case constraint.EnforcementSet:
		return alterEnforcement(s, sub, *constraint)
	case constraint.NoInheritSet:
		return newEffect("R-AT-ALTERCON-INH", "the constraint's inheritance changes in the catalog; no row is read")
	default:
		return newEffect("R-AT-ALTERCON", "the constraint's deferrability changes in the catalog; no row is read and the referenced table is not locked")
	}
}

// alterEnforcement is R-AT-ALTERCON-ENF.
func alterEnforcement(s scope, sub pg_parse.Subcommand, constraint pg_parse.ConstraintDef) effect {
	e := newEffect("R-AT-ALTERCON-ENF", "")
	referencedLock := pg_contract.LockAccessExclusive
	if constraint.NotEnforced {
		e.message = fmt.Sprintf("constraint %q stops being checked; no existing row is read", sub.Name)
	} else {
		e.op = pg_contract.OpKindScan
		e.message = fmt.Sprintf("constraint %q starts being checked, so every existing row is read against it", sub.Name)
		referencedLock = pg_contract.LockShareRowExclusive
	}

	if existing, ok := s.constraint(sub.Name); ok && existing.Type == "f" {
		if referenced, ok := s.catalog.ByOID(existing.ForeignRelID); ok {
			e.extra = append(e.extra, pg_contract.Target{
				Relation: referenced.Contract(),
				Lock:     referencedLock,
				OpKind:   pg_contract.OpKindMetadata,
				Role:     pg_contract.TargetRoleImplicit,
			})
		}
	}
	return e
}

// ruleRenameConstraint is R-AT-RENAMECON.
func ruleRenameConstraint(_ scope, sub pg_parse.Subcommand) effect {
	return newEffect("R-AT-RENAMECON", fmt.Sprintf("constraint %q is renamed to %q in the catalog", sub.Name, sub.NewName))
}

// constraint resolves one named constraint of the table.
func (s scope) constraint(name string) (pg_catalog.Constraint, bool) {
	if !s.relation().Exists() || name == "" {
		return pg_catalog.Constraint{}, false
	}
	return s.catalog.ConstraintByName(s.relation().Relation.OID, name)
}
