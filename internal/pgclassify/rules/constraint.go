package rules

import (
	"fmt"

	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse/ir"
)

// ruleAddConstraint handle every ADD CONSTRAINT.
func ruleAddConstraint(s engine.Scope, sub ir.Subcommand) engine.Effect {
	constraint := sub.Constraint
	if constraint == nil {
		e := engine.NewEffect(s, "R-AT-ADDCON", "the constraint definition did not reach the IR, so a full scan is assumed")
		return e
	}

	var e engine.Effect
	switch constraint.Type {
	case ir.ConstraintCheck:
		e = addCheck(s, *constraint)
	case ir.ConstraintNotNull:
		e = addNotNull(s, sub, *constraint)
	case ir.ConstraintPrimaryKey, ir.ConstraintUnique, ir.ConstraintExclusion:
		e = addKey(s, *constraint)
	case ir.ConstraintForeignKey:
		e = addForeignKey(s, *constraint)
	default:
		e = engine.NewEffect(s, "R-AT-ADDCON", "an unmodelled constraint; a full scan is assumed")
	}

	return e
}

// addCheck is R-AT-ADDCHECK.
func addCheck(s engine.Scope, constraint ir.ConstraintDef) engine.Effect {
	switch {
	case constraint.NotEnforced:
		return engine.NewEffect(s, "R-AT-ADDCHECK-NE", "NOT ENFORCED is a permanent opt-out: the constraint is recorded and never checked, now or later")
	case constraint.NotValid:
		return engine.NewEffect(s, "R-AT-ADDCHECK-NV", "NOT VALID records the constraint and enforces it for new rows; the existing ones are read by a later VALIDATE CONSTRAINT")
	}
	e := engine.NewEffect(s, "R-AT-ADDCHECK", "every existing row is read to prove the predicate holds")
	return e
}

// addNotNull is the PG18 spelling of a NOT NULL.
func addNotNull(s engine.Scope, sub ir.Subcommand, constraint ir.ConstraintDef) engine.Effect {
	if constraint.NotValid {
		return engine.NewEffect(s, "R-AT-ADDNN-NV", "NOT VALID records the constraint and enforces it for new rows; the existing ones are read by a later VALIDATE CONSTRAINT")
	}
	column := sub.Name
	if len(constraint.Columns) > 0 {
		column = constraint.Columns[0]
	}
	return ruleSetNotNull(s, ir.Subcommand{Name: column})
}

// addKey is R-AT-ADDPK, R-AT-ADDUNIQUE and R-AT-ADDUSINGIDX.
func addKey(s engine.Scope, constraint ir.ConstraintDef) engine.Effect {
	if constraint.UsingIndex != "" {
		e := engine.NewEffect(s, "R-AT-ADDUSINGIDX", fmt.Sprintf("the constraint adopts the existing index %q instead of building one, so no row is read", constraint.UsingIndex))
		e.Touches = append(e.Touches, s.IndexTarget(constraint.UsingIndex, pgcontract.LockShareUpdateExclusive, pgcontract.OpKindMetadata))
		return e
	}

	switch constraint.Type {
	case ir.ConstraintExclusion:
		return engine.NewEffect(s, "R-AT-ADDEXCL", "the index behind the exclusion constraint is built under ACCESS EXCLUSIVE, reading every row")
	case ir.ConstraintUnique:
		return engine.NewEffect(s, "R-AT-ADDUNIQUE", "the index behind the constraint is built under ACCESS EXCLUSIVE, reading every row; CREATE INDEX CONCURRENTLY followed by ADD CONSTRAINT … USING INDEX does the same work without the lock")
	}

	e := engine.NewEffect(s, "R-AT-ADDPK", "the index behind the primary key is built under ACCESS EXCLUSIVE, reading every row, and the key columns are made NOT NULL with it")
	// The clause table holds the partitions at SHARE on 15-17, which only holds while the key columns
	// are NOT NULL already: otherwise 15-17 set NOT NULL on every partition first, under ACCESS EXCLUSIVE.
	// An unknown production version takes the stronger reading.
	// A nullable key column is made NOT NULL first, which holds and reads every partition and
	// inheritance child at ACCESS EXCLUSIVE, on every version.
	if !s.NotNull(constraint.Columns) {
		e.Below = engine.Below{Partitions: engine.Worked, Children: engine.Worked}
		e.Message += "; a key column is nullable, so it is made NOT NULL first on every partition and child, each one read under ACCESS EXCLUSIVE"
		return e
	}
	if s.Partitioned() && !s.AtLeast18() {
		e.Message += "; on PostgreSQL 15-17 each partition is held at SHARE rather than ACCESS EXCLUSIVE"
	}
	return e
}

// addForeignKey is R-AT-ADDFK and R-AT-ADDFK-NV.
func addForeignKey(s engine.Scope, constraint ir.ConstraintDef) engine.Effect {
	var e engine.Effect
	if constraint.NotValid {
		e = engine.NewEffect(s, "R-AT-ADDFK-NV", "NOT VALID records the key and enforces it for new rows; no existing row is read")
	} else {
		e = engine.NewEffect(s, "R-AT-ADDFK", "every existing row is read and matched against the referenced table; writes are blocked on both tables, reads on neither")
	}

	// The key's action triggers go onto the referenced table, and the existing rows are proved
	// against it unless the key is NOT VALID.
	referenced := s.Resolve(constraint.References)
	e.Touches = append(e.Touches, s.AddTrigger(referenced))
	if !constraint.NotValid {
		e.Touches = append(e.Touches, s.ValidateForeignKey(referenced))
	}

	// a partitioned table only accepts a NOT VALID foreign key from 18 on.
	if constraint.NotValid && s.Partitioned() && !s.AtLeast18() {
		e = e.Reject("a NOT VALID foreign key on a partitioned table requires PostgreSQL 18", s.VersionsWhen(pgcontract.AtLeast(pgcontract.Version18)))
	}
	return e
}

// ruleValidateConstraint is R-AT-VALIDATE.
func ruleValidateConstraint(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-VALIDATE", "every existing row is read to prove the constraint holds, under a lock that blocks neither reads nor writes")

	constraint, ok := namedConstraint(s, sub.Name)
	if !ok {
		return e
	}
	if constraint.Validated {
		e.Op = pgcontract.OpKindMetadata
		e.Message = fmt.Sprintf("constraint %q is already validated, so the statement is a no-op", sub.Name)
	}
	if constraint.Type == "f" {
		if referenced, ok := s.RelationByOID(constraint.ForeignRelID); ok {
			e.Touches = append(e.Touches, s.ValidateForeignKey(referenced))
		}
	}
	return e
}

// ruleDropConstraint is R-AT-DROPCON.
func ruleDropConstraint(s engine.Scope, sub ir.Subcommand) engine.Effect {
	e := engine.NewEffect(s, "R-AT-DROPCON", fmt.Sprintf("constraint %q is removed from the catalog", sub.Name))

	if constraint, ok := namedConstraint(s, sub.Name); ok && constraint.Type == "f" {
		if referenced, ok := s.RelationByOID(constraint.ForeignRelID); ok {
			e.Touches = append(e.Touches, s.RemoveTrigger(referenced))
			e.Message += "; the referenced table is locked with it, because the key's trigger lives on that side too"
		}
	}

	return e
}

// ruleAlterConstraint is R-AT-ALTERCON.
func ruleAlterConstraint(s engine.Scope, sub ir.Subcommand) engine.Effect {
	constraint := sub.Constraint
	if constraint == nil {
		return engine.NewEffect(s, "R-AT-ALTERCON", "the constraint's attributes change; no row is read and the referenced table is not locked")
	}

	switch {
	case constraint.EnforcementSet:
		return alterEnforcement(s, sub, *constraint)
	case constraint.NoInheritSet:
		return engine.NewEffect(s, "R-AT-ALTERCON-INH", "the constraint's inheritance changes in the catalog; no row is read")
	default:
		return engine.NewEffect(s, "R-AT-ALTERCON", "the constraint's deferrability changes in the catalog; no row is read and the referenced table is not locked")
	}
}

// alterEnforcement is R-AT-ALTERCON-ENF.
func alterEnforcement(s engine.Scope, sub ir.Subcommand, constraint ir.ConstraintDef) engine.Effect {
	e := engine.NewEffect(s, "R-AT-ALTERCON-ENF", "")
	if constraint.NotEnforced {
		e.Message = fmt.Sprintf("constraint %q stops being checked; no existing row is read", sub.Name)
	} else {
		e.Op = pgcontract.OpKindScan
		e.Message = fmt.Sprintf("constraint %q starts being checked, so every existing row is read against it", sub.Name)
	}

	// Enforcing a foreign key puts its triggers back on the referenced table, not enforcing it takes them off.
	if existing, ok := namedConstraint(s, sub.Name); ok && existing.Type == "f" {
		if referenced, ok := s.RelationByOID(existing.ForeignRelID); ok {
			if constraint.NotEnforced {
				e.Touches = append(e.Touches, s.RemoveTrigger(referenced))
			} else {
				e.Touches = append(e.Touches, s.AddTrigger(referenced))
			}
		}
	}
	return e
}

// ruleRenameConstraint is R-AT-RENAMECON.
func ruleRenameConstraint(s engine.Scope, sub ir.Subcommand) engine.Effect {
	return engine.NewEffect(s, "R-AT-RENAMECON", fmt.Sprintf("constraint %q is renamed to %q in the catalog", sub.Name, sub.NewName))
}

// namedConstraint resolves one named constraint of the table.
func namedConstraint(s engine.Scope, name string) (snapshot.Constraint, bool) {
	if !s.Relation().Exists() || name == "" {
		return snapshot.Constraint{}, false
	}
	return s.Catalog.ConstraintByName(s.Relation().Relation.OID, name)
}
