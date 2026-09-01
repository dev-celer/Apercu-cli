package pg_parse

import "apercu-cli/helper/pg_contract"

// FeatureName identifies one construct that does not exist on every supported server. §8 turns
// the set a statement uses into a bound on the versions the migration can run on, which is what
// V-05 reports when production sits below it.
type FeatureName string

const (
	// G-03 — syntax libpg_query 17 cannot parse at all, recognised by the shim.
	FeatureNotEnforced        FeatureName = "NOT ENFORCED"
	FeatureNotNullConstraint  FeatureName = "NOT NULL CONSTRAINT"
	FeatureConstraintInherit  FeatureName = "ALTER CONSTRAINT INHERIT"
	FeatureVacuumOnly         FeatureName = "VACUUM ONLY"
	FeatureAnalyzeOnly        FeatureName = "ANALYZE ONLY"
	FeatureWithoutOverlaps    FeatureName = "WITHOUT OVERLAPS"
	FeatureForeignKeyPeriod   FeatureName = "FOREIGN KEY PERIOD"
	FeatureDropConstraintOnly FeatureName = "DROP CONSTRAINT ONLY"
	// G-04 — a generated column with no STORED or VIRTUAL keyword. PG18 reads it as VIRTUAL,
	// everything older rejects it outright.
	FeatureVirtualGenerated FeatureName = "GENERATED VIRTUAL"
	FeatureBareGenerated    FeatureName = "GENERATED WITHOUT STORAGE KEYWORD"

	// G-01 and G-02 — syntax that parses on 17 but only runs on a newer server, so it is
	// recognised from the IR rather than from the tokens.
	FeatureSetExpression       FeatureName = "SET EXPRESSION AS"
	FeatureStatisticsDefault   FeatureName = "SET STATISTICS DEFAULT"
	FeatureAccessMethodDefault FeatureName = "SET ACCESS METHOD DEFAULT"
	FeatureStorageDefault      FeatureName = "SET STORAGE DEFAULT"
	FeatureMergeReturning      FeatureName = "MERGE RETURNING"
)

// shimFeatures are the constructs libpg_query 17 cannot parse at all. The shim rewrites each one
// out of the statement to get a parse, which means the residue no longer says what the statement
// said — so every entry here needs a restore path in attachFeatures putting its meaning back.
// TestShimKeepsWhatItBlanksOut fails when one is added without it.
var shimFeatures = []FeatureName{
	FeatureNotEnforced,
	FeatureNotNullConstraint,
	FeatureConstraintInherit,
	FeatureVirtualGenerated,
	FeatureBareGenerated,
	FeatureWithoutOverlaps,
	FeatureForeignKeyPeriod,
	FeatureVacuumOnly,
	FeatureAnalyzeOnly,
}

// featureSince is the oldest server that accepts each construct.
var featureSince = map[FeatureName]pg_contract.Version{
	FeatureNotEnforced:         pg_contract.Version18,
	FeatureNotNullConstraint:   pg_contract.Version18,
	FeatureConstraintInherit:   pg_contract.Version18,
	FeatureVacuumOnly:          pg_contract.Version18,
	FeatureAnalyzeOnly:         pg_contract.Version18,
	FeatureWithoutOverlaps:     pg_contract.Version18,
	FeatureForeignKeyPeriod:    pg_contract.Version18,
	FeatureVirtualGenerated:    pg_contract.Version18,
	FeatureBareGenerated:       pg_contract.Version18,
	FeatureSetExpression:       pg_contract.Version17,
	FeatureStatisticsDefault:   pg_contract.Version17,
	FeatureAccessMethodDefault: pg_contract.Version17,
	FeatureStorageDefault:      pg_contract.Version16,
	FeatureMergeReturning:      pg_contract.Version17,
}

// Feature is one version-bound construct a statement uses.
type Feature struct {
	Name FeatureName
	// Since is the oldest server that accepts it.
	Since pg_contract.Version
	// Ambiguous marks a construct whose meaning, not just its availability, depends on the
	// version. G-04's bare GENERATED is the only one: it is virtual on 18 and a syntax error
	// below, so it stays flagged even when production is known to be 18.
	Ambiguous bool
	// Detail is the construct's argument when it has one: the column a PG18 NOT NULL constraint
	// names, or which of ENFORCED and NOT ENFORCED was written.
	Detail string
	// clause and subclause are where the construct sat: how many commas outside parentheses
	// precede it, and how many precede it inside the statement's outermost parentheses. The
	// normaliser uses whichever level the statement's subcommand list came from to attach a
	// construct the shim rewrote away to the subcommand it belonged to. A negative clause means
	// the construct belongs to the statement as a whole.
	clause    int
	subclause int
	// deferralNamed records whether the clause the construct sat in also spelled a deferral out.
	// Only ALTER CONSTRAINT reads it, and only because PG18 gave that clause a second thing to
	// say; see attachFeatures.
	deferralNamed bool
}

func newFeature(name FeatureName, clause, subclause int, detail string) Feature {
	return Feature{
		Name:      name,
		Since:     featureSince[name],
		Ambiguous: name == FeatureBareGenerated,
		Detail:    detail,
		clause:    clause,
		subclause: subclause,
	}
}

// addFeature records a construct once. A statement that writes NOT ENFORCED on two constraints
// still bounds the version range once.
func addFeature(list []Feature, name FeatureName) []Feature {
	for _, existing := range list {
		if existing.Name == name {
			return list
		}
	}
	return append(list, newFeature(name, -1, -1, ""))
}
