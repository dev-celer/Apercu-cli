package ir

import (
	"apercu-cli/internal/pgcontract"
)

// FeatureName identifies one construct that does not exist on every supported server. §8 turns
// the set a statement uses into a bound on the versions the migration can run on, which is what
// V-02 reports when production sits below it.
type FeatureName string

const (
	// G-03 — syntax libpg_query 17 cannot parse at all, recognised by the shim.
	FeatureNotEnforced       FeatureName = "NOT ENFORCED"
	FeatureNotNullConstraint FeatureName = "NOT NULL CONSTRAINT"
	FeatureConstraintInherit FeatureName = "ALTER CONSTRAINT INHERIT"
	FeatureVacuumOnly        FeatureName = "VACUUM ONLY"
	FeatureAnalyzeOnly       FeatureName = "ANALYZE ONLY"
	FeatureWithoutOverlaps   FeatureName = "WITHOUT OVERLAPS"
	FeatureForeignKeyPeriod  FeatureName = "FOREIGN KEY PERIOD"
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

// featureSince is the oldest server that accepts each construct.
var featureSince = map[FeatureName]pgcontract.Version{
	FeatureNotEnforced:         pgcontract.Version18,
	FeatureNotNullConstraint:   pgcontract.Version18,
	FeatureConstraintInherit:   pgcontract.Version18,
	FeatureVacuumOnly:          pgcontract.Version18,
	FeatureAnalyzeOnly:         pgcontract.Version18,
	FeatureWithoutOverlaps:     pgcontract.Version18,
	FeatureForeignKeyPeriod:    pgcontract.Version18,
	FeatureVirtualGenerated:    pgcontract.Version18,
	FeatureBareGenerated:       pgcontract.Version18,
	FeatureSetExpression:       pgcontract.Version17,
	FeatureStatisticsDefault:   pgcontract.Version17,
	FeatureAccessMethodDefault: pgcontract.Version17,
	FeatureStorageDefault:      pgcontract.Version16,
	FeatureMergeReturning:      pgcontract.Version17,
}

// Feature is one version-bound construct a statement uses.
type Feature struct {
	Name FeatureName
	// Since is the oldest server that accepts it.
	Since pgcontract.Version
	// Detail is the construct's argument when it has one: the column a PG18 NOT NULL constraint
	// names, or which of ENFORCED and NOT ENFORCED was written.
	Detail string
}

func NewFeature(name FeatureName, detail string) Feature {
	return Feature{
		Name:   name,
		Since:  featureSince[name],
		Detail: detail,
	}
}
