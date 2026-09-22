package pg_classify

import (
	"testing"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// locksOn is every lock a statement takes on one relation, one per finding.
func locksOn(analysis pg_contract.StatementAnalysis, relation string) []string {
	var locks []string
	for _, finding := range analysis.Findings {
		for _, target := range finding.Targets {
			if target.Relation.Name.String() == relation {
				locks = append(locks, target.Lock.Short())
			}
		}
	}
	return locks
}

// relationsOf is every relation a statement names, in the order the findings name them.
func relationsOf(analysis pg_contract.StatementAnalysis) []string {
	seen := map[string]bool{}
	var out []string
	for _, finding := range analysis.Findings {
		for _, target := range finding.Targets {
			name := target.Relation.Name.String()
			if !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
		}
	}
	return out
}

// TestOneLockForTheWholeStatement test that ALTER TABLE takes one lock per object before it starts and holds it to the end.
func TestOneLockForTheWholeStatement(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)

	t.Run("a weak clause is raised by a strong one", func(t *testing.T) {
		analysis := single(t, catalog, "ALTER TABLE orders ALTER COLUMN status SET STATISTICS 100, ADD COLUMN z int")
		assert.Equal(t, []string{"AEL", "AEL"}, locksOn(analysis, "apercu_snapshot_test.orders"))
	})

	t.Run("the order the clauses are written in does not matter", func(t *testing.T) {
		analysis := single(t, catalog, "ALTER TABLE orders ADD COLUMN z int, ALTER COLUMN status SET STATISTICS 100")
		assert.Equal(t, []string{"AEL", "AEL"}, locksOn(analysis, "apercu_snapshot_test.orders"))
	})

	t.Run("clauses that agree stay where they are", func(t *testing.T) {
		analysis := single(t, catalog, "ALTER TABLE orders ALTER COLUMN status SET STATISTICS 100, ALTER COLUMN status SET (n_distinct = 10)")
		assert.Equal(t, []string{"SUE", "SUE"}, locksOn(analysis, "apercu_snapshot_test.orders"))
	})

	t.Run("each clause keeps its own operation", func(t *testing.T) {
		analysis := single(t, catalog, "ALTER TABLE orders ADD COLUMN z int, ADD CONSTRAINT c CHECK (total >= 0)")
		assert.Equal(t, pg_contract.OpKindMetadata, targetOf(t, findingOf(t, analysis, "R-AT-ADDCOL"), "apercu_snapshot_test.orders").OpKind)
		assert.Equal(t, pg_contract.OpKindScan, targetOf(t, findingOf(t, analysis, "R-AT-ADDCHECK"), "apercu_snapshot_test.orders").OpKind)
	})

	t.Run("a foreign key does not raise the lock on the table it references", func(t *testing.T) {
		analysis := single(t, catalog, "ALTER TABLE orders ADD COLUMN z int, ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users (id)")
		assert.Equal(t, []string{"AEL", "AEL"}, locksOn(analysis, "apercu_snapshot_test.orders"))
		assert.Equal(t, []string{"SRE"}, locksOn(analysis, "apercu_snapshot_test.users"))
	})

	t.Run("the statement lists what each clause did", func(t *testing.T) {
		analysis := single(t, catalog, "ALTER TABLE orders ADD COLUMN z int, DROP COLUMN status")
		assert.Equal(t, []string{"ADD COLUMN", "DROP COLUMN"}, analysis.Subcommands)
	})
}

// TestRecursionReachesDescendants validate the inheritance lookup.
func TestRecursionReachesDescendants(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		sql       string
		code      pg_contract.Code
		relations []string
	}{
		{
			name:      "ADD COLUMN reaches the partitions",
			sql:       "ALTER TABLE events ADD COLUMN z int",
			code:      "R-AT-ADDCOL",
			relations: []string{"apercu_snapshot_test.events", "apercu_snapshot_test.events_2025", "apercu_snapshot_test.events_default"},
		},
		{
			name:      "and the classic inheritance children",
			sql:       "ALTER TABLE legacy_parent ADD COLUMN z int",
			code:      "R-AT-ADDCOL",
			relations: []string{"apercu_snapshot_test.legacy_parent", "apercu_snapshot_test.legacy_child"},
		},
		{
			name:      "ONLY stops a recursing clause at the named table",
			sql:       "ALTER TABLE ONLY events ALTER COLUMN id SET DEFAULT 1",
			code:      "R-AT-SETDEF",
			relations: []string{"apercu_snapshot_test.events"},
		},
		{
			name:      "a parent-only clause never reaches them",
			sql:       "ALTER TABLE events SET SCHEMA public",
			code:      "R-AT-SETSCHEMA",
			relations: []string{"apercu_snapshot_test.events"},
		},
		{
			name:      "and neither does one on a table with no children",
			sql:       "ALTER TABLE orders ADD COLUMN z int",
			code:      "R-AT-ADDCOL",
			relations: []string{"apercu_snapshot_test.orders"},
		},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			finding := findingOf(t, single(t, catalog, testCase.sql), testCase.code)
			assert.ElementsMatch(t, testCase.relations, relationsOf(pg_contract.StatementAnalysis{Findings: []pg_contract.Finding{finding}}))
			for _, target := range finding.Targets {
				if target.Relation.Name.String() == testCase.relations[0] {
					assert.Equal(t, pg_contract.TargetRoleDirect, target.Role)
					continue
				}
				assert.Equal(t, pg_contract.TargetRoleExpanded, target.Role)
			}
		})
	}
}

// TestPartitionedParentHoldsNoRows validate that partitioned parent operation are metadata only.
func TestPartitionedParentHoldsNoRows(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		code pg_contract.Code
	}{
		{name: "a check is proved leaf by leaf", sql: "ALTER TABLE events ADD CONSTRAINT c CHECK (id > 0)", code: "R-AT-ADDCHECK"},
		{name: "a primary key is built leaf by leaf", sql: "ALTER TABLE events ADD CONSTRAINT pk PRIMARY KEY (id, at)", code: "R-AT-ADDPK"},
	}

	catalog := testCatalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			finding := findingOf(t, single(t, catalog, testCase.sql), testCase.code)
			assert.Equal(t, pg_contract.OpKindMetadata, targetOf(t, finding, "apercu_snapshot_test.events").OpKind)
			assert.Equal(t, pg_contract.OpKindScan, targetOf(t, finding, "apercu_snapshot_test.events_2025").OpKind)
		})
	}

	t.Run("a parent-only clause that moves bytes moves none of the parent's", func(t *testing.T) {
		finding := findingOf(t, single(t, catalog, "ALTER TABLE events SET TABLESPACE fast"), "R-AT-TABLESPACE")
		require.Len(t, finding.Targets, 1)
		assert.Equal(t, pg_contract.OpKindMetadata, finding.Targets[0].OpKind)
	})
}

// TestRejectedClauses validate that the rejected clause are handled correctly.
func TestRejectedClauses(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		sql     string
		catalog func(*testing.T) *pg_catalog.Catalog
		message string
		valid   string
	}{
		{
			name:    "storage parameters on a partitioned table",
			sql:     "ALTER TABLE events SET (fillfactor = 70)",
			catalog: testCatalog,
			message: "storage parameters cannot be specified",
			valid:   "any",
		},
		{
			name:    "clustering a partitioned table",
			sql:     "ALTER TABLE events CLUSTER ON events_pkey",
			catalog: testCatalog,
			message: "cannot be marked for clustering",
			valid:   "any",
		},
		{
			name:    "adding a constraint to only the parent",
			sql:     "ALTER TABLE ONLY events ADD CONSTRAINT c CHECK (id > 0)",
			catalog: testCatalog,
			message: "added to the child tables too",
			valid:   "any",
		},
		{
			name:    "changing the persistence of a partitioned table on 18",
			sql:     "ALTER TABLE events SET UNLOGGED",
			catalog: func(t *testing.T) *pg_catalog.Catalog { return testCatalogOn(t, pg_contract.Version18) },
			message: "PostgreSQL 18 refuses",
			valid:   "any",
		},
		{
			name:    "and on an unknown version, which says where it would have worked",
			sql:     "ALTER TABLE events SET UNLOGGED",
			catalog: testCatalog,
			message: "PostgreSQL 18 refuses",
			valid:   "<=17",
		},
		{
			name:    "a NOT VALID foreign key on a partitioned table before 18",
			sql:     "ALTER TABLE events ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES orders (id) NOT VALID",
			catalog: func(t *testing.T) *pg_catalog.Catalog { return testCatalogOn(t, pg_contract.Version17) },
			message: "requires PostgreSQL 18",
			valid:   "any",
		},
		{
			name:    "dropping a constraint from only the parent before 18",
			sql:     "ALTER TABLE ONLY events DROP CONSTRAINT c",
			catalog: func(t *testing.T) *pg_catalog.Catalog { return testCatalogOn(t, pg_contract.Version15) },
			message: "only the partitioned table",
			valid:   "any",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			analysis := single(t, testCase.catalog(t), testCase.sql)
			assert.Empty(t, analysis.Findings, "a rejected clause never runs, so it locks nothing")
			require.Len(t, analysis.Errors, 1)
			assert.Contains(t, analysis.Errors[0].Message, testCase.message)
			assert.Equal(t, testCase.valid, analysis.Errors[0].Versions.String())
		})
	}
}

// TestRecursionReachesDescendants validate the inheritance lookup when the ONLY keyword is used.
func TestOnlyOnAParentWithChildren(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		sql      string
		version  pg_contract.Version
		refused  bool
		children []string
	}{
		{name: "a column cannot be added to only the parent", sql: "ALTER TABLE ONLY events ADD COLUMN z int", refused: true},
		{name: "nor to only a classic inheritance parent", sql: "ALTER TABLE ONLY legacy_parent ADD COLUMN z int", refused: true},
		{name: "a column's type cannot be changed on only the parent", sql: "ALTER TABLE ONLY events ALTER COLUMN id TYPE int", refused: true},
		{name: "a column cannot be renamed on only the parent", sql: "ALTER TABLE ONLY events RENAME COLUMN id TO ident", refused: true},
		{name: "a constraint cannot be added to only the parent", sql: "ALTER TABLE ONLY events ADD CONSTRAINT c CHECK (id > 0)", refused: true},
		{name: "a column cannot be dropped from only a partitioned parent", sql: "ALTER TABLE ONLY events DROP COLUMN id", refused: true},
		{
			name:     "but it can be from a classic inheritance parent, which still locks the children",
			sql:      "ALTER TABLE ONLY legacy_parent DROP COLUMN id",
			children: []string{"apercu_snapshot_test.legacy_child"},
		},
		{
			name:    "a constraint cannot be dropped from only a partitioned parent before 18",
			sql:     "ALTER TABLE ONLY events DROP CONSTRAINT c",
			version: pg_contract.Version17,
			refused: true,
		},
		{
			name:     "and from 18 it can, still locking the partitions",
			sql:      "ALTER TABLE ONLY events DROP CONSTRAINT c",
			version:  pg_contract.Version18,
			children: []string{"apercu_snapshot_test.events_2025", "apercu_snapshot_test.events_default"},
		},
		{
			name:     "dropping NOT NULL from only the parent locks the children from 18",
			sql:      "ALTER TABLE ONLY events ALTER COLUMN id DROP NOT NULL",
			version:  pg_contract.Version18,
			children: []string{"apercu_snapshot_test.events_2025", "apercu_snapshot_test.events_default"},
		},
		{
			name:    "and is refused outright before it",
			sql:     "ALTER TABLE ONLY events ALTER COLUMN id DROP NOT NULL",
			version: pg_contract.Version17,
			refused: true,
		},
		{name: "setting NOT NULL on only the parent is allowed and stops there", sql: "ALTER TABLE ONLY events ALTER COLUMN id SET NOT NULL"},
		{name: "so is setting a default", sql: "ALTER TABLE ONLY events ALTER COLUMN id SET DEFAULT 1"},
		{name: "so is setting the storage", sql: "ALTER TABLE ONLY events ALTER COLUMN id SET STORAGE PLAIN"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			catalog := testCatalog(t)
			if testCase.version != pg_contract.VersionUnknown {
				catalog = testCatalogOn(t, testCase.version)
			}
			analysis := single(t, catalog, testCase.sql)

			if testCase.refused {
				assert.Empty(t, analysis.Findings)
				require.Len(t, analysis.Errors, 1)
				return
			}
			require.Empty(t, analysis.Errors)
			require.Len(t, analysis.Findings, 1)

			locked := relationsOf(analysis)[1:]
			assert.ElementsMatch(t, testCase.children, locked, "what ONLY still reaches")
		})
	}
}

func TestRejectedClausesDoNotStopTheOthers(t *testing.T) {
	t.Parallel()

	analysis := single(t, testCatalog(t), "ALTER TABLE events SET (fillfactor = 70), ADD COLUMN z int")
	assert.Len(t, analysis.Errors, 1)
	assert.Equal(t, []pg_contract.Code{"R-AT-ADDCOL"}, codesOf(analysis))
}

// TestUnknownRelationFailsSafe
func TestUnknownRelationFailsSafe(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)

	t.Run("the lock does not depend on the contents", func(t *testing.T) {
		finding := findingOf(t, single(t, catalog, "ALTER TABLE no_such_table ADD COLUMN z int"), "R-AT-ADDCOL")
		assert.Equal(t, pg_contract.LockAccessExclusive, targetOf(t, finding, "apercu_snapshot_test.no_such_table").Lock)
	})

	t.Run("a scan that cannot be ruled out happens", func(t *testing.T) {
		finding := findingOf(t, single(t, catalog, "ALTER TABLE no_such_table ALTER COLUMN c SET NOT NULL"), "R-AT-SETNOTNULL")
		assert.Equal(t, pg_contract.OpKindScan, targetOf(t, finding, "apercu_snapshot_test.no_such_table").OpKind)
	})

	t.Run("a type change that cannot be proved free rewrites", func(t *testing.T) {
		finding := findingOf(t, single(t, catalog, "ALTER TABLE no_such_table ALTER COLUMN c TYPE int"), "R-AT-TYPE")
		assert.Equal(t, pg_contract.OpKindRewrite, targetOf(t, finding, "apercu_snapshot_test.no_such_table").OpKind)
	})
}

// TestClassificationFollowsTheSessionContext validate that classification work on the shadow catalog.
