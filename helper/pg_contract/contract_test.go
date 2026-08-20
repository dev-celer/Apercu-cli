package pg_contract

import (
	"apercu-cli/helper"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestLockOrdinals(t *testing.T) {
	t.Parallel()

	// The ordinals are PostgreSQL's own LOCKMODE numbering; rules compare with
	// `<` and max(), so a shifted value silently mis-grades every statement.
	expected := map[Lock]uint8{
		LockNone:                 0,
		LockAccessShare:          1,
		LockRowShare:             2,
		LockRowExclusive:         3,
		LockShareUpdateExclusive: 4,
		LockShare:                5,
		LockShareRowExclusive:    6,
		LockExclusive:            7,
		LockAccessExclusive:      8,
	}

	for lock, ordinal := range expected {
		assert.Equal(t, ordinal, uint8(lock), "%s", lock)
	}
	assert.False(t, LockNone.IsValid())
	assert.True(t, LockAccessShare.IsValid())
	assert.True(t, LockAccessExclusive.IsValid())
	assert.False(t, Lock(9).IsValid())
}

func TestLockBlocking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lock         Lock
		blocksReads  bool
		blocksWrites bool
	}{
		{LockNone, false, false},
		{LockAccessShare, false, false},
		{LockRowShare, false, false},
		{LockRowExclusive, false, false},
		{LockShareUpdateExclusive, false, false},
		{LockShare, false, true},
		{LockShareRowExclusive, false, true},
		{LockExclusive, false, true},
		{LockAccessExclusive, true, true},
	}

	for _, test := range tests {
		t.Run(test.lock.String(), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.blocksReads, test.lock.IsReadBlocking())
			assert.Equal(t, test.blocksWrites, test.lock.IsWriteBlocking())
		})
	}
}

func TestParseLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected Lock
		wantErr  bool
	}{
		{name: "canonical", input: "ACCESS_EXCLUSIVE", expected: LockAccessExclusive},
		{name: "short", input: "AEL", expected: LockAccessExclusive},
		{name: "spaced", input: "ACCESS EXCLUSIVE", expected: LockAccessExclusive},
		{name: "lowercase", input: "access exclusive", expected: LockAccessExclusive},
		{name: "pg_locks mode", input: "AccessExclusiveLock", expected: LockAccessExclusive},
		{name: "pg_locks share", input: "ShareLock", expected: LockShare},
		{name: "pg_locks row share", input: "RowShareLock", expected: LockRowShare},
		{name: "padded", input: "  SUE  ", expected: LockShareUpdateExclusive},
		{name: "empty is none", input: "", expected: LockNone},
		{name: "explicit none", input: "NONE", expected: LockNone},
		{name: "unknown", input: "SUPER EXCLUSIVE", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			lock, err := ParseLock(test.input)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.expected, lock)
		})
	}
}

func TestLockSerialization(t *testing.T) {
	t.Parallel()

	for lock := LockNone; lock <= LockAccessExclusive; lock++ {
		t.Run(lock.String(), func(t *testing.T) {
			t.Parallel()

			asJSON, err := json.Marshal(lock)
			require.NoError(t, err)

			var fromJSON Lock
			require.NoError(t, json.Unmarshal(asJSON, &fromJSON))
			assert.Equal(t, lock, fromJSON)

			asYAML, err := yaml.Marshal(lock)
			require.NoError(t, err)

			var fromYAML Lock
			require.NoError(t, yaml.Unmarshal(asYAML, &fromYAML))
			assert.Equal(t, lock, fromYAML)
		})
	}

	// State files written by the previous classifier hold the canonical spelling;
	// they must keep round-tripping byte for byte.
	asJSON, err := json.Marshal(LockShareUpdateExclusive)
	require.NoError(t, err)
	assert.JSONEq(t, `"SHARE_UPDATE_EXCLUSIVE"`, string(asJSON))
}

func TestMaxLock(t *testing.T) {
	t.Parallel()

	assert.Equal(t, LockNone, MaxLock())
	assert.Equal(t, LockAccessShare, MaxLock(LockAccessShare))
	assert.Equal(t, LockAccessExclusive, MaxLock(LockShare, LockAccessExclusive, LockRowShare))
	assert.Equal(t, LockShare, MaxLock(LockNone, LockShare, LockShareUpdateExclusive))
}

func TestOpKind(t *testing.T) {
	t.Parallel()

	expected := map[OpKind]uint8{
		OpKindNone:       0,
		OpKindMetadata:   1,
		OpKindDML:        2,
		OpKindConcurrent: 3,
		OpKindScan:       4,
		OpKindRewrite:    5,
	}
	for kind, ordinal := range expected {
		assert.Equal(t, ordinal, uint8(kind), "%s", kind)
	}

	assert.True(t, OpKindScan.ScalesWithTableSize())
	assert.True(t, OpKindRewrite.ScalesWithTableSize())
	assert.False(t, OpKindConcurrent.ScalesWithTableSize())
	assert.Equal(t, OpKindRewrite, MaxOpKind(OpKindMetadata, OpKindRewrite, OpKindScan))
	assert.Equal(t, OpKindNone, MaxOpKind())
}

func TestParseOpKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected OpKind
		wantErr  bool
	}{
		{input: "METADATA", expected: OpKindMetadata},
		{input: "metadata", expected: OpKindMetadata},
		{input: "REWRITE", expected: OpKindRewrite},
		{input: "", expected: OpKindNone},
		{input: "VACUUM", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			t.Parallel()
			kind, err := ParseOpKind(test.input)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.expected, kind)
		})
	}
}

func TestOpKindSerialization(t *testing.T) {
	t.Parallel()

	for kind := OpKindNone; kind <= OpKindRewrite; kind++ {
		asJSON, err := json.Marshal(kind)
		require.NoError(t, err)

		var fromJSON OpKind
		require.NoError(t, json.Unmarshal(asJSON, &fromJSON))
		assert.Equal(t, kind, fromJSON)

		asYAML, err := yaml.Marshal(kind)
		require.NoError(t, err)

		var fromYAML OpKind
		require.NoError(t, yaml.Unmarshal(asYAML, &fromYAML))
		assert.Equal(t, kind, fromYAML)
	}
}

func TestRelationKindFromRelkind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		relkind  string
		expected RelationKind
	}{
		{name: "ordinary table", relkind: "r", expected: RelationKindTable},
		{name: "partitioned table", relkind: "p", expected: RelationKindPartitionedTable},
		{name: "index", relkind: "i", expected: RelationKindIndex},
		{name: "partitioned index is case sensitive", relkind: "I", expected: RelationKindPartitionedIndex},
		{name: "view", relkind: "v", expected: RelationKindView},
		{name: "materialized view", relkind: "m", expected: RelationKindMaterializedView},
		{name: "sequence is uppercase", relkind: "S", expected: RelationKindSequence},
		{name: "lowercase s is not a sequence", relkind: "s", expected: RelationKindUnknown},
		{name: "foreign table", relkind: "f", expected: RelationKindForeignTable},
		{name: "toast", relkind: "t", expected: RelationKindToastTable},
		{name: "empty", relkind: "", expected: RelationKindUnknown},
		{name: "too long", relkind: "rr", expected: RelationKindUnknown},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.expected, RelationKindFromRelkind(test.relkind))
		})
	}
}

func TestRelationKindPredicates(t *testing.T) {
	t.Parallel()

	assert.True(t, RelationKindTable.IsTable())
	assert.True(t, RelationKindPartitionedTable.IsTable())
	assert.False(t, RelationKindView.IsTable())
	assert.True(t, RelationKindPartitionedIndex.IsIndex())
	assert.True(t, RelationKindPartitionedTable.IsPartitioned())
	assert.False(t, RelationKindIndex.IsPartitioned())
}

func TestTargetSerializationKeepsKind(t *testing.T) {
	t.Parallel()

	// helper.FullTableName marshals itself to a bare string, so a Relation that
	// embedded it would serialize as a name and silently drop the kind.
	target := Target{
		Relation: NewRelation("public", "orders", RelationKindPartitionedTable),
		Lock:     LockAccessExclusive,
		OpKind:   OpKindRewrite,
		Role:     TargetRoleExpanded,
	}

	asJSON, err := json.Marshal(target)
	require.NoError(t, err)
	assert.JSONEq(t,
		`{"relation":{"name":"public.orders","kind":"PARTITIONED_TABLE"},"lock":"ACCESS_EXCLUSIVE","op_kind":"REWRITE","role":"EXPANDED"}`,
		string(asJSON))

	var fromJSON Target
	require.NoError(t, json.Unmarshal(asJSON, &fromJSON))
	assert.Equal(t, target, fromJSON)

	asYAML, err := yaml.Marshal(target)
	require.NoError(t, err)

	var fromYAML Target
	require.NoError(t, yaml.Unmarshal(asYAML, &fromYAML))
	assert.Equal(t, target, fromYAML)
}

func TestTargetRoleAndSeverity(t *testing.T) {
	t.Parallel()

	// The zero value is the common case: a relation the statement names itself.
	assert.Equal(t, TargetRoleDirect, TargetRole(0))

	role, err := ParseTargetRole("implicit")
	require.NoError(t, err)
	assert.Equal(t, TargetRoleImplicit, role)

	_, err = ParseTargetRole("inferred")
	assert.Error(t, err)

	severity, err := ParseSeverity("warn")
	require.NoError(t, err)
	assert.Equal(t, SeverityWarn, severity)

	assert.Equal(t, "ERROR", SeverityError.String())

	asJSON, err := json.Marshal(SeverityError)
	require.NoError(t, err)
	assert.JSONEq(t, `"ERROR"`, string(asJSON))
}

func TestVersionFromNum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		num      int
		expected Version
	}{
		{name: "15.4", num: 150004, expected: Version15},
		{name: "17.0", num: 170000, expected: Version17},
		{name: "18.1", num: 180001, expected: Version18},
		{name: "9.6 is out of range", num: 90600, expected: VersionUnknown},
		{name: "zero", num: 0, expected: VersionUnknown},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.expected, VersionFromNum(test.num))
		})
	}

	assert.True(t, Version15.IsSupported())
	assert.True(t, Version18.IsSupported())
	assert.False(t, Version(14).IsSupported())
	assert.False(t, VersionUnknown.IsSupported())
}

func TestVersionRangeContains(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rng      VersionRange
		version  Version
		expected bool
	}{
		{name: "unbounded contains 15", rng: AnyVersion, version: Version15, expected: true},
		{name: "unbounded does not contains unknown", rng: AnyVersion, version: VersionUnknown, expected: false},
		{name: "at least 18 excludes 17", rng: AtLeast(Version18), version: Version17, expected: false},
		{name: "at least 18 contains 18", rng: AtLeast(Version18), version: Version18, expected: true},
		{name: "at most 16 contains 15", rng: AtMost(Version16), version: Version15, expected: true},
		{name: "at most 16 excludes 17", rng: AtMost(Version16), version: Version17, expected: false},
		{name: "between contains bound", rng: Between(Version15, Version18), version: Version18, expected: true},
		{name: "exactly", rng: Exactly(Version17), version: Version17, expected: true},
		// With production unreachable there is no version to gate on, so a
		// version-conditional rule must not fire on a guess.
		{name: "bounded excludes unknown", rng: AtLeast(Version18), version: VersionUnknown, expected: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.expected, test.rng.Contains(test.version))
		})
	}
}

func TestVersionRangeOverlaps(t *testing.T) {
	t.Parallel()

	assert.True(t, AtLeast(Version18).Overlaps(Between(Version17, Version18)))
	assert.False(t, AtLeast(Version18).Overlaps(AtMost(Version17)))
	assert.True(t, AnyVersion.Overlaps(Exactly(Version15)))
	assert.False(t, Exactly(Version15).Overlaps(Exactly(Version16)))
}

func TestVersionRangeString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rng      VersionRange
		expected string
	}{
		{rng: AnyVersion, expected: "any"},
		{rng: Exactly(Version17), expected: "17"},
		{rng: AtLeast(Version18), expected: "18+"},
		{rng: AtMost(Version16), expected: "<=16"},
		{rng: Between(Version15, Version18), expected: "15-18"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.expected, test.rng.String())

			parsed, err := ParseVersionRange(test.expected)
			require.NoError(t, err)
			assert.Equal(t, test.rng, parsed)
		})
	}

	_, err := ParseVersionRange("fifteen")
	assert.Error(t, err)
}

func TestStatementAnalysisAggregates(t *testing.T) {
	t.Parallel()

	orders := helper.FullRelationName{Schema: "public", Table: "orders"}
	customers := helper.FullRelationName{Schema: "public", Table: "customers"}

	analysis := StatementAnalysis{
		RawSQL:      "ALTER TABLE orders ADD COLUMN c int, ALTER COLUMN d TYPE bigint",
		TxnGroup:    1,
		Command:     "ALTER TABLE",
		Subcommands: []string{"ADD COLUMN", "ALTER COLUMN TYPE"},
		Findings: []Finding{
			{
				Code:     "R-AT-101",
				Severity: SeverityInfo,
				Message:  "adds a column with no default",
				Targets: []Target{
					{Relation: Relation{Name: orders}, Lock: LockAccessExclusive, OpKind: OpKindMetadata},
				},
			},
			{
				Code:     "R-AT-110",
				Severity: SeverityWarn,
				Message:  "type change rewrites the table",
				Targets: []Target{
					{Relation: Relation{Name: orders}, Lock: LockAccessExclusive, OpKind: OpKindRewrite},
					{Relation: Relation{Name: customers}, Lock: LockShare, OpKind: OpKindScan, Role: TargetRoleImplicit},
				},
			},
		},
	}

	assert.Equal(t, LockAccessExclusive, analysis.MaxLock())
	assert.Equal(t, OpKindRewrite, analysis.MaxOpKind())
	assert.Equal(t, LockAccessExclusive, analysis.LockOn(orders))
	assert.Equal(t, LockShare, analysis.LockOn(customers))
	assert.Equal(t, LockNone, analysis.LockOn(helper.FullRelationName{Schema: "public", Table: "absent"}))
	assert.False(t, analysis.HasErrors())

	empty := StatementAnalysis{}
	assert.Equal(t, LockNone, empty.MaxLock())
	assert.Equal(t, OpKindNone, empty.MaxOpKind())
}

func TestError(t *testing.T) {
	t.Parallel()

	versioned := Error{
		Code:     "G-03",
		Message:  "CHECK ... NOT ENFORCED is not supported",
		Versions: AtLeast(Version18),
	}
	assert.Equal(t, "G-03: CHECK ... NOT ENFORCED is not supported (valid on 18+)", versioned.Error())

	plain := Error{Code: "V-01", Message: "CREATE INDEX CONCURRENTLY cannot run in a transaction block"}
	assert.Equal(t, "V-01: CREATE INDEX CONCURRENTLY cannot run in a transaction block", plain.Error())

	analysis := StatementAnalysis{Errors: []Error{plain}}
	assert.True(t, analysis.HasErrors())
}
