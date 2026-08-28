package pg_catalog

import (
	"apercu-cli/helper"
	"apercu-cli/helper/pg_contract"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// name is the qualified name of a seeded object.
func name(relation string) helper.FullRelationName {
	return helper.FullRelationName{Schema: testSchema, Table: relation}
}

// newTestCatalog builds the lookups over one committed fixture.
func newTestCatalog(t *testing.T, version int) *Catalog {
	t.Helper()

	catalog, err := NewCatalog(CatalogOptions{
		Pre:  loadFixture(t, version, SourcePreview),
		Prod: loadFixture(t, version, SourceProd),
	})
	require.NoError(t, err)
	return catalog
}

// eachVersion runs one check against every fixture, so anything version-dependent is caught.
func eachVersion(t *testing.T, check func(t *testing.T, catalog *Catalog, version int)) {
	t.Helper()

	for _, version := range fixtureVersions {
		t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
			t.Parallel()
			check(t, newTestCatalog(t, version), version)
		})
	}
}

func findOID(t *testing.T, catalog *Catalog, relation string) OID {
	t.Helper()

	info := catalog.Resolve(name(relation), nil)
	require.True(t, info.Exists(), "%s must resolve", relation)
	return info.Relation.OID
}

func TestNewCatalogRejectsWrongSnapshots(t *testing.T) {
	t.Parallel()

	pre := &Snapshot{Source: SourcePreview, PIT: PITPre}

	_, err := NewCatalog(CatalogOptions{})
	assert.ErrorContains(t, err, "pre-migration snapshot is required")

	_, err = NewCatalog(CatalogOptions{Pre: &Snapshot{Source: SourcePreview, PIT: PITPost}})
	assert.ErrorContains(t, err, `is a "post" capture`)

	_, err = NewCatalog(CatalogOptions{Pre: pre, Post: &Snapshot{PIT: PITPre}})
	assert.ErrorContains(t, err, `is a "pre" capture`)

	_, err = NewCatalog(CatalogOptions{Pre: pre, Prod: &Snapshot{Source: SourcePreview}})
	assert.ErrorContains(t, err, `comes from "preview"`)

	_, err = NewCatalog(CatalogOptions{Pre: pre})
	assert.NoError(t, err, "a preview on its own is enough to classify")
}

// P-01 + C-01
func TestResolve(t *testing.T) {
	eachVersion(t, func(t *testing.T, catalog *Catalog, _ int) {
		qualified := catalog.Resolve(name("users"), nil)
		require.True(t, qualified.Exists())
		assert.Equal(t, OriginExisting, qualified.Origin)
		assert.Equal(t, "r", qualified.Relation.Kind)
		assert.Positive(t, qualified.PreviewBytes)

		// An unqualified name only means something through the path in force at that point.
		unqualified := helper.FullRelationName{Table: "users"}
		assert.False(t, catalog.Resolve(unqualified, []string{"public"}).Exists())
		assert.True(t, catalog.Resolve(unqualified, []string{"public", testSchema}).Exists())

		// The relation is reported against the first schema on the path, which is where the
		// statement expected it.
		missing := catalog.Resolve(helper.FullRelationName{Table: "nowhere"}, []string{"public", testSchema})
		assert.False(t, missing.Exists())
		assert.Equal(t, OriginUnknown, missing.Origin)
		assert.Equal(t, "public", missing.Name.Schema)
		assert.Zero(t, missing.PreviewBytes)
		assert.Contains(t, catalog.Unresolved(), helper.FullRelationName{Schema: "public", Table: "nowhere"})
	})
}

// P-01 + C-06: an object the migration creates resolves, and is empty whatever it holds.
func TestResolveCreatedByMigration(t *testing.T) {
	t.Parallel()

	pre := loadFixture(t, 17, SourcePreview)
	post := &Snapshot{
		Source: SourcePreview,
		PIT:    PITPost,
		// The post capture is what makes an object created by the migration resolvable; only
		// the ones that do not survive it need the shadow catalog.
		Relations: []Relation{{OID: 999001, Namespace: testSchema, Name: "created_here", Kind: "r", TotalBytes: 8192}},
	}

	catalog, err := NewCatalog(CatalogOptions{Pre: pre, Post: post})
	require.NoError(t, err)

	created := catalog.Resolve(name("created_here"), nil)
	require.True(t, created.Exists())
	assert.Equal(t, OriginCreated, created.Origin)
	assert.Zero(t, created.PreviewBytes, "A-03 floors severity on an object this migration created")

	// A temporary object never reaches either snapshot, so the classifier declares it.
	scratch := name("scratch")
	assert.False(t, catalog.Resolve(scratch, nil).Exists())
	assert.Contains(t, catalog.Unresolved(), scratch)

	catalog.Declare(scratch, "r")
	declared := catalog.Resolve(scratch, nil)
	assert.Equal(t, OriginCreated, declared.Origin)
	assert.NotContains(t, catalog.Unresolved(), scratch, "declaring it clears the fail-safe count")

	// Declaring an object that already exists changes nothing about it.
	catalog.Declare(name("users"), "r")
	assert.Equal(t, OriginExisting, catalog.Resolve(name("users"), nil).Origin)
}

// P-01: the partitioned parent reports nothing for itself, so its size is its tree.
func TestSizeSumsThePartitionTree(t *testing.T) {
	for _, version := range fixtureVersions {
		t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
			t.Parallel()

			// Without production the preview is all there is.
			catalog, err := NewCatalog(CatalogOptions{Pre: loadFixture(t, version, SourcePreview)})
			require.NoError(t, err)

			parent := catalog.Resolve(name("events"), nil)
			require.True(t, parent.Exists())
			assert.Zero(t, parent.Relation.TotalBytes, "measured: a partitioned parent has no storage")
			assert.Positive(t, parent.PreviewBytes, "but P-01 sums its leaves")

			var leaves int64
			for _, oid := range catalog.PartitionDescendants(parent.Relation.OID) {
				leaf, ok := catalog.ByOID(oid)
				require.True(t, ok)
				leaves += leaf.TotalBytes
			}
			assert.Equal(t, leaves, parent.PreviewBytes)

			// A classic INHERITS parent keeps its own storage, so nothing is summed into it.
			legacy := catalog.Resolve(name("legacy_parent"), nil)
			require.True(t, legacy.Exists())
			assert.Empty(t, catalog.PartitionDescendants(legacy.Relation.OID))
			assert.Equal(t, legacy.Relation.TotalBytes, legacy.PreviewBytes)
		})
	}
}

// P-01: the two measurements are kept side by side, because the preview is subsetted and
// production is what the migration will actually run against.
func TestSizeKeepsBothMeasurements(t *testing.T) {
	t.Parallel()

	pre := loadFixture(t, 17, SourcePreview)
	prod := loadFixture(t, 17, SourceProd)

	// Production holds far more than the subsetted preview does.
	for i := range prod.TableStats {
		prod.TableStats[i].TotalBytes *= 1000
	}

	catalog, err := NewCatalog(CatalogOptions{Pre: pre, Prod: prod})
	require.NoError(t, err)

	previewOnly, err := NewCatalog(CatalogOptions{Pre: loadFixture(t, 17, SourcePreview)})
	require.NoError(t, err)

	users := catalog.Resolve(name("users"), nil)
	assert.Equal(t, previewOnly.Resolve(name("users"), nil).PreviewBytes, users.PreviewBytes,
		"production never overwrites what the preview measured")
	assert.Equal(t, users.PreviewBytes*1000, users.ProdBytes)

	// A partitioned parent reports 0 on production too, so the tree is summed over its numbers.
	events := catalog.Resolve(name("events"), nil)
	assert.Positive(t, events.PreviewBytes)
	assert.Equal(t, events.PreviewBytes*1000, events.ProdBytes)

	// pg_stat_user_tables carries no index, so an index has a preview size and no production one.
	index := catalog.Resolve(name("orders_open_idx"), nil)
	require.True(t, index.Exists())
	assert.Positive(t, index.PreviewBytes)
	assert.EqualValues(t, -1, index.ProdBytes, "unknown on production is not the same as empty")

	// A relation production has never heard of leaves ProdBytes unknown rather than zero.
	missingOnProd := &Snapshot{Source: SourceProd, PIT: PITPre}
	fallback, err := NewCatalog(CatalogOptions{Pre: loadFixture(t, 17, SourcePreview), Prod: missingOnProd})
	require.NoError(t, err)
	unknown := fallback.Resolve(name("users"), nil)
	assert.Positive(t, unknown.PreviewBytes)
	assert.EqualValues(t, -1, unknown.ProdBytes)

	// Without any production snapshot at all every relation reports the same way.
	assert.EqualValues(t, -1, previewOnly.Resolve(name("users"), nil).ProdBytes)

	// A name no snapshot explains, and one the migration creates, carry neither measurement.
	missing := previewOnly.Resolve(name("nowhere"), nil)
	assert.Zero(t, missing.PreviewBytes)
	assert.EqualValues(t, -1, missing.ProdBytes)
}

// P-02 and P-17
func TestIndexLookups(t *testing.T) {
	eachVersion(t, func(t *testing.T, catalog *Catalog, _ int) {
		orders := findOID(t, catalog, "orders")
		partial := findOID(t, catalog, "orders_open_idx")

		index, ok := catalog.Index(partial)
		require.True(t, ok)
		assert.Equal(t, orders, index.RelID)
		assert.NotEmpty(t, index.Predicate, "the seeded index is partial")

		// An index names its table nowhere, which is what P-02 is for.
		table, ok := catalog.TableOfIndex(partial)
		require.True(t, ok)
		assert.Equal(t, "orders", table.Name)

		assert.Len(t, catalog.IndexesOf(orders), 2, "the primary key and the partial index")
		assert.Empty(t, catalog.InvalidIndexes(orders), "nothing in the fixture is left over from a failed CIC")
	})
}

// P-03 and P-04
func TestConstraintAndForeignKeyGraph(t *testing.T) {
	eachVersion(t, func(t *testing.T, catalog *Catalog, _ int) {
		users := findOID(t, catalog, "users")
		orders := findOID(t, catalog, "orders")

		notValid, ok := catalog.ConstraintByName(orders, "orders_total_check")
		require.True(t, ok)
		assert.Equal(t, "c", notValid.Type)
		assert.False(t, notValid.Validated)

		// orders references users, so a statement naming only orders still locks users.
		from := catalog.ForeignKeysFrom(orders)
		require.Len(t, from, 1)
		assert.Equal(t, users, from[0].ForeignRelID)

		// The reverse direction is what makes a DROP reach tables nothing named.
		to := catalog.ForeignKeysTo(users)
		require.Len(t, to, 2, "orders and profiles both point at users")
		for _, fk := range to {
			assert.Equal(t, users, fk.ForeignRelID)
			assert.NotEqual(t, users, fk.RelID)
		}
		assert.Empty(t, catalog.ForeignKeysTo(orders))
	})
}

// P-05
func TestSequenceOwner(t *testing.T) {
	eachVersion(t, func(t *testing.T, catalog *Catalog, _ int) {
		users := findOID(t, catalog, "users")

		owner, attnum, ok := catalog.SequenceOwner(findOID(t, catalog, "users_id_seq"))
		require.True(t, ok)
		assert.Equal(t, users, owner)
		assert.EqualValues(t, 1, attnum)

		_, _, ok = catalog.SequenceOwner(findOID(t, catalog, "standalone_seq"))
		assert.False(t, ok, "a standalone sequence has no owner")
	})
}

// P-06
func TestPartitionTree(t *testing.T) {
	eachVersion(t, func(t *testing.T, catalog *Catalog, _ int) {
		events := findOID(t, catalog, "events")
		defaultPart := findOID(t, catalog, "events_default")
		legacyParent := findOID(t, catalog, "legacy_parent")
		legacyChild := findOID(t, catalog, "legacy_child")

		assert.Len(t, catalog.Children(events), 2)
		assert.ElementsMatch(t, []OID{defaultPart, findOID(t, catalog, "events_2025")},
			catalog.PartitionDescendants(events))

		// ATTACH and DETACH lock the default partition without ever naming it.
		found, ok := catalog.DefaultPartition(events)
		require.True(t, ok)
		assert.Equal(t, defaultPart, found)
		_, ok = catalog.DefaultPartition(legacyParent)
		assert.False(t, ok)

		assert.Empty(t, catalog.PendingDetach(events))

		// Classic inheritance is in the closure a TRUNCATE expands, but not in the partition one.
		assert.Equal(t, []OID{legacyChild}, catalog.Descendants(legacyParent))
		assert.Empty(t, catalog.PartitionDescendants(legacyParent))

		parents := catalog.Parents(legacyChild)
		require.Len(t, parents, 1)
		assert.Equal(t, legacyParent, parents[0].Parent)
		assert.False(t, parents[0].ParentIsPartitioned)
	})
}

// P-07, P-08, P-09 and P-10
func TestVolatility(t *testing.T) {
	eachVersion(t, func(t *testing.T, catalog *Catalog, _ int) {
		var userFunc, now, textOut OID
		for _, proc := range catalog.pre.Procs {
			switch {
			case proc.Namespace == testSchema && proc.Name == "next_code":
				userFunc = proc.OID
			case proc.Namespace == "pg_catalog" && proc.Name == "now":
				now = proc.OID
			case proc.Namespace == "pg_catalog" && proc.Name == "textout":
				textOut = proc.OID
			}
		}
		require.NotZero(t, userFunc)

		assert.Equal(t, VolatilityVolatile, catalog.ProcVolatility(userFunc))
		assert.True(t, catalog.ProcVolatility(userFunc).MayBeVolatile())
		assert.Equal(t, VolatilityStable, catalog.ProcVolatility(now))
		assert.Equal(t, VolatilityImmutable, catalog.ProcVolatility(textOut))

		// A function nothing knows about is graded as if it were the worst case.
		assert.Equal(t, VolatilityUnknown, catalog.ProcVolatility(999999))
		assert.True(t, catalog.ProcVolatility(999999).MayBeVolatile())

		// An operator is exactly as volatile as the function behind it.
		var equalsText OID
		for _, operator := range catalog.pre.Operators {
			if operator.Name == "=" && operator.Code != 0 {
				if proc, ok := catalog.Proc(operator.Code); ok && proc.Name == "texteq" {
					equalsText = operator.OID
				}
			}
		}
		require.NotZero(t, equalsText)
		assert.Equal(t, VolatilityImmutable, catalog.OperatorVolatility(equalsText))

		text := builtinType(t, catalog, "text")
		assert.Equal(t, VolatilityImmutable, catalog.TypeInputVolatility(text))
		// Converting a value to itself runs no code at all.
		assert.Equal(t, VolatilityImmutable, catalog.CastVolatility(text, text))
		assert.Equal(t, VolatilityUnknown, catalog.CastVolatility(text, 999999))
	})
}

func builtinType(t *testing.T, catalog *Catalog, typeName string) OID {
	t.Helper()

	for _, typ := range catalog.pre.Types {
		if typ.Namespace == "pg_catalog" && typ.Name == typeName {
			return typ.OID
		}
	}
	require.FailNow(t, "type not found", typeName)
	return 0
}

func userType(t *testing.T, catalog *Catalog, typeName string) OID {
	t.Helper()

	for _, typ := range catalog.pre.Types {
		if typ.Namespace == testSchema && typ.Name == typeName {
			return typ.OID
		}
	}
	require.FailNow(t, "type not found", typeName)
	return 0
}

// P-11 and P-12
func TestTypeChangeRewrite(t *testing.T) {
	eachVersion(t, func(t *testing.T, catalog *Catalog, _ int) {
		text := builtinType(t, catalog, "text")
		varchar := builtinType(t, catalog, "varchar")
		int4 := builtinType(t, catalog, "int4")
		constrained := userType(t, catalog, "positive_int")
		loose := userType(t, catalog, "loose_text")

		// P-11: a constrained domain is what forces ADD COLUMN to rewrite.
		assert.True(t, catalog.DomainHasConstraints(constrained))
		assert.False(t, catalog.DomainHasConstraints(loose))
		assert.False(t, catalog.DomainHasConstraints(text), "a base type is not a domain")

		// P-12: text and varchar share a representation.
		assert.True(t, catalog.BinaryCoercible(text, varchar))
		assert.False(t, catalog.TypeChangeRequiresRewrite(text, -1, varchar, -1))
		assert.False(t, catalog.BinaryCoercible(int4, text))
		assert.True(t, catalog.TypeChangeRequiresRewrite(int4, -1, text, -1))

		// varchar's typmod is the length limit itself, so relaxing it proves no rewrite while
		// tightening it cannot. 14 and 24 are varchar(10) and varchar(20).
		assert.False(t, catalog.TypeChangeRequiresRewrite(varchar, 14, varchar, 24))
		assert.True(t, catalog.TypeChangeRequiresRewrite(varchar, 24, varchar, 14))
		assert.False(t, catalog.TypeChangeRequiresRewrite(varchar, 14, varchar, -1))
		assert.True(t, catalog.TypeChangeRequiresRewrite(varchar, -1, varchar, 14))

		// A binary coercion carries no modifier of its own, so a constrained target still has
		// its length checked. 9 is varchar(5).
		assert.True(t, catalog.TypeChangeRequiresRewrite(text, -1, varchar, 9))

		// An unconstrained domain adds a name and nothing else.
		assert.False(t, catalog.TypeChangeRequiresRewrite(text, -1, loose, -1))
		assert.True(t, catalog.TypeChangeRequiresRewrite(text, -1, constrained, -1))
	})
}

// P-12, one family of type modifiers at a time. TestTypmodRelaxationMatchesServer puts the same
// question to a live server; this keeps the answers in reach without one.
func TestTypmodRelaxation(t *testing.T) {
	// The modifiers as the server packs them, so the cases below read as declarations.
	varcharMod := func(length int32) int32 { return length + varHdrSz }
	numericMod := func(precision, scale int32) int32 { return (precision<<16 | scale) + varHdrSz }
	intervalMod := func(fields, digits int32) int32 { return fields<<16 | digits }
	const (
		intervalYear    = 1 << 2
		intervalMonth   = 1 << 1
		intervalDay     = 1 << 3
		fullDigits      = 0xffff
		unlimitedTypmod = int32(-1)
	)

	cases := []struct {
		what    string
		typ     string
		old     int32
		new     int32
		rewrite bool
	}{
		{"varchar grows", "varchar", varcharMod(10), varcharMod(20), false},
		{"varchar shrinks", "varchar", varcharMod(20), varcharMod(10), true},
		{"varchar drops its limit", "varchar", varcharMod(10), unlimitedTypmod, false},
		{"varchar gains a limit", "varchar", unlimitedTypmod, varcharMod(10), true},

		// bpchar pads to the new length, so it has no support function to erase the call.
		{"bpchar grows", "bpchar", varcharMod(10), varcharMod(20), true},
		{"bpchar drops its limit", "bpchar", varcharMod(10), unlimitedTypmod, false},

		{"numeric gains digits", "numeric", numericMod(10, 2), numericMod(12, 2), false},
		{"numeric loses digits", "numeric", numericMod(12, 2), numericMod(10, 2), true},
		{"numeric moves the point", "numeric", numericMod(10, 2), numericMod(12, 4), true},
		{"numeric gains a limit", "numeric", unlimitedTypmod, numericMod(10, 2), true},

		{"timestamp gains digits", "timestamp", 3, 6, false},
		{"timestamp loses digits", "timestamp", 6, 3, true},
		{"timestamp holds its maximum", "timestamp", unlimitedTypmod, 6, false},
		{"timestamp drops below its maximum", "timestamp", unlimitedTypmod, 3, true},

		{"interval keeps every field year had", "interval", intervalMod(intervalYear, fullDigits), intervalMod(intervalDay, fullDigits), false},
		{"interval drops to a coarser field", "interval", intervalMod(intervalYear|intervalMonth, fullDigits), intervalMod(intervalYear, fullDigits), true},
		{"interval holds its maximum", "interval", unlimitedTypmod, intervalMod(0x7fff, 6), false},
		{"interval drops below its maximum", "interval", unlimitedTypmod, intervalMod(0x7fff, 3), true},

		// An array is rewritten whichever way the modifier moves: the per-element call is
		// wrapped in a node ALTER TABLE will not look through.
		{"varchar array grows", "_varchar", varcharMod(10), varcharMod(20), true},
		{"varchar array drops its limit", "_varchar", varcharMod(10), unlimitedTypmod, false},
	}

	eachVersion(t, func(t *testing.T, catalog *Catalog, _ int) {
		for _, c := range cases {
			t.Run(c.what, func(t *testing.T) {
				typeOID := builtinType(t, catalog, c.typ)
				assert.Equal(t, c.rewrite, catalog.TypeChangeRequiresRewrite(typeOID, c.old, typeOID, c.new))
			})
		}
	})
}

// P-13
func TestColumnMetadata(t *testing.T) {
	eachVersion(t, func(t *testing.T, catalog *Catalog, _ int) {
		orders := findOID(t, catalog, "orders")

		code, ok := catalog.Column(orders, "code")
		require.True(t, ok)
		assert.True(t, code.HasDefault)
		assert.Contains(t, code.Default.Expr, "next_code()")
		require.Len(t, code.Default.ReferencedProcs, 1, "a user function is pinned by pg_depend")
		assert.Equal(t, VolatilityVolatile, catalog.ProcVolatility(code.Default.ReferencedProcs[0]))

		status, ok := catalog.Column(orders, "status")
		require.True(t, ok)
		assert.False(t, status.HasDefault)
		assert.EqualValues(t, 500, *status.StatsTarget)

		// A collation resolves through S-19, and a non-collatable column carries none.
		labels := findOID(t, catalog, "labels")
		labelName, ok := catalog.Column(labels, "name")
		require.True(t, ok)
		assert.Equal(t, "case_sensitive", labelName.Collation.Name)
		assert.Equal(t, testSchema, labelName.Collation.Namespace)

		weight, ok := catalog.Column(labels, "weight")
		require.True(t, ok)
		assert.Zero(t, weight.Collation.OID)

		_, ok = catalog.Column(orders, "no_such_column")
		assert.False(t, ok)
		assert.NotEmpty(t, catalog.Columns(orders))
	})
}

// P-14
func TestNotNullProof(t *testing.T) {
	eachVersion(t, func(t *testing.T, catalog *Catalog, version int) {
		users := findOID(t, catalog, "users")
		orders := findOID(t, catalog, "orders")

		proof := catalog.NotNullProof(users, "email")
		assert.True(t, proof.Proven())
		if version >= 18 {
			// From 18 a NOT NULL is a constraint row, and the row is what proves it.
			assert.Equal(t, NotNullConstraint, proof)
		} else {
			assert.Equal(t, NotNullAttribute, proof)
		}

		// A validated CHECK that rules out NULL lets SET NOT NULL skip its own scan.
		status, ok := catalog.Column(orders, "status")
		require.True(t, ok)
		require.False(t, status.NotNull)
		assert.Equal(t, NotNullCheck, catalog.NotNullProof(orders, "status"))

		// Nothing proves it for a plain nullable column.
		assert.Equal(t, NotNullUnproven, catalog.NotNullProof(orders, "total"))
		assert.False(t, catalog.NotNullProof(orders, "total").Proven())
		assert.Equal(t, NotNullUnproven, catalog.NotNullProof(orders, "no_such_column"))
	})
}

// P-14: the PG 18 shapes that attnotnull alone gets wrong.
func TestNotNullConstraintOverridesAttnotnull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		constraint Constraint
		expected   NotNullProof
	}{
		{
			name:       "validated and enforced proves it",
			constraint: Constraint{RelID: 1, Type: "n", Key: []int16{1}, Validated: true, Enforced: true},
			expected:   NotNullConstraint,
		},
		{
			name:       "NOT VALID proves nothing, whatever attnotnull says",
			constraint: Constraint{RelID: 1, Type: "n", Key: []int16{1}, Validated: false, Enforced: true},
			expected:   NotNullUnproven,
		},
		{
			name:       "NOT ENFORCED proves nothing either",
			constraint: Constraint{RelID: 1, Type: "n", Key: []int16{1}, Validated: true, Enforced: false},
			expected:   NotNullUnproven,
		},
		{
			name:       "a constraint on another column says nothing about this one",
			constraint: Constraint{RelID: 1, Type: "n", Key: []int16{2}, Validated: false, Enforced: true},
			expected:   NotNullAttribute,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			catalog, err := NewCatalog(CatalogOptions{Pre: &Snapshot{
				Source: SourcePreview, PIT: PITPre,
				Relations:   []Relation{{OID: 1, Namespace: "public", Name: "t", Kind: "r"}},
				Columns:     []Column{{RelID: 1, Num: 1, Name: "c", NotNull: true}, {RelID: 1, Num: 2, Name: "other"}},
				Constraints: []Constraint{test.constraint},
			}})
			require.NoError(t, err)
			assert.Equal(t, test.expected, catalog.NotNullProof(1, "c"))
		})
	}
}

// P-14: only the two forms PostgreSQL itself recognises count as a proof.
func TestCheckProvesNotNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		def      string
		column   string
		expected bool
	}{
		{def: "CHECK ((status IS NOT NULL))", column: "status", expected: true},
		{def: "CHECK ((NOT (status IS NULL)))", column: "status", expected: true},
		{def: `CHECK (("order status" IS NOT NULL))`, column: "order status", expected: true},
		{def: "CHECK ((status is not null))", column: "status", expected: true},
		{def: "CHECK ((status IS NOT NULL))", column: "status_code", expected: false},
		{def: "CHECK ((order_status IS NOT NULL))", column: "status", expected: false},
		{def: "CHECK ((status <> ''::text))", column: "status", expected: false},
		{def: "CHECK ((status IS NULL))", column: "status", expected: false},
		{def: "", column: "status", expected: false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s/%s", test.column, test.def), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.expected, checkProvesNotNull(test.def, test.column))
		})
	}
}

// P-15
func TestTypeDependents(t *testing.T) {
	eachVersion(t, func(t *testing.T, catalog *Catalog, _ int) {
		profiles := findOID(t, catalog, "profiles")

		columns, typed := catalog.TypeDependents(userType(t, catalog, "mood"))
		require.Len(t, columns, 1)
		assert.Equal(t, profiles, columns[0].RelID)
		assert.Equal(t, "feeling", columns[0].Name)
		assert.Empty(t, typed)

		// A column declared as a domain depends on the domain's base type just as directly.
		columns, _ = catalog.TypeDependents(builtinType(t, catalog, "int4"))
		var throughDomain bool
		for _, column := range columns {
			if column.RelID == profiles && column.Name == "score" {
				throughDomain = true
			}
		}
		assert.True(t, throughDomain, "positive_int is a domain over int4")

		// A composite type is reached through reloftype, the only whole-row dependency.
		_, typed = catalog.TypeDependents(userType(t, catalog, "address"))
		assert.Equal(t, []OID{findOID(t, catalog, "places")}, typed)
	})
}

// P-16
func TestViewDependents(t *testing.T) {
	eachVersion(t, func(t *testing.T, catalog *Catalog, _ int) {
		users := findOID(t, catalog, "users")
		activeUsers := findOID(t, catalog, "active_users")

		deps := catalog.ViewDependents(users)
		require.NotEmpty(t, deps)
		for _, dep := range deps {
			assert.Equal(t, users, dep.ReferencedRelID)
		}

		email, ok := catalog.Column(users, "email")
		require.True(t, ok)
		onColumn := catalog.ViewDependentsOfColumn(users, email.Num)
		require.NotEmpty(t, onColumn, "a column-level edge is what makes DROP COLUMN decidable")
		assert.Equal(t, activeUsers, onColumn[0].DependentRelID)

		// created_at is in the table but in no view, so dropping it cascades to nothing.
		createdAt, ok := catalog.Column(users, "created_at")
		require.True(t, ok)
		assert.Empty(t, catalog.ViewDependentsOfColumn(users, createdAt.Num))
	})
}

// P-18
func TestVersionGating(t *testing.T) {
	eachVersion(t, func(t *testing.T, catalog *Catalog, version int) {
		assert.EqualValues(t, version, catalog.Version())
		assert.Equal(t, pg_contract.Exactly(pg_contract.Version(version)), catalog.VersionRange())
		assert.EqualValues(t, version, catalog.PreviewVersion())
	})
}

// P-18: without production the range stays open, because the preview proves nothing about it.
func TestVersionRangeWithoutProduction(t *testing.T) {
	t.Parallel()

	catalog, err := NewCatalog(CatalogOptions{Pre: loadFixture(t, 16, SourcePreview)})
	require.NoError(t, err)

	assert.Equal(t, pg_contract.VersionUnknown, catalog.Version())
	assert.EqualValues(t, 16, catalog.PreviewVersion())

	open := pg_contract.Between(pg_contract.MinSupportedVersion, pg_contract.MaxSupportedVersion)
	assert.Equal(t, open, catalog.VersionRange())
	assert.False(t, open.Contains(pg_contract.VersionUnknown))
}

// P-19
func TestSessionDefaults(t *testing.T) {
	eachVersion(t, func(t *testing.T, catalog *Catalog, _ int) {
		value, ok := catalog.Setting("search_path")
		require.True(t, ok)
		assert.NotEmpty(t, value)

		_, ok = catalog.Setting("no_such_setting")
		assert.False(t, ok)

		assert.NotEmpty(t, catalog.SearchPath())
	})
}

func TestParseSearchPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		value    string
		user     string
		expected []string
	}{
		{value: `"$user", public`, user: "postgres", expected: []string{"postgres", "public"}},
		{value: "public", expected: []string{"public"}},
		{value: ` app , "public" `, expected: []string{"app", "public"}},
		{value: `"$user"`, expected: []string{}},
		{value: "", expected: []string{}},
	}

	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.expected, ParseSearchPath(test.value, test.user))
		})
	}
}

// P-20
func TestTableHeat(t *testing.T) {
	eachVersion(t, func(t *testing.T, catalog *Catalog, _ int) {
		assert.True(t, catalog.HasProdActivity())

		// Activity is keyed by name: the production relids come from another database, where
		// the preview's OIDs mean nothing.
		stat, ok := catalog.Heat(name("users"))
		require.True(t, ok)
		assert.Positive(t, stat.LiveTup)
		assert.Positive(t, stat.TotalBytes, "production measures its own sizes, S-02 does not")
		assert.GreaterOrEqual(t, stat.TotalBytes, stat.HeapBytes)

		_, ok = catalog.Heat(name("nowhere"))
		assert.False(t, ok)
	})
}

func TestTableHeatWithoutProduction(t *testing.T) {
	t.Parallel()

	catalog, err := NewCatalog(CatalogOptions{Pre: loadFixture(t, 17, SourcePreview)})
	require.NoError(t, err)

	assert.False(t, catalog.HasProdActivity())
	_, ok := catalog.Heat(name("users"))
	assert.False(t, ok)
}

func TestRelationContract(t *testing.T) {
	t.Parallel()

	catalog := newTestCatalog(t, 18)
	events := catalog.Resolve(name("events"), nil)
	require.True(t, events.Exists())

	contract := events.Relation.Contract()
	assert.Equal(t, pg_contract.RelationKindPartitionedTable, contract.Kind)
	assert.Equal(t, testSchema+".events", contract.Name.String())
}
