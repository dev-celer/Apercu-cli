package pg_classify

import (
	"path/filepath"
	"testing"
	"time"

	"apercu-cli/helper"
	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_parse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testCatalog is one committed snapshot, for Unit-testing
func testCatalog(t *testing.T) *pg_catalog.Catalog {
	t.Helper()

	pre, err := pg_catalog.LoadJSON(filepath.Join("..", "pg_catalog", "testdata", "snapshot_pg17_preview.json.gz"))
	require.NoError(t, err)
	catalog, err := pg_catalog.NewCatalog(pg_catalog.CatalogOptions{Pre: pre})
	require.NoError(t, err)
	return catalog
}

// run walks a migration the way the classifier will: one proxy event, split into statements, each one handed to the session in order.
func run(t *testing.T, catalog *pg_catalog.Catalog, script string) ([]pg_parse.Statement, []Context) {
	t.Helper()

	statements := pg_parse.Parse(script)
	require.NotEmpty(t, statements)
	return statements, NewSession(catalog).Walk(statements)
}

// groups is the per-statement group id.
func groups(contexts []Context) []int {
	out := make([]int, 0, len(contexts))
	for _, context := range contexts {
		out = append(out, context.Group)
	}
	return out
}

// TestTransactionGrouping validate transaction block is handled
func TestTransactionGrouping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		script string
		groups []int
		inTx   []bool
	}{
		{
			name:   "autocommit statements each get their own group",
			script: "ALTER TABLE a ADD COLUMN x int; ALTER TABLE b ADD COLUMN y int",
			groups: []int{1, 2},
			inTx:   []bool{false, false},
		},
		{
			name:   "a block is one group, brackets included",
			script: "BEGIN; ALTER TABLE a ADD COLUMN x int; ALTER TABLE b ADD COLUMN y int; COMMIT",
			groups: []int{1, 1, 1, 1},
			inTx:   []bool{true, true, true, true},
		},
		{
			name:   "a second block is a second group",
			script: "BEGIN; ALTER TABLE a ADD COLUMN x int; COMMIT; BEGIN; ALTER TABLE b ADD COLUMN y int; COMMIT",
			groups: []int{1, 1, 1, 2, 2, 2},
			inTx:   []bool{true, true, true, true, true, true},
		},
		{
			name:   "a statement after a COMMIT is back to autocommit",
			script: "BEGIN; ALTER TABLE a ADD COLUMN x int; COMMIT; ALTER TABLE b ADD COLUMN y int",
			groups: []int{1, 1, 1, 2},
			inTx:   []bool{true, true, true, false},
		},
		{
			name:   "ROLLBACK closes the group as surely as COMMIT",
			script: "BEGIN; ALTER TABLE a ADD COLUMN x int; ROLLBACK; ALTER TABLE b ADD COLUMN y int",
			groups: []int{1, 1, 1, 2},
			inTx:   []bool{true, true, true, false},
		},
		{
			// The server warns and carries on with the block already open.
			name:   "a nested BEGIN does not open a second group",
			script: "BEGIN; BEGIN; ALTER TABLE a ADD COLUMN x int; COMMIT",
			groups: []int{1, 1, 1, 1},
			inTx:   []bool{true, true, true, true},
		},
		{
			name:   "a stray COMMIT is its own group",
			script: "ALTER TABLE a ADD COLUMN x int; COMMIT; ALTER TABLE b ADD COLUMN y int",
			groups: []int{1, 2, 3},
			inTx:   []bool{false, false, false},
		},
		{
			name:   "savepoints do not split the group",
			script: "BEGIN; SAVEPOINT s1; ALTER TABLE a ADD COLUMN x int; RELEASE SAVEPOINT s1; COMMIT",
			groups: []int{1, 1, 1, 1, 1},
			inTx:   []bool{true, true, true, true, true},
		},
		{
			name:   "COMMIT AND CHAIN starts the next group without leaving the block",
			script: "BEGIN; ALTER TABLE a ADD COLUMN x int; COMMIT AND CHAIN; ALTER TABLE b ADD COLUMN y int; COMMIT",
			groups: []int{1, 1, 1, 2, 2},
			inTx:   []bool{true, true, true, true, true},
		},
		{
			name:   "and so does ROLLBACK AND CHAIN",
			script: "BEGIN; ALTER TABLE a ADD COLUMN x int; ROLLBACK AND CHAIN; ALTER TABLE b ADD COLUMN y int; COMMIT",
			groups: []int{1, 1, 1, 2, 2},
			inTx:   []bool{true, true, true, true, true},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			statements, contexts := run(t, nil, tc.script)
			require.Len(t, contexts, len(tc.groups), "script has %d statements", len(statements))
			assert.Equal(t, tc.groups, groups(contexts))
			for i, want := range tc.inTx {
				assert.Equalf(t, want, contexts[i].InTransaction, "statement %d: %s", i, statements[i].RawSQL)
			}
		})
	}
}

// TestSavepointsAreVisible validate that savepoints are recorded and visible.
func TestSavepointsAreVisible(t *testing.T) {
	t.Parallel()

	script := "BEGIN; SAVEPOINT a; SAVEPOINT b; ROLLBACK TO SAVEPOINT a; RELEASE SAVEPOINT a; COMMIT"
	_, contexts := run(t, nil, script)
	require.Len(t, contexts, 6)

	assert.Empty(t, contexts[0].Savepoints) // BEGIN
	assert.Empty(t, contexts[1].Savepoints) // SAVEPOINT a, taken before it opens
	assert.Equal(t, []string{"a"}, contexts[2].Savepoints)
	assert.Equal(t, []string{"a", "b"}, contexts[3].Savepoints)
	// ROLLBACK TO leaves the savepoint open.
	assert.Equal(t, []string{"a"}, contexts[4].Savepoints)
	assert.Empty(t, contexts[5].Savepoints)
}

// TestSettingScopes validate the way SET / SET LOCAL are recorded.
func TestSettingScopes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		script string
		want   []time.Duration
	}{
		{
			name:   "a session SET carries past the COMMIT",
			script: "BEGIN; SET lock_timeout = '5s'; COMMIT; ALTER TABLE a ADD COLUMN x int",
			want:   []time.Duration{0, 0, 5 * time.Second, 5 * time.Second},
		},
		{
			name:   "a LOCAL SET does not",
			script: "BEGIN; SET LOCAL lock_timeout = '5s'; COMMIT; ALTER TABLE a ADD COLUMN x int",
			want:   []time.Duration{0, 0, 5 * time.Second, 0},
		},
		{
			// A transaction that aborts never set anything, so the statements after it run bare.
			name:   "a ROLLBACK undoes a session SET too",
			script: "BEGIN; SET lock_timeout = '5s'; ROLLBACK; ALTER TABLE a ADD COLUMN x int",
			want:   []time.Duration{0, 0, 5 * time.Second, 0},
		},
		{
			// Inside a block the value in force is the last one written, LOCAL or not. What LOCAL
			// decides is only what is left standing after the COMMIT.
			name:   "the last write wins inside the block",
			script: "BEGIN; SET lock_timeout = '9s'; SET LOCAL lock_timeout = '5s'; ALTER TABLE a ADD COLUMN x int; COMMIT; ALTER TABLE b ADD COLUMN y int",
			want:   []time.Duration{0, 0, 9 * time.Second, 5 * time.Second, 5 * time.Second, 9 * time.Second},
		},
		{
			// The same script the other way round. A session SET after a SET LOCAL takes the value
			// in force with it, which is the half of the rule that is easy to get backwards.
			name:   "and a session SET after a LOCAL one is still the last write",
			script: "BEGIN; SET LOCAL lock_timeout = '5s'; SET lock_timeout = '9s'; ALTER TABLE a ADD COLUMN x int; COMMIT; ALTER TABLE b ADD COLUMN y int",
			want:   []time.Duration{0, 0, 5 * time.Second, 9 * time.Second, 9 * time.Second, 9 * time.Second},
		},
		{
			name:   "RESET drops back to the baseline",
			script: "SET lock_timeout = '5s'; RESET lock_timeout; ALTER TABLE a ADD COLUMN x int",
			want:   []time.Duration{0, 5 * time.Second, 0},
		},
		{
			name:   "SET x TO DEFAULT is the same statement as RESET",
			script: "SET lock_timeout = '5s'; SET lock_timeout TO DEFAULT; ALTER TABLE a ADD COLUMN x int",
			want:   []time.Duration{0, 5 * time.Second, 0},
		},
		{
			name:   "RESET ALL drops every tracked parameter",
			script: "SET lock_timeout = '5s'; RESET ALL; ALTER TABLE a ADD COLUMN x int",
			want:   []time.Duration{0, 5 * time.Second, 0},
		},
		{
			// RESET is session level and has no LOCAL spelling, but it is still the last write, so
			// the LOCAL value it lands on top of goes with it.
			name:   "RESET takes a LOCAL value with it",
			script: "BEGIN; SET LOCAL lock_timeout = '5s'; RESET lock_timeout; ALTER TABLE a ADD COLUMN x int; COMMIT",
			want:   []time.Duration{0, 0, 5 * time.Second, 0, 0},
		},
		{
			name:   "and so does RESET ALL",
			script: "BEGIN; SET LOCAL lock_timeout = '5s'; RESET ALL; ALTER TABLE a ADD COLUMN x int; COMMIT; ALTER TABLE b ADD COLUMN y int",
			want:   []time.Duration{0, 0, 5 * time.Second, 0, 0, 0},
		},
		{
			// SET LOCAL x TO DEFAULT is the baseline for the rest of the block only.
			name:   "a LOCAL reset stops at the COMMIT",
			script: "SET lock_timeout = '5s'; BEGIN; SET LOCAL lock_timeout TO DEFAULT; ALTER TABLE a ADD COLUMN x int; COMMIT; ALTER TABLE b ADD COLUMN y int",
			want:   []time.Duration{0, 5 * time.Second, 5 * time.Second, 0, 0, 5 * time.Second},
		},
		{
			// The chained block is a new one, so what SET LOCAL wrote in the old one is gone.
			name:   "COMMIT AND CHAIN drops the LOCAL values with the block it closes",
			script: "SET lock_timeout = '9s'; BEGIN; SET LOCAL lock_timeout = '5s'; COMMIT AND CHAIN; ALTER TABLE a ADD COLUMN x int; COMMIT",
			want:   []time.Duration{0, 9 * time.Second, 9 * time.Second, 5 * time.Second, 9 * time.Second, 9 * time.Second},
		},
		{
			name:   "SET x FROM CURRENT moves nothing",
			script: "SET lock_timeout = '5s'; SET lock_timeout FROM CURRENT; ALTER TABLE a ADD COLUMN x int",
			want:   []time.Duration{0, 5 * time.Second, 5 * time.Second},
		},
		{
			name:   "SET LOCAL outside of a transaction does nothing",
			script: "SET LOCAL lock_timeout = '5s'; SELECT 1",
			want:   []time.Duration{0, 0},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			statements, contexts := run(t, nil, tc.script)
			require.Len(t, contexts, len(tc.want))
			for i, want := range tc.want {
				assert.Equalf(t, want, contexts[i].LockTimeout.Duration, "statement %d: %s", i, statements[i].RawSQL)
			}
		})
	}
}

// TestSavepointUnwindsSettings validate savepoint rollback capability.
func TestSavepointUnwindsSettings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		script string
		want   []time.Duration
	}{
		{
			name: "ROLLBACK TO restores what was set after the savepoint",
			script: "BEGIN; SET lock_timeout = '1s'; SAVEPOINT s; SET lock_timeout = '9s';" +
				" ROLLBACK TO SAVEPOINT s; ALTER TABLE a ADD COLUMN x int; COMMIT",
			want: []time.Duration{0, 0, time.Second, time.Second, 9 * time.Second, time.Second, time.Second},
		},
		{
			name: "and unsets what the savepoint found unset",
			script: "BEGIN; SAVEPOINT s; SET lock_timeout = '9s'; ROLLBACK TO SAVEPOINT s;" +
				" ALTER TABLE a ADD COLUMN x int; COMMIT",
			want: []time.Duration{0, 0, 0, 9 * time.Second, 0, 0},
		},
		{
			name: "RELEASE keeps the writes",
			script: "BEGIN; SAVEPOINT s; SET lock_timeout = '9s'; RELEASE SAVEPOINT s;" +
				" ALTER TABLE a ADD COLUMN x int; COMMIT",
			want: []time.Duration{0, 0, 0, 9 * time.Second, 9 * time.Second, 9 * time.Second},
		},
		{
			// The released frame's undo has to move up, or the outer ROLLBACK would stop short.
			name: "a released savepoint's writes are still undone by the outer ROLLBACK",
			script: "BEGIN; SAVEPOINT s; SET lock_timeout = '9s'; RELEASE SAVEPOINT s; ROLLBACK;" +
				" ALTER TABLE a ADD COLUMN x int",
			want: []time.Duration{0, 0, 0, 9 * time.Second, 9 * time.Second, 0},
		},
		{
			name: "the innermost savepoint of a name is the one rolled back to",
			script: "BEGIN; SET lock_timeout = '1s'; SAVEPOINT s; SET lock_timeout = '2s'; SAVEPOINT s;" +
				" SET lock_timeout = '3s'; ROLLBACK TO SAVEPOINT s; ALTER TABLE a ADD COLUMN x int; COMMIT",
			want: []time.Duration{0, 0, time.Second, time.Second, 2 * time.Second, 2 * time.Second,
				3 * time.Second, 2 * time.Second, 2 * time.Second},
		},
		{
			name: "a LOCAL set is unwound by ROLLBACK TO as well",
			script: "BEGIN; SET LOCAL lock_timeout = '1s'; SAVEPOINT s; SET LOCAL lock_timeout = '9s';" +
				" ROLLBACK TO SAVEPOINT s; ALTER TABLE a ADD COLUMN x int; COMMIT",
			want: []time.Duration{0, 0, time.Second, time.Second, 9 * time.Second, time.Second, time.Second},
		},
		{
			name: "a savepoint outside a block is nothing to roll back to",
			script: "SET lock_timeout = '1s'; SAVEPOINT s; SET lock_timeout = '9s'; ROLLBACK TO SAVEPOINT s;" +
				" ALTER TABLE a ADD COLUMN x int",
			want: []time.Duration{0, time.Second, time.Second, 9 * time.Second, 9 * time.Second},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			statements, contexts := run(t, nil, tc.script)
			require.Len(t, contexts, len(tc.want))
			for i, want := range tc.want {
				assert.Equalf(t, want, contexts[i].LockTimeout.Duration, "statement %d: %s", i, statements[i].RawSQL)
			}
		})
	}
}

// TestSearchPathMovesWithTheFile validate unqualified name resolution based on search_path.
func TestSearchPathMovesWithTheFile(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	statements, contexts := run(t, catalog,
		"ALTER TABLE users ADD COLUMN x int;"+
			" SET search_path = public;"+
			" ALTER TABLE users ADD COLUMN y int;"+
			" SET search_path TO DEFAULT;"+
			" ALTER TABLE users ADD COLUMN z int")
	require.Len(t, contexts, 5)

	assert.Equal(t, []string{"apercu_snapshot_test"}, contexts[0].SearchPath)
	assert.Equal(t, []string{"public"}, contexts[2].SearchPath)
	assert.Equal(t, []string{"apercu_snapshot_test"}, contexts[4].SearchPath)

	unqualified := statements[0].Relations[0].Name
	assert.True(t, catalog.Resolve(unqualified, contexts[0].SearchPath).Exists())
	assert.Equal(t, helper.FullRelationName{Schema: "apercu_snapshot_test", Table: "users"}, catalog.Resolve(unqualified, contexts[0].SearchPath).Name)
	assert.False(t, catalog.Resolve(unqualified, contexts[2].SearchPath).Exists())
	assert.Equal(t, helper.FullRelationName{Schema: "public", Table: "users"}, catalog.Resolve(unqualified, contexts[2].SearchPath).Name)
	assert.True(t, catalog.Resolve(unqualified, contexts[4].SearchPath).Exists())
	assert.Equal(t, helper.FullRelationName{Schema: "apercu_snapshot_test", Table: "users"}, catalog.Resolve(unqualified, contexts[4].SearchPath).Name)
}

// TestSearchPathSubstitutesUser covers the one entry that is not a schema name.
func TestSearchPathSubstitutesUser(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	_, contexts := run(t, catalog, `SET search_path = "$user", public; SELECT 1`)
	assert.Equal(t, []string{catalog.SnapshotUser(), "public"}, contexts[1].SearchPath)
}

// TestShadowCatalogRecordsWhatWasCreated validate that the shadow catalog record the relation created and their kind.
func TestShadowCatalogRecordsWhatWasCreated(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sql     string
		table   string
		relkind string
	}{
		{"CREATE TABLE t_plain (a int)", "t_plain", "r"},
		{"CREATE TABLE t_parted (a int) PARTITION BY RANGE (a)", "t_parted", "p"},
		{"CREATE TABLE t_ctas AS SELECT 1 AS a", "t_ctas", "r"},
		{"CREATE VIEW v_new AS SELECT 1", "v_new", "v"},
		{"CREATE MATERIALIZED VIEW m_new AS SELECT 1", "m_new", "m"},
		{"CREATE SEQUENCE s_new", "s_new", "S"},
		{"CREATE INDEX i_new ON users (id)", "i_new", "i"},
		{"CREATE INDEX i_parted ON events (id)", "i_parted", "I"},
	}

	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			t.Parallel()

			catalog := testCatalog(t)
			run(t, catalog, tc.sql)

			info := catalog.Resolve(helper.FullRelationName{Schema: "apercu_snapshot_test", Table: tc.table}, nil)
			require.Truef(t, info.Exists(), "%s should have been declared", tc.table)
			assert.Equal(t, tc.relkind, info.Relation.Kind)
		})
	}
}

func TestShadowCatalogDoesNotShadowARealRelation(t *testing.T) {
	t.Parallel()

	catalog := testCatalog(t)
	before := catalog.Resolve(helper.FullRelationName{Schema: "apercu_snapshot_test", Table: "users"}, nil)
	require.Equal(t, pg_catalog.OriginExisting, before.Origin)

	run(t, catalog, "CREATE TEMP TABLE users (a int)")

	after := catalog.Resolve(helper.FullRelationName{Schema: "apercu_snapshot_test", Table: "users"}, nil)
	assert.Equal(t, pg_catalog.OriginExisting, after.Origin)
	assert.Equal(t, before.Relation.OID, after.Relation.OID)
}

// TestBaselineComesFromTheSnapshot chack that with no SET at all, the context is what the collector's session reported.
func TestBaselineComesFromTheSnapshot(t *testing.T) {
	t.Parallel()

	_, contexts := run(t, testCatalog(t), "SELECT 1")
	context := contexts[0]

	assert.Equal(t, []string{"apercu_snapshot_test"}, context.SearchPath)
	assert.Equal(t, "Etc/UTC", context.TimeZone)
	assert.True(t, context.TimeZoneIsUTC())
	assert.False(t, context.LockTimeout.Set())
	assert.True(t, context.LockTimeout.Valid, "the baseline reports 0, which reads fine")
	assert.False(t, context.StatementTimeout.Set())
	assert.Equal(t, ReplicationOrigin, context.ReplicationRole)
	assert.True(t, context.ReplicationRole.EnforcesConstraints())
}

func TestReplicationRoleFollowsTheFile(t *testing.T) {
	t.Parallel()

	_, contexts := run(t, testCatalog(t),
		"SET session_replication_role = replica; ALTER TABLE users ADD COLUMN x int;"+
			" RESET session_replication_role; ALTER TABLE users ADD COLUMN y int")
	require.Len(t, contexts, 4)

	assert.Equal(t, ReplicationReplica, contexts[1].ReplicationRole)
	assert.False(t, contexts[1].ReplicationRole.EnforcesConstraints())
	assert.Equal(t, ReplicationOrigin, contexts[3].ReplicationRole)
	assert.True(t, contexts[3].ReplicationRole.EnforcesConstraints())
}

func TestTimeZoneFollowsTheFile(t *testing.T) {
	t.Parallel()

	_, contexts := run(t, testCatalog(t),
		"ALTER TABLE users ALTER COLUMN c TYPE timestamptz;"+
			" SET TimeZone = 'Europe/Paris';"+
			" ALTER TABLE users ALTER COLUMN d TYPE timestamptz;"+
			" SET timezone TO 'UTC';"+
			" ALTER TABLE users ALTER COLUMN e TYPE timestamptz")
	require.Len(t, contexts, 5)

	assert.True(t, contexts[0].TimeZoneIsUTC())
	assert.False(t, contexts[2].TimeZoneIsUTC(), "Europe/Paris is a real zone")
	assert.Equal(t, "UTC", contexts[4].TimeZone)
	assert.True(t, contexts[4].TimeZoneIsUTC())
}

func TestTimeZoneIsUTC(t *testing.T) {
	t.Parallel()

	for _, zone := range []string{"UTC", "utc", "Etc/UTC", "GMT", "Etc/GMT", "Zulu", "Universal", " UTC "} {
		assert.Truef(t, Context{TimeZone: zone}.TimeZoneIsUTC(), "%q is UTC", zone)
	}
	for _, zone := range []string{"", "Europe/Paris", "Etc/GMT+2", "US/Eastern", "localtime"} {
		assert.Falsef(t, Context{TimeZone: zone}.TimeZoneIsUTC(), "%q is not UTC", zone)
	}
}

func TestParseTimeout(t *testing.T) {
	t.Parallel()

	cases := []struct {
		raw   string
		want  time.Duration
		valid bool
	}{
		{"0", 0, true},
		{"5000", 5 * time.Second, true},
		{"5s", 5 * time.Second, true},
		{"5 s", 5 * time.Second, true},
		{"500ms", 500 * time.Millisecond, true},
		{"250us", 250 * time.Microsecond, true},
		{"2min", 2 * time.Minute, true},
		{"1h", time.Hour, true},
		{"1d", 24 * time.Hour, true},
		{"0.5s", 500 * time.Millisecond, true},
		{"'5s'", 5 * time.Second, true},
		{"5S", 5 * time.Second, true},
		{"", 0, false},
		{"soon", 0, false},
		{"-1", 0, false},
		{"5 fortnights", 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			t.Parallel()

			got := parseTimeout(tc.raw)
			assert.Equal(t, tc.want, got.Duration)
			assert.Equal(t, tc.valid, got.Valid)
			assert.Equal(t, tc.want > 0 && tc.valid, got.Set())
		})
	}
}

// TestUnreadableTimeoutIsNotATimeout: a value the server would reject must not be recorded.
func TestUnreadableTimeoutIsNotATimeout(t *testing.T) {
	t.Parallel()

	_, contexts := run(t, nil, "SET lock_timeout = 'whenever'; ALTER TABLE a ADD COLUMN x int")
	assert.False(t, contexts[1].LockTimeout.Set())
	assert.False(t, contexts[1].LockTimeout.Valid)
	assert.Equal(t, "whenever", contexts[1].LockTimeout.Raw)
}

func TestUntrackedParametersAreIgnored(t *testing.T) {
	t.Parallel()

	_, contexts := run(t, testCatalog(t),
		"SET work_mem = '256MB'; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; ALTER TABLE users ADD COLUMN x int")
	assert.Equal(t, []string{"apercu_snapshot_test"}, contexts[2].SearchPath)
	assert.False(t, contexts[2].LockTimeout.Set())
}

func TestUnparseableStatementsStillAdvanceTheSession(t *testing.T) {
	t.Parallel()

	statements, contexts := run(t, testCatalog(t), "BEGIN; NOT SQL AT ALL; COMMIT; SELECT 1")
	require.Len(t, contexts, 4)

	assert.False(t, statements[1].Parsed())
	assert.Equal(t, []int{1, 1, 1, 2}, groups(contexts))
	assert.True(t, contexts[1].InTransaction)
}
