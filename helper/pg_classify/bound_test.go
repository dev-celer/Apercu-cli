package pg_classify

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// attachedOp is what R-AT-ATTACH reports doing to the partition it attaches.
func attachedOp(t *testing.T, analysis pg_contract.StatementAnalysis, partition string) pg_contract.OpKind {
	t.Helper()

	finding := findingOf(t, analysis, "R-AT-ATTACH")
	for _, target := range finding.Targets {
		if target.Relation.Name.Table == partition {
			return target.OpKind
		}
	}
	require.Failf(t, "missing target", "R-AT-ATTACH does not name %s", partition)
	return pg_contract.OpKindNone
}

func TestAttachBoundProof(t *testing.T) {
	t.Parallel()

	const bound = " FOR VALUES FROM (100) TO (200)"
	cases := []struct {
		name      string
		script    string
		partition string
		proven    bool
	}{
		{name: "a CHECK that says IS NOT NULL and the range", partition: "rd_proved", proven: true,
			script: "ALTER TABLE readings ATTACH PARTITION rd_proved" + bound},
		{name: "IS NOT NULL in a CHECK of its own", partition: "rd_split", proven: true,
			script: "ALTER TABLE readings ATTACH PARTITION rd_split" + bound},
		{name: "a NOT NULL column", partition: "rd_notnull", proven: true,
			script: "ALTER TABLE readings ATTACH PARTITION rd_notnull" + bound},
		{name: "a nullable key the CHECK leaves open", partition: "rd_bound",
			script: "ALTER TABLE readings ATTACH PARTITION rd_bound" + bound},
		{name: "a bound wider than the CHECK", partition: "rd_proved", proven: true,
			script: "ALTER TABLE readings ATTACH PARTITION rd_proved FOR VALUES FROM (MINVALUE) TO (300)"},
		{name: "a bound narrower than the CHECK", partition: "rd_proved",
			script: "ALTER TABLE readings ATTACH PARTITION rd_proved FOR VALUES FROM (100) TO (150)"},
		{name: "a NOT VALID CHECK", partition: "rd_notvalid",
			script: "ALTER TABLE readings ATTACH PARTITION rd_notvalid" + bound},
		{name: "a HASH bound", partition: "rd_proved",
			script: "ALTER TABLE readings_hash ATTACH PARTITION rd_proved FOR VALUES WITH (MODULUS 2, REMAINDER 0)"},
		{name: "a DEFAULT partition", partition: "rd_proved",
			script: "ALTER TABLE readings ATTACH PARTITION rd_proved DEFAULT"},
		{name: "a list CHECK spelling the NULL branch", partition: "rd_list_null", proven: true,
			script: "ALTER TABLE readings_list ATTACH PARTITION rd_list_null FOR VALUES IN (NULL, 1, 2, 3)"},
		{name: "a list CHECK without IS NOT NULL", partition: "rd_list",
			script: "ALTER TABLE readings_list ATTACH PARTITION rd_list FOR VALUES IN (1, 2)"},
		{name: "a CHECK added earlier in the migration", partition: "rd_plain", proven: true,
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND at BETWEEN 100 AND 199); " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "a CHECK added NOT VALID, then validated", partition: "rd_plain", proven: true,
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at >= 100 AND at < 200) NOT VALID; " +
				"ALTER TABLE rd_plain VALIDATE CONSTRAINT b; ALTER TABLE rd_plain ALTER COLUMN at SET NOT NULL; " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "a CHECK added NOT VALID and left so", partition: "rd_plain",
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND at >= 100 AND at < 200) NOT VALID; " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "a fractional constant on an integer key", partition: "rd_plain",
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND at >= 99.5 AND at < 200); " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "an OR of ranges inside the bound", partition: "rd_plain", proven: true,
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND (at >= 100 AND at < 150 OR at >= 150 AND at < 200)); " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "an OR with an arm outside the bound", partition: "rd_plain",
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND (at >= 100 AND at < 150 OR at >= 250)); " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "an OR with an arm the proof cannot read", partition: "rd_plain",
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND (at >= 100 AND at < 150 OR abs(at) < 10)); " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "an unnamed CHECK dropped under the name the server gave it", partition: "rd_plain",
			script: "ALTER TABLE rd_plain ADD CHECK (at IS NOT NULL AND at >= 100 AND at < 200); " +
				"ALTER TABLE rd_plain DROP CONSTRAINT rd_plain_at_check; ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "an unnamed CHECK dropped under a suffix DDL nothing could read made the server pick", partition: "rd_plain",
			script: "DO $$ BEGIN END $$; ALTER TABLE rd_plain ADD CHECK (at IS NOT NULL AND at >= 100 AND at < 200); " +
				"ALTER TABLE rd_plain DROP CONSTRAINT rd_plain_at_check1; ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "an added CHECK renamed", partition: "rd_plain", proven: true,
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND at >= 100 AND at < 200); " +
				"ALTER TABLE rd_plain RENAME CONSTRAINT b TO c; ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "an added CHECK renamed, then dropped", partition: "rd_plain",
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND at >= 100 AND at < 200); " +
				"ALTER TABLE rd_plain RENAME CONSTRAINT b TO c; ALTER TABLE rd_plain DROP CONSTRAINT c; " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "a snapshot CHECK renamed", partition: "rd_proved", proven: true,
			script: "ALTER TABLE rd_proved RENAME CONSTRAINT rd_proved_b TO c; ALTER TABLE readings ATTACH PARTITION rd_proved" + bound},
		{name: "a snapshot CHECK renamed, then dropped", partition: "rd_proved",
			script: "ALTER TABLE rd_proved RENAME CONSTRAINT rd_proved_b TO c; ALTER TABLE rd_proved DROP CONSTRAINT c; " +
				"ALTER TABLE readings ATTACH PARTITION rd_proved" + bound},
		{name: "the key column renamed away and back", partition: "rd_proved", proven: true,
			script: "ALTER TABLE rd_proved RENAME COLUMN at TO old_at; ALTER TABLE rd_proved RENAME COLUMN old_at TO at; " +
				"ALTER TABLE readings ATTACH PARTITION rd_proved" + bound},
		{name: "the key column dropped and added back", partition: "rd_notnull",
			script: "ALTER TABLE rd_notnull DROP COLUMN at; ALTER TABLE rd_notnull ADD COLUMN at int; " +
				"ALTER TABLE readings ATTACH PARTITION rd_notnull" + bound},
		{name: "the key column renamed away and another added under its name", partition: "rd_proved",
			script: "ALTER TABLE rd_proved RENAME COLUMN at TO old_at; ALTER TABLE rd_proved ADD COLUMN at int; " +
				"ALTER TABLE readings ATTACH PARTITION rd_proved" + bound},
		{name: "the table dropped and created again", partition: "rd_proved",
			script: "DROP TABLE rd_proved; CREATE TABLE rd_proved (id bigint NOT NULL, at int); " +
				"ALTER TABLE readings ATTACH PARTITION rd_proved" + bound},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			analyses := analyze(t, nullableKeyCatalog(t), testCase.script)
			last := analyses[len(analyses)-1]
			want := pg_contract.OpKindScan
			if testCase.proven {
				want = pg_contract.OpKindMetadata
			}
			assert.Equal(t, want, attachedOp(t, last, testCase.partition))
			finding := findingOf(t, last, "R-AT-ATTACH")
			if testCase.proven {
				assert.Contains(t, finding.Message, "already imply the partition bound")
				assert.Nil(t, filterV03(last), "a proven bound leaves nothing for V-03 to say")
			} else {
				assert.Contains(t, finding.Message, "is read in full")
			}
		})
	}
}

func TestBoundConstants(t *testing.T) {
	t.Parallel()

	paris, err := time.LoadLocation("Europe/Paris")
	if err != nil {
		t.Skip("no time zone database")
	}
	literal := func(sql string) *pg_parse.Expr {
		statement := pg_parse.ParseOne("ALTER TABLE t ADD CHECK (k = " + sql + ")")
		require.Len(t, statement.Subcommands, 1, sql)
		return &statement.Subcommands[0].Constraint.Expr.Args[1]
	}

	cases := []struct {
		name    string
		domain  valueDomain
		a, b    string
		order   int
		ordered bool
	}{
		{"integers", domainOf("integer", nil), "100", "99", 1, true},
		{"a quoted integer", domainOf("bigint", nil), "'100'", "100", 0, true},
		{"a fraction is not an integer", domainOf("integer", nil), "99.5", "100", 0, false},
		{"numerics", domainOf("numeric(10,2)", nil), "99.5", "100", -1, true},
		{"a cast inside the family", domainOf("integer", nil), "100::bigint", "100", 0, true},
		{"a cast outside it", domainOf("integer", nil), "100::numeric", "100", 0, false},
		{"dates", domainOf("date", nil), "'2025-03-01'", "'2025-02-28'", 1, true},
		{"naive timestamps", domainOf("timestamp without time zone", nil), "'2025-01-01'", "'2025-01-01 00:00:00'", 0, true},
		{"an offset on a timestamp without time zone", domainOf("timestamp without time zone", nil), "'2025-01-01 00:00:00+00'", "'2025-01-01'", 0, false},
		{"zoned against naive with no session zone", domainOf("timestamp with time zone", nil), "'2025-01-01 00:00:00+00'", "'2025-01-01'", 0, false},
		{"zoned against naive in UTC", domainOf("timestamp with time zone", time.UTC), "'2025-01-01 00:00:00+00'", "'2025-01-01'", 0, true},
		{"zoned against naive in Paris", domainOf("timestamp with time zone", paris), "'2024-12-31 23:00:00+00'", "'2025-01-01'", 0, true},
		{"text equality", domainOf("text", nil), "'a'", "'a'", 0, true},
		{"text order is the collation's", domainOf("text", nil), "'a'", "'b'", 0, false},
		{"an unmodelled type", domainOf("uuid", nil), "'a'", "'a'", 0, false},
	}
	for _, testCase := range cases {
		a, okA := testCase.domain.read(literal(testCase.a))
		b, okB := testCase.domain.read(literal(testCase.b))
		order, ok := 0, false
		if okA && okB {
			order, ok = compare(a, b)
		}
		assert.Equal(t, testCase.ordered, ok, testCase.name)
		if testCase.ordered {
			assert.Equal(t, testCase.order, order, testCase.name)
		}
	}
}

func TestGeneratedCheckName(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "t_at_check", objectName("t", "at", "check"))
	assert.Equal(t, "t_check", objectName("t", "", "check"))
	assert.Equal(t, "t_at_check2", objectName("t", "at", "check2"))

	long := strings.Repeat("a", 60)
	name := objectName(long, "column", "check")
	assert.Len(t, name, 63)
	assert.Equal(t, strings.Repeat("a", 50)+"_column_check", name, "the longer name gives way first")

	// A multi-byte character is never split.
	accented := strings.Repeat("é", 40)
	assert.True(t, utf8.ValidString(objectName(accented, "", "check")))

	columns := func(sql string) []string {
		statement := pg_parse.ParseOne("ALTER TABLE t ADD CHECK (" + sql + ")")
		require.Len(t, statement.Subcommands, 1, sql)
		return columnsOf(statement.Subcommands[0].Constraint.Expr)
	}
	assert.Equal(t, []string{"at"}, columns("at IS NOT NULL AND t.at > 0"))
	assert.Equal(t, []string{"a", "b"}, columns("a < b"))
}

// TestBlindStatements checks what lets a DROP CONSTRAINT fall back on guessing: only DDL the file does not spell.
func TestBlindStatements(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sql   string
		blind bool
	}{
		{"DO $$ BEGIN END $$", true},
		{"CALL some_procedure()", true},
		{"CREATE EXTENSION hstore", true},
		{"THIS IS NOT SQL", true},
		{"ALTER TABLE rd_plain ADD CHECK (at > 0)", false},
		{"CREATE TABLE t_new (a int PRIMARY KEY)", false},
		{"SELECT 1", false},
	}
	for _, testCase := range cases {
		t.Run(testCase.sql, func(t *testing.T) {
			classifier := NewClassifier(nullableKeyCatalog(t))
			classifier.Analyze(pg_parse.Parse(testCase.sql))
			assert.Equal(t, testCase.blind, classifier.checks.blind)
		})
	}
}
