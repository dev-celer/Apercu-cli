package pg_classify

import (
	"testing"
	"time"

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
