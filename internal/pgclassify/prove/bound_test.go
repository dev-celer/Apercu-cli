package prove

import (
	"testing"
	"time"

	"apercu-cli/internal/pgparse"
	"apercu-cli/internal/pgparse/ir"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoundConstants(t *testing.T) {
	t.Parallel()

	paris, err := time.LoadLocation("Europe/Paris")
	if err != nil {
		t.Skip("no time zone database")
	}
	literal := func(sql string) *ir.Expr {
		statement := pgparse.ParseOne("ALTER TABLE t ADD CHECK (k = " + sql + ")")
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
