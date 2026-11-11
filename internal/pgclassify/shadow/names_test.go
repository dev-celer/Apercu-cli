package shadow

import (
	"strings"
	"testing"
	"unicode/utf8"

	"apercu-cli/internal/pgclassify/stmt"
	"apercu-cli/internal/pgparse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		statement := pgparse.ParseOne("ALTER TABLE t ADD CHECK (" + sql + ")")
		require.Len(t, statement.Subcommands, 1, sql)
		return stmt.ColumnsOf(statement.Subcommands[0].Constraint.Expr)
	}
	assert.Equal(t, []string{"at"}, columns("at IS NOT NULL AND t.at > 0"))
	assert.Equal(t, []string{"a", "b"}, columns("a < b"))
}
