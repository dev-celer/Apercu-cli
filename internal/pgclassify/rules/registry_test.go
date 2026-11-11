package rules_test

import (
	"testing"

	"apercu-cli/internal/pgclassify/classifytest"
)

// TestTransactionAndSessionControlClassifyToNothing is the commands the registry deliberately ignores.
func TestTransactionAndSessionControlClassifyToNothing(t *testing.T) {
	t.Parallel()

	cases := []ruleCase{
		{name: "R-TX-BEGIN", sql: "BEGIN"},
		{name: "R-TX-COMMIT", sql: "COMMIT"},
		{name: "R-TX-ROLLBACK", sql: "ROLLBACK"},
		{name: "R-TX-SAVEPOINT", sql: "SAVEPOINT s"},
		{name: "R-TX-SET", sql: "SET LOCAL lock_timeout = '5s'"},
		{name: "R-TX-RESET", sql: "RESET ALL"},
	}

	catalog := classifytest.Catalog(t)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, catalog) })
	}
}
