package pgclassify_test

import (
	"testing"

	"apercu-cli/internal/pgclassify"
	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgparse"

	"github.com/stretchr/testify/assert"
)

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
			classifier := pgclassify.NewClassifier(classifytest.NullableKeyCatalog(t))
			classifier.Analyze(pgparse.Parse(testCase.sql))
			assert.Equal(t, testCase.blind, classifier.Checks().Blind)
		})
	}
}
