package main

import (
	"apercu-cli/helper/metrics"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStripLeadingComments(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"only whitespace", "   \t\n", ""},
		{"plain sql untouched", "SELECT 1", "SELECT 1"},
		{"leading whitespace trimmed", "   SELECT 1", "SELECT 1"},
		{"single line comment", "-- hi\nSELECT 1", "SELECT 1"},
		{"multiple line comments", "-- hi\n-- ho\nSELECT 1", "SELECT 1"},
		{"block comment", "/* hello */ SELECT 1", "SELECT 1"},
		{"line then block", "-- one\n/* two */ SELECT 1", "SELECT 1"},
		{"unterminated line comment yields empty", "-- forever", ""},
		{"unterminated block comment yields empty", "/* forever", ""},
		{"comment after sql is preserved", "SELECT /* mid */ 1", "SELECT /* mid */ 1"},
		{"line comment with crlf-ish", "-- hi\nSELECT 1\n", "SELECT 1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := stripLeadingComments(tc.in); got != tc.want {
				t.Errorf("stripLeadingComments(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestCollapseSpaces(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"single token", "SELECT", "SELECT"},
		{"runs of spaces collapse", "SELECT   *", "SELECT *"},
		{"tab becomes space", "SELECT\t*", "SELECT *"},
		{"newline becomes space", "SELECT\n*", "SELECT *"},
		{"mixed whitespace collapses", "SELECT \t\n*", "SELECT *"},
		{"leading and trailing not trimmed", "  SELECT  ", " SELECT "},
		{"whitespace inside double quotes preserved", `SELECT * FROM "my  table"`, `SELECT * FROM "my  table"`},
		{"newline inside double quotes preserved", "\"a\nb\"", "\"a\nb\""},
		{"whitespace inside single quotes preserved", `SELECT 'a   b'`, `SELECT 'a   b'`},
		{"escaped doubled double quote", `"foo""bar"`, `"foo""bar"`},
		{"escaped doubled single quote", `'a''b'`, `'a''b'`},
		{"mixed inside and outside literals", `SELECT   "x  y" ,   'a  b'`, `SELECT "x  y" , 'a  b'`},
		{"empty quoted", `""`, `""`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := collapseSpaces(tc.in); got != tc.want {
				t.Errorf("collapseSpaces(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestGetLockTimeoutValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sql       string
		wantBool  bool
		wantValue *int64
	}{
		{"empty", "", false, nil},
		{"unrelated query", "SELECT * FROM users", false, nil},
		{"other set query", "SET my_value TO 'test'", false, nil},

		{"set integer", "SET lock_timeout = 5000", true, new(int64(5000))},
		{"set default", "SET lock_timeout = DEFAULT", true, nil},
		{"reset", "reset lock_timeout", true, nil},
		{"reset all", "reset all", true, nil},
		{"set duration string", "SET lock_timeout = '5s'", true, new(int64(5000))},
		{"set quoted integer", "SET lock_timeout = '5000'", true, new(int64(5000))},
		{"set to", "SET lock_timeout TO 5000", true, new(int64(5000))},
		{"set local", "SET LOCAL lock_timeout = 5000", true, new(int64(5000))},
		{"set session", "SET SESSION lock_timeout = 5000", true, new(int64(5000))},
		{"alter system set", "ALTER SYSTEM SET lock_timeout = 5000", true, new(int64(5000))},
		{"set without space", "SET lock_timeout=5000", true, new(int64(5000))},
		{"set with one space", "SET lock_timeout= 5000", true, new(int64(5000))},
		{"set with one space 2", "SET lock_timeout =5000", true, new(int64(5000))},
		{"set with trailing semicolon", "SET lock_timeout = 5000;", true, new(int64(5000))},
		{"set with 1min value", "SET lock_timeout = '1min'", true, new(int64(60000))},
		{"set with 1d value", "SET lock_timeout = '1d'", true, new(int64(86400000))},

		{"invalid duration string", "SET lock_timeout = 'test'", false, nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ev := &metrics.QueryEvent{SQL: tc.sql}
			gotBool, gotValue := getLockTimeoutValue(ev)
			assert.Equal(t, tc.wantBool, gotBool)
			if tc.wantValue != nil {
				assert.NotNil(t, gotValue)
				if gotValue != nil {
					assert.Equal(t, *tc.wantValue, *gotValue)
				}
			} else {
				assert.Nil(t, gotValue)
			}
		})
	}
}
