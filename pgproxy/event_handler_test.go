package main

import (
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

func TestStatementExtraction(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "a named migration keeps its statement",
			sql:  "-- name: add the column\nALTER TABLE t ADD COLUMN a int",
			want: "ALTER TABLE t ADD COLUMN a int",
		},
		{
			name: "several leading comments of both kinds",
			sql:  "-- migrate:up\n/* tool */\n-- and again\nSELECT\n\t1,\n\t2",
			want: "SELECT 1, 2",
		},
		{
			name: "a comment with nothing after it is nothing",
			sql:  "-- just a note",
			want: "",
		},
		{
			name: "a newline inside a literal is data and stays",
			sql:  "INSERT INTO t VALUES ('a\nb')",
			want: "INSERT INTO t VALUES ('a\nb')",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.want, collapseSpaces(stripLeadingComments(testCase.sql)))
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
