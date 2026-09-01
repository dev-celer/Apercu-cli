package pg_parse

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Split cuts one proxy event's SQL into the statements it holds.
func Split(sql string) []string {
	statements, err := pg_query.SplitWithScanner(sql, true)
	if err != nil {
		// The scanner only fails on input it cannot tokenize at all. Handing the whole event on
		// as one statement keeps the caller classifying instead of dropping it.
		if trimmed := strings.TrimSpace(sql); trimmed != "" {
			return []string{trimmed}
		}
		return nil
	}

	out := make([]string, 0, len(statements))
	for _, statement := range statements {
		if trimmed := strings.TrimSpace(statement); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// Parse turns one proxy event's SQL into the IR, one Statement per statement it holds.
func Parse(sql string) []Statement {
	pieces := Split(sql)
	out := make([]Statement, 0, len(pieces))
	for _, piece := range pieces {
		out = append(out, ParseOne(piece))
	}
	return out
}

// ParseOne normalises a single statement. A statement nothing can parse comes back with Unparsed set.
func ParseOne(sql string) Statement {
	statement := Statement{RawSQL: sql}

	tree, err := pg_query.Parse(sql)
	if err == nil {
		return normalize(tree, statement)
	}

	// pg_query parser failed, try to shim PG18+ statement before classifying as unparsed.
	shimmed, features, ok := shimPg18(sql)
	if !ok {
		// Failed to understand the statement of failed to shim anything
		statement.Unparsed = err.Error()
		return statement
	}
	tree, shimErr := pg_query.Parse(shimmed)
	if shimErr != nil {
		// Parsing failed even after shimming
		statement.Unparsed = err.Error()
		return statement
	}

	statement.Features = features
	return normalize(tree, statement)
}

// normalize converts the single statement a parse tree holds.
func normalize(tree *pg_query.ParseResult, statement Statement) Statement {
	if tree == nil || len(tree.Stmts) == 0 || tree.Stmts[0].Stmt == nil {
		statement.Command = "EMPTY"
		return statement
	}
	return normalizeNode(tree.Stmts[0].Stmt, statement)
}
