package main

import (
	"strings"

	"apercu-cli/helper/pg_parse"

	"github.com/jackc/pgx/v5/pgproto3"
)

// A simple Query holding several statements report invalid per-statement duration.
// This is caused by postgresql batch sending its CommandComplete response.
//
// The proxy need splits the statements and runs them as an extended-protocol pipeline.
//
// This file contain the rewriting logic.

// rewriteQuery expands one simple Query into the extended-protocol messages that run the same statements separately.
// It answers nil for a Query that must be forwarded as it is.
func rewriteQuery(sql string) []pgproto3.FrontendMessage {
	statements := pg_parse.Split(sql)
	if len(statements) < 2 {
		// One statement already completes on its own, and nothing is gained by rewriting it. A
		// Query the scanner found nothing in — ";;" — has nothing to run.
		return nil
	}
	for _, statement := range statements {
		if !rewritable(statement) {
			return nil
		}
	}

	messages := make([]pgproto3.FrontendMessage, 0, len(statements)*5+1)
	for _, statement := range statements {
		messages = append(messages,
			&pgproto3.Parse{Query: statement},
			&pgproto3.Bind{},
			// Describe is used only to get back RowDescription, which simple protocol ask for implicitly.
			&pgproto3.Describe{ObjectType: 'P'},
			&pgproto3.Execute{},
			// Calling flush after execute to prevent batch sending the response.
			&pgproto3.Flush{},
		)
	}
	return append(messages, &pgproto3.Sync{})
}

// rewritable reports whether one statement can run as an extended-protocol Execute and look the same to the client.
func rewritable(statement string) bool {
	// COPY carries its own message choreography — CopyInResponse and the client's CopyData, or
	// CopyOutResponse and the server's — and rewriting the statement it belongs to would leave the
	// proxy translating a conversation rather than a statement.
	return !startsWith(statement, "copy")
}

// startsWith reports whether the statement's first keyword match the one being passed.
func startsWith(statement, keyword string) bool {
	rest := strings.TrimSpace(statement)
	for {
		switch {
		case strings.HasPrefix(rest, "--"):
			_, after, found := strings.Cut(rest, "\n")
			if !found {
				return false
			}
			rest = strings.TrimSpace(after)
		case strings.HasPrefix(rest, "/*"):
			_, after, found := strings.Cut(rest, "*/")
			if !found {
				return false
			}
			rest = strings.TrimSpace(after)
		default:
			word, _, _ := strings.Cut(rest, " ")
			return strings.EqualFold(strings.TrimRight(word, "("), keyword)
		}
	}
}

// rewriteNoise reports whether a message from the server is bookkeeping the rewrite caused.
func rewriteNoise(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	case *pgproto3.ParseComplete, *pgproto3.BindComplete,
		*pgproto3.ParameterDescription, *pgproto3.NoData:
		return true
	default:
		return false
	}
}
