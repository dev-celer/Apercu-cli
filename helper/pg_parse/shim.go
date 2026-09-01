package pg_parse

import (
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// The bundled libpg_query is 17, so the PG18 grammar does not parse.
//
// The shim works on tokens. It rewrites each PG18-only construct into the nearest PG17 equivalent, records what it removed, and re-parses the residue.

// token is one lexeme, with where it came from and where it sits in the statement's structure.
// it is a wrapper around the output of the pg_query.Scan
type token struct {
	text  string
	upper string
	start int
	end   int
	// depth is how many parentheses enclose the token.
	depth int
	// clause is how many commas outside parentheses precede the token.
	// This corresponds to the subcommand of an ALTER statement the token is a part of
	clause int
	// subclause counts how many commas inside parentheses precede the token.
	// For example, this corresponds to which column of an CREATE TABLE the token is a part of
	// example: CREATE TABLE t (subclause 0, subclause 1, subclause 2)
	subclause int
	// word is true for an identifier or a keyword, the only tokens the shim matches on.
	word bool
}

// edit is one replacement the shim makes. Blanking with spaces rather than deleting keeps the
// statement's other offsets readable in an error message.
type edit struct {
	start, end int
	with       string
}

// tokenize scans a statement and drops its comments.
func tokenize(sql string) ([]token, error) {
	scanned, err := pg_query.Scan(sql)
	if err != nil {
		return nil, err
	}

	tokens := make([]token, 0, len(scanned.Tokens))
	depth, clause, subclause := 0, 0, 0
	for _, scan := range scanned.Tokens {
		if scan.Token == pg_query.Token_SQL_COMMENT || scan.Token == pg_query.Token_C_COMMENT {
			continue
		}
		if int(scan.End) > len(sql) {
			break
		}
		text := sql[scan.Start:scan.End]

		switch text {
		case ")":
			if depth > 0 {
				depth--
			}
			if depth == 0 {
				// A closed top-level group ends the run of subclauses it held.
				subclause = 0
			}
		case ",":
			switch depth {
			case 0:
				clause++
				subclause = 0
			case 1:
				subclause++
			}
		}

		tokens = append(tokens, token{
			text:      text,
			upper:     strings.ToUpper(text),
			start:     int(scan.Start),
			end:       int(scan.End),
			depth:     depth,
			clause:    clause,
			subclause: subclause,
			word:      scan.Token == pg_query.Token_IDENT || scan.KeywordKind != pg_query.KeywordKind_NO_KEYWORD,
		})

		if text == "(" {
			depth++
		}
	}
	return tokens, nil
}

// shimPg18 rewrites the PG18-only syntax out of a statement.
// It reports false when it recognized nothing, or failed to tokenize
func shimPg18(sql string) (string, []Feature, bool) {
	tokens, err := tokenize(sql)
	if err != nil {
		return "", nil, false
	}

	var edits []edit
	var features []Feature
	record := func(e edit, name FeatureName, at token, detail string) {
		edits = append(edits, e)
		for _, existing := range features {
			if existing.Name == name && existing.clause == at.clause && existing.subclause == at.subclause {
				return
			}
		}
		feature := newFeature(name, at.clause, at.subclause, detail)
		feature.deferralNamed = clauseNamesDeferral(tokens, at)
		features = append(features, feature)
	}

	command := ""
	if len(tokens) > 0 {
		command = tokens[0].upper
	}

	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		if !tok.word {
			continue
		}

		switch tok.upper {
		case "ENFORCED":
			// PG17 knows no ENFORCED keyword, so the token is an identifier here and cannot be anything else at this position.
			//
			// ALTER TABLE t ADD CONSTRAINT c CHECK (x > 0) NOT ENFORCED	-- PG18+
			// ALTER TABLE t ALTER CONSTRAINT c NOT ENFORCED				-- PG18+
			// ALTER TABLE t ALTER CONSTRAINT c ENFORCED					-- PG18+
			if i > 0 && tokens[i-1].upper == "NOT" && tokens[i-1].word {
				record(edit{tokens[i-1].start, tok.end, blanks(tok.end - tokens[i-1].start)},
					FeatureNotEnforced, tok, "NOT ENFORCED")
			} else {
				record(edit{tok.start, tok.end, blanks(tok.end - tok.start)},
					FeatureNotEnforced, tok, "ENFORCED")
			}

		case "INHERIT":
			// INHERIT is PG18-only directly after ALTER CONSTRAINT <name>.
			// Elsewhere, it is accepted by every supported version
			//
			// ALTER TABLE t ALTER CONSTRAINT c INHERIT					-- PG18+
			// ALTER TABLE t ALTER CONSTRAINT c NO INHERIT				-- PG18+
			// ALTER TABLE t INHERIT p									-- PG15+
			// ALTER TABLE t ADD CONSTRAINT c CHECK (x > 0) NO INHERIT	-- PG15+
			start, detail, at := tok.start, "INHERIT", i
			if i > 0 && tokens[i-1].upper == "NO" && tokens[i-1].word {
				start, detail, at = tokens[i-1].start, "NO INHERIT", i-1
			}
			if !precededByNamed(tokens, at, "ALTER", "CONSTRAINT") {
				continue
			}
			record(edit{start, tok.end, blanks(tok.end - start)},
				FeatureConstraintInherit, tok, detail)

		case "NULL":
			// A NOT NULL written as a table constraint names its column, which is what separates
			// it from the column constraint every version accepts.
			//
			// ALTER TABLE t ADD CONSTRAINT c NOT NULL a	-- PG18+
			// ALTER TABLE t ADD NOT NULL a					-- PG18+
			// ALTER TABLE t ALTER COLUMN a SET NOT NULL	-- PG15+
			//
			// No PG17 spelling exists, so it becomes the CHECK it is equivalent to and the IR is corrected afterward.
			if i == 0 || tokens[i-1].upper != "NOT" || !tokens[i-1].word {
				continue
			}
			if i+1 >= len(tokens) || !tokens[i+1].word {
				continue
			}
			if !precededByNamed(tokens, i-1, "ADD", "CONSTRAINT") && !precededBy(tokens, i-1, "ADD") {
				continue
			}
			column := tokens[i+1].text
			record(edit{tokens[i-1].start, tokens[i+1].end, "CHECK (" + column + " IS NOT NULL)"},
				FeatureNotNullConstraint, tok, column)
			i++

		case "VIRTUAL":
			// Only meaningful as the storage keyword closing a GENERATED clause.
			// Anywhere else it is an ordinary identifier.
			//
			// ALTER TABLE t ADD COLUMN g int GENERATED ALWAYS AS (a + 1) VIRTUAL	-- PG18+
			// ALTER TABLE t ADD COLUMN g int GENERATED ALWAYS AS (a + 1) STORED	-- PG15+
			if i == 0 || tokens[i-1].text != ")" || !clauseContainsBefore(tokens, i, "GENERATED") {
				continue
			}
			record(edit{tok.start, tok.end, "STORED "}, FeatureVirtualGenerated, tok, "")

		case "GENERATED":
			// With neither STORED nor VIRTUAL the column is virtual on 18 and a syntax error below,
			// STORED is appended to make it parse and the feature records the ambiguity.
			//
			// ALTER TABLE t ADD COLUMN g int GENERATED ALWAYS AS (a + 1)			-- PG18+, and means VIRTUAL there
			// ALTER TABLE t ADD COLUMN g int GENERATED ALWAYS AS (a + 1) STORED	-- PG15+
			// ALTER TABLE t ADD COLUMN g int GENERATED ALWAYS AS IDENTITY			-- PG15+
			end, ok := generatedExprEnd(tokens, i)
			if !ok {
				continue
			}
			if end+1 < len(tokens) && tokens[end+1].word {
				switch tokens[end+1].upper {
				case "STORED", "VIRTUAL":
					continue
				}
			}
			record(edit{tokens[end].end, tokens[end].end, " STORED"},
				FeatureBareGenerated, tok, "")

		case "WITHOUT":
			// WITHOUT OVERLAPS makes the last key column a temporal range, PG18 only.
			//
			// ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY (id, v WITHOUT OVERLAPS)	-- PG18+
			// ALTER TABLE t SET WITHOUT CLUSTER									-- PG15+
			// ALTER TABLE t SET WITHOUT OIDS										-- PG15+
			if i+1 >= len(tokens) || tokens[i+1].upper != "OVERLAPS" || !tokens[i+1].word {
				continue
			}
			record(edit{tok.start, tokens[i+1].end, blanks(tokens[i+1].end - tok.start)},
				FeatureWithoutOverlaps, tok, "")
			i++

		case "PERIOD":
			// PERIOD prefixes a column inside a foreign key's column list.
			//
			// ALTER TABLE t ADD FOREIGN KEY (id, PERIOD v) REFERENCES r (id, PERIOD v)	-- PG18+
			if tok.depth == 0 || i == 0 || i+1 >= len(tokens) || !tokens[i+1].word {
				continue
			}
			if tokens[i-1].text != "(" && tokens[i-1].text != "," {
				continue
			}
			if !clauseContainsBefore(tokens, i, "REFERENCES") && !clauseContainsBefore(tokens, i, "KEY") {
				continue
			}
			record(edit{tok.start, tok.end, blanks(tok.end - tok.start)},
				FeatureForeignKeyPeriod, tok, "")

		case "ONLY":
			// VACUUM and ANALYZE gained ONLY in 18. Every other ONLY predates the supported range.
			//
			// VACUUM ONLY t						-- PG18+
			// ANALYZE ONLY t						-- PG18+
			// TRUNCATE ONLY t						-- PG15+
			// ALTER TABLE ONLY t ADD COLUMN a int	-- PG15+
			if tok.depth != 0 {
				continue
			}
			name := FeatureVacuumOnly
			switch command {
			case "VACUUM":
			case "ANALYZE", "ANALYSE":
				name = FeatureAnalyzeOnly
			default:
				continue
			}
			record(edit{tok.start, tok.end, blanks(tok.end - tok.start)}, name, tok, "")
		}
	}

	if len(edits) == 0 {
		return "", nil, false
	}
	return applyEdits(sql, edits), features, true
}

// precededBy reports whether the words run immediately before the token at i.
func precededBy(tokens []token, i int, words ...string) bool {
	start := i - len(words)
	if start < 0 {
		return false
	}
	for offset, want := range words {
		at := start + offset
		if !tokens[at].word || tokens[at].upper != want {
			return false
		}
	}
	return true
}

// precededByNamed is precededBy for the shapes that put an object name between the keywords and the token:
// ADD CONSTRAINT <name> NOT NULL, ALTER CONSTRAINT <name> INHERIT.
func precededByNamed(tokens []token, i int, words ...string) bool {
	if i-1 < 0 || !tokens[i-1].word {
		return false
	}
	return precededBy(tokens, i-1, words...)
}

// clauseNamesDeferral reports whether the clause holding the token also spells a deferral out.
func clauseNamesDeferral(tokens []token, at token) bool {
	for _, tok := range tokens {
		if !tok.word || tok.clause != at.clause || tok.subclause != at.subclause {
			continue
		}
		switch tok.upper {
		case "DEFERRABLE", "INITIALLY":
			return true
		}
	}
	return false
}

// clauseContainsBefore reports whether the word appears earlier in the same clause.
func clauseContainsBefore(tokens []token, i int, word string) bool {
	for at := clauseStart(tokens, i); at < i; at++ {
		if tokens[at].word && tokens[at].upper == word {
			return true
		}
	}
	return false
}

func clauseStart(tokens []token, i int) int {
	clause := tokens[i].clause
	start := i
	for start > 0 && tokens[start-1].clause == clause {
		start--
	}
	// The comma that opened the clause belongs to the previous one only by position.
	if tokens[start].text == "," {
		start++
	}
	return start
}

// generatedExprEnd finds the closing parenthesis of a GENERATED … AS ( … ) clause,
// or reports false for the identity form, which takes no expression.
func generatedExprEnd(tokens []token, i int) (int, bool) {
	at := i + 1
	for at < len(tokens) && tokens[at].word && tokens[at].upper != "AS" {
		at++
	}
	if at >= len(tokens) || tokens[at].upper != "AS" {
		return 0, false
	}
	at++
	if at >= len(tokens) || tokens[at].text != "(" {
		return 0, false
	}
	depth := tokens[at].depth
	for at++; at < len(tokens); at++ {
		if tokens[at].text == ")" && tokens[at].depth == depth {
			return at, true
		}
	}
	return 0, false
}

func blanks(n int) string { return strings.Repeat(" ", n) }

// applyEdits rebuilds the statement with every replacement in place.
func applyEdits(sql string, edits []edit) string {
	sort.SliceStable(edits, func(a, b int) bool { return edits[a].start < edits[b].start })

	var out strings.Builder
	out.Grow(len(sql))
	at := 0
	for _, e := range edits {
		if e.start < at {
			continue
		}
		out.WriteString(sql[at:e.start])
		out.WriteString(e.with)
		at = e.end
	}
	out.WriteString(sql[at:])
	return out.String()
}
