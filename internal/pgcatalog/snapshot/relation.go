package snapshot

import (
	"regexp"
	"strings"

	"apercu-cli/internal/pgcontract"
)

// Contract converts a Relation into the vocabulary the findings speak.
func (r Relation) Contract() pgcontract.Relation {
	return pgcontract.Relation{
		Name: r.RelationName(),
		Kind: pgcontract.RelationKindFromRelkind(r.Kind),
	}
}

// PartitionKeyColumns splits a partitioned table's PartitionKey, as pg_get_partkeydef spells it, into its
// strategy and the columns it partitions on. A key element that is an expression has no column and is left out.
func (r Relation) PartitionKeyColumns() (string, []string) {
	open, closing := strings.Index(r.PartitionKey, "("), strings.LastIndex(r.PartitionKey, ")")
	if open < 0 || closing < open {
		return "", nil
	}
	strategy := strings.TrimSpace(r.PartitionKey[:open])

	var columns []string
	for _, element := range splitTopLevel(r.PartitionKey[open+1 : closing]) {
		if column, ok := leadingIdentifier(strings.TrimSpace(element)); ok {
			columns = append(columns, column)
		}
	}
	return strategy, columns
}

// splitTopLevel splits a list on the commas that sit outside parentheses and double quotes.
func splitTopLevel(list string) []string {
	var parts []string
	depth, quoted, start := 0, false, 0
	for i, char := range list {
		switch {
		case char == '"':
			quoted = !quoted
		case quoted:
		case char == '(':
			depth++
		case char == ')':
			depth--
		case char == ',' && depth == 0:
			parts = append(parts, list[start:i])
			start = i + 1
		}
	}
	return append(parts, list[start:])
}

// leadingIdentifier is the column a partition key element names, when it names one: a bare or quoted
// identifier, optionally followed by a COLLATE clause or an operator class.
func leadingIdentifier(element string) (string, bool) {
	var name, rest string
	if strings.HasPrefix(element, `"`) {
		end := 1
		for end < len(element) {
			if element[end] == '"' {
				if end+1 < len(element) && element[end+1] == '"' {
					end += 2
					continue
				}
				break
			}
			end++
		}
		if end >= len(element) {
			return "", false
		}
		name, rest = strings.ReplaceAll(element[1:end], `""`, `"`), element[end+1:]
	} else {
		match := identifierPattern.FindString(element)
		if match == "" {
			return "", false
		}
		name, rest = match, element[len(match):]
	}
	if rest != "" && !strings.HasPrefix(rest, " ") {
		// A call or an operator: the element is an expression.
		return "", false
	}
	return name, true
}

var identifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_$]*`)
