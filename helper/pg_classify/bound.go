package pg_classify

import (
	"math/big"
	"regexp"
	"slices"
	"strings"
	"time"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_parse"
)

// boundProven reports whether the partition's validated CHECKs and NOT NULLs already imply the constraint the bound puts on it.
func (s scope) boundProven(parent, partition pg_catalog.RelationInfo, bound *pg_parse.PartitionBound) bool {
	if bound == nil || !parent.Exists() || !partition.Exists() {
		return false
	}

	var predicate []disjunction
	var keys []string
	domains := map[string]valueDomain{}
	for level, levelBound := parent, bound; ; {
		terms, key, domain, ok := s.levelConstraint(level, levelBound)
		if !ok {
			return false
		}
		predicate = append(predicate, terms...)
		if _, seen := domains[key]; !seen {
			keys = append(keys, key)
			domains[key] = domain
		}

		above, ok := s.partitionParent(level)
		if !ok {
			break
		}
		if level.Relation.PartitionBound == "" {
			// A partition the migration attached has no bound in the snapshot to add.
			return false
		}
		parsed := pg_parse.ParseOne("CREATE TABLE t PARTITION OF p " + level.Relation.PartitionBound)
		if !parsed.Parsed() || len(parsed.Subcommands) == 0 || parsed.Subcommands[len(parsed.Subcommands)-1].Bound == nil {
			return false
		}
		level, levelBound = above, parsed.Subcommands[len(parsed.Subcommands)-1].Bound
	}

	var clauses []knownClause
	for _, key := range keys {
		if s.neverNull(partition, key) {
			clauses = append(clauses, knownClause{{{kind: atomNotNull, column: key}}})
		}
	}
	for _, check := range s.checksOn(partition) {
		clauses = append(clauses, clausesOf(check.expr, domains)...)
	}

	for _, term := range predicate {
		if !implied(term, clauses) {
			return false
		}
	}
	return true
}

// levelConstraint is the constraint one level's bound implies.
func (s scope) levelConstraint(parent pg_catalog.RelationInfo, bound *pg_parse.PartitionBound) ([]disjunction, string, valueDomain, bool) {
	if bound.Default {
		return nil, "", valueDomain{}, false
	}
	strategy, keys := parent.Relation.PartitionKeyColumns()
	if len(keys) != 1 || strategy != bound.Strategy {
		return nil, "", valueDomain{}, false
	}
	column, ok := s.catalog.Column(parent.Relation.OID, keys[0])
	if !ok {
		return nil, "", valueDomain{}, false
	}
	domain := domainOf(column.FormattedType, s.context.zone())
	key := s.checks.currentColumn(parent.Key(), keys[0])
	terms, ok := partitionConstraint(key, domain, bound)
	return terms, key, domain, ok
}

func (s scope) partitionParent(child pg_catalog.RelationInfo) (pg_catalog.RelationInfo, bool) {
	for _, link := range s.catalog.Parents(child.Key()) {
		if !link.ParentIsPartitioned {
			continue
		}
		return s.catalog.RelationByKey(link.Parent)
	}
	return pg_catalog.RelationInfo{}, false
}

// atomKind is what one comparison in a constraint says about a column.
type atomKind uint8

const (
	atomNotNull atomKind = iota
	atomNull
	atomCompare
)

// atom is one test of a column: IS [NOT] NULL, or a comparison with a constant.
type atom struct {
	kind   atomKind
	column string
	// op is one of = < <= > >=, for atomCompare.
	op    string
	value constant
}

// disjunction is atoms joined by OR
type disjunction []atom

// knownClause is one conjunct of the table's own constraints: arms joined by OR, each arm atoms joined by AND.
type knownClause [][]atom

// partitionConstraint build the constraint required by the bound.
func partitionConstraint(key string, domain valueDomain, bound *pg_parse.PartitionBound) ([]disjunction, bool) {
	notNull := disjunction{{kind: atomNotNull, column: key}}
	switch bound.Strategy {
	case "RANGE":
		if len(bound.From) != 1 || len(bound.To) != 1 {
			return nil, false
		}
		terms := []disjunction{notNull}
		for _, side := range []struct {
			datum pg_parse.Expr
			op    string
		}{{bound.From[0], ">="}, {bound.To[0], "<"}} {
			if side.datum.IsColumnRef("minvalue") || side.datum.IsColumnRef("maxvalue") {
				continue
			}
			value, ok := domain.read(&side.datum)
			if !ok {
				return nil, false
			}
			terms = append(terms, disjunction{{kind: atomCompare, column: key, op: side.op, value: value}})
		}
		return terms, true

	case "LIST":
		// The server only expands an array of at most 100 values into the OR the proof walks.
		if len(bound.In) > 100 {
			return nil, false
		}
		var values disjunction
		acceptsNull := false
		for i := range bound.In {
			if bound.In[i].Kind == pg_parse.ExprConst && bound.In[i].Literal == "NULL" {
				acceptsNull = true
				continue
			}
			value, ok := domain.read(&bound.In[i])
			if !ok {
				return nil, false
			}
			values = append(values, atom{kind: atomCompare, column: key, op: "=", value: value})
		}
		if acceptsNull {
			return []disjunction{append(disjunction{{kind: atomNull, column: key}}, values...)}, true
		}
		return []disjunction{notNull, values}, true
	}
	return nil, false
}

// implied reports whether one clause implies the term on its own: every arm of the clause has an atom that implies an arm
// of the term. An atom the arm ANDs with it only narrows the arm further.
func implied(term disjunction, clauses []knownClause) bool {
	for _, candidate := range clauses {
		if len(candidate) > 0 && !slices.ContainsFunc(candidate, func(arm []atom) bool { return !armImplies(arm, term) }) {
			return true
		}
	}
	return false
}

func armImplies(arm []atom, term disjunction) bool {
	for _, have := range arm {
		for _, wanted := range term {
			if atomImplies(have, wanted) {
				return true
			}
		}
	}
	return false
}

// atomImplies is weak implication between two tests of a column: whenever the first is not false, neither is the second.
// A comparison is NULL on a NULL column, which counts as not false, so it never proves IS NOT NULL.
func atomImplies(have, predicate atom) bool {
	if have.column != predicate.column || have.kind != predicate.kind {
		return false
	}
	if have.kind != atomCompare {
		return true
	}
	return rangeOf(have).within(rangeOf(predicate))
}

// maxArms bounds the OR arms an AND of ORs multiplies into; past it the clause is dropped.
const maxArms = 64

// clausesOf reads a CHECK expression into the clauses its top-level AND joins. A conjunct it cannot read is left out:
// it proves nothing here.
func clausesOf(expr *pg_parse.Expr, domains map[string]valueDomain) []knownClause {
	if expr == nil {
		return nil
	}
	if expr.Kind == pg_parse.ExprBoolean && len(expr.Name) == 1 && expr.Name[0] == "AND" {
		var out []knownClause
		for i := range expr.Args {
			out = append(out, clausesOf(&expr.Args[i], domains)...)
		}
		return out
	}
	if arms, ok := armsOf(expr, domains); ok && len(arms) > 0 {
		return []knownClause{arms}
	}
	return nil
}

// armsOf reads an expression into OR arms of ANDed atoms. An AND may drop a part it cannot read, since an arm that says
// less is still implied by the CHECK; an OR may not, since the arm it drops could be the one a row satisfies.
func armsOf(expr *pg_parse.Expr, domains map[string]valueDomain) (knownClause, bool) {
	switch expr.Kind {
	case pg_parse.ExprBoolean:
		if len(expr.Name) != 1 {
			return nil, false
		}
		switch expr.Name[0] {
		case "OR":
			var out knownClause
			for i := range expr.Args {
				arms, ok := armsOf(&expr.Args[i], domains)
				if !ok {
					return nil, false
				}
				out = append(out, arms...)
			}
			return out, len(out) <= maxArms
		case "AND":
			out := knownClause{{}}
			for i := range expr.Args {
				arms, ok := armsOf(&expr.Args[i], domains)
				if !ok {
					continue
				}
				var product knownClause
				for _, left := range out {
					for _, right := range arms {
						product = append(product, append(slices.Clone(left), right...))
					}
				}
				if len(product) > maxArms {
					continue
				}
				out = product
			}
			return out, true
		case "NOT":
			// NOT (c IS NULL) is how a CHECK sometimes spells IS NOT NULL.
			if len(expr.Args) == 1 && expr.Args[0].Kind == pg_parse.ExprNullTest {
				test, ok := nullTest(&expr.Args[0])
				if !ok {
					return nil, false
				}
				if test.kind == atomNull {
					test.kind = atomNotNull
				} else {
					test.kind = atomNull
				}
				return knownClause{{test}}, true
			}
		}
		return nil, false

	case pg_parse.ExprNullTest:
		test, ok := nullTest(expr)
		if !ok {
			return nil, false
		}
		return knownClause{{test}}, true

	case pg_parse.ExprOperator:
		if len(expr.Args) != 2 {
			return nil, false
		}
		op := operatorName(expr)
		switch {
		case expr.Form == "" && op == "BETWEEN":
			// The server stores BETWEEN as the two comparisons it stands for.
			column, ok := columnOf(&expr.Args[0])
			bounds := expr.Args[1]
			if !ok || bounds.Kind != pg_parse.ExprList || len(bounds.Args) != 2 {
				return nil, false
			}
			var arm []atom
			for i, op := range []string{">=", "<="} {
				if value, ok := domains[column].read(&bounds.Args[i]); ok {
					arm = append(arm, atom{kind: atomCompare, column: column, op: op, value: value})
				}
			}
			return knownClause{arm}, true
		case expr.Form == "":
			comparison, ok := comparisonOf(op, &expr.Args[0], &expr.Args[1], domains)
			if !ok {
				return nil, false
			}
			return knownClause{{comparison}}, true
		case expr.Form == "ANY":
			// IN (…) and = ANY (ARRAY[…]) are an OR of equalities, the way the server expands them.
			column, ok := columnOf(&expr.Args[0])
			list := stripCasts(&expr.Args[1])
			if op != "=" || !ok || list.Kind != pg_parse.ExprList || len(list.Args) > 100 {
				return nil, false
			}
			var out knownClause
			for i := range list.Args {
				value, ok := domains[column].read(&list.Args[i])
				if !ok {
					return nil, false
				}
				out = append(out, []atom{{kind: atomCompare, column: column, op: "=", value: value}})
			}
			return out, len(out) > 0
		}
	}
	return nil, false
}

func nullTest(expr *pg_parse.Expr) (atom, bool) {
	if len(expr.Args) != 1 || len(expr.Name) != 1 {
		return atom{}, false
	}
	column, ok := columnOf(&expr.Args[0])
	if !ok {
		return atom{}, false
	}
	if expr.Name[0] == "IS NOT NULL" {
		return atom{kind: atomNotNull, column: column}, true
	}
	return atom{kind: atomNull, column: column}, true
}

// commuted is the operator that says the same thing with its operands swapped.
var commuted = map[string]string{"=": "=", "<": ">", "<=": ">=", ">": "<", ">=": "<="}

// comparisonOf reads column op constant, or constant op column. A column that is no key has no domain and reads nothing.
func comparisonOf(op string, left, right *pg_parse.Expr, domains map[string]valueDomain) (atom, bool) {
	if _, ok := commuted[op]; !ok {
		return atom{}, false
	}
	if column, ok := columnOf(left); ok {
		value, ok := domains[column].read(right)
		return atom{kind: atomCompare, column: column, op: op, value: value}, ok
	}
	if column, ok := columnOf(right); ok {
		value, ok := domains[column].read(left)
		return atom{kind: atomCompare, column: column, op: commuted[op], value: value}, ok
	}
	return atom{}, false
}

// columnOf is the column a bare reference names. A cast or a call on the column is another expression, which the
// server's proof does not see through either.
func columnOf(expr *pg_parse.Expr) (string, bool) {
	if expr.Kind != pg_parse.ExprColumnRef {
		return "", false
	}
	parts := strings.Split(expr.Literal, ".")
	return parts[len(parts)-1], true
}

// operatorName is the operator without the pg_catalog qualification OPERATOR(pg_catalog.=) carries.
func operatorName(expr *pg_parse.Expr) string {
	if len(expr.Name) == 0 {
		return ""
	}
	return expr.Name[len(expr.Name)-1]
}

func stripCasts(expr *pg_parse.Expr) *pg_parse.Expr {
	for expr.Kind == pg_parse.ExprCast && len(expr.Args) == 1 {
		expr = &expr.Args[0]
	}
	return expr
}

// valueDomain is how the constants compared with a key column are ordered, which depends on the column's type.
type valueDomain struct {
	kind domainKind
	// zone reads a timestamptz literal that carries no offset, as the server does in the session's TimeZone. Nil when
	// that zone is not one the proof can load: such a literal then only compares with another one without an offset.
	zone *time.Location
}

type domainKind uint8

const (
	// domainNone is a type whose order is not modelled: no constant is comparable.
	domainNone domainKind = iota
	domainInteger
	domainNumeric
	domainDate
	domainTimestamp
	domainTimestampTZ
	// domainText only knows equality: the order is the collation's.
	domainText
)

// domainOf maps a column's formatted type to the domain its constants are compared in.
func domainOf(formatted string, zone *time.Location) valueDomain {
	return valueDomain{kind: kindOf(formatted), zone: zone}
}

func kindOf(formatted string) domainKind {
	base := strings.ToLower(formatted)
	if i := strings.Index(base, "("); i >= 0 {
		base = strings.TrimSpace(base[:i]) + base[strings.Index(base, ")")+1:]
	}
	switch strings.TrimSpace(base) {
	case "smallint", "integer", "bigint":
		return domainInteger
	case "numeric":
		return domainNumeric
	case "date":
		return domainDate
	case "timestamp without time zone":
		return domainTimestamp
	case "timestamp with time zone":
		return domainTimestampTZ
	case "text", "character varying":
		return domainText
	}
	return domainNone
}

// castFamilies are the casts a constant may carry and still be compared with the column directly. A cast outside the
// family makes the server cast the column instead, and the comparison is no longer on the column.
var castFamilies = map[domainKind]map[string]bool{
	domainInteger:     {"int2": true, "int4": true, "int8": true, "smallint": true, "integer": true, "int": true, "bigint": true},
	domainNumeric:     {"int2": true, "int4": true, "int8": true, "smallint": true, "integer": true, "int": true, "bigint": true, "numeric": true, "decimal": true},
	domainDate:        {"date": true},
	domainTimestamp:   {"date": true, "timestamp": true},
	domainTimestampTZ: {"date": true, "timestamp": true, "timestamptz": true},
	domainText:        {"text": true, "varchar": true},
}

// constant is a comparable value. Exactly one of its fields is set.
type constant struct {
	number *big.Rat
	moment *time.Time
	// zoned says the moment carried an explicit offset. One without is read in the session's time zone, which is unknown,
	// so it only compares with another one without.
	zoned bool
	text  *string
}

var (
	integerLiteral = regexp.MustCompile(`^-?\d+$`)
	numericLiteral = regexp.MustCompile(`^-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$`)
	dateLiteral    = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
)

var naiveLayouts = []string{"2006-01-02", "2006-01-02 15:04", "2006-01-02 15:04:05", "2006-01-02 15:04:05.999999",
	"2006-01-02T15:04", "2006-01-02T15:04:05", "2006-01-02T15:04:05.999999"}
var zonedLayouts = []string{"2006-01-02 15:04:05-07", "2006-01-02 15:04:05-07:00", "2006-01-02 15:04:05.999999-07",
	"2006-01-02 15:04:05.999999-07:00", "2006-01-02T15:04:05Z07:00", "2006-01-02 15:04-07", "2006-01-02 15:04:05Z"}

// read turns a constant expression into a comparable value in the domain, false when it is not one.
func (d valueDomain) read(expr *pg_parse.Expr) (constant, bool) {
	for expr.Kind == pg_parse.ExprCast && len(expr.Args) == 1 {
		if !castFamilies[d.kind][castName(expr)] {
			return constant{}, false
		}
		expr = &expr.Args[0]
	}
	if expr.Kind != pg_parse.ExprConst || expr.Literal == "NULL" {
		return constant{}, false
	}
	literal := expr.Literal
	quoted := strings.HasPrefix(literal, "'") && strings.HasSuffix(literal, "'") && len(literal) >= 2
	if quoted {
		literal = literal[1 : len(literal)-1]
	}

	switch d.kind {
	case domainInteger, domainNumeric:
		pattern := numericLiteral
		if d.kind == domainInteger {
			// A fractional constant makes the server compare the column as numeric.
			pattern = integerLiteral
		}
		if !pattern.MatchString(strings.TrimSpace(literal)) {
			return constant{}, false
		}
		number, ok := new(big.Rat).SetString(strings.TrimSpace(literal))
		return constant{number: number}, ok
	case domainDate:
		if !quoted || !dateLiteral.MatchString(literal) {
			return constant{}, false
		}
		moment, err := time.Parse("2006-01-02", literal)
		return constant{moment: &moment}, err == nil
	case domainTimestamp, domainTimestampTZ:
		if !quoted {
			return constant{}, false
		}
		for _, layout := range naiveLayouts {
			if d.kind == domainTimestampTZ && d.zone != nil {
				if moment, err := time.ParseInLocation(layout, literal, d.zone); err == nil {
					return constant{moment: &moment, zoned: true}, true
				}
				continue
			}
			if moment, err := time.Parse(layout, literal); err == nil {
				return constant{moment: &moment}, true
			}
		}
		if d.kind == domainTimestamp {
			// A timestamp without time zone drops an offset, which is not worth guessing about.
			return constant{}, false
		}
		for _, layout := range zonedLayouts {
			if moment, err := time.Parse(layout, literal); err == nil {
				return constant{moment: &moment, zoned: true}, true
			}
		}
		return constant{}, false
	case domainText:
		if !quoted {
			return constant{}, false
		}
		return constant{text: &literal}, true
	}
	return constant{}, false
}

func castName(expr *pg_parse.Expr) string {
	if len(expr.Name) == 0 {
		return ""
	}
	return strings.ToLower(expr.Name[len(expr.Name)-1])
}

// compare orders two constants, false when the domain cannot tell.
func compare(a, b constant) (int, bool) {
	switch {
	case a.number != nil && b.number != nil:
		return a.number.Cmp(b.number), true
	case a.moment != nil && b.moment != nil:
		if a.zoned != b.zoned {
			return 0, false
		}
		return a.moment.Compare(*b.moment), true
	case a.text != nil && b.text != nil:
		// Only equality is collation-free.
		return 0, *a.text == *b.text
	}
	return 0, false
}

// interval is the set of values a comparison accepts. A nil end is unbounded.
type interval struct {
	low, high         *constant
	lowOpen, highOpen bool
}

func rangeOf(a atom) interval {
	value := a.value
	switch a.op {
	case "=":
		return interval{low: &value, high: &value}
	case "<":
		return interval{high: &value, highOpen: true}
	case "<=":
		return interval{high: &value}
	case ">":
		return interval{low: &value, lowOpen: true}
	default:
		return interval{low: &value}
	}
}

// within reports whether every value of the interval is in the outer one.
func (i interval) within(outer interval) bool {
	if outer.low != nil {
		if i.low == nil {
			return false
		}
		order, ok := compare(*i.low, *outer.low)
		if !ok || order < 0 || (order == 0 && outer.lowOpen && !i.lowOpen) {
			return false
		}
	}
	if outer.high != nil {
		if i.high == nil {
			return false
		}
		order, ok := compare(*i.high, *outer.high)
		if !ok || order > 0 || (order == 0 && outer.highOpen && !i.highOpen) {
			return false
		}
	}
	return true
}
