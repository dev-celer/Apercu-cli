package pg_contract

import (
	"apercu-cli/helper"
	"apercu-cli/helper/warning_interface"
	"fmt"
)

type Severity uint8

const (
	SeverityInfo Severity = iota + 1
	SeverityWarn
	SeverityError
)

var severityNames = map[Severity]string{
	SeverityInfo:  "INFO",
	SeverityWarn:  "WARN",
	SeverityError: "ERROR",
}

var severityAliases = buildAliases(severityNames, nil)

func (s Severity) String() string {
	if name, ok := severityNames[s]; ok {
		return name
	}
	return "UNKNOWN"
}

// ParseSeverity resolves any spelling of a severity.
func ParseSeverity(s string) (Severity, error) {
	return parseEnum(s, severityAliases)
}

func (s Severity) MarshalText() ([]byte, error) {
	return marshalEnum(s, severityNames)
}

func (s *Severity) UnmarshalText(data []byte) error {
	parsed, err := ParseSeverity(string(data))
	if err != nil {
		return err
	}
	*s = parsed
	return nil
}

func (s Severity) MarshalYAML() (any, error) {
	return s.String(), nil
}

func (s *Severity) UnmarshalYAML(unmarshal func(any) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	return s.UnmarshalText([]byte(str))
}

// Code identifies the rule that produced a finding or an error.
type Code string

// Command is the statement's top-level command, e.g. "ALTER TABLE".
type Command string

// TxnGroup numbers the transaction a statement belongs to.
type TxnGroup int

// Finding is one thing a rule observed about a statement.
type Finding struct {
	Code     Code     `json:"code" yaml:"code"`
	Severity Severity `json:"severity" yaml:"severity"`
	Message  string   `json:"message" yaml:"message"`
	Targets  []Target `json:"targets,omitempty" yaml:"targets,omitempty"`
}

// MaxLock is the strongest lock any of the finding's targets takes.
func (f Finding) MaxLock() Lock {
	strongest := LockNone
	for _, t := range f.Targets {
		strongest = MaxLock(strongest, t.Lock)
	}
	return strongest
}

// MaxOpKind is the most severe operation any of the finding's targets performs.
func (f Finding) MaxOpKind() OpKind {
	worst := OpKindNone
	for _, t := range f.Targets {
		worst = MaxOpKind(worst, t.OpKind)
	}
	return worst
}

// Error is a statement that cannot run as written, or whose syntax does not exist on the production version.
// It is reported like a finding and is non-blocking
type Error struct {
	Code    Code   `json:"code" yaml:"code"`
	Message string `json:"message" yaml:"message"`
	// Versions is the range on which the error appear. Only used if the real version could not be determined from the production database
	Versions VersionRange `json:"versions" yaml:"versions"`
}

func (e Error) Error() string {
	if e.Versions.IsUnbounded() {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s (valid on %s)", e.Code, e.Message, e.Versions)
}

// StatementAnalysis is the parser's record for one statement.
type StatementAnalysis struct {
	RawSQL      string                      `json:"raw_sql" yaml:"raw_sql"`
	TxnGroup    TxnGroup                    `json:"txn_group" yaml:"txn_group"`
	Command     Command                     `json:"command" yaml:"command"`
	Subcommands []string                    `json:"subcommands,omitempty" yaml:"subcommands,omitempty"`
	Findings    []Finding                   `json:"findings,omitempty" yaml:"findings,omitempty"`
	Warnings    []warning_interface.Warning `json:"warnings,omitempty" yaml:"warnings,omitempty"`
	Errors      []Error                     `json:"errors,omitempty" yaml:"errors,omitempty"`
}

// MaxLock is the strongest lock the statement takes on any relation. A statement
// holds every lock it takes until it commits, so this is what the user waits on.
func (s StatementAnalysis) MaxLock() Lock {
	strongest := LockNone
	for _, f := range s.Findings {
		strongest = MaxLock(strongest, f.MaxLock())
	}
	return strongest
}

// MaxOpKind is the most severe operation the statement performs on any relation.
func (s StatementAnalysis) MaxOpKind() OpKind {
	worst := OpKindNone
	for _, f := range s.Findings {
		worst = MaxOpKind(worst, f.MaxOpKind())
	}
	return worst
}

// LockOn is the strongest lock the statement takes on one relation, LockNone if
// it never touches it.
func (s StatementAnalysis) LockOn(relation helper.FullRelationName) Lock {
	strongest := LockNone
	for _, f := range s.Findings {
		for _, t := range f.Targets {
			if t.Relation.Name == relation {
				strongest = MaxLock(strongest, t.Lock)
			}
		}
	}
	return strongest
}

// HasErrors reports whether the statement produced any error.
func (s StatementAnalysis) HasErrors() bool {
	return len(s.Errors) > 0
}
