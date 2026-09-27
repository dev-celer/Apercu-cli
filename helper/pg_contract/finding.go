package pg_contract

import "fmt"

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

// Finding is one thing a rule observed about a statement.
type Finding struct {
	Code     Code     `json:"code" yaml:"code"`
	Severity Severity `json:"severity" yaml:"severity"`
	Message  string   `json:"message" yaml:"message"`
	Targets  []Target `json:"targets,omitempty" yaml:"targets,omitempty"`
	Level    Level    `json:"level,omitempty" yaml:"level,omitempty"`
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
	// Versions is the range on which the error disappear.
	// Only used if the real version could not be determined from the production database
	Versions VersionRange `json:"versions" yaml:"versions"`
}

func (e Error) Error() string {
	if e.Versions.IsUnbounded() {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s (valid on %s)", e.Code, e.Message, e.Versions)
}

// Level describe the impact of the finding, it can be converted to warning's level.
//
// LevelUnset is used for most of the finding as a temporary state, until it receive prod database metrics.
type Level uint8

const (
	LevelUnset  Level = 0
	LevelLow    Level = 1
	LevelMedium Level = 2
	LevelHigh   Level = 3
)

var levelNames = map[Level]string{
	LevelUnset:  "UNSET",
	LevelLow:    "LOW",
	LevelMedium: "MEDIUM",
	LevelHigh:   "HIGH",
}

var levelAliases = buildAliases(levelNames, map[string]Level{
	"": LevelUnset,
})

func (l Level) String() string {
	if name, ok := levelNames[l]; ok {
		return name
	}
	return "UNKNOWN"
}

// Decided reports whether the level is already set or if it is waiting for prod metrics.
func (l Level) Decided() bool { return l != LevelUnset }

// MaxLevel returns the highest of the given levels, LevelUnset for none.
func MaxLevel(levels ...Level) Level {
	highest := LevelUnset
	for _, l := range levels {
		if l > highest {
			highest = l
		}
	}
	return highest
}

// ParseLevel accepts any spelling of a level. Empty is LevelUnset.
func ParseLevel(s string) (Level, error) {
	return parseEnum(s, levelAliases)
}

func (l Level) MarshalText() ([]byte, error) {
	return marshalEnum(l, levelNames)
}

func (l *Level) UnmarshalText(data []byte) error {
	parsed, err := ParseLevel(string(data))
	if err != nil {
		return err
	}
	*l = parsed
	return nil
}

func (l Level) MarshalYAML() (any, error) {
	return l.String(), nil
}

func (l *Level) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	return l.UnmarshalText([]byte(s))
}
