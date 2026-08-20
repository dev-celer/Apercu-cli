package pg_contract

import (
	"fmt"
	"strconv"
	"strings"
)

// Version is a PostgreSQL major version. The parser targets 15 through 18.
// The zero value means unknown, which happens when production is unreachable.
type Version uint8

const (
	VersionUnknown Version = 0
	Version15      Version = 15
	Version16      Version = 16
	Version17      Version = 17
	Version18      Version = 18

	MinSupportedVersion = Version15
	MaxSupportedVersion = Version18
)

func (v Version) String() string {
	if v == VersionUnknown {
		return "unknown"
	}
	return strconv.Itoa(int(v))
}

func (v Version) IsSupported() bool {
	return v >= MinSupportedVersion && v <= MaxSupportedVersion
}

// VersionFromNum converts a server_version_num (170004) to a major version.
func VersionFromNum(num int) Version {
	if num < 100000 || num >= 255*10000 {
		return VersionUnknown
	}
	return Version(num / 10000)
}

// VersionRange is a closed interval of major versions. A zero bound is unbounded on that side.
type VersionRange struct {
	Min Version `json:"min,omitempty" yaml:"min,omitempty"`
	Max Version `json:"max,omitempty" yaml:"max,omitempty"`
}

var AnyVersion = VersionRange{}

// AtLeast is [min, ∞).
func AtLeast(low Version) VersionRange { return VersionRange{Min: low} }

// AtMost is (∞, max].
func AtMost(high Version) VersionRange { return VersionRange{Max: high} }

// Between is [min, max].
func Between(low, high Version) VersionRange { return VersionRange{Min: low, Max: high} }

// Exactly is the single-version range [v, v].
func Exactly(v Version) VersionRange { return VersionRange{Min: v, Max: v} }

// Contains reports whether the version falls in the range. Unknown is never contained
func (r VersionRange) Contains(v Version) bool {
	if v == VersionUnknown {
		return false
	}
	if r.Min != VersionUnknown && v < r.Min {
		return false
	}
	if r.Max != VersionUnknown && v > r.Max {
		return false
	}
	return true
}

func (r VersionRange) IsUnbounded() bool {
	return r.Min == VersionUnknown && r.Max == VersionUnknown
}

// Overlaps reports whether the two ranges share at least one version.
func (r VersionRange) Overlaps(other VersionRange) bool {
	if r.Min != VersionUnknown && other.Max != VersionUnknown && r.Min > other.Max {
		return false
	}
	if other.Min != VersionUnknown && r.Max != VersionUnknown && other.Min > r.Max {
		return false
	}
	return true
}

func (r VersionRange) String() string {
	switch {
	case r.IsUnbounded():
		return "any"
	case r.Min == r.Max:
		return r.Min.String()
	case r.Max == VersionUnknown:
		return fmt.Sprintf("%s+", r.Min)
	case r.Min == VersionUnknown:
		return fmt.Sprintf("<=%s", r.Max)
	default:
		return fmt.Sprintf("%s-%s", r.Min, r.Max)
	}
}

// ParseVersionRange reads back the String form: "any", "17", "15+", "<=17", "15-18".
func ParseVersionRange(s string) (VersionRange, error) {
	s = strings.TrimSpace(s)
	parse := func(field string) (Version, error) {
		n, err := strconv.Atoi(field)
		if err != nil || n < 0 || n > 255 {
			return VersionUnknown, fmt.Errorf("pg_contract: invalid version range %q", s)
		}
		return Version(n), nil
	}

	switch {
	case s == "" || s == "any":
		return AnyVersion, nil
	case strings.HasSuffix(s, "+"):
		low, err := parse(strings.TrimSuffix(s, "+"))
		return AtLeast(low), err
	case strings.HasPrefix(s, "<="):
		high, err := parse(strings.TrimPrefix(s, "<="))
		return AtMost(high), err
	}

	before, after, found := strings.Cut(s, "-")
	if !found {
		v, err := parse(s)
		return Exactly(v), err
	}
	low, err := parse(before)
	if err != nil {
		return AnyVersion, err
	}
	high, err := parse(after)
	if err != nil {
		return AnyVersion, err
	}
	return Between(low, high), nil
}
