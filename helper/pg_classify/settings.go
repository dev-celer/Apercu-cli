package pg_classify

import (
	"strconv"
	"strings"
	"time"
)

const (
	paramSearchPath       = "search_path"
	paramTimeZone         = "timezone"
	paramLockTimeout      = "lock_timeout"
	paramStatementTimeout = "statement_timeout"
	paramReplicationRole  = "session_replication_role"
)

var trackedParams = map[string]string{
	paramSearchPath:       "search_path",
	paramTimeZone:         "TimeZone",
	paramLockTimeout:      "lock_timeout",
	paramStatementTimeout: "statement_timeout",
	paramReplicationRole:  "session_replication_role",
}

// canonical folds a parameter name the way the server does.
func canonical(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// Timeout is a lock_timeout or statement_timeout as it stands for one statement.
type Timeout struct {
	Raw string
	// Duration is Raw read the way the server reads it. Zero is the server's own default: disabled.
	Duration time.Duration
	// Valid says Raw could be read at all.
	Valid bool
}

// Set reports whether a timeout is actually in force. A parameter set to 0 is disabled.
func (t Timeout) Set() bool { return t.Valid && t.Duration > 0 }

func (t Timeout) String() string {
	if !t.Valid {
		return t.Raw + " (unreadable)"
	}
	if t.Duration == 0 {
		return "disabled"
	}
	return t.Duration.String()
}

// timeUnits are the suffixes a GUC of type time accepts.
var timeUnits = []struct {
	suffix string
	unit   time.Duration
}{
	{"us", time.Microsecond},
	{"ms", time.Millisecond},
	{"min", time.Minute},
	{"s", time.Second},
	{"h", time.Hour},
	{"d", 24 * time.Hour},
}

// parseTimeout reads a lock_timeout / statement_timeout value.
func parseTimeout(raw string) Timeout {
	out := Timeout{Raw: raw}

	value := strings.ToLower(strings.TrimSpace(strings.Trim(strings.TrimSpace(raw), `'"`)))
	if value == "" {
		return out
	}

	unit := time.Millisecond
	for _, candidate := range timeUnits {
		if trimmed, found := strings.CutSuffix(value, candidate.suffix); found {
			// "5 s" is as legal as "5s".
			value, unit = strings.TrimSpace(trimmed), candidate.unit
			break
		}
	}

	// The server accepts a fractional value and rounds it to the parameter's base unit.
	amount, err := strconv.ParseFloat(value, 64)
	if err != nil || amount < 0 {
		return out
	}
	out.Duration = time.Duration(amount * float64(unit))
	out.Valid = true
	return out
}

// ReplicationRole is session_replication_role.
type ReplicationRole uint8

const (
	// ReplicationOrigin is the default: triggers and foreign keys fire.
	ReplicationOrigin ReplicationRole = iota
	// ReplicationReplica suppresses non-replica triggers and foreign-key enforcement.
	ReplicationReplica
	// ReplicationLocal fires triggers but is not the origin of the changes.
	ReplicationLocal
)

func (r ReplicationRole) String() string {
	switch r {
	case ReplicationReplica:
		return "replica"
	case ReplicationLocal:
		return "local"
	default:
		return "origin"
	}
}

func (r ReplicationRole) EnforcesConstraints() bool { return r != ReplicationReplica }

func parseReplicationRole(raw string) ReplicationRole {
	switch strings.ToLower(strings.Trim(strings.TrimSpace(raw), `'"`)) {
	case "replica":
		return ReplicationReplica
	case "local":
		return ReplicationLocal
	default:
		return ReplicationOrigin
	}
}

// utcZones are the time zone names that are exactly UTC with no daylight saving.
var utcZones = map[string]bool{
	"utc": true, "uct": true, "universal": true, "zulu": true,
	"etc/utc": true, "etc/uct": true, "etc/universal": true, "etc/zulu": true,
	"gmt": true, "gmt0": true, "gmt+0": true, "gmt-0": true, "greenwich": true,
	"etc/gmt": true, "etc/gmt0": true, "etc/gmt+0": true, "etc/gmt-0": true, "etc/greenwich": true,
}
