package pg_contract

import "strings"

// Lock is a PostgreSQL table-level lock mode. Comparison is by ordinal, so `a < b` and max() work directly.
type Lock uint8

const (
	LockNone                 Lock = 0
	LockAccessShare          Lock = 1
	LockRowShare             Lock = 2
	LockRowExclusive         Lock = 3
	LockShareUpdateExclusive Lock = 4
	LockShare                Lock = 5
	LockShareRowExclusive    Lock = 6
	LockExclusive            Lock = 7
	LockAccessExclusive      Lock = 8
)

var lockNames = map[Lock]string{
	LockNone:                 "NONE",
	LockAccessShare:          "ACCESS_SHARE",
	LockRowShare:             "ROW_SHARE",
	LockRowExclusive:         "ROW_EXCLUSIVE",
	LockShareUpdateExclusive: "SHARE_UPDATE_EXCLUSIVE",
	LockShare:                "SHARE",
	LockShareRowExclusive:    "SHARE_ROW_EXCLUSIVE",
	LockExclusive:            "EXCLUSIVE",
	LockAccessExclusive:      "ACCESS_EXCLUSIVE",
}

var lockShortNames = map[Lock]string{
	LockNone:                 "-",
	LockAccessShare:          "AS",
	LockRowShare:             "RS",
	LockRowExclusive:         "RE",
	LockShareUpdateExclusive: "SUE",
	LockShare:                "SH",
	LockShareRowExclusive:    "SRE",
	LockExclusive:            "EXCL",
	LockAccessExclusive:      "AEL",
}

var lockAliases = buildAliases(lockNames, map[string]Lock{
	"":     LockNone,
	"AS":   LockAccessShare,
	"RS":   LockRowShare,
	"RE":   LockRowExclusive,
	"SUE":  LockShareUpdateExclusive,
	"SH":   LockShare,
	"SRE":  LockShareRowExclusive,
	"EXCL": LockExclusive,
	"AEL":  LockAccessExclusive,
})

// String is the canonical name, matching the spelling used in persisted output.
func (l Lock) String() string {
	if name, ok := lockNames[l]; ok {
		return name
	}
	return "UNKNOWN"
}

// Short is the abbreviation.
func (l Lock) Short() string {
	if name, ok := lockShortNames[l]; ok {
		return name
	}
	return "?"
}

// IsReadBlocking reports whether the lock conflicts with the ACCESS SHARE a read takes.
func (l Lock) IsReadBlocking() bool {
	return l.ConflictsWith(LockAccessShare)
}

// IsWriteBlocking reports whether the lock conflicts with the ROW EXCLUSIVE a write takes.
func (l Lock) IsWriteBlocking() bool {
	return l.ConflictsWith(LockRowExclusive)
}

// IsValid reports whether the value is a real lock mode. LockNone is not.
func (l Lock) IsValid() bool {
	return l >= LockAccessShare && l <= LockAccessExclusive
}

// ParseLock accepts any spelling of a lock mode.
func ParseLock(s string) (Lock, error) {
	return parseEnum(s, lockAliases)
}

// MaxLock returns the strongest of the given modes by ordinal, LockNone for none. It is how the server combines the
// locks one statement takes on a relation; across statements of a transaction, use LockSet.
func MaxLock(locks ...Lock) Lock {
	strongest := LockNone
	for _, l := range locks {
		if l > strongest {
			strongest = l
		}
	}
	return strongest
}

// lockConflicts is PostgreSQL's LockConflicts table.
var lockConflicts = [...]LockSet{
	LockNone:        0,
	LockAccessShare: LockSetOf(LockAccessExclusive),
	LockRowShare:    LockSetOf(LockExclusive, LockAccessExclusive),
	LockRowExclusive: LockSetOf(LockShare, LockShareRowExclusive,
		LockExclusive, LockAccessExclusive),
	LockShareUpdateExclusive: LockSetOf(LockShareUpdateExclusive, LockShare, LockShareRowExclusive,
		LockExclusive, LockAccessExclusive),
	LockShare: LockSetOf(LockRowExclusive, LockShareUpdateExclusive, LockShareRowExclusive,
		LockExclusive, LockAccessExclusive),
	LockShareRowExclusive: LockSetOf(LockRowExclusive, LockShareUpdateExclusive, LockShare, LockShareRowExclusive,
		LockExclusive, LockAccessExclusive),
	LockExclusive: LockSetOf(LockRowShare, LockRowExclusive, LockShareUpdateExclusive, LockShare, LockShareRowExclusive,
		LockExclusive, LockAccessExclusive),
	LockAccessExclusive: LockSetOf(LockAccessShare, LockRowShare, LockRowExclusive, LockShareUpdateExclusive, LockShare,
		LockShareRowExclusive, LockExclusive, LockAccessExclusive),
}

// Conflicts is every mode another transaction cannot be granted while this one is held.
func (l Lock) Conflicts() LockSet {
	if !l.IsValid() {
		return 0
	}
	return lockConflicts[l]
}

// ConflictsWith reports whether two transactions cannot hold these modes on one relation at the same time.
func (l Lock) ConflictsWith(other Lock) bool {
	return l.Conflicts().Has(other)
}

// LockSet is a set of lock modes.
type LockSet uint16

// LockSetOf is the set of the given modes, LockNone left out.
func LockSetOf(locks ...Lock) LockSet {
	var set LockSet
	for _, l := range locks {
		set = set.With(l)
	}
	return set
}

// With is the set with one more mode.
func (s LockSet) With(l Lock) LockSet {
	if !l.IsValid() {
		return s
	}
	return s | 1<<l
}

// Has reports whether the mode is in the set.
func (s LockSet) Has(l Lock) bool {
	return l.IsValid() && s&(1<<l) != 0
}

// Locks lists the modes in the set, by ordinal.
func (s LockSet) Locks() []Lock {
	var out []Lock
	for l := LockAccessShare; l <= LockAccessExclusive; l++ {
		if s.Has(l) {
			out = append(out, l)
		}
	}
	return out
}

// Conflicts is every mode another transaction cannot be granted while the whole set is held.
func (s LockSet) Conflicts() LockSet {
	var conflicts LockSet
	for _, l := range s.Locks() {
		conflicts |= l.Conflicts()
	}
	return conflicts
}

// Covers reports whether holding the set already blocks everything taking the lock would. The lock then adds nothing
// to what the transaction blocks, and the server grants it without waiting, even with a conflicting request queued.
func (s LockSet) Covers(l Lock) bool {
	return l.Conflicts()&^s.Conflicts() == 0
}

func (s LockSet) String() string {
	names := make([]string, 0, len(s.Locks()))
	for _, l := range s.Locks() {
		names = append(names, l.Short())
	}
	return "{" + strings.Join(names, ",") + "}"
}

func (l Lock) MarshalText() ([]byte, error) {
	return marshalEnum(l, lockNames)
}

func (l *Lock) UnmarshalText(data []byte) error {
	parsed, err := ParseLock(string(data))
	if err != nil {
		return err
	}
	*l = parsed
	return nil
}

func (l Lock) MarshalYAML() (any, error) {
	return l.String(), nil
}

func (l *Lock) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	return l.UnmarshalText([]byte(s))
}
