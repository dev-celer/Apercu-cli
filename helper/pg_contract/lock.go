package pg_contract

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

func (l Lock) IsReadBlocking() bool {
	return l == LockAccessExclusive
}

func (l Lock) IsWriteBlocking() bool {
	return l >= LockShare
}

// IsValid reports whether the value is a real lock mode. LockNone is not.
func (l Lock) IsValid() bool {
	return l >= LockAccessShare && l <= LockAccessExclusive
}

// ParseLock accepts any spelling of a lock mode.
func ParseLock(s string) (Lock, error) {
	return parseEnum(s, lockAliases)
}

// MaxLock returns the strongest of the given modes, LockNone for none.
func MaxLock(locks ...Lock) Lock {
	strongest := LockNone
	for _, l := range locks {
		if l > strongest {
			strongest = l
		}
	}
	return strongest
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
