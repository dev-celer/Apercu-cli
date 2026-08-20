package pg_contract

// OpKind is what a statement actually does to a relation's data.
type OpKind uint8

const (
	// OpKindNone means no user relation is touched.
	OpKindNone OpKind = 0
	// OpKindMetadata is a catalog-only change, O(1) in table size.
	OpKindMetadata OpKind = 1
	// OpKindDML is row-level writes, O(rows touched).
	OpKindDML OpKind = 2
	// OpKindConcurrent is long but holds a weak lock, and waits on other transactions rather than blocking them.
	OpKindConcurrent OpKind = 3
	// OpKindScan reads every row under the lock, O(table size).
	OpKindScan OpKind = 4
	// OpKindRewrite writes a new relfilenode and rebuilds indexes, O(table size) and roughly 2x disk.
	OpKindRewrite OpKind = 5
)

var opKindNames = map[OpKind]string{
	OpKindNone:       "NONE",
	OpKindMetadata:   "METADATA",
	OpKindDML:        "DML",
	OpKindConcurrent: "CONCURRENT",
	OpKindScan:       "SCAN",
	OpKindRewrite:    "REWRITE",
}

var opKindAliases = buildAliases(opKindNames, map[string]OpKind{
	"": OpKindNone,
})

func (k OpKind) String() string {
	if name, ok := opKindNames[k]; ok {
		return name
	}
	return "UNKNOWN"
}

// ScalesWithTableSize reports whether the duration of the operation grows with the size of the relation.
func (k OpKind) ScalesWithTableSize() bool {
	return k == OpKindScan || k == OpKindRewrite
}

// ParseOpKind resolves any spelling of an operation kind. Empty is OpKindNone.
func ParseOpKind(s string) (OpKind, error) {
	return parseEnum(s, opKindAliases)
}

// MaxOpKind returns the most severe of the given kinds, OpKindNone for none.
func MaxOpKind(kinds ...OpKind) OpKind {
	worst := OpKindNone
	for _, k := range kinds {
		if k > worst {
			worst = k
		}
	}
	return worst
}

func (k OpKind) MarshalText() ([]byte, error) {
	return marshalEnum(k, opKindNames)
}

func (k *OpKind) UnmarshalText(data []byte) error {
	parsed, err := ParseOpKind(string(data))
	if err != nil {
		return err
	}
	*k = parsed
	return nil
}

func (k OpKind) MarshalYAML() (any, error) {
	return k.String(), nil
}

func (k *OpKind) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	return k.UnmarshalText([]byte(s))
}
