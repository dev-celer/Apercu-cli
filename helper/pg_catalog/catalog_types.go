package pg_catalog

// Volatility mirrors pg_proc.provolatile.
type Volatility uint8

const (
	VolatilityUnknown Volatility = iota
	VolatilityImmutable
	VolatilityStable
	VolatilityVolatile
)

func volatilityFromChar(provolatile string) Volatility {
	switch provolatile {
	case "i":
		return VolatilityImmutable
	case "s":
		return VolatilityStable
	case "v":
		return VolatilityVolatile
	default:
		return VolatilityUnknown
	}
}

func (v Volatility) String() string {
	switch v {
	case VolatilityImmutable:
		return "IMMUTABLE"
	case VolatilityStable:
		return "STABLE"
	case VolatilityVolatile:
		return "VOLATILE"
	default:
		return "UNKNOWN"
	}
}

// MayBeVolatile reports whether the expression has to be treated as volatile. An unknown volatility counts.
func (v Volatility) MayBeVolatile() bool {
	return v == VolatilityVolatile || v == VolatilityUnknown
}

// worst returns the more volatile of the two, so a composite expression is graded on its least predictable part.
func (v Volatility) worst(other Volatility) Volatility {
	if v == VolatilityUnknown || other == VolatilityUnknown {
		return VolatilityUnknown
	}
	if other > v {
		return other
	}
	return v
}

// castKey identifies one cast by the pair it converts.
type castKey struct {
	source OID
	target OID
}

// typeIndex holds the lookups keyed on a type, a function or a cast.
type typeIndex struct {
	procs     map[OID]Proc
	types     map[OID]Type
	operators map[OID]Operator
	casts     map[castKey]Cast

	// columnsOfType map array of Column using a Type, keyed on the Type OID.
	columnsOfType map[OID][]Column
	// typedTables map array of typed table based on a composite Type, keyed on the Type OID.
	typedTables map[OID][]OID
	// derivedFrom map array of Type based on another Type. Keyed on the root Type.
	// Derived type can be domain or array
	derivedFrom map[OID][]OID
}

func (t *typeIndex) build(snapshot *Snapshot) {
	t.procs = make(map[OID]Proc, len(snapshot.Procs))
	t.types = make(map[OID]Type, len(snapshot.Types))
	t.operators = make(map[OID]Operator, len(snapshot.Operators))
	t.casts = make(map[castKey]Cast, len(snapshot.Casts))
	t.columnsOfType = map[OID][]Column{}
	t.typedTables = map[OID][]OID{}
	t.derivedFrom = map[OID][]OID{}

	for _, proc := range snapshot.Procs {
		t.procs[proc.OID] = proc
	}
	for _, typ := range snapshot.Types {
		t.types[typ.OID] = typ
		if typ.BaseTypeID != 0 {
			t.derivedFrom[typ.BaseTypeID] = append(t.derivedFrom[typ.BaseTypeID], typ.OID)
		}
		if typ.ElemID != 0 {
			t.derivedFrom[typ.ElemID] = append(t.derivedFrom[typ.ElemID], typ.OID)
		}
	}
	for _, operator := range snapshot.Operators {
		t.operators[operator.OID] = operator
	}
	for _, cast := range snapshot.Casts {
		t.casts[castKey{cast.Source, cast.Target}] = cast
	}
	for _, column := range snapshot.Columns {
		t.columnsOfType[column.TypeID] = append(t.columnsOfType[column.TypeID], column)
	}
	for _, rel := range snapshot.Relations {
		if rel.OfType != 0 {
			t.typedTables[rel.OfType] = append(t.typedTables[rel.OfType], rel.OID)
		}
	}
}

// Proc resolves one function.
func (c *Catalog) Proc(oid OID) (Proc, bool) {
	proc, ok := c.types.procs[oid]
	return proc, ok
}

// Type resolves one type.
func (c *Catalog) Type(oid OID) (Type, bool) {
	typ, ok := c.types.types[oid]
	return typ, ok
}

// ProcVolatility resolves the volatility of a function.
func (c *Catalog) ProcVolatility(oid OID) Volatility {
	proc, ok := c.types.procs[oid]
	if !ok {
		return VolatilityUnknown
	}
	return volatilityFromChar(proc.Volatility)
}

// OperatorVolatility resolves the volatility of an operator.
func (c *Catalog) OperatorVolatility(oid OID) Volatility {
	operator, ok := c.types.operators[oid]
	if !ok {
		return VolatilityUnknown
	}
	return c.ProcVolatility(operator.Code)
}

// TypeInputVolatility return the Volatility of the text -> Type conversion function.
func (c *Catalog) TypeInputVolatility(typeOID OID) Volatility {
	typ, ok := c.types.types[typeOID]
	if !ok {
		return VolatilityUnknown
	}
	return c.ProcVolatility(typ.Input)
}

// typeOutputVolatility return the Volatility of the Type -> text conversion function.
func (c *Catalog) typeOutputVolatility(typeOID OID) Volatility {
	typ, ok := c.types.types[typeOID]
	if !ok {
		return VolatilityUnknown
	}
	return c.ProcVolatility(typ.Output)
}

// CastVolatility return the Volatility of the type conversion source -> target.
func (c *Catalog) CastVolatility(source, target OID) Volatility {
	if source == target {
		return VolatilityImmutable
	}
	cast, ok := c.types.casts[castKey{source, target}]
	if !ok {
		return VolatilityUnknown
	}
	switch cast.Method {
	case "b":
		return VolatilityImmutable
	case "f":
		return c.ProcVolatility(cast.Func)
	case "i":
		return c.typeOutputVolatility(source).worst(c.TypeInputVolatility(target))
	default:
		return VolatilityUnknown
	}
}

// DomainHasConstraints return, for domain type, if any domain constraint apply to it.
func (c *Catalog) DomainHasConstraints(typeOID OID) bool {
	typ, ok := c.types.types[typeOID]
	if !ok {
		return false
	}
	return typ.Type == "d" && typ.DomainConstraints > 0
}

// BinaryCoercible return if the cast for source -> target is binary coercible.
func (c *Catalog) BinaryCoercible(source, target OID) bool {
	if source == target {
		return true
	}
	cast, ok := c.types.casts[castKey{source, target}]
	return ok && cast.Method == "b"
}

// TypeChangeRequiresRewrite answer does ALTER COLUMN TYPE have to rewrite the table.
//
// Three shapes escape the rewrite:
//   - the cast for source -> target is binary coercible
//   - the type does not change and the modifier is being relaxed rather than tightened
//   - the target is an unconstrained domain over a base the source already fits, which adds a name and nothing else.
func (c *Catalog) TypeChangeRequiresRewrite(source OID, sourceMod int32, target OID, targetMod int32) bool {
	if source == target {
		return !c.typmodRelaxed(target, sourceMod, targetMod)
	}
	if c.BinaryCoercible(source, target) {
		// A binary coercion is a relabel and a relabel carries no modifier of its own, so a
		// constrained target still has its modifier coercion applied on top of the relabel.
		return !c.typmodRelaxed(target, -1, targetMod)
	}

	// An unconstrained domain is a label on its base type.
	if typ, ok := c.types.types[target]; ok && typ.Type == "d" && typ.DomainConstraints == 0 {
		if base := typ.BaseTypeID; base != 0 {
			return c.TypeChangeRequiresRewrite(source, sourceMod, base, typ.TypeMod)
		}
	}
	// The same holds on the way out: dropping a domain for its own base type changes nothing.
	if typ, ok := c.types.types[source]; ok && typ.Type == "d" {
		if base := typ.BaseTypeID; base != 0 && base != source {
			return c.TypeChangeRequiresRewrite(base, typ.TypeMod, target, targetMod)
		}
	}
	return true
}

// Built-in type OIDs with a prosupport function for a cast to itself
const (
	oidVarchar     OID = 1043
	oidTime        OID = 1083
	oidTimestamp   OID = 1114
	oidTimestampTz OID = 1184
	oidInterval    OID = 1186
	oidTimeTz      OID = 1266
	oidVarbit      OID = 1562
	oidNumeric     OID = 1700
)

// typmodRelaxers holds, per type, what that type's support function use to determine a rewrite can be skipped.
var typmodRelaxers = map[OID]func(oldMod, newMod int32) bool{
	oidVarchar:     lengthRelaxed,
	oidVarbit:      lengthRelaxed,
	oidTime:        precisionRelaxed,
	oidTimeTz:      precisionRelaxed,
	oidTimestamp:   precisionRelaxed,
	oidTimestampTz: precisionRelaxed,
	oidNumeric:     numericRelaxed,
	oidInterval:    intervalRelaxed,
}

// typmodRelaxed reports whether changing nothing but the type modifier is free.
//
// A negative modifier asks for no coercion at all, whatever the type. Past that the question is
// which node the planner leaves for ALTER TABLE to look at:
//
//   - a type with no modifier coercion of its own gets a bare relabel, so the heap is kept. The
//     server does not check the values either: a narrower modifier is recorded over data that no
//     longer fits it, silently.
//   - an array whose element type has one gets the per-element call wrapped in a node ALTER TABLE
//     will not look through, so an array rewrites even when its element type would not.
//   - anything else is the type's own support function, or a rewrite when we cannot name it.
func (c *Catalog) typmodRelaxed(typeOID OID, oldMod, newMod int32) bool {
	if oldMod == newMod || newMod == -1 {
		return true
	}
	coerced, isArray := c.coercedType(typeOID)
	if relaxed, ok := typmodRelaxers[coerced]; ok {
		return !isArray && relaxed(oldMod, newMod)
	}
	return !c.hasModifierCoercion(coerced)
}

// for array-like type, coercedType return (the underlying Type OID, isArray boolean)
func (c *Catalog) coercedType(typeOID OID) (OID, bool) {
	typ, ok := c.types.types[typeOID]
	if !ok || typ.ElemID == 0 || typ.Len != -1 {
		return typeOID, false
	}
	return typ.ElemID, true
}

// hasModifierCoercion reports whether the type owns a cast to itself, which will be used for typmod coercion.
func (c *Catalog) hasModifierCoercion(typeOID OID) bool {
	cast, ok := c.types.casts[castKey{typeOID, typeOID}]
	return ok && cast.Method == "f"
}

// varHdrSz is the four bytes the varlena types add to a packed modifier.
const varHdrSz int32 = 4

// maxFractionalDigits is the finest the time, timestamp and interval types can store.
const maxFractionalDigits int32 = 6

// lengthRelaxed is varchar and varbit, the two types whose modifier is the limit itself. An
// absent modifier is unbounded, so putting any limit on one is a tightening.
func lengthRelaxed(oldMod, newMod int32) bool {
	return oldMod >= 0 && newMod > oldMod
}

// precisionRelaxed is the time and timestamp family, whose modifier is a digit count.
func precisionRelaxed(oldMod, newMod int32) bool {
	return newMod >= fractionalDigits(oldMod)
}

func fractionalDigits(mod int32) int32 {
	if mod < 0 {
		return maxFractionalDigits
	}
	return mod
}

// numericRelaxed compares two numeric modifiers, which pack a precision and a scale into one
// integer and so cannot be compared as they stand. Room for more digits is free; a different
// scale moves the decimal point and rewrites. An absent modifier is unconstrained, so any
// modifier at all is a tightening.
func numericRelaxed(oldMod, newMod int32) bool {
	if oldMod < 0 {
		return false
	}
	oldPrecision, oldScale := numericTypmod(oldMod)
	newPrecision, newScale := numericTypmod(newMod)
	return newScale == oldScale && newPrecision >= oldPrecision
}

// numericTypmod unpack the typmod into the precision and scale that compose it
// the upper 16 bits is the precision
// the lower 16 bits is the scale (stored as an 11-bit signed integer)
func numericTypmod(mod int32) (precision, scale int32) {
	// varHdrSz is added after packing for compatibility reason, so we need to remove it before unpacking
	packed := mod - varHdrSz
	return (packed >> 16) & 0xffff,
		((packed & 0x7ff) ^ 1024) - 1024 // scale is an 11-bit signed integer so we need to convert it to a 32-bit signed integer before returning
}

// intervalRelaxed compares two interval modifiers, which pack the set of fields the value keeps
// with a digit count. A value already truncated to a coarser field has nothing left for a finer
// one to lose, so widening the range is free and only dropping to a coarser field is not:
// interval year to interval day keeps everything interval year had.
func intervalRelaxed(oldMod, newMod int32) bool {
	if intervalLeastField(newMod) > intervalLeastField(oldMod) {
		return false
	}
	return intervalDigits(newMod) >= intervalDigits(oldMod)
}

// intervalFieldMasks are 1 << the field codes in datetime.h, finest field first.
var intervalFieldMasks = [...]int32{
	1 << 12, // SECOND
	1 << 11, // MINUTE
	1 << 10, // HOUR
	1 << 3,  // DAY
	1 << 1,  // MONTH
	1 << 2,  // YEAR
}

// intervalLeastField return the finest field a modifier keeps, second first, so a smaller rank is a finer interval.
// This is based on the first part (most significant bits) of the packed 32-bit typmod
func intervalLeastField(mod int32) int32 {
	if mod < 0 {
		return 0
	}
	fields := (mod >> 16) & 0x7fff
	for rank, mask := range intervalFieldMasks {
		if fields&mask != 0 {
			return int32(rank)
		}
	}
	return 0
}

// intervalDigits is the fractional-second digit count, with interval's own unlimited marker and
// an absent modifier both standing for the maximum the type can store.
// This is based on the second part (least significant bits) of the packed 32-bit typmod
func intervalDigits(mod int32) int32 {
	if mod < 0 {
		return maxFractionalDigits
	}
	if digits := mod & 0xffff; digits != 0xffff {
		return digits
	}
	return maxFractionalDigits
}

// ColumnRef is one column of one relation.
type ColumnRef struct {
	RelID OID
	Num   int16
	Name  string
}

// TypeDependents report everything that has to be locked when a type changes.
func (c *Catalog) TypeDependents(typeOID OID) (columns []ColumnRef, typedTables []OID) {
	for _, oid := range c.typeClosure(typeOID) {
		for _, column := range c.types.columnsOfType[oid] {
			columns = append(columns, ColumnRef{RelID: column.RelID, Num: column.Num, Name: column.Name})
		}
		typedTables = append(typedTables, c.types.typedTables[oid]...)
	}
	return columns, typedTables
}

// typeClosure is the type itself plus every type built on top of it, transitively:
// domains over it, arrays of it, domains over those arrays.
func (c *Catalog) typeClosure(typeOID OID) []OID {
	closure := []OID{typeOID}
	seen := map[OID]bool{typeOID: true}
	for i := 0; i < len(closure); i++ {
		for _, derived := range c.types.derivedFrom[closure[i]] {
			if seen[derived] {
				continue
			}
			seen[derived] = true
			closure = append(closure, derived)
		}
	}
	return closure
}
