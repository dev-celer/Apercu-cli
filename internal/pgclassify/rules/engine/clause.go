package engine

import (
	"apercu-cli/internal/pgcontract"
)

// clause is the static effect of one rule code on the relation the statement names.
type clause struct {
	lock pgcontract.Lock
	op   pgcontract.OpKind
	// tableLock is, for a clause acting on an index, the lock on the table that owns it.
	tableLock pgcontract.Lock
	below     Below
	// underOnly replaces below when the statement wrote ONLY.
	underOnly Below
	// partitioned is the versions that accept the clause on a partitioned table; the zero value accepts every one.
	partitioned pgcontract.VersionRange
	// refusal is the reason given on a version outside partitioned.
	refusal string
	// onlyRefusal is the reason given when ONLY meets a refused reach.
	onlyRefusal string
	// changes restate the reach from a version on, in increasing version order.
	changes []change
}

// change is the reach a clause takes from one version on.
type change struct {
	from      pgcontract.Version
	below     Below
	underOnly Below
}

// The reaches the clause table spells most often.
var (
	Everywhere   = Below{Partitions: Worked, Children: Worked}
	ToPartitions = Below{Partitions: Worked}
	Nowhere      = Below{}
)

// fallbackClause is what a code with no row reads as: the strongest lock, reaching every child.
var fallbackClause = clause{lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere}

// clauseFor is the row of a code, or the fallback.
func clauseFor(code pgcontract.Code) clause {
	if row, ok := clauses[code]; ok {
		return row
	}
	return fallbackClause
}

// reachAt is the reach on one version. An unknown version takes the strongest reading of all of them.
func (c clause) reachAt(version pgcontract.Version) (Below, Below) {
	reach, underOnly := c.below, c.underOnly
	if version == pgcontract.VersionUnknown {
		for _, later := range c.changes {
			reach = c.strongest(reach, later.below)
			underOnly = c.strongest(underOnly, later.underOnly)
		}
		return reach, underOnly
	}
	for _, later := range c.changes {
		if version >= later.from {
			reach, underOnly = later.below, later.underOnly
		}
	}
	return reach, underOnly
}

// strongest merges two readings of the same clause into the one that locks the most.
func (c clause) strongest(a, b Below) Below {
	out := a
	if b.Partitions.rank() > out.Partitions.rank() {
		out.Partitions = b.Partitions
	}
	if b.Children.rank() > out.Children.rank() {
		out.Children = b.Children
	}
	out.PartitionLock = pgcontract.MaxLock(c.partitionLockOf(a), c.partitionLockOf(b))
	if out.PartitionLock == c.lock {
		out.PartitionLock = pgcontract.LockNone
	}
	return out
}

// partitionLockOf is the lock a reading takes on the partitions.
func (c clause) partitionLockOf(b Below) pgcontract.Lock {
	if b.PartitionLock != pgcontract.LockNone {
		return b.PartitionLock
	}
	return c.lock
}

// onlyAcceptedFrom is the versions on which ONLY no longer meets a refusal.
func (c clause) onlyAcceptedFrom() pgcontract.VersionRange {
	for _, later := range c.changes {
		if later.underOnly.Partitions != Refused {
			return pgcontract.AtLeast(later.from)
		}
	}
	return pgcontract.AnyVersion
}

// accepts reports whether the version runs the clause on a partitioned table. An unknown version does.
func (c clause) accepts(version pgcontract.Version) bool {
	return version == pgcontract.VersionUnknown || c.partitioned.IsUnbounded() || c.partitioned.Contains(version)
}

var clauses = map[pgcontract.Code]clause{
	// columns.
	"R-AT-ADDCOL": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	// ONLY stops the column being dropped from the children, not the children being locked.
	"R-AT-DROPCOL":    {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere, underOnly: Everywhere},
	"R-AT-TYPE":       {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindRewrite, below: Everywhere},
	"R-AT-SETDEF":     {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-DROPDEF":    {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-SETNOTNULL": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-SETEXPR":    {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-DROPEXPR":   {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	// The identity reaches the partitions from 17 only, and never the classic children.
	"R-AT-ADDIDENT": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere,
		changes: []change{{from: pgcontract.Version17, below: ToPartitions}}},
	"R-AT-SETIDENT":       {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-DROPIDENT":      {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-SETSTATS":       {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-ATTROPT":        {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-AT-SETSTORAGE":     {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-SETCOMPRESSION": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-RENAMECOL":      {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	// From 18 a NOT NULL is a constraint with an inheritance count, so the children are opened to
	// have theirs corrected even under ONLY. Up to 17 it is nothing but attnotnull on the parent's
	// own row: ONLY really does stop there, and a partitioned parent with partitions refuses it (42P16).
	"R-AT-DROPNOTNULL": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere,
		underOnly:   Below{Partitions: Refused},
		onlyRefusal: "PostgreSQL 15-17 cannot remove a constraint from only the partitioned table while partitions exist",
		changes:     []change{{from: pgcontract.Version18, below: Everywhere, underOnly: Everywhere}}},

	// constraints.
	"R-AT-ADDCON":      {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindScan, below: Everywhere},
	"R-AT-ADDCHECK":    {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindScan, below: Everywhere},
	"R-AT-ADDCHECK-NE": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-ADDCHECK-NV": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-ADDNN-NV":    {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-ADDUSINGIDX": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-ADDFK": {lock: pgcontract.LockShareRowExclusive, op: pgcontract.OpKindScan, below: ToPartitions,
		changes: []change{{from: pgcontract.Version18, below: Below{Partitions: Worked, Children: Locked}}}},
	"R-AT-ADDFK-NV": {lock: pgcontract.LockShareRowExclusive, op: pgcontract.OpKindMetadata, below: ToPartitions,
		changes: []change{{from: pgcontract.Version18, below: Below{Partitions: Worked, Children: Locked}}}},
	"R-AT-VALIDATE":     {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindScan, below: Everywhere},
	"R-AT-ALTERCON":     {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-ALTERCON-INH": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-ALTERCON-ENF": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-RENAMECON":    {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	// 15-17 hold SHARE on each partition while the parent's index is built, 18 holds ACCESS EXCLUSIVE;
	// UNIQUE and EXCLUDE leave the classic children alone up to 17.
	"R-AT-ADDUNIQUE": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindScan,
		below:   Below{Partitions: Worked, PartitionLock: pgcontract.LockShare},
		changes: []change{{from: pgcontract.Version18, below: Below{Partitions: Worked, Children: Locked}}}},
	"R-AT-ADDEXCL": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindScan,
		below:       Below{Partitions: Worked, PartitionLock: pgcontract.LockShare},
		partitioned: pgcontract.AtLeast(pgcontract.Version17),
		refusal:     "PostgreSQL 15 and 16 do not support exclusion constraints on a partitioned table",
		changes:     []change{{from: pgcontract.Version18, below: Below{Partitions: Worked, Children: Locked}}}},
	"R-AT-ADDPK": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindScan,
		below:   Below{Partitions: Worked, Children: Locked, PartitionLock: pgcontract.LockShare},
		changes: []change{{from: pgcontract.Version18, below: Below{Partitions: Worked, Children: Locked}}}},
	// ONLY stops the constraint being dropped from the children, not the children being locked.
	"R-AT-DROPCON": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere,
		underOnly:   Below{Partitions: Refused, Children: Worked},
		onlyRefusal: "PostgreSQL 15-17 cannot remove a constraint from only the partitioned table while partitions exist",
		changes:     []change{{from: pgcontract.Version18, below: Everywhere, underOnly: Everywhere}}},

	// triggers, rules, row-level security.
	"R-AT-TRIG":      {lock: pgcontract.LockShareRowExclusive, op: pgcontract.OpKindMetadata, below: ToPartitions},
	"R-AT-TRIG-REP":  {lock: pgcontract.LockShareRowExclusive, op: pgcontract.OpKindMetadata, below: ToPartitions},
	"R-AT-RULE":      {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-AT-RLS":       {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-AT-RLS-FORCE": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},

	// storage, layout, ownership.
	// The server never recurses storage parameters into the partitions or the inheritance children.
	"R-AT-RELOPT":     {lock: pgcontract.LockNone, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-AT-TABLESPACE": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindRewrite, below: Nowhere},
	"R-AT-ACCESSMETHOD": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere,
		partitioned: pgcontract.AtLeast(pgcontract.Version17),
		refusal:     "PostgreSQL 15 and 16 cannot change the access method of a partitioned table"},
	"R-AT-LOGGED":      {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindRewrite, below: Nowhere},
	"R-AT-CLUSTERON":   {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-AT-WITHOUTOIDS": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-OWNER":       {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-AT-REPLIDENT":   {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-AT-RENAME":      {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-AT-SETSCHEMA":   {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},

	// partitions and inheritance.
	"R-AT-ATTACH":      {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-AT-DETACH":      {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-AT-DETACH-CONC": {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-AT-DETACH-FIN":  {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-AT-INHERIT":     {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-AT-OF":          {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},

	// index DDL.
	"R-IX-CREATE":      {lock: pgcontract.LockShare, op: pgcontract.OpKindScan, below: ToPartitions},
	"R-IX-CREATE-CONC": {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindConcurrent, below: Nowhere},
	// A partitioned index takes every index of its tree with it, each one through its table.
	"R-IX-DROP":         {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: ToPartitions},
	"R-IX-DROP-CONC":    {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: ToPartitions},
	"R-IX-SETOPT":       {lock: pgcontract.LockNone, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-IX-SETSTATS":     {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-IX-TABLESPACE":   {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindRewrite, below: Nowhere},
	"R-IX-ATTACH":       {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-IX-RENAME":       {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-IX-REINDEX":      {lock: pgcontract.LockShare, op: pgcontract.OpKindScan, below: Nowhere},
	"R-IX-REINDEX-CONC": {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindConcurrent, below: Nowhere},
	"R-IX-REINDEX-WIDE": {lock: pgcontract.LockShare, op: pgcontract.OpKindScan, below: Nowhere},

	// table creation and destruction.
	"R-TB-CREATE":          {lock: pgcontract.LockNone, op: pgcontract.OpKindNone, below: Nowhere},
	"R-TB-CREATE-LIKE":     {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-TB-CREATE-INHERITS": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-TB-CREATE-PARTOF":   {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-TB-CTAS":            {lock: pgcontract.LockAccessShare, op: pgcontract.OpKindScan, below: Nowhere},
	"R-TB-DROP":            {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},

	// views, matviews, sequences.
	"R-OB-REPLACEVIEW":    {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-OB-CREATEVIEW":     {lock: pgcontract.LockAccessShare, op: pgcontract.OpKindNone, below: Nowhere},
	"R-OB-DROPVIEW":       {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-OB-CREATEMV":       {lock: pgcontract.LockAccessShare, op: pgcontract.OpKindScan, below: Nowhere},
	"R-OB-REFRESHMV":      {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindRewrite, below: Nowhere},
	"R-OB-REFRESHMV-CONC": {lock: pgcontract.LockExclusive, op: pgcontract.OpKindDML, below: Nowhere},
	"R-OB-CREATESEQ":      {lock: pgcontract.LockNone, op: pgcontract.OpKindNone, below: Nowhere},
	"R-OB-ALTERSEQ":       {lock: pgcontract.LockShareRowExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-OB-DROPSEQ":        {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},

	// the objects that hang off a table.
	"R-OB-CREATETRIG": {lock: pgcontract.LockShareRowExclusive, op: pgcontract.OpKindMetadata, below: ToPartitions},
	"R-OB-DROPTRIG":   {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: ToPartitions},
	"R-OB-ALTERTRIG":  {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: ToPartitions},
	"R-OB-POLICY":     {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-OB-RULE":       {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-OB-STATS":      {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-OB-COMMENT":    {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	// PG18 opens the relation at ACCESS SHARE, 15 to 17 take no lock at all; the rule picks.
	"R-OB-GRANT":  {lock: pgcontract.LockNone, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-OB-DEFACL": {lock: pgcontract.LockNone, op: pgcontract.OpKindNone, below: Nowhere},
	// A partitioned table is published as itself; a classic parent publishes its children with it.
	"R-OB-PUB":       {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindMetadata, below: Below{Children: Worked}},
	"R-OB-SUB":       {lock: pgcontract.LockNone, op: pgcontract.OpKindNone, below: Nowhere},
	"R-OB-EXTENSION": {lock: pgcontract.LockNone, op: pgcontract.OpKindNone, below: Nowhere},
	"R-OB-SCHEMA":    {lock: pgcontract.LockNone, op: pgcontract.OpKindNone, below: Nowhere},

	// types and domains.
	"R-TY-ADDVALUE":    {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-TY-RENAMEVALUE": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-TY-ATTR":        {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},
	"R-TY-DOMAIN-ADD":  {lock: pgcontract.LockShare, op: pgcontract.OpKindScan, below: Nowhere},
	"R-TY-DROP":        {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Nowhere},

	// maintenance. VACUUM and ANALYZE reach the partitions on every version plus the classic inheritance children from PG18.
	"R-MT-VACUUM": {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindScan, below: ToPartitions,
		changes: []change{{from: pgcontract.Version18, below: Everywhere}}},
	"R-MT-ANALYZE": {lock: pgcontract.LockShareUpdateExclusive, op: pgcontract.OpKindScan, below: ToPartitions,
		changes: []change{{from: pgcontract.Version18, below: Everywhere}}},
	"R-MT-VACUUMFULL": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindRewrite, below: ToPartitions,
		changes: []change{{from: pgcontract.Version18, below: Everywhere}}},
	"R-MT-CLUSTER":  {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindRewrite, below: ToPartitions},
	"R-MT-TRUNCATE": {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-MT-LOCK":     {lock: pgcontract.LockAccessExclusive, op: pgcontract.OpKindMetadata, below: Everywhere},
	"R-MT-COPYFROM": {lock: pgcontract.LockRowExclusive, op: pgcontract.OpKindDML, below: ToPartitions},
	"R-MT-COPYTO":   {lock: pgcontract.LockAccessShare, op: pgcontract.OpKindNone, below: Nowhere},

	// data backfills.
	"R-DML-INSERT":    {lock: pgcontract.LockRowExclusive, op: pgcontract.OpKindDML, below: ToPartitions},
	"R-DML-UPDATE":    {lock: pgcontract.LockRowExclusive, op: pgcontract.OpKindDML, below: Everywhere},
	"R-DML-DELETE":    {lock: pgcontract.LockRowExclusive, op: pgcontract.OpKindDML, below: Everywhere},
	"R-DML-MERGE":     {lock: pgcontract.LockRowExclusive, op: pgcontract.OpKindDML, below: Everywhere},
	"R-DML-SELECTFOR": {lock: pgcontract.LockRowShare, op: pgcontract.OpKindNone, below: Everywhere},
}

// reach is how far a touch goes into one kind of table below its relation.
type reach uint8

const (
	// untouched leaves the tables below unopened.
	Untouched reach = iota
	// locked opens them at the touch's lock and does none of its work in them.
	Locked
	// worked opens them and does the touch's work in them too.
	Worked
	// refused only appears under ONLY: the server errors when such tables exist.
	Refused
)

// rank orders the reaches from the weakest reading to the strongest. A refusal ranks lowest,
// so any version that accepts the clause wins over one that refuses it.
func (r reach) rank() int {
	if r == Refused {
		return -1
	}
	return int(r)
}

// Below is how a touch reaches the tables under its relation.
type Below struct {
	Partitions reach
	// children are the classic INHERITS children.
	Children reach
	// partitionLock replaces the touch's lock on the partitions; LockNone keeps it.
	PartitionLock pgcontract.Lock
}
