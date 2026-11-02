package pg_classify

import (
	"apercu-cli/helper/pg_contract"
)

// clause is the static effect of one rule code on the relation the statement names.
type clause struct {
	lock pg_contract.Lock
	op   pg_contract.OpKind
	// tableLock is, for a clause acting on an index, the lock on the table that owns it.
	tableLock pg_contract.Lock
	below     below
	// underOnly replaces below when the statement wrote ONLY.
	underOnly below
	// partitioned is the versions that accept the clause on a partitioned table; the zero value accepts every one.
	partitioned pg_contract.VersionRange
	// refusal is the reason given on a version outside partitioned.
	refusal string
	// onlyRefusal is the reason given when ONLY meets a refused reach.
	onlyRefusal string
	// changes restate the reach from a version on, in increasing version order.
	changes []change
}

// change is the reach a clause takes from one version on.
type change struct {
	from      pg_contract.Version
	below     below
	underOnly below
}

// The reaches the clause table spells most often.
var (
	everywhere   = below{partitions: worked, children: worked}
	toPartitions = below{partitions: worked}
	nowhere      = below{}
)

// fallbackClause is what a code with no row reads as: the strongest lock, reaching every child.
var fallbackClause = clause{lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere}

// clauseFor is the row of a code, or the fallback.
func clauseFor(code pg_contract.Code) clause {
	if row, ok := clauses[code]; ok {
		return row
	}
	return fallbackClause
}

// reachAt is the reach on one version. An unknown version takes the strongest reading of all of them.
func (c clause) reachAt(version pg_contract.Version) (below, below) {
	reach, underOnly := c.below, c.underOnly
	if version == pg_contract.VersionUnknown {
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
func (c clause) strongest(a, b below) below {
	out := a
	if b.partitions.rank() > out.partitions.rank() {
		out.partitions = b.partitions
	}
	if b.children.rank() > out.children.rank() {
		out.children = b.children
	}
	out.partitionLock = pg_contract.MaxLock(c.partitionLockOf(a), c.partitionLockOf(b))
	if out.partitionLock == c.lock {
		out.partitionLock = pg_contract.LockNone
	}
	return out
}

// partitionLockOf is the lock a reading takes on the partitions.
func (c clause) partitionLockOf(b below) pg_contract.Lock {
	if b.partitionLock != pg_contract.LockNone {
		return b.partitionLock
	}
	return c.lock
}

// onlyAcceptedFrom is the versions on which ONLY no longer meets a refusal.
func (c clause) onlyAcceptedFrom() pg_contract.VersionRange {
	for _, later := range c.changes {
		if later.underOnly.partitions != refused {
			return pg_contract.AtLeast(later.from)
		}
	}
	return pg_contract.AnyVersion
}

// accepts reports whether the version runs the clause on a partitioned table. An unknown version does.
func (c clause) accepts(version pg_contract.Version) bool {
	return version == pg_contract.VersionUnknown || c.partitioned.IsUnbounded() || c.partitioned.Contains(version)
}

var clauses = map[pg_contract.Code]clause{
	// columns.
	"R-AT-ADDCOL": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	// ONLY stops the column being dropped from the children, not the children being locked.
	"R-AT-DROPCOL":    {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere, underOnly: everywhere},
	"R-AT-TYPE":       {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindRewrite, below: everywhere},
	"R-AT-SETDEF":     {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-DROPDEF":    {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-SETNOTNULL": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-SETEXPR":    {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-DROPEXPR":   {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	// The identity reaches the partitions from 17 only, and never the classic children.
	"R-AT-ADDIDENT": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere,
		changes: []change{{from: pg_contract.Version17, below: toPartitions}}},
	"R-AT-SETIDENT":       {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-DROPIDENT":      {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-SETSTATS":       {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-ATTROPT":        {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-AT-SETSTORAGE":     {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-SETCOMPRESSION": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-RENAMECOL":      {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	// From 18 a NOT NULL is a constraint with an inheritance count, so the children are opened to
	// have theirs corrected even under ONLY. Up to 17 it is nothing but attnotnull on the parent's
	// own row: ONLY really does stop there, and a partitioned parent with partitions refuses it (42P16).
	"R-AT-DROPNOTNULL": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere,
		underOnly:   below{partitions: refused},
		onlyRefusal: "PostgreSQL 15-17 cannot remove a constraint from only the partitioned table while partitions exist",
		changes:     []change{{from: pg_contract.Version18, below: everywhere, underOnly: everywhere}}},

	// constraints.
	"R-AT-ADDCON":      {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindScan, below: everywhere},
	"R-AT-ADDCHECK":    {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindScan, below: everywhere},
	"R-AT-ADDCHECK-NE": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-ADDCHECK-NV": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-ADDNN-NV":    {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-ADDUSINGIDX": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-ADDFK": {lock: pg_contract.LockShareRowExclusive, op: pg_contract.OpKindScan, below: toPartitions,
		changes: []change{{from: pg_contract.Version18, below: below{partitions: worked, children: locked}}}},
	"R-AT-ADDFK-NV": {lock: pg_contract.LockShareRowExclusive, op: pg_contract.OpKindMetadata, below: toPartitions,
		changes: []change{{from: pg_contract.Version18, below: below{partitions: worked, children: locked}}}},
	"R-AT-VALIDATE":     {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindScan, below: everywhere},
	"R-AT-ALTERCON":     {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-ALTERCON-INH": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-ALTERCON-ENF": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-RENAMECON":    {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	// 15-17 hold SHARE on each partition while the parent's index is built, 18 holds ACCESS EXCLUSIVE;
	// UNIQUE and EXCLUDE leave the classic children alone up to 17.
	"R-AT-ADDUNIQUE": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindScan,
		below:   below{partitions: worked, partitionLock: pg_contract.LockShare},
		changes: []change{{from: pg_contract.Version18, below: below{partitions: worked, children: locked}}}},
	"R-AT-ADDEXCL": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindScan,
		below:       below{partitions: worked, partitionLock: pg_contract.LockShare},
		partitioned: pg_contract.AtLeast(pg_contract.Version17),
		refusal:     "PostgreSQL 15 and 16 do not support exclusion constraints on a partitioned table",
		changes:     []change{{from: pg_contract.Version18, below: below{partitions: worked, children: locked}}}},
	"R-AT-ADDPK": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindScan,
		below:   below{partitions: worked, children: locked, partitionLock: pg_contract.LockShare},
		changes: []change{{from: pg_contract.Version18, below: below{partitions: worked, children: locked}}}},
	// ONLY stops the constraint being dropped from the children, not the children being locked.
	"R-AT-DROPCON": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere,
		underOnly:   below{partitions: refused, children: worked},
		onlyRefusal: "PostgreSQL 15-17 cannot remove a constraint from only the partitioned table while partitions exist",
		changes:     []change{{from: pg_contract.Version18, below: everywhere, underOnly: everywhere}}},

	// triggers, rules, row-level security.
	"R-AT-TRIG":      {lock: pg_contract.LockShareRowExclusive, op: pg_contract.OpKindMetadata, below: toPartitions},
	"R-AT-TRIG-REP":  {lock: pg_contract.LockShareRowExclusive, op: pg_contract.OpKindMetadata, below: toPartitions},
	"R-AT-RULE":      {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-AT-RLS":       {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-AT-RLS-FORCE": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},

	// storage, layout, ownership.
	// The server never recurses storage parameters into the partitions or the inheritance children.
	"R-AT-RELOPT":     {lock: pg_contract.LockNone, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-AT-TABLESPACE": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindRewrite, below: nowhere},
	"R-AT-ACCESSMETHOD": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere,
		partitioned: pg_contract.AtLeast(pg_contract.Version17),
		refusal:     "PostgreSQL 15 and 16 cannot change the access method of a partitioned table"},
	"R-AT-LOGGED":      {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindRewrite, below: nowhere},
	"R-AT-CLUSTERON":   {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-AT-WITHOUTOIDS": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-OWNER":       {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-AT-REPLIDENT":   {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-AT-RENAME":      {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-AT-SETSCHEMA":   {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},

	// partitions and inheritance.
	"R-AT-ATTACH":      {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-AT-DETACH":      {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-AT-DETACH-CONC": {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-AT-DETACH-FIN":  {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-AT-INHERIT":     {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-AT-OF":          {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},

	// index DDL.
	"R-IX-CREATE":      {lock: pg_contract.LockShare, op: pg_contract.OpKindScan, below: toPartitions},
	"R-IX-CREATE-CONC": {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindConcurrent, below: nowhere},
	// A partitioned index takes every index of its tree with it, each one through its table.
	"R-IX-DROP":         {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: toPartitions},
	"R-IX-DROP-CONC":    {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: toPartitions},
	"R-IX-SETOPT":       {lock: pg_contract.LockNone, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-IX-SETSTATS":     {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-IX-TABLESPACE":   {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindRewrite, below: nowhere},
	"R-IX-ATTACH":       {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-IX-RENAME":       {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-IX-REINDEX":      {lock: pg_contract.LockShare, op: pg_contract.OpKindScan, below: nowhere},
	"R-IX-REINDEX-CONC": {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindConcurrent, below: nowhere},
	"R-IX-REINDEX-WIDE": {lock: pg_contract.LockShare, op: pg_contract.OpKindScan, below: nowhere},

	// table creation and destruction.
	"R-TB-CREATE":          {lock: pg_contract.LockNone, op: pg_contract.OpKindNone, below: nowhere},
	"R-TB-CREATE-LIKE":     {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-TB-CREATE-INHERITS": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-TB-CREATE-PARTOF":   {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-TB-CTAS":            {lock: pg_contract.LockAccessShare, op: pg_contract.OpKindScan, below: nowhere},
	"R-TB-DROP":            {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},

	// views, matviews, sequences.
	"R-OB-REPLACEVIEW":    {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-OB-CREATEVIEW":     {lock: pg_contract.LockAccessShare, op: pg_contract.OpKindNone, below: nowhere},
	"R-OB-DROPVIEW":       {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-OB-CREATEMV":       {lock: pg_contract.LockAccessShare, op: pg_contract.OpKindScan, below: nowhere},
	"R-OB-REFRESHMV":      {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindRewrite, below: nowhere},
	"R-OB-REFRESHMV-CONC": {lock: pg_contract.LockExclusive, op: pg_contract.OpKindDML, below: nowhere},
	"R-OB-CREATESEQ":      {lock: pg_contract.LockNone, op: pg_contract.OpKindNone, below: nowhere},
	"R-OB-ALTERSEQ":       {lock: pg_contract.LockShareRowExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-OB-DROPSEQ":        {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},

	// the objects that hang off a table.
	"R-OB-CREATETRIG": {lock: pg_contract.LockShareRowExclusive, op: pg_contract.OpKindMetadata, below: toPartitions},
	"R-OB-DROPTRIG":   {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: toPartitions},
	"R-OB-ALTERTRIG":  {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: toPartitions},
	"R-OB-POLICY":     {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-OB-RULE":       {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-OB-STATS":      {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-OB-COMMENT":    {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	// PG18 opens the relation at ACCESS SHARE, 15 to 17 take no lock at all; the rule picks.
	"R-OB-GRANT":  {lock: pg_contract.LockNone, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-OB-DEFACL": {lock: pg_contract.LockNone, op: pg_contract.OpKindNone, below: nowhere},
	// A partitioned table is published as itself; a classic parent publishes its children with it.
	"R-OB-PUB":       {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindMetadata, below: below{children: worked}},
	"R-OB-SUB":       {lock: pg_contract.LockNone, op: pg_contract.OpKindNone, below: nowhere},
	"R-OB-EXTENSION": {lock: pg_contract.LockNone, op: pg_contract.OpKindNone, below: nowhere},
	"R-OB-SCHEMA":    {lock: pg_contract.LockNone, op: pg_contract.OpKindNone, below: nowhere},

	// types and domains.
	"R-TY-ADDVALUE":    {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-TY-RENAMEVALUE": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-TY-ATTR":        {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},
	"R-TY-DOMAIN-ADD":  {lock: pg_contract.LockShare, op: pg_contract.OpKindScan, below: nowhere},
	"R-TY-DROP":        {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: nowhere},

	// maintenance. VACUUM and ANALYZE reach the partitions on every version plus the classic inheritance children from PG18.
	"R-MT-VACUUM": {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindScan, below: toPartitions,
		changes: []change{{from: pg_contract.Version18, below: everywhere}}},
	"R-MT-ANALYZE": {lock: pg_contract.LockShareUpdateExclusive, op: pg_contract.OpKindScan, below: toPartitions,
		changes: []change{{from: pg_contract.Version18, below: everywhere}}},
	"R-MT-VACUUMFULL": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindRewrite, below: toPartitions,
		changes: []change{{from: pg_contract.Version18, below: everywhere}}},
	"R-MT-CLUSTER":  {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindRewrite, below: toPartitions},
	"R-MT-TRUNCATE": {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-MT-LOCK":     {lock: pg_contract.LockAccessExclusive, op: pg_contract.OpKindMetadata, below: everywhere},
	"R-MT-COPYFROM": {lock: pg_contract.LockRowExclusive, op: pg_contract.OpKindDML, below: toPartitions},
	"R-MT-COPYTO":   {lock: pg_contract.LockAccessShare, op: pg_contract.OpKindNone, below: nowhere},

	// data backfills.
	"R-DML-INSERT":    {lock: pg_contract.LockRowExclusive, op: pg_contract.OpKindDML, below: toPartitions},
	"R-DML-UPDATE":    {lock: pg_contract.LockRowExclusive, op: pg_contract.OpKindDML, below: everywhere},
	"R-DML-DELETE":    {lock: pg_contract.LockRowExclusive, op: pg_contract.OpKindDML, below: everywhere},
	"R-DML-MERGE":     {lock: pg_contract.LockRowExclusive, op: pg_contract.OpKindDML, below: everywhere},
	"R-DML-SELECTFOR": {lock: pg_contract.LockRowShare, op: pg_contract.OpKindNone, below: everywhere},
}
