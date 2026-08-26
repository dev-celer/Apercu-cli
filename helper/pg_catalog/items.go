package pg_catalog

import (
	"context"
	"database/sql"

	"github.com/lib/pq"
)

// queryRows runs one snapshot query and scans every row through scan.
func queryRows[T any](ctx context.Context, tx *sql.Tx, query string, scan func(*sql.Rows) (T, error)) ([]T, error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]T, 0)
	for rows.Next() {
		row, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// oids converts a scanned oid[] into the snapshot's own OID type.
func oids(raw pq.Int64Array) []OID {
	if len(raw) == 0 {
		return nil
	}
	out := make([]OID, len(raw))
	for i, v := range raw {
		out[i] = OID(v)
	}
	return out
}

// attnums converts a scanned int2[] of column numbers.
func attnums(raw pq.Int64Array) []int16 {
	if len(raw) == 0 {
		return nil
	}
	out := make([]int16, len(raw))
	for i, v := range raw {
		out[i] = int16(v)
	}
	return out
}

const schemasQuery = `
SELECT n.oid, n.nspname, pg_get_userbyid(n.nspowner) AS owner
FROM pg_namespace n WHERE ` + userNS

// collectSchemas is S-01.
func collectSchemas(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	rows, err := queryRows(ctx, tx, schemasQuery, func(r *sql.Rows) (Schema, error) {
		s := Schema{}
		err := r.Scan(&s.OID, &s.Name, &s.Owner)
		return s, err
	})
	snapshot.Schemas = rows
	return err
}

const relationsQuery = `
SELECT c.oid, n.nspname, c.relname, c.relkind, c.relpersistence,
       c.relispartition, c.relrowsecurity, c.relhassubclass,
       c.reltuples, c.relpages, c.reloptions, c.reltablespace, c.reloftype,
       am.amname AS access_method, pg_get_userbyid(c.relowner) AS owner,
       CASE WHEN c.relkind IN ('r','m','i','t','S')
            THEN pg_relation_size(c.oid) ELSE 0 END AS heap_bytes,
       CASE WHEN c.relkind IN ('r','m') THEN pg_total_relation_size(c.oid) ELSE 0 END AS total_bytes,
       pg_get_expr(c.relpartbound, c.oid) AS partition_bound,
       CASE WHEN c.relkind IN ('p','I') THEN pg_get_partkeydef(c.oid) END AS partition_key
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace AND ` + userNS + `
LEFT JOIN pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r','p','i','I','m','v','S','f','c')`

// collectRelations is S-02.
func collectRelations(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	rows, err := queryRows(ctx, tx, relationsQuery, func(r *sql.Rows) (Relation, error) {
		rel := Relation{}
		var options pq.StringArray
		var accessMethod, partitionBound, partitionKey sql.NullString
		err := r.Scan(
			&rel.OID, &rel.Namespace, &rel.Name, &rel.Kind, &rel.Persistence,
			&rel.IsPartition, &rel.RowSecurity, &rel.HasSubclass,
			&rel.Tuples, &rel.Pages, &options, &rel.Tablespace, &rel.OfType,
			&accessMethod, &rel.Owner, &rel.HeapBytes, &rel.TotalBytes,
			&partitionBound, &partitionKey,
		)
		rel.Options = options
		rel.AccessMethod = accessMethod.String
		rel.PartitionBound = partitionBound.String
		rel.PartitionKey = partitionKey.String
		return rel, err
	})
	snapshot.Relations = rows
	return err
}

const columnsQuery = `
SELECT a.attrelid, a.attnum, a.attname, a.atttypid,
       format_type(a.atttypid, a.atttypmod) AS formatted_type, a.atttypmod,
       a.attnotnull, a.attgenerated, a.attidentity, a.attstorage,
       a.attcompression, a.attislocal, a.attinhcount, a.attcollation, a.attstattarget
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace AND ` + userNS + `
WHERE a.attnum > 0 AND NOT a.attisdropped`

// collectColumns is S-03.
func collectColumns(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	rows, err := queryRows(ctx, tx, columnsQuery, func(r *sql.Rows) (Column, error) {
		c := Column{}
		var statsTarget sql.NullInt32
		err := r.Scan(
			&c.RelID, &c.Num, &c.Name, &c.TypeID, &c.FormattedType, &c.TypeMod,
			&c.NotNull, &c.Generated, &c.Identity, &c.Storage,
			&c.Compression, &c.IsLocal, &c.InhCount, &c.Collation, &statsTarget,
		)
		// attstattarget is NOT NULL with -1 for "default" on PG 15/16, and
		// nullable with NULL for "default" on 17/18. Both mean the same thing.
		if statsTarget.Valid && statsTarget.Int32 >= 0 {
			value := statsTarget.Int32
			c.StatsTarget = &value
		}
		return c, err
	})
	snapshot.Columns = rows
	return err
}

const defaultsQuery = `
SELECT d.adrelid, d.adnum, pg_get_expr(d.adbin, d.adrelid) AS default_expr,
       ARRAY(SELECT dep.refobjid FROM pg_depend dep
             WHERE dep.objid = d.oid AND dep.classid = 'pg_attrdef'::regclass
               AND dep.refclassid = 'pg_proc'::regclass) AS referenced_procs,
       ARRAY(SELECT dep.refobjid FROM pg_depend dep
             WHERE dep.objid = d.oid AND dep.classid = 'pg_attrdef'::regclass
               AND dep.refclassid = 'pg_operator'::regclass) AS referenced_operators
FROM pg_attrdef d
JOIN pg_class c ON c.oid = d.adrelid
JOIN pg_namespace n ON n.oid = c.relnamespace AND ` + userNS

// collectDefaults is S-04.
func collectDefaults(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	rows, err := queryRows(ctx, tx, defaultsQuery, func(r *sql.Rows) (ColumnDefault, error) {
		d := ColumnDefault{}
		var procs, operators pq.Int64Array
		var expr sql.NullString
		err := r.Scan(&d.RelID, &d.Num, &expr, &procs, &operators)
		d.Expr = expr.String
		d.ReferencedProcs = oids(procs)
		d.ReferencedOperators = oids(operators)
		return d, err
	})
	snapshot.Defaults = rows
	return err
}

// constraintsQuery is S-05. conperiod and conenforced do not exist before PG 18,
// so they are appended conditionally and defaulted on load.
func constraintsQuery(serverVersionNum int) string {
	query := `
SELECT k.oid, k.conname, k.conrelid, k.confrelid, k.contype, k.convalidated,
       k.condeferrable, k.condeferred, k.conislocal, k.coninhcount, k.connoinherit,
       k.conkey, k.confkey, k.conindid, k.confupdtype, k.confdeltype, k.contypid,
       pg_get_constraintdef(k.oid) AS def`
	if serverVersionNum >= 180000 {
		query += `, k.conperiod, k.conenforced`
	}
	return query + `
FROM pg_constraint k
JOIN pg_namespace n ON n.oid = k.connamespace AND ` + userNS
}

// collectConstraints is S-05.
func collectConstraints(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	version := snapshot.Header.ServerVersionNum
	query := constraintsQuery(version)

	rows, err := queryRows(ctx, tx, query, func(r *sql.Rows) (Constraint, error) {
		// A constraint has no period and is enforced on every version that does not know about the columns.
		c := Constraint{Period: false, Enforced: true}
		var key, foreignKey pq.Int64Array
		var def sql.NullString

		targets := []any{
			&c.OID, &c.Name, &c.RelID, &c.ForeignRelID, &c.Type, &c.Validated,
			&c.Deferrable, &c.Deferred, &c.IsLocal, &c.InhCount, &c.NoInherit,
			&key, &foreignKey, &c.IndexID, &c.FKUpdateType, &c.FKDeleteType, &c.TypeID,
			&def,
		}
		if version >= 180000 {
			targets = append(targets, &c.Period, &c.Enforced)
		}

		err := r.Scan(targets...)
		c.Key = attnums(key)
		c.ForeignKey = attnums(foreignKey)
		c.Def = def.String
		return c, err
	})
	snapshot.Constraints = rows
	return err
}

const indexesQuery = `
SELECT i.indexrelid, i.indrelid, i.indisunique, i.indisprimary, i.indisexclusion,
       i.indisvalid, i.indisready, i.indislive, i.indisclustered,
       i.indnatts, i.indnkeyatts,
       string_to_array(i.indkey::text, ' ')::int2[] AS columns,
       string_to_array(i.indcollation::text, ' ')::oid[] AS collations,
       pg_get_indexdef(i.indexrelid) AS def,
       pg_get_expr(i.indpred, i.indrelid) AS predicate
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_namespace n ON n.oid = c.relnamespace AND ` + userNS

// collectIndexes is S-06.
func collectIndexes(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	rows, err := queryRows(ctx, tx, indexesQuery, func(r *sql.Rows) (Index, error) {
		i := Index{}
		var columns, collations pq.Int64Array
		var def, predicate sql.NullString
		err := r.Scan(
			&i.IndexRelID, &i.RelID, &i.IsUnique, &i.IsPrimary, &i.IsExclusion,
			&i.IsValid, &i.IsReady, &i.IsLive, &i.IsClustered,
			&i.NAtts, &i.NKeyAtts, &columns, &collations, &def, &predicate,
		)
		i.Columns = attnums(columns)
		i.Collations = oids(collations)
		i.Def = def.String
		i.Predicate = predicate.String
		return i, err
	})
	snapshot.Indexes = rows
	return err
}

const inheritsQuery = `
SELECT h.inhparent, h.inhrelid, h.inhseqno, h.inhdetachpending,
       pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT' AS is_default_partition,
       p.relkind = 'p' AS parent_is_partitioned
FROM pg_inherits h
JOIN pg_class c ON c.oid = h.inhrelid
JOIN pg_class p ON p.oid = h.inhparent
JOIN pg_namespace n ON n.oid = p.relnamespace AND ` + userNS

// collectInherits is S-07.
func collectInherits(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	rows, err := queryRows(ctx, tx, inheritsQuery, func(r *sql.Rows) (InheritEdge, error) {
		e := InheritEdge{}
		// A classic INHERITS child has no partition bound, so the comparison is NULL rather than false.
		var isDefault sql.NullBool
		err := r.Scan(&e.Parent, &e.Child, &e.SeqNo, &e.DetachPending, &isDefault, &e.ParentIsPartitioned)
		e.IsDefaultPartition = isDefault.Valid && isDefault.Bool
		return e, err
	})
	snapshot.Inherits = rows
	return err
}

const sequencesQuery = `
SELECT s.oid AS seqrelid, sq.seqtypid, sq.seqmax, sq.seqcycle,
       d.refobjid AS owner_table, d.refobjsubid AS owner_attnum, d.deptype
FROM pg_class s
JOIN pg_namespace n ON n.oid = s.relnamespace AND ` + userNS + `
JOIN pg_sequence sq ON sq.seqrelid = s.oid
LEFT JOIN pg_depend d ON d.objid = s.oid AND d.classid = 'pg_class'::regclass
                     AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a','i')
WHERE s.relkind = 'S'`

// collectSequences is S-08.
func collectSequences(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	rows, err := queryRows(ctx, tx, sequencesQuery, func(r *sql.Rows) (Sequence, error) {
		s := Sequence{}
		var ownerTable sql.NullInt64
		var ownerAttNum sql.NullInt32
		var depType sql.NullString
		err := r.Scan(&s.SeqRelID, &s.TypeID, &s.Max, &s.Cycle, &ownerTable, &ownerAttNum, &depType)
		s.OwnerTable = OID(ownerTable.Int64)
		s.OwnerAttNum = int16(ownerAttNum.Int32)
		s.DepType = depType.String
		return s, err
	})
	snapshot.Sequences = rows
	return err
}

const typesQuery = `
SELECT t.oid, n.nspname, t.typname, t.typtype, t.typbasetype, t.typtypmod,
       t.typnotnull, t.typelem, t.typrelid, t.typcategory, t.typlen,
       t.typinput::oid, t.typoutput::oid,
       CASE WHEN t.typtype = 'e'
            THEN (SELECT array_agg(enumlabel ORDER BY enumsortorder)
                  FROM pg_enum WHERE enumtypid = t.oid) END::text[] AS enum_labels,
       (SELECT count(*) FROM pg_constraint WHERE contypid = t.oid) AS n_domain_constraints
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE t.typtype IN ('b','d','e','c','r','m')`

// collectTypes is S-09.
func collectTypes(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	rows, err := queryRows(ctx, tx, typesQuery, func(r *sql.Rows) (Type, error) {
		t := Type{}
		var labels pq.StringArray
		err := r.Scan(
			&t.OID, &t.Namespace, &t.Name, &t.Type, &t.BaseTypeID, &t.TypeMod,
			&t.NotNull, &t.ElemID, &t.RelID, &t.Category, &t.Len,
			&t.Input, &t.Output, &labels, &t.DomainConstraints,
		)
		t.EnumLabels = labels
		return t, err
	})
	snapshot.Types = rows
	return err
}

const triggersQuery = `
SELECT g.oid, g.tgrelid, g.tgname, g.tgenabled, g.tgtype, g.tgconstraint,
       pg_get_triggerdef(g.oid) AS def
FROM pg_trigger g
JOIN pg_class c ON c.oid = g.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace AND ` + userNS + `
WHERE NOT g.tgisinternal`

const policiesQuery = `
SELECT p.oid, p.polrelid, p.polname, p.polcmd, p.polpermissive
FROM pg_policy p
JOIN pg_class c ON c.oid = p.polrelid
JOIN pg_namespace n ON n.oid = c.relnamespace AND ` + userNS

// collectTriggersAndPolicies is S-10.
func collectTriggersAndPolicies(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	triggers, err := queryRows(ctx, tx, triggersQuery, func(r *sql.Rows) (Trigger, error) {
		t := Trigger{}
		var def sql.NullString
		err := r.Scan(&t.OID, &t.RelID, &t.Name, &t.Enabled, &t.Type, &t.ConstraintOID, &def)
		t.Def = def.String
		return t, err
	})
	snapshot.Triggers = triggers
	if err != nil {
		return err
	}

	policies, err := queryRows(ctx, tx, policiesQuery, func(r *sql.Rows) (Policy, error) {
		p := Policy{}
		err := r.Scan(&p.OID, &p.RelID, &p.Name, &p.Cmd, &p.Permissive)
		return p, err
	})
	snapshot.Policies = policies
	return err
}

const viewDepsQuery = `
SELECT DISTINCT r.ev_class AS dependent_relid, d.refobjid AS referenced_relid,
       d.refobjsubid AS referenced_attnum
FROM pg_depend d
JOIN pg_rewrite r ON r.oid = d.objid AND d.classid = 'pg_rewrite'::regclass
JOIN pg_class rc ON rc.oid = d.refobjid
JOIN pg_namespace n ON n.oid = rc.relnamespace AND ` + userNS + `
WHERE r.ev_class <> d.refobjid`

// collectViewDeps is S-11.
func collectViewDeps(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	rows, err := queryRows(ctx, tx, viewDepsQuery, func(r *sql.Rows) (ViewDep, error) {
		d := ViewDep{}
		err := r.Scan(&d.DependentRelID, &d.ReferencedRelID, &d.ReferencedAttNum)
		return d, err
	})
	snapshot.ViewDeps = rows
	return err
}

const dependsQuery = `
SELECT d.classid, d.objid, d.objsubid, d.refclassid, d.refobjid, d.refobjsubid, d.deptype
FROM pg_depend d
JOIN pg_class c ON c.oid = d.refobjid AND d.refclassid = 'pg_class'::regclass
JOIN pg_namespace n ON n.oid = c.relnamespace AND ` + userNS + `
WHERE d.deptype IN ('a','n','i','P')`

// collectDepends is S-12.
func collectDepends(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	rows, err := queryRows(ctx, tx, dependsQuery, func(r *sql.Rows) (DependEdge, error) {
		e := DependEdge{}
		err := r.Scan(&e.ClassID, &e.ObjID, &e.ObjSubID, &e.RefClassID, &e.RefObjID, &e.RefObjSubID, &e.DepType)
		return e, err
	})
	snapshot.Depends = rows
	return err
}

const procsQuery = `
SELECT p.oid, n.nspname, p.proname, p.provolatile, p.proisstrict, p.prokind,
       p.pronargs, p.pronargdefaults, p.provariadic <> 0 AS is_variadic,
       pg_get_function_identity_arguments(p.oid) AS identity_args
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace`

// collectProcs is S-13.
func collectProcs(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	rows, err := queryRows(ctx, tx, procsQuery, func(r *sql.Rows) (Proc, error) {
		p := Proc{}
		var identityArgs sql.NullString
		err := r.Scan(
			&p.OID, &p.Namespace, &p.Name, &p.Volatility, &p.IsStrict, &p.Kind,
			&p.NArgs, &p.NArgDefaults, &p.IsVariadic, &identityArgs,
		)
		p.IdentityArgs = identityArgs.String
		return p, err
	})
	snapshot.Procs = rows
	return err
}

const castsQuery = `SELECT castsource, casttarget, castmethod, castcontext, castfunc FROM pg_cast`

const operatorsQuery = `
SELECT o.oid, o.oprname, o.oprnamespace, o.oprleft, o.oprright, o.oprcode::oid
FROM pg_operator o`

// collectCastsAndOperators is S-14.
func collectCastsAndOperators(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	casts, err := queryRows(ctx, tx, castsQuery, func(r *sql.Rows) (Cast, error) {
		c := Cast{}
		err := r.Scan(&c.Source, &c.Target, &c.Method, &c.Context, &c.Func)
		return c, err
	})
	snapshot.Casts = casts
	if err != nil {
		return err
	}

	operators, err := queryRows(ctx, tx, operatorsQuery, func(r *sql.Rows) (Operator, error) {
		o := Operator{}
		err := r.Scan(&o.OID, &o.Name, &o.Namespace, &o.Left, &o.Right, &o.Code)
		return o, err
	})
	snapshot.Operators = operators
	return err
}

const publicationsQuery = `
SELECT p.oid, p.pubname, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete
FROM pg_publication p`

const publicationRelsQuery = `SELECT prpubid, prrelid FROM pg_publication_rel`

const extensionsQuery = `SELECT e.oid, e.extname, e.extversion, e.extnamespace FROM pg_extension e`

// collectPublications is S-15.
func collectPublications(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	publications, err := queryRows(ctx, tx, publicationsQuery, func(r *sql.Rows) (Publication, error) {
		p := Publication{}
		err := r.Scan(&p.OID, &p.Name, &p.AllTables, &p.Insert, &p.Update, &p.Delete)
		return p, err
	})
	snapshot.Publications = publications
	if err != nil {
		return err
	}

	rels, err := queryRows(ctx, tx, publicationRelsQuery, func(r *sql.Rows) (PublicationRel, error) {
		rel := PublicationRel{}
		err := r.Scan(&rel.PubID, &rel.RelID)
		return rel, err
	})
	snapshot.PublicationRels = rels
	if err != nil {
		return err
	}

	extensions, err := queryRows(ctx, tx, extensionsQuery, func(r *sql.Rows) (Extension, error) {
		e := Extension{}
		var version sql.NullString
		err := r.Scan(&e.OID, &e.Name, &version, &e.Namespace)
		e.Version = version.String
		return e, err
	})
	snapshot.Extensions = extensions
	return err
}

const settingsQuery = `
SELECT name, setting, source FROM pg_settings
WHERE name IN ('search_path','TimeZone','lock_timeout','statement_timeout',
               'deadlock_timeout','session_replication_role','default_table_access_method',
               'max_locks_per_transaction','idle_in_transaction_session_timeout',
               'default_statistics_target','maintenance_work_mem',
               'max_parallel_maintenance_workers')`

// collectSettings is S-16.
func collectSettings(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	rows, err := queryRows(ctx, tx, settingsQuery, func(r *sql.Rows) (Setting, error) {
		s := Setting{}
		err := r.Scan(&s.Name, &s.Value, &s.Source)
		return s, err
	})
	snapshot.Settings = rows
	return err
}

const tableStatsQuery = `
SELECT relid, schemaname, relname, seq_scan, idx_scan,
       n_tup_ins, n_tup_upd, n_tup_del, n_live_tup, n_dead_tup,
       last_autovacuum, last_analyze
FROM pg_stat_user_tables`

// collectTableStats is S-17.
func collectTableStats(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	rows, err := queryRows(ctx, tx, tableStatsQuery, func(r *sql.Rows) (TableStat, error) {
		s := TableStat{}
		// A table that has never been scanned through an index reports NULL, not
		// zero, and the vacuum timestamps are NULL until the first run.
		var seqScan, idxScan sql.NullInt64
		var lastAutovacuum, lastAnalyze sql.NullTime
		err := r.Scan(
			&s.RelID, &s.Namespace, &s.Name, &seqScan, &idxScan,
			&s.TupIns, &s.TupUpd, &s.TupDel, &s.LiveTup, &s.DeadTup,
			&lastAutovacuum, &lastAnalyze,
		)
		s.SeqScan = seqScan.Int64
		s.IdxScan = idxScan.Int64
		if lastAutovacuum.Valid {
			at := lastAutovacuum.Time.UTC()
			s.LastAutovacuum = &at
		}
		if lastAnalyze.Valid {
			at := lastAnalyze.Time.UTC()
			s.LastAnalyze = &at
		}
		return s, err
	})
	snapshot.TableStats = rows
	return err
}

const rolesQuery = `SELECT oid, rolname, rolsuper, rolcanlogin FROM pg_roles`

const relACLsQuery = `
SELECT c.oid AS relid, c.relacl::text[] AS acl
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace AND ` + userNS + `
WHERE c.relacl IS NOT NULL`

// collectRoles is S-18.
func collectRoles(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	roles, err := queryRows(ctx, tx, rolesQuery, func(r *sql.Rows) (Role, error) {
		role := Role{}
		err := r.Scan(&role.OID, &role.Name, &role.Super, &role.CanLogin)
		return role, err
	})
	snapshot.Roles = roles
	if err != nil {
		return err
	}

	acls, err := queryRows(ctx, tx, relACLsQuery, func(r *sql.Rows) (RelACL, error) {
		acl := RelACL{}
		var entries pq.StringArray
		err := r.Scan(&acl.RelID, &entries)
		acl.ACL = entries
		return acl, err
	})
	snapshot.RelACLs = acls
	return err
}

const collationsQuery = `
SELECT l.oid, n.nspname, l.collname, l.collencoding
FROM pg_collation l JOIN pg_namespace n ON n.oid = l.collnamespace`

// collectCollations is S-19.
func collectCollations(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	rows, err := queryRows(ctx, tx, collationsQuery, func(r *sql.Rows) (Collation, error) {
		c := Collation{}
		err := r.Scan(&c.OID, &c.Namespace, &c.Name, &c.Encoding)
		return c, err
	})
	snapshot.Collations = rows
	return err
}
