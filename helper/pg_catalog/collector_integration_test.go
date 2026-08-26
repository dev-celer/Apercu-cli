//go:build integration

// The collector's integration tests start a PostgreSQL container of each supported
// version through testcontainers, a working Docker daemon is required
package pg_catalog

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var integrationVersions = []int{15, 16, 17, 18}

// seedDDL contain the PostgreSQL queries to initialize the test databases against which the snapshot run.
const seedDDL = `
DROP SCHEMA IF EXISTS ` + testSchema + ` CASCADE;
CREATE SCHEMA ` + testSchema + `;
SET search_path TO ` + testSchema + `;

CREATE TABLE users (
    id         bigserial PRIMARY KEY,
    email      text NOT NULL,
    created_at timestamptz DEFAULT now()
);

CREATE FUNCTION next_code() RETURNS text LANGUAGE sql VOLATILE AS $$ SELECT 'c' $$;

CREATE TABLE orders (
    id      bigint PRIMARY KEY,
    user_id bigint REFERENCES users(id),
    total   numeric(10,2),
    status  text,
    code    text DEFAULT next_code()
);
ALTER TABLE orders ADD CONSTRAINT orders_total_check CHECK (total >= 0) NOT VALID;
CREATE INDEX orders_open_idx ON orders (status) WHERE status <> 'done';
ALTER TABLE orders ALTER COLUMN status SET STATISTICS 500;

CREATE TABLE events (id bigint, at timestamptz NOT NULL) PARTITION BY RANGE (at);
CREATE TABLE events_2025 PARTITION OF events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE events_default PARTITION OF events DEFAULT;

CREATE TABLE legacy_parent (id bigint);
CREATE TABLE legacy_child () INHERITS (legacy_parent);

CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);
CREATE TABLE profiles (user_id bigint PRIMARY KEY REFERENCES users(id), feeling mood, score positive_int);

CREATE VIEW active_users AS SELECT id, email FROM users;
CREATE MATERIALIZED VIEW order_totals AS SELECT user_id, sum(total) AS total FROM orders GROUP BY user_id;

CREATE FUNCTION bump() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER orders_bump BEFORE UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION bump();

CREATE SEQUENCE standalone_seq;

CREATE COLLATION case_sensitive (LC_COLLATE = 'C', LC_CTYPE = 'C');
CREATE TABLE labels (id bigint, name text COLLATE case_sensitive, weight int);
CREATE INDEX labels_name_idx ON labels (name, weight);

INSERT INTO users (email) SELECT 'user' || i || '@example.com' FROM generate_series(1, 500) i;
INSERT INTO orders (id, user_id, total, status)
    SELECT i, (i % 500) + 1, i, 'open' FROM generate_series(1, 500) i;
INSERT INTO events (id, at) SELECT i, '2025-06-01'::timestamptz FROM generate_series(1, 200) i;
ANALYZE;
`

// startPostgres brings up one PostgreSQL of the given major version and returns a connection to it.
// The container is torn down when the test ends.
func startPostgres(t *testing.T, version int) *sql.DB {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx, fmt.Sprintf("postgres:%d", version),
		postgres.WithDatabase("app"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("pg"),
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sql.Open("postgres", url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.Ping())
	return db
}

func TestCollectAgainstServer(t *testing.T) {
	t.Parallel()

	for _, version := range integrationVersions {
		t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
			t.Parallel()
			collectAndVerify(t, startPostgres(t, version))
		})
	}
}

// collectAndVerify seeds the passed database, snapshots it, and checks the validity of the snapshot.
func collectAndVerify(t *testing.T, db *sql.DB) {
	ctx := context.Background()

	_, err := db.ExecContext(ctx, seedDDL)
	require.NoError(t, err)

	var serverVersionNum int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT current_setting('server_version_num')::int").Scan(&serverVersionNum))
	if serverVersionNum >= 180000 {
		// NOT ENFORCED exists only on 18, and is the only way to see conenforced carry anything but true.
		_, err = db.ExecContext(ctx, `SET search_path TO `+testSchema+`;
			ALTER TABLE orders ADD CONSTRAINT orders_code_check CHECK (code <> '') NOT ENFORCED;`)
		require.NoError(t, err)
	}

	baseline, err := Collect(ctx, db, CollectOptions{Source: SourceBaseline, PIT: PITPre})
	require.NoError(t, err)

	prod, err := Collect(ctx, db, CollectOptions{Source: SourceProd, PIT: PITPre})
	require.NoError(t, err)

	version := baseline.Header.Version
	t.Logf("collected against PostgreSQL %s (%d)", version, baseline.Header.ServerVersionNum)
	require.True(t, version.IsSupported(), "unsupported server version %s", version)

	t.Run("header", func(t *testing.T) {
		assert.NotEmpty(t, baseline.Header.Database)
		assert.NotEmpty(t, baseline.Header.SearchPath)
		assert.NotEmpty(t, baseline.Header.TimeZone)
		assert.False(t, baseline.Header.FromReplica)
	})

	t.Run("routing", func(t *testing.T) {
		// The baseline carries the schema, production carries the activity.
		assert.True(t, baseline.Has("S-02"))
		assert.False(t, baseline.Has("S-17"))
		assert.True(t, prod.Has("S-17"))
		assert.False(t, prod.Has("S-02"))
		assert.NotEmpty(t, prod.TableStats)
		assert.Empty(t, prod.Relations)
	})

	t.Run("scope excludes system schemas", func(t *testing.T) {
		for _, rel := range baseline.Relations {
			assert.NotEqual(t, "information_schema", rel.Namespace)
			assert.NotContains(t, rel.Namespace, "pg_")
		}
	})

	t.Run("inheritance edges", func(t *testing.T) {
		events, _ := findRelation(baseline, "events")
		defaultPart, _ := findRelation(baseline, "events_default")
		legacyParent, _ := findRelation(baseline, "legacy_parent")

		var sawDefault, sawLegacy bool
		for _, edge := range baseline.Inherits {
			if edge.Parent == events.OID && edge.Child == defaultPart.OID {
				sawDefault = true
				assert.True(t, edge.IsDefaultPartition)
				assert.True(t, edge.ParentIsPartitioned)
			}
			if edge.Parent == legacyParent.OID {
				sawLegacy = true
				// Classic inheritance has no partition bound at all.
				assert.False(t, edge.IsDefaultPartition)
				assert.False(t, edge.ParentIsPartitioned)
			}
		}
		assert.True(t, sawDefault, "default partition edge missing")
		assert.True(t, sawLegacy, "classic inheritance edge missing")
	})

	t.Run("statistics target normalizes across versions", func(t *testing.T) {
		orders, ok := findRelation(baseline, "orders")
		require.True(t, ok)

		// -1 on PG 15/16 and NULL on 17/18 both mean "database default".
		id, ok := findColumn(baseline, orders.OID, "id")
		require.True(t, ok)
		assert.Nil(t, id.StatsTarget)

		status, ok := findColumn(baseline, orders.OID, "status")
		require.True(t, ok)
		require.NotNil(t, status.StatsTarget)
		assert.Equal(t, int32(500), *status.StatsTarget)
	})

	t.Run("constraints", func(t *testing.T) {
		orders, _ := findRelation(baseline, "orders")
		users, _ := findRelation(baseline, "users")

		var check, foreignKey Constraint
		for _, c := range baseline.Constraints {
			if c.RelID == orders.OID && c.Name == "orders_total_check" {
				check = c
			}
			if c.RelID == orders.OID && c.Type == "f" {
				foreignKey = c
			}
		}

		require.NotZero(t, check.OID)
		assert.Equal(t, "c", check.Type)
		assert.False(t, check.Validated, "NOT VALID constraint must not read as validated")
		assert.Contains(t, check.Def, "NOT VALID")
		// conenforced does not exist before 18, and every constraint there is
		// enforced by definition.
		assert.True(t, check.Enforced)
		assert.False(t, check.Period)

		require.NotZero(t, foreignKey.OID)
		assert.Equal(t, users.OID, foreignKey.ForeignRelID)
		assert.NotEmpty(t, foreignKey.Key)
		assert.NotEmpty(t, foreignKey.ForeignKey)

		// NOT NULL became a real constraint row in 18. Before that, nullability lives only in S-03.attnotnull.
		var notNullConstraints, unenforced int
		for _, c := range baseline.Constraints {
			if c.RelID == users.OID && c.Type == "n" {
				notNullConstraints++
			}
			if c.RelID == orders.OID && !c.Enforced {
				unenforced++
			}
		}
		if version >= 18 {
			assert.Positive(t, notNullConstraints, "PG 18 records NOT NULL as a constraint")
			assert.Equal(t, 1, unenforced, "the NOT ENFORCED constraint must read as unenforced")
		} else {
			assert.Zero(t, notNullConstraints, "NOT NULL is not a constraint before PG 18")
			assert.Zero(t, unenforced, "every constraint is enforced before PG 18")
		}
	})

	t.Run("defaults record dependency for user functions", func(t *testing.T) {
		orders, _ := findRelation(baseline, "orders")
		code, _ := findColumn(baseline, orders.OID, "code")

		var userDefined ColumnDefault
		for _, d := range baseline.Defaults {
			if d.RelID == orders.OID && d.Num == code.Num {
				userDefined = d
			}
		}

		// This work only for user-created functions, pinned functions does not appear in the dependencies.
		assert.Contains(t, userDefined.Expr, "next_code()")
		assert.Len(t, userDefined.ReferencedProcs, 1)
	})

	t.Run("indexes", func(t *testing.T) {
		orders, _ := findRelation(baseline, "orders")

		var partial Index
		for _, idx := range baseline.Indexes {
			if idx.RelID != orders.OID {
				continue
			}
			if idx.Predicate != "" {
				partial = idx
			}
			assert.True(t, idx.IsValid, "seed indexes are all valid")
		}
		require.NotZero(t, partial.IndexRelID)
		assert.Contains(t, partial.Predicate, "status")
		assert.NotEmpty(t, partial.Columns)
	})

	t.Run("types and domains", func(t *testing.T) {
		var mood, domain Type
		for _, typ := range baseline.Types {
			if typ.Namespace == testSchema && typ.Name == "mood" {
				mood = typ
			}
			if typ.Namespace == testSchema && typ.Name == "positive_int" {
				domain = typ
			}
		}

		require.NotZero(t, mood.OID)
		assert.Equal(t, "e", mood.Type)
		assert.Equal(t, []string{"sad", "ok", "happy"}, mood.EnumLabels)

		require.NotZero(t, domain.OID)
		assert.Equal(t, "d", domain.Type)
		assert.Equal(t, 1, domain.DomainConstraints)

		// Reference data is captured whole, built-in types included.
		var sawBuiltin bool
		for _, typ := range baseline.Types {
			if typ.Namespace == "pg_catalog" && typ.Name == "int8" {
				sawBuiltin = true
			}
		}
		assert.True(t, sawBuiltin, "built-in types must be captured as reference data")
	})

	t.Run("triggers exclude internal ones", func(t *testing.T) {
		orders, _ := findRelation(baseline, "orders")

		names := []string{}
		for _, trigger := range baseline.Triggers {
			if trigger.RelID == orders.OID {
				names = append(names, trigger.Name)
			}
		}
		// The FK on orders installs internal triggers; only the user one shows.
		assert.Equal(t, []string{"orders_bump"}, names)
	})

	t.Run("view dependencies keep the column", func(t *testing.T) {
		users, _ := findRelation(baseline, "users")
		view, _ := findRelation(baseline, "active_users")

		var sawColumnEdge bool
		for _, dep := range baseline.ViewDeps {
			if dep.DependentRelID == view.OID && dep.ReferencedRelID == users.OID && dep.ReferencedAttNum > 0 {
				sawColumnEdge = true
			}
		}
		assert.True(t, sawColumnEdge, "The view dependencies edge should be in the snapshot")
	})

	t.Run("collations", func(t *testing.T) {
		labels, ok := findRelation(baseline, "labels")
		require.True(t, ok)
		name, _ := findColumn(baseline, labels.OID, "name")
		weight, _ := findColumn(baseline, labels.OID, "weight")

		byOID := map[OID]Collation{}
		for _, c := range baseline.Collations {
			byOID[c.OID] = c
		}
		// Reference data, captured whole: the built-ins a user column can point at are there.
		assert.NotEmpty(t, baseline.Collations)
		var sawBuiltin bool
		for _, c := range baseline.Collations {
			if c.Namespace == "pg_catalog" && c.Name == "C" {
				sawBuiltin = true
			}
		}
		assert.True(t, sawBuiltin)

		// A user collation resolves through the same table as a built-in one.
		userCollation, ok := byOID[name.Collation]
		require.True(t, ok, "the column's collation must resolve")
		assert.Equal(t, testSchema, userCollation.Namespace)
		assert.Equal(t, "case_sensitive", userCollation.Name)

		// A non-collatable column carries no collation at all.
		assert.Zero(t, weight.Collation)

		var index Index
		for _, i := range baseline.Indexes {
			if i.RelID == labels.OID {
				index = i
			}
		}
		require.NotZero(t, index.IndexRelID)
		// indcollation is an oidvector like indkey: one entry per column, in the same order,
		// and 0 wherever the column is not collatable.
		require.Len(t, index.Collations, len(index.Columns))
		assert.Equal(t, name.Collation, index.Collations[0])
		assert.Zero(t, index.Collations[1])
	})

	t.Run("relkinds", func(t *testing.T) {
		kinds := map[string]bool{}
		for _, rel := range baseline.Relations {
			if rel.Namespace == testSchema {
				kinds[rel.Kind] = true
			}
		}
		for _, kind := range []string{"r", "p", "i", "v", "m", "S"} {
			assert.True(t, kinds[kind], "relkind %q missing from the inventory", kind)
		}
	})

	t.Run("reference data", func(t *testing.T) {
		assert.NotEmpty(t, baseline.Procs)
		assert.NotEmpty(t, baseline.Casts)
		assert.NotEmpty(t, baseline.Operators)
		assert.NotEmpty(t, baseline.Settings)
		assert.NotEmpty(t, baseline.Roles)
	})

	if os.Getenv("APERCU_UPDATE_FIXTURES") == "" {
		return
	}
	require.NoError(t, os.MkdirAll("testdata", 0o755))
	for _, snapshot := range []*Snapshot{baseline, prod} {
		path := filepath.Join("testdata", fmt.Sprintf("snapshot_pg%s_%s.json.gz", version, snapshot.Source))
		require.NoError(t, snapshot.WriteJSON(path))
		t.Logf("wrote fixture %s", path)
	}
}
