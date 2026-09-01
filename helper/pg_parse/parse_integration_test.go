//go:build integration

package pg_parse

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"apercu-cli/helper/pg_contract"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var integrationVersions = []pg_contract.Version{15, 16, 17, 18}

const (
	syntaxError         = "42601"
	featureNotSupported = "0A000"
)

// notOffered are the statements that must not be handed to a live server.
var notOffered = map[string]string{
	"R-OB-SUB":      "CREATE SUBSCRIPTION dials the publisher before it reports anything",
	"R-MT-COPYFROM": "COPY FROM STDIN leaves the connection mid-copy",
	"R-MT-COPYTO":   "COPY TO STDOUT answers with a CopyOutResponse the driver rejects",
}

func startPostgres(t *testing.T, version pg_contract.Version) *sql.DB {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx,
		fmt.Sprintf("postgres:%d-alpine", version),
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

// acceptsSyntax reports whether the server has the construct the statement uses at all. It runs
// inside a transaction that is always rolled back, and the corpus names objects that do not
// exist, so nothing the statement would have done survives.
func acceptsSyntax(t *testing.T, db *sql.DB, statement string) bool {
	t.Helper()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, statement)
	if err == nil {
		return true
	}
	var pgErr *pq.Error
	if !assert.ErrorAs(t, err, &pgErr, "unexpected non-server error for %q", statement) {
		return true
	}
	code := string(pgErr.Code)
	return code != syntaxError && code != featureNotSupported
}

// TestFeatureVersionsMatchServer is the oracle for version gating features. It fails when the IR claims a statement
// runs on a version that rejects it, and equally when it claims a version bound the servers do not agree with.
func TestFeatureVersionsMatchServer(t *testing.T) {
	t.Parallel()

	entries := loadCorpus(t)
	accepted := make([]map[string]bool, len(integrationVersions))

	// One container per version, all four at once — the same shape the collector's suite uses.
	var wait sync.WaitGroup
	for i, version := range integrationVersions {
		wait.Add(1)
		go func() {
			defer wait.Done()
			t.Run(fmt.Sprintf("pg%s", version), func(t *testing.T) {
				db := startPostgres(t, version)
				seen := make(map[string]bool, len(entries))
				for _, entry := range entries {
					if _, skip := notOffered[entry.Rule]; skip {
						continue
					}
					seen[entry.Rule] = acceptsSyntax(t, db, entry.SQL)
				}
				accepted[i] = seen
			})
		}()
	}
	wait.Wait()

	for _, entry := range entries {
		if reason, skip := notOffered[entry.Rule]; skip {
			t.Logf("%s not offered to a server: %s", entry.Rule, reason)
			continue
		}

		observed := pg_contract.VersionUnknown
		for i, version := range integrationVersions {
			if accepted[i] != nil && accepted[i][entry.Rule] {
				observed = version
				break
			}
		}
		require.NotEqualf(t, pg_contract.VersionUnknown, observed,
			"%s: no supported server accepts %q", entry.Rule, entry.SQL)

		claimed := ParseOne(entry.SQL).MinVersion()
		assert.Equalf(t, observed, claimed,
			"%s: servers accept %q from %s, the IR claims %s", entry.Rule, entry.SQL, observed, claimed)
	}
}
