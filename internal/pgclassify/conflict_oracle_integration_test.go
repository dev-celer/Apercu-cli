//go:build integration

package pgclassify_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"

	"apercu-cli/helper"
	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgclassify"
	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// conflictTable is the table the conflict oracle locks. It has no child, index, rule or trigger, so a LOCK TABLE on
// it takes the mode it names and nothing else.
var conflictTable = helper.FullRelationName{Schema: classifytest.OracleSchema, Table: "conflict_probe"}

const conflictDDL = `
CREATE SCHEMA ` + classifytest.OracleSchema + `;
CREATE TABLE ` + classifytest.OracleSchema + `.conflict_probe (id bigint);
`

// everyLock is every real lock mode, by ordinal.
var everyLock = []pgcontract.Lock{
	pgcontract.LockAccessShare, pgcontract.LockRowShare, pgcontract.LockRowExclusive, pgcontract.LockShareUpdateExclusive,
	pgcontract.LockShare, pgcontract.LockShareRowExclusive, pgcontract.LockExclusive, pgcontract.LockAccessExclusive,
}

// lockConflictTable is the LOCK TABLE statement that takes one mode on the conflict table.
func lockConflictTable(lock pgcontract.Lock) string {
	return "LOCK TABLE " + conflictTable.String() + " IN " + strings.ReplaceAll(lock.String(), "_", " ") + " MODE"
}

// coveredScripts are the lock sequences the covered oracle takes in one transaction: every pair of modes, and the
// triples where two modes held together cover a third that neither one covers alone.
func coveredScripts() [][]pgcontract.Lock {
	var scripts [][]pgcontract.Lock
	for _, first := range everyLock {
		for _, second := range everyLock {
			scripts = append(scripts, []pgcontract.Lock{first, second})
		}
	}
	return append(scripts,
		[]pgcontract.Lock{pgcontract.LockShare, pgcontract.LockRowExclusive, pgcontract.LockShareRowExclusive},
		[]pgcontract.Lock{pgcontract.LockShare, pgcontract.LockShareUpdateExclusive, pgcontract.LockShareRowExclusive},
		[]pgcontract.Lock{pgcontract.LockRowExclusive, pgcontract.LockShareUpdateExclusive, pgcontract.LockShare},
	)
}

func lockSequenceName(locks []pgcontract.Lock) string {
	names := make([]string, 0, len(locks))
	for _, lock := range locks {
		names = append(names, lock.Short())
	}
	return strings.Join(names, " then ")
}

// TestLockConflictOracle validate the lock matrix.
func TestLockConflictOracle(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	for _, version := range classifytest.Versions {
		wg.Add(1)
		go func(version pgcontract.Version) {
			defer wg.Done()
			t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
				db := classifytest.StartPostgres(t, version)
				_, err := db.Exec(conflictDDL)
				require.NoError(t, err)
				catalog := classifytest.OracleCatalog(t, db)

				for _, held := range everyLock {
					for _, requested := range everyLock {
						t.Run(fmt.Sprintf("matrix/%s held/%s requested", held.Short(), requested.Short()), func(t *testing.T) {
							conflictMatrix(t, db, held, requested)
						})
					}
				}
				for _, locks := range coveredScripts() {
					t.Run("covered/"+lockSequenceName(locks), func(t *testing.T) { coveredFootprint(t, db, catalog, locks) })
				}
			})
		}(version)
	}
	wg.Wait()
}

// conflictMatrix holds one mode from a session and requests another from a second one, NOWAIT.
// The server should refuse the second lock when the matrix say they are conflicting.
func conflictMatrix(t *testing.T, db *sql.DB, held, requested pgcontract.Lock) {
	t.Helper()
	ctx := context.Background()

	holder := classifytest.OracleConn(t, db)
	_, err := holder.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)
	defer func() { _, _ = holder.ExecContext(ctx, "ROLLBACK") }()
	_, err = holder.ExecContext(ctx, lockConflictTable(held))
	require.NoError(t, err)

	assert.Equalf(t, held.ConflictsWith(requested), refusedNowait(t, db, requested),
		"%s requested while another transaction holds %s", requested.Short(), held.Short())
}

// coveredFootprint takes a lock sequence in one transaction and measures, after every step, what the transaction
// blocks. The measure has to be what the matrix predicts for the modes held, and a step has to leave it unchanged
// exactly where Analyze marks its envelope covered.
func coveredFootprint(t *testing.T, db *sql.DB, catalog *pgcatalog.Catalog, locks []pgcontract.Lock) {
	t.Helper()
	ctx := context.Background()

	events := []pgcontract.QueryEvent{classifytest.Event("BEGIN")}
	for _, lock := range locks {
		events = append(events, classifytest.Event(lockConflictTable(lock)))
	}
	analysis := pgclassify.Analyze(catalog, classifytest.Apart(append(events, classifytest.Event("COMMIT"))...))
	require.Len(t, analysis.Statements, len(locks)+2)

	opened := map[int]pgcontract.LockEnvelope{}
	for _, envelope := range analysis.EnvelopesOn(conflictTable) {
		opened[envelope.OpenedBy] = envelope
	}
	require.Len(t, opened, len(locks), "one envelope per LOCK, %v", analysis.Envelopes)

	conn := classifytest.OracleConn(t, db)
	_, err := conn.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)
	defer func() { _, _ = conn.ExecContext(ctx, "ROLLBACK") }()

	var held, blocked pgcontract.LockSet
	for i, lock := range locks {
		_, err = conn.ExecContext(ctx, lockConflictTable(lock))
		require.NoError(t, err)
		held = held.With(lock)

		measured := footprint(t, db)
		assert.Equalf(t, held.Conflicts(), measured, "holding %v, the matrix against what the server refuses", held)

		envelope := opened[i+1]
		require.Equal(t, lock, envelope.Lock)
		assert.Equalf(t, blocked == measured, envelope.Covered,
			"%s taken: the server blocked %v and now %v, the envelope says covered=%v", lock.Short(), blocked, measured, envelope.Covered)
		blocked = measured
	}
}

// footprint is what the transactions holding locks on the conflict table block, as measured: every mode a fresh
// transaction is refused there.
func footprint(t *testing.T, db *sql.DB) pgcontract.LockSet {
	t.Helper()

	var blocked pgcontract.LockSet
	for _, lock := range everyLock {
		if refusedNowait(t, db, lock) {
			blocked = blocked.With(lock)
		}
	}
	return blocked
}

// refusedNowait requests one mode on the conflict table from a fresh transaction, NOWAIT, and reports whether the
// server refused it.
func refusedNowait(t *testing.T, db *sql.DB, lock pgcontract.Lock) bool {
	t.Helper()

	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	_, err = tx.Exec(lockConflictTable(lock) + " NOWAIT")
	if classifytest.LockTimedOut(err) {
		return true
	}
	require.NoErrorf(t, err, "%s", lockConflictTable(lock))
	return false
}
