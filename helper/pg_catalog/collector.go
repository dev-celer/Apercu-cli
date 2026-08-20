package pg_catalog

import (
	"apercu-cli/helper/pg_contract"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

// userNS is the filter that exclude all PostgreSQL internal namespace
const userNS = `n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\_%'`

// CollectOptions says which database is being captured and when.
type CollectOptions struct {
	Source Source
	PIT    PIT
	// ProdAvailable tells the collector whether the production database can be used.
	ProdAvailable bool
}

// sourcePolicy is where an item may be captured from.
type sourcePolicy uint8

const (
	fromBaseline sourcePolicy = iota
	fromProd
	fromProdPreferred
)

// pitPolicy is when an item is captured. Pre or post migration.
type pitPolicy uint8

const (
	preOnly pitPolicy = iota
	prePost
)

// item is one S-nn entry.
type item struct {
	id      string
	source  sourcePolicy
	pit     pitPolicy
	collect func(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error
}

// items contain the list of all snapshot items captured with their S-XX id and their capture policies.
// S-00 is absent because it is a prerequisite for other items.
var items = []item{
	{id: "S-01", source: fromBaseline, pit: prePost, collect: collectSchemas},
	{id: "S-02", source: fromBaseline, pit: prePost, collect: collectRelations},
	{id: "S-03", source: fromBaseline, pit: prePost, collect: collectColumns},
	{id: "S-04", source: fromBaseline, pit: prePost, collect: collectDefaults},
	{id: "S-05", source: fromBaseline, pit: prePost, collect: collectConstraints},
	{id: "S-06", source: fromBaseline, pit: prePost, collect: collectIndexes},
	{id: "S-07", source: fromBaseline, pit: prePost, collect: collectInherits},
	{id: "S-08", source: fromBaseline, pit: prePost, collect: collectSequences},
	{id: "S-09", source: fromBaseline, pit: prePost, collect: collectTypes},
	{id: "S-10", source: fromBaseline, pit: prePost, collect: collectTriggersAndPolicies},
	{id: "S-11", source: fromBaseline, pit: prePost, collect: collectViewDeps},
	{id: "S-12", source: fromBaseline, pit: prePost, collect: collectDepends},
	{id: "S-13", source: fromBaseline, pit: prePost, collect: collectProcs},
	{id: "S-14", source: fromBaseline, pit: prePost, collect: collectCastsAndOperators},
	{id: "S-15", source: fromProdPreferred, pit: preOnly, collect: collectPublications},
	{id: "S-16", source: fromBaseline, pit: preOnly, collect: collectSettings},
	{id: "S-17", source: fromProd, pit: preOnly, collect: collectTableStats},
	{id: "S-18", source: fromBaseline, pit: prePost, collect: collectRoles},
}

// wanted reports whether the item should be captured under these options.
func (i item) wanted(opts CollectOptions) bool {
	if i.pit == preOnly && opts.PIT != PITPre {
		return false
	}
	switch i.source {
	case fromProd:
		return opts.Source == SourceProd
	case fromProdPreferred:
		return opts.Source == SourceProd || !opts.ProdAvailable
	default:
		return opts.Source == SourceBaseline
	}
}

// Collect captures one snapshot. Everything run inside a single Read-Only transaction.
func Collect(ctx context.Context, db *sql.DB, opts CollectOptions) (*Snapshot, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to open the snapshot transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	snapshot := &Snapshot{
		Source:     opts.Source,
		PIT:        opts.PIT,
		CapturedAt: time.Now().UTC(),
		Collected:  []string{"S-00"},
	}

	if err := collectHeader(ctx, tx, snapshot); err != nil {
		return nil, err
	}

	for _, it := range items {
		if !it.wanted(opts) {
			continue
		}
		started := time.Now()
		if err := it.collect(ctx, tx, snapshot); err != nil {
			return nil, fmt.Errorf("Failed to collect snapshot item %s: %v", it.id, err)
		}
		slog.Debug("Collected snapshot item", "item", it.id, "source", opts.Source,
			"pit", opts.PIT, "duration", time.Since(started))
		snapshot.Collected = append(snapshot.Collected, it.id)
	}

	_ = tx.Rollback()
	return snapshot, nil
}

// Has reports whether the item was captured for this snapshot.
func (s *Snapshot) Has(itemID string) bool {
	for _, id := range s.Collected {
		if id == itemID {
			return true
		}
	}
	return false
}

const headerQuery = `
SELECT current_setting('server_version_num')::int AS server_version_num,
       current_database(), current_user, current_setting('search_path') AS search_path,
       current_setting('TimeZone') AS timezone,
       pg_is_in_recovery() AS from_replica`

// collectHeader is S-00.
func collectHeader(ctx context.Context, tx *sql.Tx, snapshot *Snapshot) error {
	h := Header{}
	err := tx.QueryRowContext(ctx, headerQuery).Scan(
		&h.ServerVersionNum, &h.Database, &h.User, &h.SearchPath, &h.TimeZone, &h.FromReplica,
	)
	if err != nil {
		return fmt.Errorf("Failed to collect snapshot item S-00: %v", err)
	}
	h.Version = pg_contract.VersionFromNum(h.ServerVersionNum)
	snapshot.Header = h
	return nil
}
