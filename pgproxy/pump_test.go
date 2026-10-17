package main

import (
	"testing"

	"apercu-cli/helper/pg_contract"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// drive feeds messages through the state machine in order and collects what it published.
func drive(state *connState, messages ...any) []pg_contract.QueryEvent {
	var events []pg_contract.QueryEvent
	for _, msg := range messages {
		switch m := msg.(type) {
		case pgproto3.FrontendMessage:
			observeClient(m, state)
		case pgproto3.BackendMessage:
			events = append(events, state.observeUpstream(m)...)
		}
	}
	return events
}

func TestSimpleQueryOneStatement(t *testing.T) {
	state := newConnState()

	observeClient(&pgproto3.Query{String: "ALTER TABLE t ADD COLUMN a int"}, state)
	events := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)

	require.Len(t, events, 1)
	assert.Equal(t, "ALTER TABLE t ADD COLUMN a int", events[0].SQL)
	assert.Equal(t, "ALTER TABLE", events[0].CommandTag)
}

// Validate extended protocol pipeline.
func TestPipelinedExecutes(t *testing.T) {
	state := newConnState()

	for i, sql := range []string{"UPDATE a SET x = 1", "UPDATE b SET x = 2", "UPDATE c SET x = 3"} {
		name := string(rune('p' + i))
		drive(state,
			&pgproto3.Parse{Name: name, Query: sql},
			&pgproto3.Bind{PreparedStatement: name, DestinationPortal: name},
			&pgproto3.Execute{Portal: name},
		)
	}

	drive(state, &pgproto3.Sync{})

	var events []pg_contract.QueryEvent
	for _, rows := range []string{"UPDATE 1", "UPDATE 2", "UPDATE 3"} {
		events = append(events, state.observeUpstream(&pgproto3.CommandComplete{CommandTag: []byte(rows)})...)
	}
	events = append(events, state.observeUpstream(&pgproto3.ReadyForQuery{TxStatus: 'I'})...)

	require.Len(t, events, 3, "every statement of the batch is reported, not just the last")
	assert.Equal(t, "UPDATE a SET x = 1", events[0].SQL)
	assert.Equal(t, "UPDATE c SET x = 3", events[2].SQL)
	assert.Equal(t, int64(1), events[0].RowsAffected)
	assert.Equal(t, int64(3), events[2].RowsAffected)

	for _, ev := range events {
		assert.Equal(t, events[0].Cycle, ev.Cycle, "one Sync, so one implicit transaction")
	}
}

func TestAbortedExtendedProtocol(t *testing.T) {
	state := newConnState()

	for i, sql := range []string{"UPDATE nope SET x = 1", "UPDATE b SET x = 2", "UPDATE c SET x = 3"} {
		name := string(rune('p' + i))
		drive(state,
			&pgproto3.Parse{Name: name, Query: sql},
			&pgproto3.Bind{PreparedStatement: name, DestinationPortal: name},
			&pgproto3.Execute{Portal: name},
		)
	}

	drive(state, &pgproto3.Sync{})
	events := drive(state,
		&pgproto3.ErrorResponse{Message: `relation "nope" does not exist`},
		&pgproto3.ReadyForQuery{TxStatus: 'E'},
	)

	require.Len(t, events, 1, "only the statement that reached the server is reported")
	assert.Equal(t, "UPDATE nope SET x = 1", events[0].SQL)
	assert.Equal(t, `relation "nope" does not exist`, events[0].Error)
}

func TestSuspendedPortal(t *testing.T) {
	state := newConnState()

	drive(state,
		&pgproto3.Parse{Name: "p", Query: "SELECT * FROM orders FOR UPDATE"},
		&pgproto3.Bind{PreparedStatement: "p", DestinationPortal: "p"},
		&pgproto3.Execute{Portal: "p", MaxRows: 100},
	)
	drive(state, &pgproto3.Sync{})
	assert.Empty(t, drive(state, &pgproto3.PortalSuspended{}, &pgproto3.ReadyForQuery{TxStatus: 'T'}), "the statement is not finished")

	// The client comes back for the rest.
	drive(state, &pgproto3.Execute{Portal: "p", MaxRows: 100})
	events := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 150")},
		&pgproto3.ReadyForQuery{TxStatus: 'T'},
	)

	require.Len(t, events, 1, "resuming a portal is not a second statement")
	assert.Equal(t, "SELECT * FROM orders FOR UPDATE", events[0].SQL)
}

func TestPortalInterleaved(t *testing.T) {
	state := newConnState()

	drive(state,
		&pgproto3.Parse{Name: "p", Query: "SELECT * FROM orders FOR UPDATE"},
		&pgproto3.Bind{PreparedStatement: "p", DestinationPortal: "p"},
		&pgproto3.Parse{Name: "q", Query: "UPDATE users SET seen = now()"},
		&pgproto3.Bind{PreparedStatement: "q", DestinationPortal: "q"},
	)

	// p fetches its first batch and leaves the portal open, then q is executed in the gap, then p
	// is resumed — all before a single Sync.
	drive(state, &pgproto3.Execute{Portal: "p", MaxRows: 100})
	drive(state, &pgproto3.PortalSuspended{})

	drive(state, &pgproto3.Execute{Portal: "q"})
	fromQ := drive(state, &pgproto3.CommandComplete{CommandTag: []byte("UPDATE 3")})

	require.Len(t, fromQ, 1, "q completed, so q is what is published")
	assert.Equal(t, "UPDATE users SET seen = now()", fromQ[0].SQL)
	assert.Equal(t, "UPDATE 3", fromQ[0].CommandTag)

	drive(state, &pgproto3.Execute{Portal: "p", MaxRows: 100}, &pgproto3.Sync{})
	fromP := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 150")},
		&pgproto3.ReadyForQuery{TxStatus: 'T'},
	)

	require.Len(t, fromP, 1, "p is published once, when it finishes")
	assert.Equal(t, "SELECT * FROM orders FOR UPDATE", fromP[0].SQL)
	assert.Equal(t, "SELECT 150", fromP[0].CommandTag)
}

func TestAQueryAheadOfAPipeline(t *testing.T) {
	state := newConnState()

	observeClient(&pgproto3.Query{String: "BEGIN"}, state)
	drive(state,
		&pgproto3.Parse{Name: "p", Query: "UPDATE a SET x = 1"},
		&pgproto3.Bind{PreparedStatement: "p", DestinationPortal: "p"},
		&pgproto3.Execute{Portal: "p"},
		&pgproto3.Sync{},
	)

	fromQuery := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
		&pgproto3.ReadyForQuery{TxStatus: 'T'},
	)
	require.Len(t, fromQuery, 1)
	assert.Equal(t, "BEGIN", fromQuery[0].SQL)

	fromPipeline := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("UPDATE 7")},
		&pgproto3.ReadyForQuery{TxStatus: 'T'},
	)
	require.Len(t, fromPipeline, 1)
	assert.Equal(t, "UPDATE a SET x = 1", fromPipeline[0].SQL)
	assert.Equal(t, int64(7), fromPipeline[0].RowsAffected)
}

func TestTwoPipelineQueuedAtOnce(t *testing.T) {
	state := newConnState()

	for i, sql := range []string{"UPDATE a SET x = 1", "UPDATE b SET x = 2"} {
		name := string(rune('p' + i))
		drive(state,
			&pgproto3.Parse{Name: name, Query: sql},
			&pgproto3.Bind{PreparedStatement: name, DestinationPortal: name},
			&pgproto3.Execute{Portal: name},
			&pgproto3.Sync{},
		)
	}

	first := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("UPDATE 1")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)
	require.Len(t, first, 1, "the first Sync's ReadyForQuery closes only its own cycle")
	assert.Equal(t, "UPDATE a SET x = 1", first[0].SQL)

	second := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("UPDATE 2")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)
	require.Len(t, second, 1, "and the second is still there to be completed")
	assert.Equal(t, "UPDATE b SET x = 2", second[0].SQL)
	assert.Equal(t, int64(2), second[0].RowsAffected)
	assert.NotEqual(t, first[0].Cycle, second[0].Cycle)
}

func TestReadyForQueryWithNoCycleStillPublishesWhatIsQueued(t *testing.T) {
	state := newConnState()

	drive(state,
		&pgproto3.Parse{Name: "p", Query: "UPDATE a SET x = 1"},
		&pgproto3.Bind{PreparedStatement: "p", DestinationPortal: "p"},
		&pgproto3.Execute{Portal: "p"},
	)
	events := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("UPDATE 1")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)

	require.Len(t, events, 1, "the statement is published rather than stranded")
	assert.Equal(t, "UPDATE a SET x = 1", events[0].SQL)
	assert.Empty(t, state.pending, "and the queue does not grow")
}

func TestReadyForQueryOutsideAStatementPublishesNothing(t *testing.T) {
	state := newConnState()

	assert.Empty(t, drive(state, &pgproto3.ReadyForQuery{TxStatus: 'I'}))
	assert.Empty(t, drive(state, &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}))
}

func TestMultiStatementQueryIsOneEvent(t *testing.T) {
	state := newConnState()

	observeClient(&pgproto3.Query{String: "BEGIN; ALTER TABLE t ADD COLUMN a int; COMMIT"}, state)
	for _, tag := range []string{"BEGIN", "ALTER TABLE", "COMMIT"} {
		assert.Empty(t, drive(state, &pgproto3.CommandComplete{CommandTag: []byte(tag)}),
			"one of its statements completing is not the Query completing")
	}
	events := drive(state, &pgproto3.ReadyForQuery{TxStatus: 'I'})

	require.Len(t, events, 1, "the message is split by the intake, not by the proxy")
	assert.Equal(t, "BEGIN; ALTER TABLE t ADD COLUMN a int; COMMIT", events[0].SQL)
	assert.Equal(t, "COMMIT", events[0].CommandTag)
}

func TestMultiStatementQueryAborted(t *testing.T) {
	state := newConnState()

	observeClient(&pgproto3.Query{String: "BEGIN; ALTER TABLE nope ADD COLUMN a int; COMMIT"}, state)
	events := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
		&pgproto3.ErrorResponse{Message: `relation "nope" does not exist`},
		&pgproto3.ReadyForQuery{TxStatus: 'E'},
	)

	require.Len(t, events, 1)
	assert.Equal(t, "BEGIN; ALTER TABLE nope ADD COLUMN a int; COMMIT", events[0].SQL)
	assert.Equal(t, `relation "nope" does not exist`, events[0].Error)
}

func TestTwoQueriesQueuedAtOnce(t *testing.T) {
	state := newConnState()

	observeClient(&pgproto3.Query{String: "BEGIN; ALTER TABLE a ADD COLUMN z int"}, state)
	observeClient(&pgproto3.Query{String: "COMMIT"}, state)

	first := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
		&pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")},
		&pgproto3.ReadyForQuery{TxStatus: 'T'},
	)
	require.Len(t, first, 1, "the first ReadyForQuery closes only the first Query")
	assert.Equal(t, "BEGIN; ALTER TABLE a ADD COLUMN z int", first[0].SQL)
	assert.Equal(t, "ALTER TABLE", first[0].CommandTag)

	second := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)

	require.Len(t, second, 1, "the second Query is published by its own ReadyForQuery")
	assert.Equal(t, "COMMIT", second[0].SQL)
	assert.Equal(t, "COMMIT", second[0].CommandTag)
	assert.NotEqual(t, first[0].Cycle, second[0].Cycle)
}
