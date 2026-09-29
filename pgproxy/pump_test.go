package main

import (
	"testing"
	"time"

	"apercu-cli/helper/metrics"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clockedState give a state where we can manipulate its clock, to check reported statement duration.
func clockedState() (*connState, func(d time.Duration)) {
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	state := newConnState()
	state.now = func() time.Time { return at }
	return state, func(d time.Duration) { at = at.Add(d) }
}

// drive feeds messages through the state machine in order and collects what it published.
func drive(state *connState, messages ...any) []metrics.QueryEvent {
	var events []metrics.QueryEvent
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
	state, advance := clockedState()

	observeClient(&pgproto3.Query{String: "ALTER TABLE t ADD COLUMN a int"}, state)
	advance(3 * time.Second)
	events := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)

	require.Len(t, events, 1)
	assert.Equal(t, "ALTER TABLE t ADD COLUMN a int", events[0].SQL)
	assert.Equal(t, 3*time.Second, events[0].Duration)
	assert.Equal(t, "ALTER TABLE", events[0].CommandTag)
	assert.Empty(t, events[0].Statements, "a breakdown of one says nothing the event does not")
}

func TestSimpleQueryTimesMultipleStatement(t *testing.T) {
	state, advance := clockedState()

	observeClient(&pgproto3.Query{String: "BEGIN; ALTER TABLE t ADD COLUMN a int; COMMIT"}, state)
	advance(10 * time.Millisecond)
	events := drive(state, &pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
	assert.Empty(t, events, "the batch is not finished until ReadyForQuery")

	advance(4 * time.Second)
	drive(state, &pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")})
	advance(20 * time.Millisecond)
	drive(state, &pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
	advance(5 * time.Millisecond)
	events = drive(state, &pgproto3.ReadyForQuery{TxStatus: 'I'})

	require.Len(t, events, 1, "a simple Query is still one event: only the proxy cannot split its SQL")
	assert.Equal(t, 4035*time.Millisecond, events[0].Duration)

	require.Len(t, events[0].Statements, 3)
	assert.Equal(t, []metrics.StatementTiming{
		{CommandTag: "BEGIN", Duration: 10 * time.Millisecond},
		{CommandTag: "ALTER TABLE", Duration: 4 * time.Second},
		{CommandTag: "COMMIT", Duration: 20 * time.Millisecond},
	}, events[0].Statements)

	// Assert that the total duration = each individual statement duration added
	var total time.Duration
	for _, timing := range events[0].Statements {
		total += timing.Duration
	}
	assert.Less(t, total, events[0].Duration)
}

// Validate extended protocol pipeline.
func TestPipelinedExecutes(t *testing.T) {
	state, advance := clockedState()

	for i, sql := range []string{"UPDATE a SET x = 1", "UPDATE b SET x = 2", "UPDATE c SET x = 3"} {
		name := string(rune('p' + i))
		drive(state,
			&pgproto3.Parse{Name: name, Query: sql},
			&pgproto3.Bind{PreparedStatement: name, DestinationPortal: name},
			&pgproto3.Execute{Portal: name},
		)
	}

	drive(state, &pgproto3.Sync{})

	var events []metrics.QueryEvent
	for _, rows := range []string{"UPDATE 1", "UPDATE 2", "UPDATE 3"} {
		advance(time.Second)
		events = append(events, state.observeUpstream(&pgproto3.CommandComplete{CommandTag: []byte(rows)})...)
	}
	events = append(events, state.observeUpstream(&pgproto3.ReadyForQuery{TxStatus: 'I'})...)

	require.Len(t, events, 3, "every statement of the batch is reported, not just the last")
	assert.Equal(t, "UPDATE a SET x = 1", events[0].SQL)
	assert.Equal(t, "UPDATE c SET x = 3", events[2].SQL)
	assert.Equal(t, int64(1), events[0].RowsAffected)
	assert.Equal(t, int64(3), events[2].RowsAffected)

	for _, ev := range events {
		assert.Equal(t, time.Second, ev.Duration)
		assert.Empty(t, ev.Statements, "one Execute is one statement and needs no breakdown")
	}
}

func TestAbortedSimpleProtocol(t *testing.T) {
	state, advance := clockedState()

	observeClient(&pgproto3.Query{String: "BEGIN; ALTER TABLE nope ADD COLUMN a int; COMMIT"}, state)
	advance(time.Millisecond)
	drive(state, &pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
	advance(2 * time.Millisecond)
	events := drive(state,
		&pgproto3.ErrorResponse{Message: `relation "nope" does not exist`},
		&pgproto3.ReadyForQuery{TxStatus: 'E'},
	)

	require.Len(t, events, 1)
	assert.Equal(t, `relation "nope" does not exist`, events[0].Error)
	assert.Equal(t, 3*time.Millisecond, events[0].Duration)

	require.Len(t, events[0].Statements, 1)
	assert.Equal(t, "BEGIN", events[0].Statements[0].CommandTag)
	assert.Equal(t, time.Millisecond, events[0].Statements[0].Duration)
}

func TestAbortedExtendedProtocol(t *testing.T) {
	state, advance := clockedState()

	for i, sql := range []string{"UPDATE nope SET x = 1", "UPDATE b SET x = 2", "UPDATE c SET x = 3"} {
		name := string(rune('p' + i))
		drive(state,
			&pgproto3.Parse{Name: name, Query: sql},
			&pgproto3.Bind{PreparedStatement: name, DestinationPortal: name},
			&pgproto3.Execute{Portal: name},
		)
	}

	drive(state, &pgproto3.Sync{})
	advance(5 * time.Millisecond)
	events := drive(state,
		&pgproto3.ErrorResponse{Message: `relation "nope" does not exist`},
		&pgproto3.ReadyForQuery{TxStatus: 'E'},
	)

	require.Len(t, events, 1, "only the statement that reached the server is reported")
	assert.Equal(t, "UPDATE nope SET x = 1", events[0].SQL)
	assert.Equal(t, `relation "nope" does not exist`, events[0].Error)
	assert.Equal(t, 5*time.Millisecond, events[0].Duration)
}

func TestSuspendedPortal(t *testing.T) {
	state, advance := clockedState()

	drive(state,
		&pgproto3.Parse{Name: "p", Query: "SELECT * FROM orders FOR UPDATE"},
		&pgproto3.Bind{PreparedStatement: "p", DestinationPortal: "p"},
		&pgproto3.Execute{Portal: "p", MaxRows: 100},
	)
	drive(state, &pgproto3.Sync{})
	advance(time.Second)
	assert.Empty(t, drive(state, &pgproto3.PortalSuspended{}, &pgproto3.ReadyForQuery{TxStatus: 'T'}), "the statement is not finished")

	// The client spends a second on the first batch, then comes back for the rest.
	advance(time.Second)
	drive(state, &pgproto3.Execute{Portal: "p", MaxRows: 100})
	advance(time.Second)
	events := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 150")},
		&pgproto3.ReadyForQuery{TxStatus: 'T'},
	)

	require.Len(t, events, 1, "resuming a portal is not a second statement")
	assert.Equal(t, "SELECT * FROM orders FOR UPDATE", events[0].SQL)
	assert.Equal(t, 3*time.Second, events[0].Duration)
}

func TestPortalInterleaved(t *testing.T) {
	state, advance := clockedState()

	drive(state,
		&pgproto3.Parse{Name: "p", Query: "SELECT * FROM orders FOR UPDATE"},
		&pgproto3.Bind{PreparedStatement: "p", DestinationPortal: "p"},
		&pgproto3.Parse{Name: "q", Query: "UPDATE users SET seen = now()"},
		&pgproto3.Bind{PreparedStatement: "q", DestinationPortal: "q"},
	)

	// p fetches its first batch and leaves the portal open, then q is executed in the gap, then p
	// is resumed — all before a single Sync.
	drive(state, &pgproto3.Execute{Portal: "p", MaxRows: 100})
	advance(time.Second)
	drive(state, &pgproto3.PortalSuspended{})

	drive(state, &pgproto3.Execute{Portal: "q"})
	advance(2 * time.Second)
	fromQ := drive(state, &pgproto3.CommandComplete{CommandTag: []byte("UPDATE 3")})

	require.Len(t, fromQ, 1, "q completed, so q is what is published")
	assert.Equal(t, "UPDATE users SET seen = now()", fromQ[0].SQL)
	assert.Equal(t, "UPDATE 3", fromQ[0].CommandTag)
	assert.Equal(t, 2*time.Second, fromQ[0].Duration)

	drive(state, &pgproto3.Execute{Portal: "p", MaxRows: 100}, &pgproto3.Sync{})
	advance(3 * time.Second)
	fromP := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 150")},
		&pgproto3.ReadyForQuery{TxStatus: 'T'},
	)

	require.Len(t, fromP, 1, "p is published once, when it finishes")
	assert.Equal(t, "SELECT * FROM orders FOR UPDATE", fromP[0].SQL)
	assert.Equal(t, "SELECT 150", fromP[0].CommandTag)

	assert.Equal(t, 4*time.Second, fromP[0].Duration)
	assert.Equal(t, 6*time.Second, fromP[0].Duration+fromQ[0].Duration)
}

func TestTwoQueriesQueuedAtOnce(t *testing.T) {
	state, advance := clockedState()

	observeClient(&pgproto3.Query{String: "BEGIN; ALTER TABLE a ADD COLUMN z int"}, state)
	observeClient(&pgproto3.Query{String: "COMMIT"}, state)

	advance(time.Millisecond)
	drive(state, &pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
	advance(2 * time.Second)
	drive(state, &pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")})
	first := drive(state, &pgproto3.ReadyForQuery{TxStatus: 'T'})

	require.Len(t, first, 1, "the first ReadyForQuery closes the first Query and nothing else")
	assert.Equal(t, "BEGIN; ALTER TABLE a ADD COLUMN z int", first[0].SQL)
	require.Len(t, first[0].Statements, 2)
	assert.Equal(t, "BEGIN", first[0].Statements[0].CommandTag)
	assert.Equal(t, 2*time.Second, first[0].Statements[1].Duration)

	advance(3 * time.Millisecond)
	second := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)

	require.Len(t, second, 1, "the second Query is published by its own ReadyForQuery")
	assert.Equal(t, "COMMIT", second[0].SQL)
	assert.Equal(t, "COMMIT", second[0].CommandTag)
	assert.Empty(t, second[0].Statements, "and it collected none of the first one's timings")
}

func TestAQueryAheadOfAPipeline(t *testing.T) {
	state, advance := clockedState()

	observeClient(&pgproto3.Query{String: "BEGIN"}, state)
	drive(state,
		&pgproto3.Parse{Name: "p", Query: "UPDATE a SET x = 1"},
		&pgproto3.Bind{PreparedStatement: "p", DestinationPortal: "p"},
		&pgproto3.Execute{Portal: "p"},
		&pgproto3.Sync{},
	)

	advance(time.Millisecond)
	fromQuery := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
		&pgproto3.ReadyForQuery{TxStatus: 'T'},
	)
	require.Len(t, fromQuery, 1)
	assert.Equal(t, "BEGIN", fromQuery[0].SQL)

	advance(4 * time.Second)
	fromPipeline := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("UPDATE 7")},
		&pgproto3.ReadyForQuery{TxStatus: 'T'},
	)
	require.Len(t, fromPipeline, 1)
	assert.Equal(t, "UPDATE a SET x = 1", fromPipeline[0].SQL)
	assert.Equal(t, 4*time.Second, fromPipeline[0].Duration)
	assert.Equal(t, int64(7), fromPipeline[0].RowsAffected)
}

func TestTwoPipelineQueuedAtOnce(t *testing.T) {
	state, advance := clockedState()

	for i, sql := range []string{"UPDATE a SET x = 1", "UPDATE b SET x = 2"} {
		name := string(rune('p' + i))
		drive(state,
			&pgproto3.Parse{Name: name, Query: sql},
			&pgproto3.Bind{PreparedStatement: name, DestinationPortal: name},
			&pgproto3.Execute{Portal: name},
			&pgproto3.Sync{},
		)
	}

	advance(time.Second)
	first := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("UPDATE 1")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)
	require.Len(t, first, 1, "the first Sync's ReadyForQuery closes only its own cycle")
	assert.Equal(t, "UPDATE a SET x = 1", first[0].SQL)

	advance(2 * time.Second)
	second := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("UPDATE 2")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)
	require.Len(t, second, 1, "and the second is still there to be completed")
	assert.Equal(t, "UPDATE b SET x = 2", second[0].SQL)
	assert.Equal(t, 2*time.Second, second[0].Duration)
	assert.Equal(t, int64(2), second[0].RowsAffected)
}

func TestClientDelayIsNotMeasured(t *testing.T) {
	state, advance := clockedState()

	observeClient(&pgproto3.Query{String: "SELECT 1"}, state)
	advance(time.Millisecond)
	first := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)
	require.Len(t, first, 1)
	assert.Equal(t, time.Millisecond, first[0].Duration)

	// The client wait before sending another query
	advance(time.Minute)
	observeClient(&pgproto3.Query{String: "SELECT 2"}, state)
	advance(2 * time.Millisecond)
	second := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)

	require.Len(t, second, 1)
	assert.Equal(t, 2*time.Millisecond, second[0].Duration, "the minute the client spent idle is not the statement's")
}

func TestReadyForQueryWithNoCycleStillPublishesWhatIsQueued(t *testing.T) {
	state, advance := clockedState()

	drive(state,
		&pgproto3.Parse{Name: "p", Query: "UPDATE a SET x = 1"},
		&pgproto3.Bind{PreparedStatement: "p", DestinationPortal: "p"},
		&pgproto3.Execute{Portal: "p"},
	)
	advance(time.Second)
	events := drive(state,
		&pgproto3.CommandComplete{CommandTag: []byte("UPDATE 1")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)

	require.Len(t, events, 1, "the statement is published rather than stranded")
	assert.Equal(t, "UPDATE a SET x = 1", events[0].SQL)
	assert.Empty(t, state.pending, "and the queue does not grow")
}

func TestReadyForQueryOutsideAStatementPublishesNothing(t *testing.T) {
	state, _ := clockedState()

	assert.Empty(t, drive(state, &pgproto3.ReadyForQuery{TxStatus: 'I'}))
	assert.Empty(t, drive(state, &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}))
}
