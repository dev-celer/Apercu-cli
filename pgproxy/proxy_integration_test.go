//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"apercu-cli/helper/pg_contract"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// published collects what the proxy emitted.
type published struct {
	mu    sync.Mutex
	lines strings.Builder
}

func (p *published) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lines.Write(b)
}

// events return all the events the proxy emitted, as QueryEvent structs.
func (p *published) events(t *testing.T) []pg_contract.QueryEvent {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()

	var events []pg_contract.QueryEvent
	for _, line := range strings.Split(p.lines.String(), "\n") {
		if !strings.HasPrefix(line, "{") {
			continue
		}
		ev := pg_contract.QueryEvent{}
		require.NoErrorf(t, json.Unmarshal([]byte(line), &ev), "the proxy published %q", line)
		events = append(events, ev)
	}
	return events
}

// startProxy brings up a PostgreSQL and a proxy in front of it, and answers with the address a
// client should connect to and a pointer to the object the proxy will published event to.
func startProxy(t *testing.T) (string, *published) {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:17",
		postgres.WithDatabase("app"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("pg"),
		testcontainers.WithEnv(map[string]string{"POSTGRES_HOST_AUTH_METHOD": "trust"}),
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	sink := &published{}
	previous := out
	out = sink
	t.Cleanup(func() { out = previous })

	upstreamAddr = net.JoinHostPort(host, port.Port())

	serverCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	go func() {
		_ = serve(serverCtx, Config{DatabaseHost: host, DatabasePort: port.Port()}, listener)
	}()

	return listener.Addr().String(), sink
}

// upstreamAddr is the PostgreSQL the last startProxy brought up, for the tests that compare what a
// client sees through the proxy against what the same client sees without it.
var upstreamAddr string

type client struct {
	t        *testing.T
	frontend *pgproto3.Frontend
}

func dial(t *testing.T, addr string) *client {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(30*time.Second)))

	c := &client{t: t, frontend: pgproto3.NewFrontend(conn, conn)}
	c.send(&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "postgres", "database": "app"},
	})
	c.readCycles(1)
	return c
}

// send writes messages and flushes once, meaning all of them reach the server at the same time.
func (c *client) send(messages ...pgproto3.FrontendMessage) {
	c.t.Helper()
	for _, msg := range messages {
		c.frontend.Send(msg)
	}
	require.NoError(c.t, c.frontend.Flush())
}

// readCycles reads until the server has reported ready the given number of times.
func (c *client) readCycles(count int) []string {
	c.t.Helper()

	var tags []string
	for count > 0 {
		msg, err := c.frontend.Receive()
		require.NoError(c.t, err)
		switch m := msg.(type) {
		case *pgproto3.CommandComplete:
			tags = append(tags, string(m.CommandTag))
		case *pgproto3.ReadyForQuery:
			count--
		}
	}
	return tags
}

func (c *client) prepare(name, sql string) []pgproto3.FrontendMessage {
	return []pgproto3.FrontendMessage{
		&pgproto3.Parse{Name: name, Query: sql},
		&pgproto3.Bind{PreparedStatement: name, DestinationPortal: name},
	}
}

// dialDirect connects straight to PostgreSQL, bypassing the proxy, so a test can compare what a
// client sees with the rewrite against what it sees without it.
func dialDirect(t *testing.T) *client {
	t.Helper()

	conn, err := net.Dial("tcp", upstreamAddr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(30*time.Second)))

	c := &client{t: t, frontend: pgproto3.NewFrontend(conn, conn)}
	c.send(&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "postgres", "database": "app"},
	})
	c.readCycles(1)
	return c
}

// seen is the sequence of message kinds the client got back.
func (c *client) seen(cycles int) []string {
	c.t.Helper()

	var out []string
	for cycles > 0 {
		msg, err := c.frontend.Receive()
		require.NoError(c.t, err)
		switch m := msg.(type) {
		case *pgproto3.CommandComplete:
			out = append(out, "CommandComplete "+string(m.CommandTag))
		case *pgproto3.RowDescription:
			fields := make([]string, 0, len(m.Fields))
			for _, f := range m.Fields {
				fields = append(fields, string(f.Name))
			}
			out = append(out, "RowDescription "+strings.Join(fields, ","))
		case *pgproto3.DataRow:
			out = append(out, "DataRow "+string(m.Values[0]))
		case *pgproto3.ErrorResponse:
			out = append(out, "ErrorResponse "+m.Code)
		case *pgproto3.EmptyQueryResponse:
			out = append(out, "EmptyQueryResponse")
		case *pgproto3.ReadyForQuery:
			cycles--
			out = append(out, "ReadyForQuery "+string(m.TxStatus))
		default:
			out = append(out, fmt.Sprintf("%T", msg))
		}
	}
	return out
}

func TestRewriteIsInvisibleToTheClient(t *testing.T) {
	addr, _ := startProxy(t)

	cases := []struct {
		name string
		sql  string
	}{
		{"ddl only", "CREATE TABLE inv1 (id int); ALTER TABLE inv1 ADD COLUMN n text; DROP TABLE inv1"},
		{"rows in the middle", "CREATE TABLE inv2 (id int); SELECT 1 AS one, 2 AS two; DROP TABLE inv2"},
		{"several row sets", "SELECT 'a' AS letter; SELECT 'b' AS letter"},
		{"explicit transaction", "BEGIN; SELECT 'x' AS v; COMMIT"},
		{"an error part way through", "SELECT 'before' AS v; SELECT nope; SELECT 'after' AS v"},
		{"trailing semicolon", "CREATE TABLE inv3 (id int); DROP TABLE inv3;"},
		{"comments in front of statements", "-- one\nSELECT 'c' AS v;\n/* two */ SELECT 'd' AS v"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			throughProxy := dial(t, addr)
			throughProxy.send(&pgproto3.Query{String: testCase.sql})
			rewritten := throughProxy.seen(1)

			direct := dialDirect(t)
			direct.send(&pgproto3.Query{String: testCase.sql})
			plain := direct.seen(1)

			assert.Equal(t, plain, rewritten, "the client has to see the same answer either way")
		})
	}
}

func TestRewriteTimesEachStatement(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	c.send(&pgproto3.Query{String: "SELECT 1; SELECT pg_sleep(0.4); SELECT 3"})
	c.readCycles(1)

	events := sink.events(t)
	require.Len(t, events, 3, "one event per statement")
	assert.Equal(t, []string{"SELECT 1", "SELECT pg_sleep(0.4)", "SELECT 3"},
		[]string{events[0].SQL, events[1].SQL, events[2].SQL})

	assert.Greater(t, events[1].Duration, 300*time.Millisecond, "the sleep is charged for its own time")
	assert.Less(t, events[0].Duration, 100*time.Millisecond)
	assert.Less(t, events[2].Duration, 100*time.Millisecond)

	// Timed apart but run as one pipeline closed by one Sync, so they share a protocol cycle.
	assert.Equal(t, events[0].Cycle, events[1].Cycle)
	assert.Equal(t, events[0].Cycle, events[2].Cycle)
}

func TestSingleQueryHaveUniqueCycle(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	c.send(&pgproto3.Query{String: "SELECT 1"})
	c.readCycles(1)
	c.send(&pgproto3.Query{String: "SELECT 2"})
	c.readCycles(1)

	events := sink.events(t)
	require.Len(t, events, 2)
	assert.NotEqual(t, events[0].Cycle, events[1].Cycle)
}

// TestRewriteKeepsTheImplicitTransaction assert that the implicit transaction created by a multiple statement simple protocol query is kept.
// This mean that a failure in the second statement of a transaction should roll back the transaction and revert the effect of the first statement.
func TestRewriteKeepsTheImplicitTransaction(t *testing.T) {
	addr, _ := startProxy(t)
	c := dial(t, addr)

	c.send(&pgproto3.Query{String: "CREATE TABLE atomic (id int primary key)"})
	c.readCycles(1)

	c.send(&pgproto3.Query{String: "INSERT INTO atomic VALUES (1); INSERT INTO atomic VALUES (1)"})
	answer := c.seen(1)
	assert.Contains(t, strings.Join(answer, " | "), "ErrorResponse 23505", "the duplicate key fails")

	c.send(&pgproto3.Query{String: "SELECT count(*)::text FROM atomic"})
	rows := c.seen(1)
	assert.Contains(t, rows, "DataRow 0", "the insert that succeeded was rolled back with it")
}

func TestRewriteIgnoreCopy(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	c.send(&pgproto3.Query{String: "CREATE TABLE copied (id int)"})
	c.readCycles(1)

	c.send(&pgproto3.Query{String: "CREATE TABLE copysrc (id int); COPY copied FROM PROGRAM 'echo 1'; DROP TABLE copysrc"})
	c.readCycles(1)

	events := sink.events(t)
	require.NotEmpty(t, events)
	last := events[len(events)-1]
	assert.Contains(t, last.SQL, "COPY copied", "the message went through as one event")
	assert.Contains(t, last.SQL, "DROP TABLE copysrc")

	// And the rows really arrived, so declining the rewrite did not break the statement.
	c.send(&pgproto3.Query{String: "SELECT count(*)::text FROM copied"})
	assert.Contains(t, c.seen(1), "DataRow 1")
}

func TestRewriteLeavesASingleStatementUntouched(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	c.send(&pgproto3.Query{String: "SELECT 1"})
	c.readCycles(1)
	c.send(&pgproto3.Query{String: "SELECT 2;"})
	c.readCycles(1)
	c.send(&pgproto3.Query{String: ";;"})
	c.readCycles(1)

	events := sink.events(t)
	require.Len(t, events, 3, "including the empty Query, which has nothing to split")
	assert.Equal(t, "SELECT 1", events[0].SQL)
	assert.Equal(t, "SELECT 2;", events[1].SQL)
}

func TestRewriteReportsTheFailingStatement(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	c.send(&pgproto3.Query{String: "CREATE TABLE good (id int); ALTER TABLE nope ADD COLUMN x int; DROP TABLE good"})
	c.readCycles(1)

	events := sink.events(t)
	require.Len(t, events, 2, "the one that ran and the one that failed, not the one abandoned")
	assert.Equal(t, "CREATE TABLE good (id int)", events[0].SQL)
	assert.Empty(t, events[0].Error)
	assert.Equal(t, "ALTER TABLE nope ADD COLUMN x int", events[1].SQL)
	assert.Contains(t, events[1].Error, "nope")
}

// TestRewriteKeepsAStatementRefusedInATransaction covers a statement that cannot run inside a
// transaction block. A multi-statement Query is one implicit transaction, so the server already
// refused it before the rewrite, and it has to refuse it the same way after.
func TestRewriteKeepsAStatementRefusedInATransaction(t *testing.T) {
	addr, _ := startProxy(t)

	sql := "CREATE TABLE vac (id int); VACUUM vac"

	throughProxy := dial(t, addr)
	throughProxy.send(&pgproto3.Query{String: sql})
	rewritten := throughProxy.seen(1)

	direct := dialDirect(t)
	direct.send(&pgproto3.Query{String: "CREATE TABLE vac2 (id int); VACUUM vac2"})
	plain := direct.seen(1)

	assert.Equal(t, plain, rewritten, "refused either way, and with the same error")
	assert.Contains(t, strings.Join(rewritten, " | "), "ErrorResponse 25001")
}

func TestExtendedProtocolPerStatementTiming(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	var messages []pgproto3.FrontendMessage
	for i, sql := range []string{"SELECT 1", "SELECT pg_sleep(0.4)", "SELECT 3"} {
		name := fmt.Sprintf("b%d", i)
		messages = append(messages, c.prepare(name, sql)...)
		messages = append(messages, &pgproto3.Execute{Portal: name})
	}
	c.send(append(messages, &pgproto3.Sync{})...)
	c.readCycles(1)

	events := sink.events(t)
	require.Len(t, events, 3)
	assert.Greater(t, events[1].Duration, 300*time.Millisecond, "the sleep is charged for its own time")
	assert.Less(t, events[0].Duration, 100*time.Millisecond, "and not the statement ahead of it")
}

func TestProxyNormalizesTheSQLItPublishes(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	c.send(&pgproto3.Query{String: "-- name: migrate\n/* tool */ SELECT\n\t1,\n\t2"})
	c.readCycles(1)

	events := sink.events(t)
	require.Len(t, events, 1)
	assert.Equal(t, "SELECT 1, 2", events[0].SQL)
}
