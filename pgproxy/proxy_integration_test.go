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

	"apercu-cli/helper/metrics"

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
func (p *published) events(t *testing.T) []metrics.QueryEvent {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()

	var events []metrics.QueryEvent
	for _, line := range strings.Split(p.lines.String(), "\n") {
		if !strings.HasPrefix(line, "{") {
			continue
		}
		ev := metrics.QueryEvent{}
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

	serverCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	go func() {
		_ = serve(serverCtx, Config{DatabaseHost: host, DatabasePort: port.Port()}, listener)
	}()

	return listener.Addr().String(), sink
}

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

func TestSimpleProtocolMultiStatement(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	c.send(&pgproto3.Query{String: "BEGIN; SELECT pg_sleep(0.2); COMMIT"})
	c.readCycles(1)

	events := sink.events(t)
	require.Len(t, events, 1, "one Query message is one event: the proxy cannot split its SQL")

	require.Len(t, events[0].Statements, 3)
	assert.Equal(t, []string{"BEGIN", "SELECT 1", "COMMIT"},
		[]string{events[0].Statements[0].CommandTag, events[0].Statements[1].CommandTag, events[0].Statements[2].CommandTag})

	// The sleeping statement is the expensive one, and the breakdown is what says so.
	assert.Greater(t, events[0].Statements[1].Duration, 150*time.Millisecond)
	assert.Less(t, events[0].Statements[0].Duration, 100*time.Millisecond)
	assert.Less(t, events[0].Statements[2].Duration, 100*time.Millisecond)
}

func TestSimpleProtocolMultipleQueries(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	c.send(
		&pgproto3.Query{String: "BEGIN; SELECT 1"},
		&pgproto3.Query{String: "COMMIT"},
	)
	c.readCycles(2)

	events := sink.events(t)
	require.Len(t, events, 2, "two Query messages are two events, each closed by its own ready")
	assert.Equal(t, "BEGIN; SELECT 1", events[0].SQL)
	assert.Len(t, events[0].Statements, 2)
	assert.Equal(t, "COMMIT", events[1].SQL)
	assert.Empty(t, events[1].Statements, "and the second collected none of the first one's timings")
}

func TestExtendedProtocol(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	var messages []pgproto3.FrontendMessage
	for i, sql := range []string{"SELECT 1", "SELECT 2", "SELECT 3"} {
		name := fmt.Sprintf("p%d", i)
		messages = append(messages, c.prepare(name, sql)...)
		messages = append(messages, &pgproto3.Execute{Portal: name})
	}
	c.send(append(messages, &pgproto3.Sync{})...)
	c.readCycles(1)

	events := sink.events(t)
	require.Len(t, events, 3, "every statement of the batch is reported")
	assert.Equal(t, []string{"SELECT 1", "SELECT 2", "SELECT 3"},
		[]string{events[0].SQL, events[1].SQL, events[2].SQL})
	for _, ev := range events {
		assert.Empty(t, ev.Statements, "one Execute is one statement and needs no breakdown")
	}
}

func TestSimpleProtocolAborted(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	c.send(&pgproto3.Query{String: "BEGIN; SELECT nope; COMMIT"})
	c.readCycles(1)

	events := sink.events(t)
	require.Len(t, events, 1)
	assert.Contains(t, events[0].Error, "nope")
	require.Len(t, events[0].Statements, 1, "only BEGIN completed, and that much is kept")
	assert.Equal(t, "BEGIN", events[0].Statements[0].CommandTag)
}

func TestExtendedProtocolSuspendedPortal(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	c.send(&pgproto3.Query{String: "BEGIN"})
	c.readCycles(1)

	c.send(append(c.prepare("cur", "SELECT g FROM generate_series(1, 5) g"),
		&pgproto3.Execute{Portal: "cur", MaxRows: 3}, &pgproto3.Sync{})...)
	c.readCycles(1)
	require.Len(t, sink.events(t), 1, "the cursor is unfinished, so only the BEGIN is published")

	c.send(&pgproto3.Execute{Portal: "cur", MaxRows: 3}, &pgproto3.Sync{})
	c.readCycles(1)

	events := sink.events(t)
	require.Len(t, events, 2, "the BEGIN and the cursor, once each")
	assert.Equal(t, "BEGIN", events[0].SQL)
	assert.Equal(t, "SELECT g FROM generate_series(1, 5) g", events[1].SQL)
	assert.Equal(t, "SELECT 2", events[1].CommandTag, "the tag of the batch that finished it")
}

func TestExtendedProtocolReportUnfinishedPortal(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	c.send(&pgproto3.Query{String: "BEGIN"})
	c.readCycles(1)

	c.send(append(c.prepare("cur", "SELECT g FROM generate_series(1, 5) g"),
		&pgproto3.Execute{Portal: "cur", MaxRows: 3}, &pgproto3.Sync{})...)
	c.readCycles(1)

	c.send(&pgproto3.Query{String: "ROLLBACK"})
	c.readCycles(1)

	events := sink.events(t)
	require.Len(t, events, 3, "the BEGIN, the ROLLBACK, and the cursor the rollback killed")

	bySQL := map[string]metrics.QueryEvent{}
	for _, ev := range events {
		bySQL[ev.SQL] = ev
	}
	cursor, found := bySQL["SELECT g FROM generate_series(1, 5) g"]
	require.True(t, found, "an abandoned cursor is not silently dropped: %v", events)
	assert.Empty(t, cursor.CommandTag, "it never completed, so it has no tag")
}

func TestProxyNormalizes(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	c.send(&pgproto3.Query{String: "-- name: migrate\n/* tool */ SELECT\n\t1,\n\t2"})
	c.readCycles(1)

	events := sink.events(t)
	require.Len(t, events, 1)
	assert.Equal(t, "SELECT 1, 2", events[0].SQL)
}
