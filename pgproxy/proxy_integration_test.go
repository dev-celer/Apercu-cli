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

func TestMultiStatementQueryReportsTheError(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	c.send(&pgproto3.Query{String: "CREATE TABLE good (id int); ALTER TABLE nope ADD COLUMN x int; DROP TABLE good"})
	c.readCycles(1)

	events := sink.events(t)
	require.Len(t, events, 1)
	assert.Equal(t, "CREATE TABLE good (id int); ALTER TABLE nope ADD COLUMN x int; DROP TABLE good", events[0].SQL)
	assert.Contains(t, events[0].Error, "nope")
}

func TestPipelineSharesOneCycle(t *testing.T) {
	addr, sink := startProxy(t)
	c := dial(t, addr)

	var messages []pgproto3.FrontendMessage
	for i, sql := range []string{"SELECT 1", "SELECT 2", "SELECT 3"} {
		name := fmt.Sprintf("b%d", i)
		messages = append(messages, c.prepare(name, sql)...)
		messages = append(messages, &pgproto3.Execute{Portal: name})
	}
	c.send(append(messages, &pgproto3.Sync{})...)
	c.readCycles(1)

	events := sink.events(t)
	require.Len(t, events, 3, "one event per Execute")
	assert.Equal(t, []string{"SELECT 1", "SELECT 2", "SELECT 3"},
		[]string{events[0].SQL, events[1].SQL, events[2].SQL})

	// Closed by one Sync, so they run as one implicit transaction.
	assert.Equal(t, events[0].Cycle, events[1].Cycle)
	assert.Equal(t, events[0].Cycle, events[2].Cycle)
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
