package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

type QueryEvent struct {
	SQL          string
	StartedAt    time.Time
	Duration     time.Duration
	CommandTag   string
	RowsAffected int64
	Error        string
}

type connState struct {
	mu            sync.Mutex
	preparedStmts map[string]string
	portals       map[string]string
	pendingSQL    string
	pendingStart  time.Time
	pendingTag    string
	pendingError  string
}

func newConnState() *connState {
	return &connState{
		preparedStmts: make(map[string]string),
		portals:       make(map[string]string),
	}
}

func (s *connState) ResetPending() {
	s.pendingSQL = ""
	s.pendingStart = time.Time{}
	s.pendingTag = ""
	s.pendingError = ""
}

func pumpConnection(backend *pgproto3.Backend, upstream *Upstream) {
	state := newConnState()
	done := make(chan struct{}, 2)

	go func() {
		defer func() { done <- struct{}{} }()
		if err := pumpClientToUpstream(backend, upstream, state); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "client->upstream:", err)
		}
		_ = upstream.Conn.Close()
	}()

	go func() {
		defer func() { done <- struct{}{} }()
		if err := pumpUpstreamToClient(backend, upstream, state); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "upstream->client:", err)
		}
	}()

	<-done
	<-done
}

func pumpClientToUpstream(backend *pgproto3.Backend, upstream *Upstream, state *connState) error {
	for {
		msg, err := backend.Receive()
		if err != nil {
			return fmt.Errorf("receive from client: %v", err)
		}

		observeClient(msg, state)

		upstream.Frontend.Send(msg)
		if err := upstream.Frontend.Flush(); err != nil {
			return fmt.Errorf("send to upstream: %v", err)
		}

		if _, isTerminate := msg.(*pgproto3.Terminate); isTerminate {
			return nil
		}
	}
}

func pumpUpstreamToClient(backend *pgproto3.Backend, upstream *Upstream, state *connState) error {
	for {
		msg, err := upstream.Frontend.Receive()
		if err != nil {
			return fmt.Errorf("receive from upstream: %v", err)
		}

		if err := syncAuthType(backend, msg); err != nil {
			return fmt.Errorf("sync auth type: %v", err)
		}

		observeUpstream(msg, state)

		backend.Send(msg)
		if err := backend.Flush(); err != nil {
			return fmt.Errorf("send to client: %v", err)
		}
	}
}

func syncAuthType(backend *pgproto3.Backend, msg pgproto3.BackendMessage) error {
	switch msg.(type) {
	case *pgproto3.AuthenticationOk:
		return backend.SetAuthType(pgproto3.AuthTypeOk)
	case *pgproto3.AuthenticationCleartextPassword:
		return backend.SetAuthType(pgproto3.AuthTypeCleartextPassword)
	case *pgproto3.AuthenticationMD5Password:
		return backend.SetAuthType(pgproto3.AuthTypeMD5Password)
	case *pgproto3.AuthenticationGSS:
		return backend.SetAuthType(pgproto3.AuthTypeGSS)
	case *pgproto3.AuthenticationGSSContinue:
		return backend.SetAuthType(pgproto3.AuthTypeGSSCont)
	case *pgproto3.AuthenticationSASL:
		return backend.SetAuthType(pgproto3.AuthTypeSASL)
	case *pgproto3.AuthenticationSASLContinue:
		return backend.SetAuthType(pgproto3.AuthTypeSASLContinue)
	case *pgproto3.AuthenticationSASLFinal:
		return backend.SetAuthType(pgproto3.AuthTypeSASLFinal)
	}
	return nil
}

func observeClient(msg pgproto3.FrontendMessage, state *connState) {
	state.mu.Lock()
	defer state.mu.Unlock()

	switch m := msg.(type) {
	case *pgproto3.Query:
		state.pendingSQL = m.String
		state.pendingStart = time.Now()
		state.pendingTag = ""
		state.pendingError = ""
	case *pgproto3.Parse:
		state.preparedStmts[m.Name] = m.Query
	case *pgproto3.Bind:
		if sql, ok := state.preparedStmts[m.PreparedStatement]; ok {
			state.portals[m.DestinationPortal] = sql
		}
	case *pgproto3.Execute:
		state.pendingSQL = state.portals[m.Portal]
		state.pendingStart = time.Now()
		state.pendingTag = ""
		state.pendingError = ""
	case *pgproto3.Close:
		switch m.ObjectType {
		case 'S':
			delete(state.preparedStmts, m.Name)
		case 'P':
			delete(state.portals, m.Name)
		}
	}
}

func observeUpstream(msg pgproto3.BackendMessage, state *connState) {
	state.mu.Lock()
	defer state.mu.Unlock()

	switch m := msg.(type) {
	case *pgproto3.CommandComplete:
		state.pendingTag = string(m.CommandTag)
	case *pgproto3.ErrorResponse:
		state.pendingError = m.Message
	case *pgproto3.ReadyForQuery:
		if state.pendingStart.IsZero() {
			return
		}
		ev := QueryEvent{
			SQL:          state.pendingSQL,
			StartedAt:    state.pendingStart,
			Duration:     time.Since(state.pendingStart),
			CommandTag:   state.pendingTag,
			Error:        state.pendingError,
			RowsAffected: parseRowsAffected(state.pendingTag),
		}

		state.ResetPending()
		emitEvent(ev)
	}
}

func parseRowsAffected(tag string) int64 {
	if tag == "" {
		return -1
	}
	parts := strings.Fields(tag)
	if len(parts) == 0 {
		return -1
	}
	n, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil {
		return -1
	}
	return n
}

func emitEvent(ev QueryEvent) {
	snippet := strings.ReplaceAll(ev.SQL, "\n", " ")
	if len(snippet) > 120 {
		snippet = snippet[:117] + "..."
	}
	ms := float64(ev.Duration.Microseconds()) / 1000
	if ev.Error != "" {
		_, _ = fmt.Fprintf(os.Stdout, "[%.2fms] ERROR %s | %q\n", ms, ev.Error, snippet)
		return
	}
	_, _ = fmt.Fprintf(os.Stdout, "[%.2fms] %s | rows=%d | %q\n", ms, ev.CommandTag, ev.RowsAffected, snippet)
}
