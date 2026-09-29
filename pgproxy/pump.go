package main

import (
	"apercu-cli/helper/metrics"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

type connState struct {
	mu            sync.Mutex
	preparedStmts map[string]string
	portals       map[string]string
	// pending is a queue of event the client has asked the server to handle.
	pending []*pendingEvent
	// boundary is when the server finished the statement before this one.
	boundary time.Time
	// cycle counts protocol cycles, it acts as an incremental identifier per event
	cycle int
	// awaiting is a queue of cycles still awaiting a ReadyForQuery from the server.
	awaiting []int
	// now is the clock, so a test can drive the state machine without waiting on one.
	now func() time.Time
}

// pendingEvent is one event the client asked the server to run: a simple Query message, which
// may hold any number of statements, or one Execute of a prepared statement.
type pendingEvent struct {
	sql   string
	start time.Time
	cycle int
	// portal is the name an Execute ran, empty for a simple Query.
	portal string
	// elapsed is the server time already passed on this statement, which is only ever non-zero for
	// a portal the server has stopped and resumed.
	elapsed time.Duration
	// suspended records that the server answered this statement's Execute with PortalSuspended: it
	// returned part of its rows and left the portal open.
	suspended bool
	// simple marks SQL that arrived as a Query message.
	simple  bool
	timings []metrics.StatementTiming
	tag     string
	err     string
}

func newConnState() *connState {
	return &connState{
		preparedStmts: make(map[string]string),
		portals:       make(map[string]string),
		now:           time.Now,
	}
}

// flush publishes the queued events the closing function filter.
func (s *connState) flush(at time.Time, closing func(*pendingEvent) bool) []metrics.QueryEvent {
	events := make([]metrics.QueryEvent, 0, len(s.pending))
	kept := s.pending[:0]

	for _, pending := range s.pending {
		if !closing(pending) {
			kept = append(kept, pending)
			continue
		}
		if !pending.reportable() {
			continue
		}

		duration := at.Sub(pending.start)
		if !pending.simple {
			duration = s.ran(pending, at)
		}
		events = append(events, pending.event(duration))
	}

	s.boundary = at
	s.pending = kept
	return events
}

// openCycle records that the client has finished a cycle and the server owes a ReadyForQuery for it.
// For simple protocol, called on a Query command, for extended protocol called on a Sync command.
func (s *connState) openCycle() {
	s.awaiting = append(s.awaiting, s.cycle)
	s.cycle++
}

// head return the first element in the pending event queue.
func (s *connState) head() *pendingEvent {
	if len(s.pending) == 0 {
		return nil
	}
	return s.pending[0]
}

// completing return the first event in the queue ready to be completed, meaning not suspended, and it's index in the queue.
func (s *connState) completing() (*pendingEvent, int) {
	for i, pending := range s.pending {
		if !pending.suspended {
			return pending, i
		}
	}
	return nil, -1
}

// resume finds the suspended event a repeated Execute of the same portal continues.
func (s *connState) resume(portal string) *pendingEvent {
	for _, pending := range s.pending {
		if pending.suspended && pending.portal == portal {
			return pending
		}
	}
	return nil
}

// ran is how long the server spent on the statement.
func (s *connState) ran(pending *pendingEvent, at time.Time) time.Duration {
	from := pending.start
	if s.boundary.After(from) {
		from = s.boundary
	}
	return pending.elapsed + at.Sub(from)
}

// reportable is used on a statement whose cycle completed.
// it answers if it ran at any point, and so, if it should be reported or not.
func (p *pendingEvent) reportable() bool {
	return p.simple || p.tag != "" || p.err != "" || p.suspended
}

// event return the output event that the proxy should emit.
func (p *pendingEvent) event(duration time.Duration) metrics.QueryEvent {
	ev := metrics.QueryEvent{
		SQL:          p.sql,
		StartedAt:    p.start,
		Duration:     duration,
		CommandTag:   p.tag,
		Error:        p.err,
		RowsAffected: parseRowsAffected(p.tag),
	}
	if len(p.timings) > 1 || (len(p.timings) > 0 && p.err != "") {
		ev.Statements = p.timings
	}
	return ev
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

	started := state.now()

	switch m := msg.(type) {
	case *pgproto3.Query:
		// A simple Query is a whole cycle: the server answers it with one ReadyForQuery.
		state.pending = append(state.pending, &pendingEvent{
			sql: m.String, start: started, simple: true, cycle: state.cycle,
		})
		state.openCycle()
	case *pgproto3.Parse:
		state.preparedStmts[m.Name] = m.Query
	case *pgproto3.Bind:
		if sql, ok := state.preparedStmts[m.PreparedStatement]; ok {
			state.portals[m.DestinationPortal] = sql
		}
	case *pgproto3.Execute:
		// Resuming a suspended portal continues the statement, nothing to enqueue.
		if pending := state.resume(m.Portal); pending != nil {
			pending.suspended = false
			return
		}
		state.pending = append(state.pending, &pendingEvent{
			sql: state.portals[m.Portal], start: started, portal: m.Portal, cycle: state.cycle,
		})
	case *pgproto3.Sync:
		// Sync ends the extended protocol cycle and is what the server answers with a ReadyForQuery.
		state.openCycle()
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
	for _, ev := range state.observeUpstream(msg) {
		handleEvent(ev)
	}
}

// observeUpstream advances the state machine and answers with whatever the server just finished.
func (s *connState) observeUpstream(msg pgproto3.BackendMessage) []metrics.QueryEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	at := s.now()

	switch m := msg.(type) {
	case *pgproto3.CommandComplete:
		pending, index := s.completing()
		if pending == nil {
			return nil
		}
		ran := s.ran(pending, at)
		s.boundary = at
		pending.tag = string(m.CommandTag)

		if !pending.simple {
			// in extended protocol, one Execute is one statement, so it is finished and reportable on its own.
			s.pending = append(s.pending[:index], s.pending[index+1:]...)
			return []metrics.QueryEvent{pending.event(ran)}
		}
		// A simple Query get CommandComplete for each of its statements separately.
		pending.timings = append(pending.timings, metrics.StatementTiming{
			CommandTag: string(m.CommandTag),
			Duration:   ran,
		})

	case *pgproto3.PortalSuspended:
		// An Execute carrying a row limit returns its batch and leaves the portal open.
		if pending, _ := s.completing(); pending != nil {
			// The server stops working on the portal here, so we add the elapsed timing.
			pending.elapsed = s.ran(pending, at)
			pending.suspended = true
			s.boundary = at
		}

	case *pgproto3.ErrorResponse:
		// The error belongs to the statement the server was running, the rest of the batch is abandoned.
		if pending, _ := s.completing(); pending != nil {
			pending.err = m.Message
		}

	case *pgproto3.ReadyForQuery:
		if len(s.awaiting) == 0 {
			// The server also reports ready at the end of authentication, with no cycle behind it.
			// If anything is queued anyway then the client sent something this state machine did not
			// model, and holding those statements back would leak them: publish and start clean.
			return s.flush(at, func(*pendingEvent) bool { return true })
		}
		cycle := s.awaiting[0]
		s.awaiting = s.awaiting[1:]

		txOpen := m.TxStatus == 'T'
		return s.flush(at, func(pending *pendingEvent) bool {
			if pending.suspended {
				// If the transaction is still opened, that mean that the portal still has things to do,
				// we should wait before publishing the event.
				return !txOpen
			}
			return pending.cycle == cycle
		})
	}
	return nil
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
