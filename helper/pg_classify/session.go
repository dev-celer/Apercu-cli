// Package pg_classify turns the statement IR into the locks and findings a migration is graded on.
package pg_classify

import (
	"strings"

	"apercu-cli/helper/pg_catalog"
	"apercu-cli/helper/pg_parse"
)

// Context is the session state one statement runs under.
type Context struct {
	// Group is the transaction group the statement belongs to.
	Group int
	// InTransaction says the group was opened by a BEGIN rather than by autocommit.
	InTransaction bool
	// Savepoints are the open savepoints.
	Savepoints []string

	SearchPath       []string
	TimeZone         string
	LockTimeout      Timeout
	StatementTimeout Timeout
	ReplicationRole  ReplicationRole
}

// TimeZoneIsUTC reports whether the zone in force is exactly UTC.
func (c Context) TimeZoneIsUTC() bool {
	return utcZones[strings.ToLower(strings.TrimSpace(c.TimeZone))]
}

// CreationSchema is the schema a CREATE with an unqualified name lands in: the first entry of the search path.
func (c Context) CreationSchema() string {
	if len(c.SearchPath) == 0 {
		return ""
	}
	return c.SearchPath[0]
}

// Session walks a migration's statements in order and answers, for each one, the context it runs under.
// It will mutate the shadow catalog during execution
type Session struct {
	catalog *pg_catalog.Catalog
	scopes  *scopes
	group   int
}

// NewSession starts a session from the settings baseline.
func NewSession(catalog *pg_catalog.Catalog) *Session {
	baseline := map[string]string{}
	if catalog != nil {
		for key, reported := range trackedParams {
			if value, ok := catalog.Setting(reported); ok {
				baseline[key] = value
			}
		}
		// S-16 may not carry search_path, but the snapshot header always does.
		if _, ok := baseline[paramSearchPath]; !ok {
			baseline[paramSearchPath] = strings.Join(catalog.SearchPath(), ", ")
		}
	}
	return &Session{catalog: catalog, scopes: newScopes(baseline)}
}

// Next advances the session by one statement and returns the context that statement runs under.
func (s *Session) Next(statement pg_parse.Statement) Context {
	s.openGroup(statement)
	context := s.context()

	s.declare(statement, context)
	s.applySetting(statement)
	s.applyTransaction(statement)
	return context
}

// Walk is Next over a whole migration, for callers that want the contexts up front.
func (s *Session) Walk(statements []pg_parse.Statement) []Context {
	contexts := make([]Context, 0, len(statements))
	for _, statement := range statements {
		contexts = append(contexts, s.Next(statement))
	}
	return contexts
}

// context reads the layers out into the shape the rules take.
func (s *Session) context() Context {
	value := func(key string) string {
		v, _ := s.scopes.value(key)
		return v
	}

	user := ""
	if s.catalog != nil {
		user = s.catalog.SnapshotUser()
	}

	return Context{
		Group:            s.group,
		InTransaction:    s.scopes.inTx,
		Savepoints:       s.scopes.savepointNames(),
		SearchPath:       pg_catalog.ParseSearchPath(value(paramSearchPath), user),
		TimeZone:         value(paramTimeZone),
		LockTimeout:      parseTimeout(value(paramLockTimeout)),
		StatementTimeout: parseTimeout(value(paramStatementTimeout)),
		ReplicationRole:  parseReplicationRole(value(paramReplicationRole)),
	}
}

// openGroup handle statement groups.
// A BEGIN opens a group and belongs to it, a statement outside any block is its own group.
func (s *Session) openGroup(statement pg_parse.Statement) {
	switch statement.Command {
	case "BEGIN":
		if !s.scopes.inTx {
			s.group++
		}
		s.scopes.begin()
	default:
		if !s.scopes.inTx {
			s.group++
		}
	}
}

// applyTransaction what closes a group, and what only unwinds part of one.
func (s *Session) applyTransaction(statement pg_parse.Statement) {
	name := ""
	if len(statement.Subcommands) == 1 {
		name = statement.Subcommands[0].Name
	}

	switch statement.Command {
	case "COMMIT", "PREPARE TRANSACTION":
		s.scopes.commit()
		s.chain(statement)
	case "ROLLBACK":
		s.scopes.rollback()
		s.chain(statement)
	case "SAVEPOINT":
		s.scopes.savepoint(name)
	case "ROLLBACK TO SAVEPOINT":
		s.scopes.rollbackTo(name)
	case "RELEASE SAVEPOINT":
		s.scopes.release(name)
	}
}

// chain is COMMIT AND CHAIN and ROLLBACK AND CHAIN, which end the block and open another one.
func (s *Session) chain(statement pg_parse.Statement) {
	if !statement.Flags.Chain {
		return
	}
	s.group++
	s.scopes.begin()
}

// applySetting record variables SET / RESET.
func (s *Session) applySetting(statement pg_parse.Statement) {
	local := statement.Command == "SET LOCAL"
	switch statement.Command {
	case "SET", "SET LOCAL", "RESET", "RESET ALL":
	default:
		return
	}
	if len(statement.Subcommands) != 1 {
		return
	}

	sub := statement.Subcommands[0]
	if statement.Command == "RESET ALL" {
		s.scopes.resetAll()
		return
	}

	key := canonical(sub.Name)
	if _, tracked := trackedParams[key]; !tracked {
		return
	}

	switch sub.Kind {
	case pg_parse.SubSetVariable:
		s.scopes.set(local, key, sub.Value)
	case pg_parse.SubResetVariable:
		s.scopes.reset(local, key)
	case pg_parse.SubSetVariableCurrent:
		// SET x FROM CURRENT writes back the value already in force, so nothing moves.
	}
}
