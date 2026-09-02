package pg_classify

import "maps"

// scopes holds the settings in place as well as the transaction specific values.
type scopes struct {
	// baseline is the default settings.
	baseline map[string]string
	// current is the settings in effect right now.
	current map[string]string
	// session is the settings written by the current transaction, if any.
	session map[string]string

	// frames is the savepoint stack. It is empty outside an explicit transaction.
	// frames[0] is the transaction itself and each SAVEPOINT pushes another.
	frames []frame
	inTx   bool
}

type frame struct {
	// name is the savepoint's, empty for the transaction's own frame.
	name string
	undo []undo
}

// undo restores one parameter, in one map, to the value it held when the frame opened.
type undo struct {
	committed bool
	key       string
	value     string
	// had says the map held a value before this operation
	had bool
}

func newScopes(baseline map[string]string) *scopes {
	return &scopes{
		baseline: baseline,
		current:  map[string]string{},
		session:  map[string]string{},
	}
}

// value answers with what is in force.
func (s *scopes) value(key string) (string, bool) {
	if v, ok := s.current[key]; ok {
		return v, true
	}
	v, ok := s.baseline[key]
	return v, ok
}

func (s *scopes) mapFor(committed bool) map[string]string {
	if committed {
		return s.session
	}
	return s.current
}

// remember records the current value of a parameter in the innermost frame.
func (s *scopes) remember(committed bool, key string) {
	if len(s.frames) == 0 {
		return
	}
	top := &s.frames[len(s.frames)-1]
	for _, entry := range top.undo {
		if entry.committed == committed && entry.key == key {
			return
		}
	}

	value, had := s.mapFor(committed)[key]
	top.undo = append(top.undo, undo{committed: committed, key: key, value: value, had: had})
}

// set writes a parameter.
func (s *scopes) set(local bool, key, value string) {
	if local && !s.inTx {
		return
	}
	s.remember(false, key)
	s.current[key] = value
	if !local {
		s.remember(true, key)
		s.session[key] = value
	}
}

// reset drops a parameter back to the baseline.
func (s *scopes) reset(local bool, key string) {
	if local && !s.inTx {
		return
	}
	s.remember(false, key)
	delete(s.current, key)
	if !local {
		s.remember(true, key)
		delete(s.session, key)
	}
}

// resetAll reset all the parameters to the baseline values.
func (s *scopes) resetAll() {
	for key := range trackedParams {
		s.reset(false, key)
	}
}

func (s *scopes) begin() {
	if s.inTx {
		return
	}
	s.inTx = true
	s.frames = []frame{{}}
}

// unwind puts back everything the frames from index i upwards changed.
func (s *scopes) unwind(from int) {
	for i := len(s.frames) - 1; i >= from; i-- {
		for j := len(s.frames[i].undo) - 1; j >= 0; j-- {
			entry := s.frames[i].undo[j]
			if entry.had {
				s.mapFor(entry.committed)[entry.key] = entry.value
			} else {
				delete(s.mapFor(entry.committed), entry.key)
			}
		}
	}
}

// commit ends the block. What the session-level writes reached is what stays in force.
func (s *scopes) commit() {
	s.current = maps.Clone(s.session)
	s.frames = nil
	s.inTx = false
}

// rollback ends the block undoing everything, session-level writes included.
func (s *scopes) rollback() {
	s.unwind(0)
	s.frames = nil
	s.inTx = false
}

func (s *scopes) savepoint(name string) {
	if !s.inTx {
		return
	}
	s.frames = append(s.frames, frame{name: name})
}

// find locates the innermost savepoint with this name. Savepoints may share a name,
// the server answers with the most recent one.
func (s *scopes) find(name string) int {
	for i := len(s.frames) - 1; i >= 1; i-- {
		if s.frames[i].name == name {
			return i
		}
	}
	return -1
}

// rollbackTo unwinds back to a savepoint.
func (s *scopes) rollbackTo(name string) {
	target := s.find(name)
	if target < 0 {
		return
	}
	s.unwind(target)
	s.frames = s.frames[:target+1]
	s.frames[target].undo = nil
}

// release folds a savepoint and everything above it into its parent, keeping their writes.
func (s *scopes) release(name string) {
	target := s.find(name)
	if target <= 0 {
		return
	}
	parent := &s.frames[target-1]
	for i := target; i < len(s.frames); i++ {
		for _, entry := range s.frames[i].undo {
			// The parent's own entry, when it has one, records the older value and is the one a later ROLLBACK has to restore.
			if !hasUndo(parent.undo, entry) {
				parent.undo = append(parent.undo, entry)
			}
		}
	}
	s.frames = s.frames[:target]
}

func hasUndo(entries []undo, want undo) bool {
	for _, entry := range entries {
		if entry.committed == want.committed && entry.key == want.key {
			return true
		}
	}
	return false
}

// savepointNames lists the open savepoints, outermost first.
func (s *scopes) savepointNames() []string {
	if len(s.frames) < 2 {
		return nil
	}
	names := make([]string, 0, len(s.frames)-1)
	for _, f := range s.frames[1:] {
		names = append(names, f.name)
	}
	return names
}
