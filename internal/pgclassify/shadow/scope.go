// Package shadow follows what the migration does to the catalog as it goes: the relations it
// creates, links, drops, renames or moves, and the constraints and columns it adds, drops or renames.
package shadow

import (
	"apercu-cli/internal/pgclassify/stmt"
)

// Scope is a view plus what the statements before it did to the tables' constraints.
type Scope struct {
	stmt.View
	// checks is what the statements before this one did to the tables' constraints.
	Checks *Checks
}
