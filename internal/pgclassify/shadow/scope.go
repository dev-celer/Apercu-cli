// Package shadow follows what the migration does to the database as it goes, and records it in the shadow overlay:
// the relations it creates, links, drops, renames or moves, and the constraints and columns it adds, drops or renames.
package shadow

import (
	"apercu-cli/internal/pgclassify/stmt"
)

// Scope is a view, with the questions about the tables' constraints and columns the rules ask of it.
type Scope struct {
	stmt.View
}
