package rules

import (
	"fmt"

	"apercu-cli/internal/pgclassify/rules/engine"
)

// ruleUnknownCommand is R-UNKNOWN-COMMAND, for a command the registry has no rule for.
func ruleUnknownCommand(s engine.Scope) engine.Effect {
	message := fmt.Sprintf("the classifier does not recognize %s: neither the locks it takes nor the work it does are read from the statement", s.Statement.Command)
	return engine.NewEffect(s, "R-UNKNOWN-COMMAND", message).OnNone()
}
