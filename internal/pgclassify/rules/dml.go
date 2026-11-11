package rules

import (
	"fmt"

	"apercu-cli/internal/pgclassify/rules/engine"
	"apercu-cli/internal/pgcontract"
)

// backfill is the rule id one DML statement carries and what it does to the rows it touches.
type backfill struct {
	code    pgcontract.Code
	message string
}

var backfills = map[pgcontract.Command]backfill{
	"INSERT": {"R-DML-INSERT", "rows are added"},
	"UPDATE": {"R-DML-UPDATE", "rows are rewritten, the old versions left for VACUUM to reclaim"},
	"DELETE": {"R-DML-DELETE", "rows are marked dead, the space left for VACUUM to reclaim"},
	"MERGE":  {"R-DML-MERGE", "rows are added, rewritten or marked dead depending on the match"},
}

// ruleDML is R-DML-INSERT, R-DML-UPDATE, R-DML-DELETE and R-DML-MERGE.
func ruleDML(s engine.Scope) engine.Effect {
	kind := backfills[s.Statement.Command]
	e := engine.NewEffect(s, kind.code,
		fmt.Sprintf("%s under ROW EXCLUSIVE, which blocks no reader and no other writer; the cost is the number of rows and the indexes that have to be maintained with them", kind.message))
	e.Touches = readTargets(s)
	return e
}

// ruleSelectFor is R-DML-SELECTFOR.
func ruleSelectFor(s engine.Scope) engine.Effect {
	strength := ""
	if len(s.Statement.Subcommands) > 0 {
		strength = " " + s.Statement.Subcommands[0].Value
	}
	e := engine.NewEffect(s, "R-DML-SELECTFOR",
		fmt.Sprintf("FOR%s takes a row lock on every row the query returns and holds it to the end of the transaction; the table itself is barely locked, the rows are what other writers queue on", strength))
	return e.OnEvery(s)
}

// readTargets is every relation the statement reads beside the one it writes.
func readTargets(s engine.Scope) []engine.Touch {
	var targets []engine.Touch
	for _, info := range s.Sources() {
		targets = append(targets, engine.Touch{
			Info: info,
			Lock: pgcontract.LockAccessShare,
			Op:   pgcontract.OpKindNone,
			Role: pgcontract.TargetRoleDirect,
		})
	}
	return targets
}
