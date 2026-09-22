package pg_classify

import (
	"fmt"

	"apercu-cli/helper/pg_contract"
)

// backfill is the rule id one DML statement carries and what it does to the rows it touches.
type backfill struct {
	code    pg_contract.Code
	message string
}

var backfills = map[pg_contract.Command]backfill{
	"INSERT": {"R-DML-INSERT", "rows are added"},
	"UPDATE": {"R-DML-UPDATE", "rows are rewritten, the old versions left for VACUUM to reclaim"},
	"DELETE": {"R-DML-DELETE", "rows are marked dead, the space left for VACUUM to reclaim"},
	"MERGE":  {"R-DML-MERGE", "rows are added, rewritten or marked dead depending on the match"},
}

// ruleDML is R-DML-INSERT, R-DML-UPDATE, R-DML-DELETE and R-DML-MERGE.
func ruleDML(s scope) effect {
	kind := backfills[s.statement.Command]
	e := newEffect(s, kind.code,
		fmt.Sprintf("%s under ROW EXCLUSIVE, which blocks no reader and no other writer; the cost is the number of rows and the indexes that have to be maintained with them", kind.message))
	e.lock = pg_contract.LockRowExclusive
	e.op = pg_contract.OpKindDML
	e.extra = s.readTargets()
	if s.statement.Command == "INSERT" {
		e.recursion = partitionsOnly
	}
	return e
}

// ruleSelectFor is R-DML-SELECTFOR.
func ruleSelectFor(s scope) effect {
	strength := ""
	if len(s.statement.Subcommands) > 0 {
		strength = " " + s.statement.Subcommands[0].Value
	}
	e := newEffect(s, "R-DML-SELECTFOR",
		fmt.Sprintf("FOR%s takes a row lock on every row the query returns and holds it to the end of the transaction; the table itself is barely locked, the rows are what other writers queue on", strength))
	e.lock = pg_contract.LockRowShare
	e.op = pg_contract.OpKindNone
	return e.onEvery(s)
}

// readTargets is every relation the statement reads beside the one it writes.
func (s scope) readTargets() []pg_contract.Target {
	var targets []pg_contract.Target
	for _, info := range s.sources() {
		targets = append(targets, pg_contract.Target{
			Relation: contract(info),
			Lock:     pg_contract.LockAccessShare,
			OpKind:   pg_contract.OpKindNone,
			Role:     pg_contract.TargetRoleDirect,
		})
	}
	return targets
}
