// Package classifytest holds what the classifier's tests share: the committed snapshots as
// catalogs, a script classified end to end, and the lookups the tests read the analyses with.
// The files built with the integration tag add the live-server harness the oracle tests use.
package classifytest

import (
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"apercu-cli/internal/pgcatalog"
	"apercu-cli/internal/pgcatalog/snapshot"
	"apercu-cli/internal/pgclassify"
	"apercu-cli/internal/pgclassify/session"
	"apercu-cli/internal/pgclassify/shadow"
	"apercu-cli/internal/pgclassify/validate"
	"apercu-cli/internal/pgcontract"
	"apercu-cli/internal/pgparse"
	"apercu-cli/internal/pgparse/ir"

	"github.com/stretchr/testify/require"
)

// testdata is the directory of the committed snapshots, wherever the test using them runs from.
func testdata() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "pgcatalog", "snapshot", "testdata")
}

// Preview is the committed preview snapshot of one server version.
func Preview(t *testing.T, version pgcontract.Version) *snapshot.Snapshot {
	t.Helper()

	pre, err := snapshot.LoadJSON(filepath.Join(testdata(), fmt.Sprintf("snapshot_pg%d_preview.json.gz", version)))
	require.NoError(t, err)
	return pre
}

// Catalog is one committed snapshot, for Unit-testing
func Catalog(t *testing.T) *pgcatalog.Catalog {
	t.Helper()

	catalog, err := pgcatalog.NewCatalog(pgcatalog.CatalogOptions{Pre: Preview(t, pgcontract.Version17)})
	require.NoError(t, err)
	return catalog
}

// CatalogOn get the catalog corresponding to the passed version, for Unit-testing.
func CatalogOn(t *testing.T, version pgcontract.Version) *pgcatalog.Catalog {
	t.Helper()

	pre := Preview(t, version)
	require.Equal(t, version, pre.Header.Version, "snapshot_pg%d_preview.json.gz was captured on another server", version)

	prod := &snapshot.Snapshot{
		Source: snapshot.SourceProd,
		PIT:    snapshot.PITPre,
		Header: snapshot.Header{Version: version, ServerVersionNum: int(version) * 10000},
	}
	catalog, err := pgcatalog.NewCatalog(pgcatalog.CatalogOptions{Pre: pre, Prod: prod})
	require.NoError(t, err)
	return catalog
}

// NullableKeyCatalog adds to the fixture a range-partitioned readings whose key at can hold NULL, a list- and a
// hash-partitioned parent alike, and tables waiting to be attached, each with its own way of proving or not proving
// the bound. The fixture's own parents all have a NOT NULL key, where the partition has to be NOT NULL too.
func NullableKeyCatalog(t *testing.T) *pgcatalog.Catalog {
	t.Helper()

	pre := Preview(t, pgcontract.Version17)

	oid := snapshot.OID(9_000_000)
	table := func(name, kind, key string, notNull bool, checks ...snapshot.Constraint) {
		oid++
		pre.Relations = append(pre.Relations, snapshot.Relation{OID: oid, Namespace: "apercu_snapshot_test", Name: name,
			Kind: kind, Persistence: "p", PartitionKey: key})
		pre.Columns = append(pre.Columns,
			snapshot.Column{RelID: oid, Num: 1, Name: "id", FormattedType: "bigint", NotNull: true},
			snapshot.Column{RelID: oid, Num: 2, Name: "at", FormattedType: "integer", NotNull: notNull})
		for i, check := range checks {
			check.OID, check.RelID, check.Type, check.Enforced = oid*10+snapshot.OID(i), oid, "c", true
			pre.Constraints = append(pre.Constraints, check)
		}
	}
	bound := func(name string, validated bool, def string) snapshot.Constraint {
		return snapshot.Constraint{Name: name, Validated: validated, Def: def}
	}
	positive := bound("readings_at_positive", true, "CHECK ((at > 0))")

	table("readings", "p", "RANGE (at)", false, positive)
	table("readings_list", "p", "LIST (at)", false)
	table("readings_hash", "p", "HASH (at)", false)
	table("rd_bound", "r", "", false, positive, bound("rd_bound_b", true, "CHECK (((at >= 100) AND (at < 200)))"))
	table("rd_proved", "r", "", false, positive, bound("rd_proved_b", true, "CHECK (((at IS NOT NULL) AND (at >= 100) AND (at < 200)))"))
	table("rd_split", "r", "", false, positive, bound("rd_split_b", true, "CHECK (((at >= 100) AND (at < 200)))"),
		bound("rd_split_nn", true, "CHECK ((at IS NOT NULL))"))
	table("rd_notnull", "r", "", true, positive, bound("rd_notnull_b", true, "CHECK (((at >= 100) AND (at < 200)))"))
	table("rd_notvalid", "r", "", false, positive, bound("rd_notvalid_b", false, "CHECK (((at >= 100) AND (at < 200))) NOT VALID"))
	table("rd_inherited", "r", "", false, positive)
	table("rd_plain", "r", "", false)
	table("rd_list", "r", "", false, bound("rd_list_b", true, "CHECK ((at = ANY (ARRAY[1, 2])))"))
	table("rd_list_null", "r", "", false, bound("rd_list_null_b", true, "CHECK (((at IS NULL) OR (at = ANY (ARRAY[1, 2]))))"))

	catalog, err := pgcatalog.NewCatalog(pgcatalog.CatalogOptions{Pre: pre})
	require.NoError(t, err)
	return catalog
}

// Analyze classifies a whole script through one classifier, session and shadow catalog.
func Analyze(t *testing.T, catalog *pgcatalog.Catalog, script string) []pgcontract.StatementAnalysis {
	t.Helper()

	statements := pgparse.Parse(script)
	require.NotEmpty(t, statements)
	return pgclassify.NewClassifier(catalog).Analyze(statements)
}

// Single classifies one statement.
func Single(t *testing.T, catalog *pgcatalog.Catalog, sql string) pgcontract.StatementAnalysis {
	t.Helper()

	analyses := Analyze(t, catalog, sql)
	require.Len(t, analyses, 1)
	return analyses[0]
}

// FindingOf is the one finding carrying a rule id.
func FindingOf(t *testing.T, analysis pgcontract.StatementAnalysis, code pgcontract.Code) pgcontract.Finding {
	t.Helper()

	for _, finding := range analysis.Findings {
		if finding.Code == code {
			return finding
		}
	}
	require.Failf(t, "missing finding", "%q produced no %s, only %v", analysis.RawSQL, code, CodesOf(analysis))
	return pgcontract.Finding{}
}

// RulesOnly is the analysis with the validation findings taken out, leaving what the rules said.
func RulesOnly(analysis pgcontract.StatementAnalysis) pgcontract.StatementAnalysis {
	analysis.Findings = slices.DeleteFunc(slices.Clone(analysis.Findings), func(finding pgcontract.Finding) bool {
		return finding.Code == validate.QueueRiskCode || finding.Code == validate.MaintenanceStatementCode || finding.Code == validate.IndexRenameCode || finding.Code == validate.PartitionOfCode ||
			finding.Code == validate.ExtensionScriptCode
	})
	return analysis
}

func CodesOf(analysis pgcontract.StatementAnalysis) []pgcontract.Code {
	codes := make([]pgcontract.Code, 0, len(analysis.Findings))
	for _, finding := range analysis.Findings {
		codes = append(codes, finding.Code)
	}
	return codes
}

// TargetOf is the entry a finding holds for one relation.
func TargetOf(t *testing.T, finding pgcontract.Finding, relation string) pgcontract.Target {
	t.Helper()

	for _, target := range finding.Targets {
		if target.Relation.Name.String() == relation {
			return target
		}
	}
	require.Failf(t, "missing target", "%s names no %s, only %v", finding.Code, relation, finding.Targets)
	return pgcontract.Target{}
}

// Event is one proxy event: a simple Query, which may hold several statements, or one Execute.
func Event(sql string) pgcontract.QueryEvent {
	return pgcontract.QueryEvent{SQL: sql}
}

// Cycle stamps one protocol cycle on a run of events, which is what one client message becomes.
// Several events in one cycle is an extended-protocol pipeline: an Execute each, closed by one Sync,
// still one implicit transaction on the server.
func Cycle(id int, events ...pgcontract.QueryEvent) []pgcontract.QueryEvent {
	for i := range events {
		events[i].Cycle = id
	}
	return events
}

// Apart is a proxy event list the client sent as separate messages, one cycle each.
func Apart(events ...pgcontract.QueryEvent) []pgcontract.QueryEvent {
	for i := range events {
		events[i].Cycle = i
	}
	return events
}

// Run walks a script statement by statement, each one handed to the session as if the client had sent it in a message of its own.
func Run(t *testing.T, catalog *pgcatalog.Catalog, script string) ([]ir.Statement, []session.Context) {
	t.Helper()

	statements := pgparse.Parse(script)
	require.NotEmpty(t, statements)
	sess := session.NewSession(catalog)
	relations := shadow.NewRelations(catalog)
	contexts := make([]session.Context, 0, len(statements))
	for _, statement := range statements {
		context := sess.Next(statement)
		relations.Declare(statement, context)
		relations.Link(statement, context)
		relations.Retire(statement, context)
		contexts = append(contexts, context)
	}
	return statements, contexts
}

// TargetNames is every relation a finding names, nil when there is no finding.
func TargetNames(finding *pgcontract.Finding) []string {
	if finding == nil {
		return nil
	}
	names := make([]string, 0, len(finding.Targets))
	for _, target := range finding.Targets {
		names = append(names, target.Relation.Name.String())
	}
	return names
}
