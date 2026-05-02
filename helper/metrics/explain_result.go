package metrics

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ExplainOutput is the top-level result of `EXPLAIN (FORMAT JSON, ...)`.
// PostgreSQL always returns a JSON array, even for a single statement.
type ExplainOutput []ExplainResult

type ExplainResult struct {
	Plan          Plan              `json:"Plan"`
	Planning      *Planning         `json:"Planning,omitempty"`
	PlanningTime  float64           `json:"Planning Time,omitempty"`
	Triggers      []Trigger         `json:"Triggers,omitempty"`
	JIT           *JIT              `json:"JIT,omitempty"`
	Settings      map[string]string `json:"Settings,omitempty"`
	QueryID       uint64            `json:"Query Identifier,omitempty"`
	ExecutionTime float64           `json:"Execution Time,omitempty"`
}

// Plan is one node in the plan tree. Children live in Plans.
type Plan struct {
	NodeType      string `json:"Node Type"`
	ParentRel     string `json:"Parent Relationship,omitempty"`
	SubplanName   string `json:"Subplan Name,omitempty"`
	Strategy      string `json:"Strategy,omitempty"`
	PartialMode   string `json:"Partial Mode,omitempty"`
	Operation     string `json:"Operation,omitempty"`
	ParallelAware bool   `json:"Parallel Aware,omitempty"`
	AsyncCapable  bool   `json:"Async Capable,omitempty"`

	// Relation / index targets
	RelationName  string   `json:"Relation Name,omitempty"`
	Schema        string   `json:"Schema,omitempty"`
	Alias         string   `json:"Alias,omitempty"`
	IndexName     string   `json:"Index Name,omitempty"`
	ScanDirection string   `json:"Scan Direction,omitempty"`
	CTEName       string   `json:"CTE Name,omitempty"`
	FunctionName  string   `json:"Function Name,omitempty"`
	Output        []string `json:"Output,omitempty"`

	// Cost & row estimates (planner)
	StartupCost float64 `json:"Startup Cost"`
	TotalCost   float64 `json:"Total Cost"`
	PlanRows    float64 `json:"Plan Rows"`
	PlanWidth   int     `json:"Plan Width"`

	// Actuals (require ANALYZE)
	ActualStartupTime float64 `json:"Actual Startup Time,omitempty"`
	ActualTotalTime   float64 `json:"Actual Total Time,omitempty"`
	ActualRows        float64 `json:"Actual Rows,omitempty"`
	ActualLoops       float64 `json:"Actual Loops,omitempty"`

	// Predicates
	IndexCond                 string  `json:"Index Cond,omitempty"`
	RecheckCond               string  `json:"Recheck Cond,omitempty"`
	Filter                    string  `json:"Filter,omitempty"`
	JoinFilter                string  `json:"Join Filter,omitempty"`
	HashCond                  string  `json:"Hash Cond,omitempty"`
	MergeCond                 string  `json:"Merge Cond,omitempty"`
	JoinType                  string  `json:"Join Type,omitempty"`
	RowsRemovedByFilter       float64 `json:"Rows Removed by Filter,omitempty"`
	RowsRemovedByIndexRecheck float64 `json:"Rows Removed by Index Recheck,omitempty"`
	RowsRemovedByJoinFilter   float64 `json:"Rows Removed by Join Filter,omitempty"`

	// Sort
	SortKey       []string `json:"Sort Key,omitempty"`
	SortMethod    string   `json:"Sort Method,omitempty"`
	SortSpaceUsed int64    `json:"Sort Space Used,omitempty"`
	SortSpaceType string   `json:"Sort Space Type,omitempty"`

	// Hash
	HashBuckets         int64 `json:"Hash Buckets,omitempty"`
	OriginalHashBuckets int64 `json:"Original Hash Buckets,omitempty"`
	HashBatches         int64 `json:"Hash Batches,omitempty"`
	OriginalHashBatches int64 `json:"Original Hash Batches,omitempty"`
	PeakMemoryUsage     int64 `json:"Peak Memory Usage,omitempty"`

	// Aggregate / GroupAggregate
	GroupKey       []string `json:"Group Key,omitempty"`
	HashAggBatches int64    `json:"HashAgg Batches,omitempty"`
	DiskUsage      int64    `json:"Disk Usage,omitempty"`

	// Buffers (require BUFFERS)
	BufferUsage

	// I/O timings (require BUFFERS in newer PG, or TIMING)
	IOReadTime  float64 `json:"I/O Read Time,omitempty"`
	IOWriteTime float64 `json:"I/O Write Time,omitempty"`

	// WAL (require WAL)
	WALRecords int64 `json:"WAL Records,omitempty"`
	WALFPI     int64 `json:"WAL FPI,omitempty"`
	WALBytes   int64 `json:"WAL Bytes,omitempty"`

	// Parallel workers
	WorkersPlanned  int      `json:"Workers Planned,omitempty"`
	WorkersLaunched int      `json:"Workers Launched,omitempty"`
	Workers         []Worker `json:"Workers,omitempty"`

	// Sub-plans (children)
	Plans []Plan `json:"Plans,omitempty"`
}

// BufferUsage is embedded in Plan, Planning and Worker.
type BufferUsage struct {
	SharedHitBlocks     int64 `json:"Shared Hit Blocks,omitempty"`
	SharedReadBlocks    int64 `json:"Shared Read Blocks,omitempty"`
	SharedDirtiedBlocks int64 `json:"Shared Dirtied Blocks,omitempty"`
	SharedWrittenBlocks int64 `json:"Shared Written Blocks,omitempty"`
	LocalHitBlocks      int64 `json:"Local Hit Blocks,omitempty"`
	LocalReadBlocks     int64 `json:"Local Read Blocks,omitempty"`
	LocalDirtiedBlocks  int64 `json:"Local Dirtied Blocks,omitempty"`
	LocalWrittenBlocks  int64 `json:"Local Written Blocks,omitempty"`
	TempReadBlocks      int64 `json:"Temp Read Blocks,omitempty"`
	TempWrittenBlocks   int64 `json:"Temp Written Blocks,omitempty"`
}

// Planning carries buffer/IO accounting for the planning phase (PG13+).
type Planning struct {
	BufferUsage
	IOReadTime  float64 `json:"I/O Read Time,omitempty"`
	IOWriteTime float64 `json:"I/O Write Time,omitempty"`
}

type Worker struct {
	WorkerNumber      int     `json:"Worker Number"`
	ActualStartupTime float64 `json:"Actual Startup Time,omitempty"`
	ActualTotalTime   float64 `json:"Actual Total Time,omitempty"`
	ActualRows        float64 `json:"Actual Rows,omitempty"`
	ActualLoops       float64 `json:"Actual Loops,omitempty"`
	BufferUsage
	IOReadTime  float64 `json:"I/O Read Time,omitempty"`
	IOWriteTime float64 `json:"I/O Write Time,omitempty"`
}

type Trigger struct {
	TriggerName    string  `json:"Trigger Name"`
	ConstraintName string  `json:"Constraint Name,omitempty"`
	Relation       string  `json:"Relation,omitempty"`
	Time           float64 `json:"Time"`
	Calls          int64   `json:"Calls"`
}

type JIT struct {
	Functions int        `json:"Functions"`
	Options   JITOptions `json:"Options"`
	Timing    JITTiming  `json:"Timing"`
}

type JITOptions struct {
	Inlining     bool `json:"Inlining"`
	Optimization bool `json:"Optimization"`
	Expressions  bool `json:"Expressions"`
	Deforming    bool `json:"Deforming"`
}

type JITTiming struct {
	Generation   float64 `json:"Generation"`
	Inlining     float64 `json:"Inlining"`
	Optimization float64 `json:"Optimization"`
	Emission     float64 `json:"Emission"`
	Total        float64 `json:"Total"`
}

// ParseExplainJSON decodes the raw JSON returned by `EXPLAIN (FORMAT JSON, ...)`.
func ParseExplainJSON(raw []byte) (ExplainOutput, error) {
	var out ExplainOutput
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode explain json: %w", err)
	}
	return out, nil
}

func (e *ExplainResult) String() string {
	var b strings.Builder
	writePlan(&b, &e.Plan, 0)

	for _, tr := range e.Triggers {
		fmt.Fprintf(&b, "Trigger %s", tr.TriggerName)
		if tr.Relation != "" {
			fmt.Fprintf(&b, " on %s", tr.Relation)
		}
		fmt.Fprintf(&b, ": time=%.3f calls=%d\n", tr.Time, tr.Calls)
	}

	if e.Planning != nil {
		if line := bufferLine(e.Planning.BufferUsage); line != "" {
			fmt.Fprintf(&b, "Planning:\n  %s\n", line)
		}
	}
	if e.PlanningTime > 0 {

		fmt.Fprintf(&b, "Planning Time: %.3f ms\n", e.PlanningTime)
	}

	if jit := e.JIT; jit != nil {
		fmt.Fprintf(&b, "JIT:\n")
		fmt.Fprintf(&b, "  Functions: %d\n", jit.Functions)
		fmt.Fprintf(&b, "  Options: Inlining %s, Optimization %s, Expressions %s, Deforming %s\n",
			yesNo(jit.Options.Inlining), yesNo(jit.Options.Optimization),
			yesNo(jit.Options.Expressions), yesNo(jit.Options.Deforming))
		fmt.Fprintf(&b, "  Timing: Generation %.3f ms, Inlining %.3f ms, Optimization %.3f ms, Emission %.3f ms, Total %.3f ms\n",
			jit.Timing.Generation, jit.Timing.Inlining, jit.Timing.Optimization,
			jit.Timing.Emission, jit.Timing.Total)
	}

	if e.ExecutionTime > 0 {
		fmt.Fprintf(&b, "Execution Time: %.3f ms\n", e.ExecutionTime)
	}

	return b.String()
}

func writePlan(b *strings.Builder, p *Plan, depth int) {
	indent := strings.Repeat("  ", depth)
	prefix := indent
	if depth > 0 {
		prefix = indent[:len(indent)-2] + "->  "
	}

	fmt.Fprintf(b, "%s%s", prefix, planHeader(p))
	fmt.Fprintf(b, "  (cost=%.2f..%.2f rows=%.0f width=%d)",
		p.StartupCost, p.TotalCost, p.PlanRows, p.PlanWidth)
	if p.ActualLoops > 0 || p.ActualTotalTime > 0 {
		fmt.Fprintf(b, " (actual time=%.3f..%.3f rows=%.0f loops=%.0f)",
			p.ActualStartupTime, p.ActualTotalTime, p.ActualRows, p.ActualLoops)
	}
	b.WriteByte('\n')

	detailIndent := indent + "  "
	writeDetails(b, p, detailIndent)

	for i := range p.Plans {
		writePlan(b, &p.Plans[i], depth+1)
	}
}

func planHeader(p *Plan) string {
	h := p.NodeType
	if p.Strategy != "" && p.Strategy != "Plain" {
		h = p.Strategy + " " + h
	}
	if p.PartialMode != "" && p.PartialMode != "Simple" {
		h = p.PartialMode + " " + h
	}
	if p.ParallelAware {
		h = "Parallel " + h
	}
	switch {
	case p.IndexName != "" && p.RelationName != "":
		h += fmt.Sprintf(" using %s on %s", p.IndexName, p.RelationName)
		if p.Alias != "" && p.Alias != p.RelationName {
			h += " " + p.Alias
		}
	case p.RelationName != "":
		h += " on " + p.RelationName
		if p.Alias != "" && p.Alias != p.RelationName {
			h += " " + p.Alias
		}
	case p.FunctionName != "":
		h += " on " + p.FunctionName
	case p.CTEName != "":
		h += " on " + p.CTEName
	}
	if p.JoinType != "" {
		h += " " + p.JoinType + " Join"
	}
	return h
}

func writeDetails(b *strings.Builder, p *Plan, indent string) {
	type kv struct{ k, v string }
	var lines []kv

	if len(p.Output) > 0 {
		lines = append(lines, kv{"Output", strings.Join(p.Output, ", ")})
	}
	if len(p.GroupKey) > 0 {
		lines = append(lines, kv{"Group Key", strings.Join(p.GroupKey, ", ")})
	}
	if len(p.SortKey) > 0 {
		lines = append(lines, kv{"Sort Key", strings.Join(p.SortKey, ", ")})
	}
	if p.SortMethod != "" {
		v := p.SortMethod
		if p.SortSpaceUsed > 0 {
			v += fmt.Sprintf("  Memory: %dkB", p.SortSpaceUsed)
		}
		lines = append(lines, kv{"Sort Method", v})
	}
	if p.IndexCond != "" {
		lines = append(lines, kv{"Index Cond", p.IndexCond})
	}
	if p.RecheckCond != "" {
		lines = append(lines, kv{"Recheck Cond", p.RecheckCond})
	}
	if p.HashCond != "" {
		lines = append(lines, kv{"Hash Cond", p.HashCond})
	}
	if p.MergeCond != "" {
		lines = append(lines, kv{"Merge Cond", p.MergeCond})
	}
	if p.JoinFilter != "" {
		lines = append(lines, kv{"Join Filter", p.JoinFilter})
	}
	if p.Filter != "" {
		lines = append(lines, kv{"Filter", p.Filter})
	}
	if p.RowsRemovedByFilter > 0 {
		lines = append(lines, kv{"Rows Removed by Filter", fmt.Sprintf("%.0f", p.RowsRemovedByFilter)})
	}
	if p.RowsRemovedByIndexRecheck > 0 {
		lines = append(lines, kv{"Rows Removed by Index Recheck", fmt.Sprintf("%.0f", p.RowsRemovedByIndexRecheck)})
	}
	if p.RowsRemovedByJoinFilter > 0 {
		lines = append(lines, kv{"Rows Removed by Join Filter", fmt.Sprintf("%.0f", p.RowsRemovedByJoinFilter)})
	}
	if p.HashBuckets > 0 {
		v := fmt.Sprintf("Buckets: %d", p.HashBuckets)
		if p.HashBatches > 0 {
			v += fmt.Sprintf("  Batches: %d", p.HashBatches)
		}
		if p.PeakMemoryUsage > 0 {
			v += fmt.Sprintf("  Memory Usage: %dkB", p.PeakMemoryUsage)
		}
		b.WriteString(indent)
		b.WriteString(v)
		b.WriteByte('\n')
	}
	if p.WorkersPlanned > 0 {
		lines = append(lines, kv{"Workers Planned", fmt.Sprintf("%d", p.WorkersPlanned)})
	}
	if p.WorkersLaunched > 0 {
		lines = append(lines, kv{"Workers Launched", fmt.Sprintf("%d", p.WorkersLaunched)})
	}
	if line := bufferLine(p.BufferUsage); line != "" {
		lines = append(lines, kv{"Buffers", line})
	}
	if p.IOReadTime > 0 || p.IOWriteTime > 0 {
		lines = append(lines, kv{"I/O", fmt.Sprintf("read=%.3f write=%.3f", p.IOReadTime, p.IOWriteTime)})
	}
	if p.WALRecords > 0 || p.WALBytes > 0 {
		lines = append(lines, kv{"WAL", fmt.Sprintf("records=%d fpi=%d bytes=%d", p.WALRecords, p.WALFPI, p.WALBytes)})
	}

	for _, l := range lines {
		fmt.Fprintf(b, "%s%s: %s\n", indent, l.k, l.v)
	}
}

func bufferLine(bu BufferUsage) string {
	var parts []string
	addPair := func(label string, hit, read, dirtied, written int64) {
		var inner []string
		if hit > 0 {
			inner = append(inner, fmt.Sprintf("hit=%d", hit))
		}
		if read > 0 {
			inner = append(inner, fmt.Sprintf("read=%d", read))
		}
		if dirtied > 0 {
			inner = append(inner, fmt.Sprintf("dirtied=%d", dirtied))
		}
		if written > 0 {
			inner = append(inner, fmt.Sprintf("written=%d", written))
		}
		if len(inner) > 0 {
			parts = append(parts, label+" "+strings.Join(inner, " "))
		}
	}
	addPair("shared", bu.SharedHitBlocks, bu.SharedReadBlocks, bu.SharedDirtiedBlocks, bu.SharedWrittenBlocks)
	addPair("local", bu.LocalHitBlocks, bu.LocalReadBlocks, bu.LocalDirtiedBlocks, bu.LocalWrittenBlocks)
	if bu.TempReadBlocks > 0 || bu.TempWrittenBlocks > 0 {
		var inner []string
		if bu.TempReadBlocks > 0 {
			inner = append(inner, fmt.Sprintf("read=%d", bu.TempReadBlocks))
		}
		if bu.TempWrittenBlocks > 0 {
			inner = append(inner, fmt.Sprintf("written=%d", bu.TempWrittenBlocks))
		}
		parts = append(parts, "temp "+strings.Join(inner, " "))
	}
	return strings.Join(parts, ", ")
}

func yesNo(v bool) string {
	if v {
		return "yes"
	}
	return "no"
}
