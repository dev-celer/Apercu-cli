package metrics

import (
	"encoding/json"
	"fmt"
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
