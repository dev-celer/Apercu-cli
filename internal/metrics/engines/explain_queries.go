package engines

import (
	"apercu-cli/config"
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"apercu-cli/output"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/montanaflynn/stats"
)

const PlanningTimeThreshold = 0.1
const ExecutionTimeThreshold = 0.1

type ExplainQueryEngine struct {
	db      *sql.DB
	queries *ExtractQueriesOutput
	output  []output.OutputDatabaseExplainQuery
}

type ExtractQueriesOutput struct {
	Queries  map[string][]string
	Warnings []string
}

func extractQueriesFromFile(path string) ([]string, error) {
	slog.Debug("Extracting queries from file", "file_path", path)

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	queries := strings.Split(string(content), ";")

	result := make([]string, 0)
	for _, query := range queries {
		if strings.TrimSpace(query) == "" {
			continue
		}
		result = append(result, strings.TrimSpace(query))
	}

	slog.Debug("Extracted queries from file", "file_path", path, "queries_found", len(result))
	return result, nil
}

func extractAllQueriesToExplain(paths []string) (*ExtractQueriesOutput, error) {
	outputData := &ExtractQueriesOutput{
		Queries:  make(map[string][]string),
		Warnings: make([]string, 0),
	}

	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				outputData.Warnings = append(outputData.Warnings, fmt.Sprintf("Explain query path not found: %s", path))
				_, _ = fmt.Fprintln(log.Writer(), "WARNING: Explain query path not found:", path)
				continue
			}
			return nil, errors.New(fmt.Sprintf("Failed to get explain query file path %s: %v", path, err))
		}

		if info.IsDir() {
			slog.Debug("Explain query path is a directory, finding all .sql files", "path", path)
			if err := filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}

				if !d.IsDir() && filepath.Ext(p) == ".sql" {
					slog.Debug("Found explain query file", "file_path", p)
					queries, err := extractQueriesFromFile(p)
					if err != nil {
						outputData.Warnings = append(outputData.Warnings, fmt.Sprintf("Failed to get queries from file %s: %v", p, err))
						_, _ = fmt.Fprintf(log.Writer(), "Failed to get queries from file %s: %v\n", p, err)
						return nil
					}
					if len(queries) == 0 {
						outputData.Warnings = append(outputData.Warnings, fmt.Sprintf("No queries found in file %s", p))
						_, _ = fmt.Fprintf(log.Writer(), "No queries found in file %s\n", p)
						return nil
					}

					outputData.Queries[p] = queries
				}

				return nil
			}); err != nil {
				outputData.Warnings = append(outputData.Warnings, fmt.Sprintf("Failed to walk through explain query directory %s: %v", path, err))
				_, _ = fmt.Fprintf(log.Writer(), "Failed to walk through explain query directory %s: %v\n", path, err)
				continue
			}
		} else {
			slog.Debug("Explain query file found", "file", path)
			queries, err := extractQueriesFromFile(path)
			if err != nil {
				outputData.Warnings = append(outputData.Warnings, fmt.Sprintf("Failed to get queries from file %s: %v", path, err))
				_, _ = fmt.Fprintf(log.Writer(), "Failed to get queries from file %s: %v\n", path, err)
				continue
			}
			if len(queries) == 0 {
				outputData.Warnings = append(outputData.Warnings, fmt.Sprintf("No queries found in file %s", path))
				_, _ = fmt.Fprintf(log.Writer(), "No queries found in file %s\n", path)
				continue
			}

			outputData.Queries[path] = queries
		}
	}

	return outputData, nil
}

func NewExplainQueryEngine(db *sql.DB, dbConfig *config.Database) (*ExplainQueryEngine, error) {
	queries, err := extractAllQueriesToExplain(dbConfig.ExplainQuery)
	if err != nil {
		return nil, err
	}

	return &ExplainQueryEngine{
		db:      db,
		queries: queries,
		output:  make([]output.OutputDatabaseExplainQuery, 0),
	}, nil
}

func (e *ExplainQueryEngine) CollectPreMigrationMetrics() error {
	for file, queries := range e.queries.Queries {
		for _, query := range queries {
			queryRun, err := e.generateQueryRun(query)
			var preMigrationRun output.OutputDatabaseMigrationExplainQueryRun
			if err != nil {
				preMigrationRun.Error = err
			} else {
				preMigrationRun.ExecutionTimes = queryRun.ExecutionTimes
				preMigrationRun.ExplainedQuery = queryRun.MedianFullResult
			}

			e.output = append(e.output, output.OutputDatabaseExplainQuery{
				File:            file,
				Query:           query,
				PreMigrationRun: &preMigrationRun,
			})
		}
	}
	return nil
}

func (e *ExplainQueryEngine) SendPgProxyLogs(s string) {}

func (e *ExplainQueryEngine) CollectPostMigrationMetrics() error {
	for file, queries := range e.queries.Queries {
		for _, query := range queries {
			idx := slices.IndexFunc(e.output, func(s output.OutputDatabaseExplainQuery) bool {
				return s.Query == query && s.File == file
			})
			if idx == -1 {
				continue
			}

			queryRun, err := e.generateQueryRun(query)
			var postMigrationRun output.OutputDatabaseMigrationExplainQueryRun
			if err != nil {
				postMigrationRun.Error = err
			} else {
				postMigrationRun.ExecutionTimes = queryRun.ExecutionTimes
				postMigrationRun.ExplainedQuery = queryRun.MedianFullResult
			}
			e.output[idx].PostMigrationRun = &postMigrationRun

			if e.output[idx].PreMigrationRun == nil || e.output[idx].PostMigrationRun == nil ||
				e.output[idx].PreMigrationRun.Error != nil || e.output[idx].PostMigrationRun.Error != nil {
				continue
			}
			if len(query) > 120 {
				query = query[:120] + "..."
			}

			// Generation execution time delta values
			medianDelta, hi, lo := bootstrapMedianRatio(e.output[idx].PreMigrationRun.ExecutionTimes, e.output[idx].PostMigrationRun.ExecutionTimes, 10_000, 0.95)
			e.output[idx].MedianDelta = medianDelta
			e.output[idx].Hi = hi
			e.output[idx].Lo = lo

			// Clear execution times pointers to allow GC to cleanup the data
			e.output[idx].PreMigrationRun.ExecutionTimes = nil
			e.output[idx].PostMigrationRun.ExecutionTimes = nil

			// Generate warnings
			if w := generateExecutionTimeWarnings(&e.output[idx]); w != nil {
				e.output[idx].Warnings = append(e.output[idx].Warnings, w)
				warning.PrintWarning(w)
			}
		}
	}
	return nil
}

func (e *ExplainQueryEngine) StoreMetricsToOutput(metrics *output.OutputDatabaseMetrics) error {
	if metrics.Explains != nil {
		metrics.Explains = append(metrics.Explains, e.output...)
	} else {
		metrics.Explains = e.output
	}

	return nil
}

type QueryRunResult struct {
	MedianFullResult *metricshelper.ExplainResult
	ExecutionTimes   []float64
}

func (e *ExplainQueryEngine) explainQuery(query string) (*metricshelper.ExplainResult, error) {
	tx, err := e.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var raw []byte
	stmt := "EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) " + query
	if err := tx.QueryRow(stmt).Scan(&raw); err != nil {
		return nil, fmt.Errorf("explain query failed: %w", err)
	}

	out, err := metricshelper.ParseExplainJSON(raw)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("explain returned no results")
	}

	return &out[0], nil
}

func (e *ExplainQueryEngine) generateQueryRun(query string) (QueryRunResult, error) {
	slog.Debug("Explaining query", "query", query)
	explainQueryResults := make([]*metricshelper.ExplainResult, 100)

	// Run the query 100 time
	for i := range explainQueryResults {
		explainResult, err := e.explainQuery(query)
		if err != nil {
			return QueryRunResult{}, err
		}

		explainQueryResults[i] = explainResult
	}

	// Discard first queries as it may a served to wake up the instance or warm the cache
	explainQueryResults = explainQueryResults[5:]

	// Extract all execution times
	executionTimes := make([]float64, len(explainQueryResults))
	for i, explainResult := range explainQueryResults {
		executionTimes[i] = explainResult.ExecutionTime
	}

	// Get median execution time
	median, err := stats.Median(executionTimes)
	if err != nil {
		return QueryRunResult{}, fmt.Errorf("error calculating median of explain results: %w", err)
	}

	// Select median result
	var medianQueryResult *metricshelper.ExplainResult
	var closestDiff float64
	for _, explainResult := range explainQueryResults {
		diff := explainResult.ExecutionTime - median
		if diff < 0 {
			diff *= -1
		}

		if medianQueryResult == nil {
			medianQueryResult = explainResult
			closestDiff = diff
			if explainResult.ExecutionTime == median {
				break
			}
			continue
		}

		// If explainQuery is the median exit loop
		if explainResult.ExecutionTime == median {
			medianQueryResult = explainResult
			break
		}

		// If the query is closer to the median update the medianQueryResult pointer
		if diff < closestDiff {
			closestDiff = diff
			medianQueryResult = explainResult
		}
	}

	return QueryRunResult{
		MedianFullResult: medianQueryResult,
		ExecutionTimes:   executionTimes,
	}, nil
}

// BootstrapMedianRatio return relative execution time median delta, hi, lo
func bootstrapMedianRatio(before, after []float64, iters int, confidence float64) (float64, float64, float64) {
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1024))
	deltas := make([]float64, iters)

	resample := func(src []float64) []float64 {
		out := make([]float64, len(src))
		for i := range out {
			out[i] = src[rng.IntN(len(src))]
		}
		return out
	}

	for i := 0; i < iters; i++ {
		bMedian, _ := stats.Median(resample(before))
		aMedian, _ := stats.Median(resample(after))
		deltas[i] = aMedian/bMedian - 1
	}

	sort.Float64s(deltas)
	alpha := (1 - confidence) / 2
	lo := deltas[int(alpha*float64(iters))]
	hi := deltas[int((1-alpha)*float64(iters))-1]
	point := deltas[iters/2]
	return point, hi, lo
}

func generateExecutionTimeWarnings(q *output.OutputDatabaseExplainQuery) warning.Warning {
	var plannedTime *warning.ExplainQueryPlannedValue
	var realTime *warning.ExplainQueryRealValue

	// Check for planned time regression
	if delta := q.PostMigrationRun.ExplainedQuery.PlanningTime/q.PreMigrationRun.ExplainedQuery.PlanningTime - 1; delta > PlanningTimeThreshold {
		level := warning.WarningLevelLow
		if delta > PlanningTimeThreshold*3 {
			level = warning.WarningLevelMedium
		}
		plannedTime = &warning.ExplainQueryPlannedValue{
			InitialCost: q.PostMigrationRun.ExplainedQuery.PlanningTime,
			FinalCost:   q.PostMigrationRun.ExplainedQuery.PlanningTime,
			Level:       level,
		}
	}

	// Check for execution time regression
	if q.Lo >= ExecutionTimeThreshold {
		level := warning.WarningLevelLow
		if q.Lo > ExecutionTimeThreshold*3 {
			level = warning.WarningLevelMedium
		}
		realTime = &warning.ExplainQueryRealValue{
			Lo:    q.Lo,
			Hi:    q.Hi,
			Med:   q.MedianDelta,
			Level: level,
		}
	}

	return warning.NewExplainQueryWarning(plannedTime, realTime)
}
