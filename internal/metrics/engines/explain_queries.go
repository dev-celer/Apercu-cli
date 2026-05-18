package engines

import (
	"apercu-cli/config"
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"apercu-cli/output"
	"database/sql"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/montanaflynn/stats"
)

const ExecutionTimeThreshold = 0.1

type ExplainQueryEngine struct {
	db             *sql.DB
	queries        map[string][]string
	fetchedQueries []string
	output         []output.OutputDatabaseExplainQuery
	warnings       map[warning.Code]warning.Warning
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

func extractAllQueriesToExplain(paths []string) (map[string][]string, []warning.Warning, error) {
	outputData := make(map[string][]string)
	warnings := make([]warning.Warning, 0)

	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			var code warning.Code
			if os.IsNotExist(err) {
				code = warning.CodeExplainQueryPathNotFound
			} else {
				code = warning.CodeExplainQueryFailedToReadFile
			}
			w := warning.NewExplainQueryFileWarning(code, path)
			if w != nil {
				warning.PrintWarning(w)
				warnings = append(warnings, w)
			}
			continue
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
						w := warning.NewExplainQueryFileWarning(warning.CodeExplainQueryFailedToReadFile, p)
						if w != nil {
							warning.PrintWarning(w)
							warnings = append(warnings, w)
						}
						return nil
					}
					if len(queries) == 0 {
						w := warning.NewExplainQueryFileWarning(warning.CodeExplainQueryNoQueries, p)
						if w != nil {
							warning.PrintWarning(w)
							warnings = append(warnings, w)
						}
						return nil
					}

					outputData[p] = queries
				}

				return nil
			}); err != nil {
				w := warning.NewExplainQueryFileWarning(warning.CodeExplainQueryFailedToReadFile, path)
				if w != nil {
					warning.PrintWarning(w)
					warnings = append(warnings, w)
				}
				continue
			}
		} else {
			slog.Debug("Explain query file found", "file", path)
			queries, err := extractQueriesFromFile(path)
			if err != nil {
				w := warning.NewExplainQueryFileWarning(warning.CodeExplainQueryFailedToReadFile, path)
				if w != nil {
					warning.PrintWarning(w)
					warnings = append(warnings, w)
				}
				continue
			}
			if len(queries) == 0 {
				w := warning.NewExplainQueryFileWarning(warning.CodeExplainQueryNoQueries, path)
				if w != nil {
					warning.PrintWarning(w)
					warnings = append(warnings, w)
				}
				continue
			}

			outputData[path] = queries
		}
	}

	return outputData, warnings, nil
}

func fetchQueriesFromProdDb(db *sql.DB) ([]string, []warning.Warning, error) {

}

func NewExplainQueryEngine(previewDb *sql.DB, dbConfig *config.Database, prodDb *sql.DB) (*ExplainQueryEngine, error) {
	queries, w1, err := extractAllQueriesToExplain(dbConfig.ExplainQuery.Queries)
	if err != nil {
		return nil, err
	}
	fetchedQueries, w2, err := fetchQueriesFromProdDb(prodDb)

	warnings := make(map[warning.Code]warning.Warning)
	for _, w := range w1 {
		warnings[w.GetWarningCode()] = w
	}
	for _, w := range w2 {
		warnings[w.GetWarningCode()] = w
	}

	return &ExplainQueryEngine{
		db:             previewDb,
		queries:        queries,
		fetchedQueries: fetchedQueries,
		output:         make([]output.OutputDatabaseExplainQuery, 0),
		warnings:       warnings,
	}, nil
}

func (e *ExplainQueryEngine) CollectPreMigrationMetrics() error {
	c := 0
	for _, query := range e.queries {
		c += len(query)
	}
	slog.Debug("Collecting pre-migration explain queries results", "queries", c, "files", len(e.queries))

	for file, queries := range e.queries {
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
	c := 0
	for _, query := range e.queries {
		c += len(query)
	}
	slog.Debug("Collecting post-migration explain queries results", "queries", c, "files", len(e.queries))

	for file, queries := range e.queries {
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

			// Generating execution time delta values
			median, hi, lo := bootstrapMedianRatio(e.output[idx].PreMigrationRun.ExecutionTimes, e.output[idx].PostMigrationRun.ExecutionTimes, 10_000, 0.95)
			e.output[idx].ExecutionDelta = output.OutputDatabaseExplainQueryDelta{
				Lo:     lo,
				Hi:     hi,
				Median: median,
			}

			// Clear execution times pointers to allow GC to cleanup the data
			e.output[idx].PreMigrationRun.ExecutionTimes = nil
			e.output[idx].PostMigrationRun.ExecutionTimes = nil

			// Generate warnings
			if w := generateExecutionTimeWarnings(&e.output[idx]); w != nil {
				e.output[idx].Warnings = append(e.output[idx].Warnings, w)
				warning.PrintWarning(w)
				// Update top level warning count
				if topWarning, ok := e.warnings[w.GetWarningCode()]; ok {
					t, ok := topWarning.(warning.ExplainQueryCount)
					if ok {
						t.Count++
						e.warnings[w.GetWarningCode()] = t
					}
				}
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

func (e *ExplainQueryEngine) GetWarnings() []warning.Warning {
	return slices.Collect(maps.Values(e.warnings))
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

	// Extract all execution & planning times
	executionTimes := make([]float64, len(explainQueryResults))
	for i, explainResult := range explainQueryResults {
		executionTimes[i] = explainResult.ExecutionTime + explainResult.PlanningTime
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
	if q.ExecutionDelta.Lo >= ExecutionTimeThreshold {
		level := warning.WarningLevelLow
		if q.ExecutionDelta.Lo > ExecutionTimeThreshold*3 {
			level = warning.WarningLevelMedium
		}
		return &warning.ExplainQueryTime{
			Hi:    q.ExecutionDelta.Hi,
			Lo:    q.ExecutionDelta.Lo,
			Med:   q.ExecutionDelta.Median,
			Level: level,
		}
	}

	return nil
}
