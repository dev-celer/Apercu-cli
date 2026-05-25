package engines

import (
	"apercu-cli/config"
	metricshelper "apercu-cli/helper/metrics"
	"apercu-cli/helper/warning"
	"apercu-cli/output"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const (
	prodFetchMinCalls = 10
	prodFetchLimit    = 50
)

// fetchProdQueriesSQL pulls top-N most expensive normalized queries from
// pg_stat_statements on the prod database. The /* apercu */ marker lets the
// regex filter exclude our own probe queries from subsequent runs.
const fetchProdQueriesSQL = `/* apercu */
SELECT s.query
FROM pg_stat_statements s
JOIN pg_database d ON d.oid = s.dbid
WHERE d.datname = current_database()
  AND s.toplevel = true
  AND s.calls >= $1
  AND s.query ~* '^\s*(SELECT|WITH|UPDATE|DELETE|MERGE|INSERT)\M'
  AND s.query !~* '\m(pg_catalog\.|information_schema\.|pg_stat_statements|pg_stat_activity|pg_locks)\M'
  AND s.query !~* '/\*\s*apercu\s*\*/'
GROUP BY s.query
ORDER BY SUM(s.total_exec_time) DESC
LIMIT $2`

type ExplainQueryEngine struct {
	db             *sql.DB
	queries        map[string][]string
	fetchedQueries []string
	prodStats      metricshelper.DatabaseMetrics
	output         []output.OutputDatabaseExplainQuery
	warnings       []warning.Warning
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
	if db == nil {
		return nil, nil, nil
	}

	warnings := make([]warning.Warning, 0)

	var installed bool
	err := db.QueryRow("/* apercu */ SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')").Scan(&installed)
	if err != nil {
		w := warning.NewExplainQueryProdFetchWarning(warning.CodeExplainQueryProdFetchFailed, err.Error())
		warning.PrintWarning(w)
		if w != nil {
			warnings = append(warnings, w)
		}
		return nil, warnings, nil
	}
	if !installed {
		w := warning.NewExplainQueryProdFetchWarning(warning.CodeExplainQueryStatStatementsMissing, "")
		warning.PrintWarning(w)
		if w != nil {
			warnings = append(warnings, w)
		}
		return nil, warnings, nil
	}

	slog.Debug("Fetching queries from prod pg_stat_statements", "min_calls", prodFetchMinCalls, "limit", prodFetchLimit)

	rows, err := db.Query(fetchProdQueriesSQL, prodFetchMinCalls, prodFetchLimit)
	if err != nil {
		w := warning.NewExplainQueryProdFetchWarning(warning.CodeExplainQueryProdFetchFailed, err.Error())
		warning.PrintWarning(w)
		if w != nil {
			warnings = append(warnings, w)
		}
		return nil, warnings, nil
	}
	defer func() { _ = rows.Close() }()

	queries := make([]string, 0)
	for rows.Next() {
		var q string
		if err := rows.Scan(&q); err != nil {
			w := warning.NewExplainQueryProdFetchWarning(warning.CodeExplainQueryProdFetchFailed, err.Error())
			warning.PrintWarning(w)
			if w != nil {
				warnings = append(warnings, w)
			}
			return nil, warnings, nil
		}
		queries = append(queries, q)
	}
	if err := rows.Err(); err != nil {
		w := warning.NewExplainQueryProdFetchWarning(warning.CodeExplainQueryProdFetchFailed, err.Error())
		warning.PrintWarning(w)
		if w != nil {
			warnings = append(warnings, w)
		}
		return nil, warnings, nil
	}

	slog.Debug("Fetched queries from prod pg_stat_statements", "count", len(queries))

	return queries, warnings, nil
}

func NewExplainQueryEngine(previewDb *sql.DB, dbConfig *config.Database, prodDb *sql.DB, prodStats metricshelper.DatabaseMetrics) (*ExplainQueryEngine, error) {
	queries, w1, err := extractAllQueriesToExplain(dbConfig.ExplainQuery.Queries)
	if err != nil {
		return nil, err
	}

	var fetchedQueries []string
	var w2 []warning.Warning
	if !dbConfig.ExplainQuery.DisableAutoQueriesFetch {
		fetchedQueries, w2, err = fetchQueriesFromProdDb(prodDb)
		if err != nil {
			return nil, err
		}
	}

	warnings := slices.Concat(w1, w2)
	return &ExplainQueryEngine{
		db:             previewDb,
		queries:        queries,
		fetchedQueries: fetchedQueries,
		prodStats:      prodStats,
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

	// Collect metrics for user provided queries
	for file, queries := range e.queries {
		for _, query := range queries {
			queryRun, err := e.explainQuery(query)
			var preMigrationRun output.OutputDatabaseMigrationExplainQueryRun
			if err != nil {
				preMigrationRun.Error = err
			} else {
				preMigrationRun.ExplainedQuery = queryRun
			}

			e.output = append(e.output, output.OutputDatabaseExplainQuery{
				File:            file,
				Query:           query,
				PreMigrationRun: &preMigrationRun,
			})
		}
	}

	// Collect metrics for fetched queries
	for _, query := range e.fetchedQueries {
		queryRun, err := e.explainQuery(query)
		var preMigrationRun output.OutputDatabaseMigrationExplainQueryRun
		if err != nil {
			preMigrationRun.Error = err
		} else {
			preMigrationRun.ExplainedQuery = queryRun
		}

		e.output = append(e.output, output.OutputDatabaseExplainQuery{
			File:            "",
			Query:           query,
			PreMigrationRun: &preMigrationRun,
		})
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

	// Collect metrics for user provided queries
	for file, queries := range e.queries {
		for _, query := range queries {
			idx := slices.IndexFunc(e.output, func(s output.OutputDatabaseExplainQuery) bool {
				return s.Query == query && s.File == file
			})
			if idx == -1 {
				continue
			}

			queryRun, err := e.explainQuery(query)
			var postMigrationRun output.OutputDatabaseMigrationExplainQueryRun
			if err != nil {
				postMigrationRun.Error = err
			} else {
				postMigrationRun.ExplainedQuery = queryRun
			}
			e.output[idx].PostMigrationRun = &postMigrationRun

			e.analyzeAndAttach(idx)
		}
	}

	// Collect metrics for fetched queries
	for _, query := range e.fetchedQueries {
		idx := slices.IndexFunc(e.output, func(s output.OutputDatabaseExplainQuery) bool {
			return s.Query == query && s.File == ""
		})
		if idx == -1 {
			continue
		}

		queryRun, err := e.explainQuery(query)
		var postMigrationRun output.OutputDatabaseMigrationExplainQueryRun
		if err != nil {
			postMigrationRun.Error = err
		} else {
			postMigrationRun.ExplainedQuery = queryRun
		}
		e.output[idx].PostMigrationRun = &postMigrationRun

		e.analyzeAndAttach(idx)
	}
	return nil
}

// analyzeAndAttach runs the plan-regression analyzer on the pre/post explain
// results for output entry idx and attaches any findings as warnings.
func (e *ExplainQueryEngine) analyzeAndAttach(idx int) {
	out := &e.output[idx]
	if out.PreMigrationRun == nil || out.PostMigrationRun == nil {
		return
	}
	if out.PreMigrationRun.Error != nil || out.PostMigrationRun.Error != nil {
		return
	}
	if out.PreMigrationRun.ExplainedQuery == nil || out.PostMigrationRun.ExplainedQuery == nil {
		return
	}

	regressions := analyzePlanRegression(out.PreMigrationRun.ExplainedQuery, out.PostMigrationRun.ExplainedQuery, e.prodStats)
	for _, r := range regressions {
		w := r.toWarning()
		if w == nil {
			continue
		}
		out.Warnings = append(out.Warnings, w)
		warning.PrintWarning(w)
		e.warnings = append(e.warnings, w)
	}
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
	return e.warnings
}

type QueryRunResult struct {
	MedianFullResult *metricshelper.ExplainResult
	ExecutionTimes   []float64
}

func (e *ExplainQueryEngine) explainQuery(query string) (*metricshelper.ExplainResult, error) {
	var raw []byte
	stmt := "EXPLAIN (FORMAT JSON, GENERIC_PLAN) " + query
	if err := e.db.QueryRow(stmt).Scan(&raw); err != nil {
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
