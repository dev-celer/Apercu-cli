package metrics

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq"
)

func GetDatabaseStorageInBytes(databaseName string, databaseUrl string) (int64, error) {
	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Failed to connect to database: %v", err))
	}
	defer func() { _ = db.Close() }()

	rows, err := db.Query("SELECT pg_database_size('" + databaseName + "')")
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Failed to query database for size: %v", err))
	}
	defer func() { _ = rows.Close() }()

	rows.Next()
	var size int64
	if err := rows.Scan(&size); err != nil {
		return 0, errors.New(fmt.Sprintf("Failed to scan sql response: %v", err))
	}
	return size, nil
}
