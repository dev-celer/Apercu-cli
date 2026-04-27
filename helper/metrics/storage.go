package metrics

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

func GetDatabaseStorageInBytes(databaseName string, databaseUrl string) (int64, error) {
	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to database: %v", err)
	}
	defer func() { _ = db.Close() }()

	var size int64
	err = db.QueryRow("SELECT pg_database_size('" + databaseName + "')").Scan(&size)
	if err != nil {
		return 0, fmt.Errorf("failed to query database for size: %v", err)
	}

	return size, nil
}
