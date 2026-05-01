package metrics

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

func GetDatabaseStorageInBytes(db *sql.DB, databaseName string) (int64, error) {
	var size int64
	err := db.QueryRow("SELECT pg_database_size('" + databaseName + "')").Scan(&size)
	if err != nil {
		return 0, fmt.Errorf("failed to query database for size: %v", err)
	}

	return size, nil
}
