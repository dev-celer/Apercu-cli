package main

import (
	"context"
	"fmt"
	"os"
)

type Config struct {
	DatabaseHost string `json:"database_host"`
	DatabasePort string `json:"database_port"`
}

func main() {
	DatabaseHost := os.Getenv("DATABASE_HOST")
	if DatabaseHost == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_HOST environment variable not set")
		return
	}
	DatabasePort := os.Getenv("DATABASE_PORT")
	if DatabasePort == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_PORT environment variable not set")
		return
	}

	config := Config{DatabaseHost: DatabaseHost, DatabasePort: DatabasePort}

	if err := StartServer(context.Background(), config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
}
