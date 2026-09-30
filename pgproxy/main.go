package main

import (
	"context"
	"fmt"
	"os"
)

type Config struct {
	DatabaseHost string `json:"database_host"`
	DatabasePort string `json:"database_port"`
	ProxyPort    string `json:"proxy_port"`
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

	proxyPort := os.Getenv("PROXY_PORT")
	if proxyPort == "" {
		proxyPort = "5432"
	}
	config := Config{
		DatabaseHost: DatabaseHost,
		DatabasePort: DatabasePort,
		ProxyPort:    proxyPort,
	}

	if err := StartServer(context.Background(), config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
}
