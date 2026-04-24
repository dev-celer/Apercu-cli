package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

func StartServer(ctx context.Context, config Config) error {
	listener, err := net.Listen("tcp", ":5432")
	if err != nil {
		return fmt.Errorf("Failed to start listener: %v", err)
	}
	defer listener.Close()

	// Cancel logic
	go func() {
		for {
			time.Sleep(1 * time.Second)
			select {
			case <-ctx.Done():
				listener.Close()
			}
		}
	}()

	// Listen for new connection
	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("Failed to accept connection: %v", err)
		}
		go connectionHandler(ctx, &config, conn)
	}
}

func getStartupMessage(conn net.Conn, backend *pgproto3.Backend) (*pgproto3.StartupMessage, error) {
	for {
		msg, err := backend.ReceiveStartupMessage()
		if err != nil {
			return nil, fmt.Errorf("Failed to receive StartupMessage: %v", err)
		}

		switch msg.(type) {
		case *pgproto3.SSLRequest:
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return nil, fmt.Errorf("Failed to write to client connection: %v", err)
			}
		case *pgproto3.GSSEncRequest:
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return nil, fmt.Errorf("Failed to write to client connection: %v", err)
			}
		case *pgproto3.CancelRequest:
			return nil, nil
		case *pgproto3.StartupMessage:
			return msg.(*pgproto3.StartupMessage), nil
		default:
			return nil, fmt.Errorf("Unexpected message from client: %v", msg)
		}
	}
}

func connectionHandler(ctx context.Context, config *Config, conn net.Conn) {
	defer conn.Close()

	backend := pgproto3.NewBackend(conn, conn)
	startup, err := getStartupMessage(conn, backend)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return
	}
	if startup == nil {
		return
	}

	user := startup.Parameters["user"]
	database := startup.Parameters["database"]
	appName := startup.Parameters["application_name"]
	_, _ = fmt.Fprintf(os.Stderr, "Client startup: user=%q database=%q app=%q\n", user, database, appName)

	upstream, err := dialUpstream(config, startup)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return
	}
	defer upstream.Conn.Close()

	_, _ = fmt.Fprintf(os.Stderr, "Upstream connected to %s:%s\n", config.DatabaseHost, config.DatabasePort)
	_ = upstream
}
