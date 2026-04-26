package main

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

const sslRequestCode uint32 = 80877103

type Upstream struct {
	Conn     net.Conn
	Frontend *pgproto3.Frontend
}

func dialUpstream(config *Config, startup *pgproto3.StartupMessage) (*Upstream, error) {
	addr := net.JoinHostPort(config.DatabaseHost, config.DatabasePort)
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("Failed to dial upstream %s: %v", addr, err)
	}

	if err := conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("Failed to set upstream deadline: %v", err)
	}

	active, err := negotiateUpstreamSSL(conn, config.DatabaseHost)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	frontend := pgproto3.NewFrontend(active, active)
	frontend.Send(startup)
	if err := frontend.Flush(); err != nil {
		_ = active.Close()
		return nil, fmt.Errorf("Failed to send StartupMessage upstream: %v", err)
	}

	if err := active.SetDeadline(time.Time{}); err != nil {
		_ = active.Close()
		return nil, fmt.Errorf("Failed to clear upstream deadline: %v", err)
	}

	return &Upstream{Conn: active, Frontend: frontend}, nil
}

func forwardCancelRequest(config *Config, cancel *pgproto3.CancelRequest) error {
	addr := net.JoinHostPort(config.DatabaseHost, config.DatabasePort)
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("Failed to dial upstream for cancel: %v", err)
	}

	if err := conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		_ = conn.Close()
		return fmt.Errorf("Failed to set cancel deadline: %v", err)
	}

	active, err := negotiateUpstreamSSL(conn, config.DatabaseHost)
	if err != nil {
		_ = conn.Close()
		return err
	}
	defer func() { _ = active.Close() }()

	frontend := pgproto3.NewFrontend(active, active)
	frontend.Send(cancel)
	if err := frontend.Flush(); err != nil {
		return fmt.Errorf("Failed to forward CancelRequest: %v", err)
	}

	return nil
}

func negotiateUpstreamSSL(conn net.Conn, serverName string) (net.Conn, error) {
	req := make([]byte, 8)
	binary.BigEndian.PutUint32(req[0:4], 8)
	binary.BigEndian.PutUint32(req[4:8], sslRequestCode)
	if _, err := conn.Write(req); err != nil {
		return nil, fmt.Errorf("Failed to send SSLRequest upstream: %v", err)
	}

	reply := make([]byte, 1)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return nil, fmt.Errorf("Failed to read SSLRequest reply: %v", err)
	}

	switch reply[0] {
	case 'S':
		tlsConn := tls.Client(conn, &tls.Config{ServerName: serverName})
		if err := tlsConn.Handshake(); err != nil {
			return nil, fmt.Errorf("TLS handshake with upstream failed: %v", err)
		}
		return tlsConn, nil
	case 'N':
		return conn, nil
	default:
		return nil, fmt.Errorf("Unexpected SSLRequest reply from upstream: %q", reply[0])
	}
}
