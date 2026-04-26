package database_url

import "testing"

func TestRewriteDatabaseUrlHostAndPort(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		host    string
		port    string
		want    string
		wantErr bool
	}{
		{
			name: "replaces host and port",
			url:  "postgresql://user:pass@oldhost:5432/db",
			host: "newhost",
			port: "6543",
			want: "postgresql://user:pass@newhost:6543/db",
		},
		{
			name: "inserts port when missing",
			url:  "postgresql://user:pass@oldhost/db",
			host: "newhost",
			port: "6543",
			want: "postgresql://user:pass@newhost:6543/db",
		},
		{
			name: "no path, with port",
			url:  "postgresql://user:pass@oldhost:5432",
			host: "newhost",
			port: "6543",
			want: "postgresql://user:pass@newhost:6543",
		},
		{
			name: "no path, no port",
			url:  "postgresql://user:pass@oldhost",
			host: "newhost",
			port: "6543",
			want: "postgresql://user:pass@newhost:6543",
		},
		{
			name: "query string, with port",
			url:  "postgresql://user:pass@oldhost:5432?sslmode=require",
			host: "newhost",
			port: "6543",
			want: "postgresql://user:pass@newhost:6543?sslmode=require",
		},
		{
			name: "query string, no port",
			url:  "postgresql://user:pass@oldhost?sslmode=require",
			host: "newhost",
			port: "6543",
			want: "postgresql://user:pass@newhost:6543?sslmode=require",
		},
		{
			name:    "invalid url",
			url:     "not-a-postgres-url",
			host:    "newhost",
			port:    "6543",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RewriteDatabaseUrlHostAndPort(tt.url, tt.host, tt.port)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
