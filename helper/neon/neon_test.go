package neon

import (
	"apercu-cli/helper"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractConnectionFieldsFromUrl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		url      string
		expected helper.ConnectionFields
	}{
		{
			name: "standard URL",
			url:  "postgresql://user:password@host.example.com:5432/mydb?sslmode=require",
			expected: helper.ConnectionFields{
				Host:     "host.example.com",
				Port:     5432,
				User:     "user",
				Password: "password",
				Database: "mydb",
				Url:      "postgresql://user:password@host.example.com:5432/mydb?sslmode=require",
			},
		},
		{
			name: "non-standard port",
			url:  "postgresql://admin:secret@db.neon.tech:6543/testdb?sslmode=require",
			expected: helper.ConnectionFields{
				Host:     "db.neon.tech",
				Port:     6543,
				User:     "admin",
				Password: "secret",
				Database: "testdb",
				Url:      "postgresql://admin:secret@db.neon.tech:6543/testdb?sslmode=require",
			},
		},
		{
			name: "special characters in password",
			url:  "postgresql://user:p%40ss%23word@host.com:5432/db?sslmode=require",
			expected: helper.ConnectionFields{
				Host:     "host.com",
				Port:     5432,
				User:     "user",
				Password: "p%40ss%23word",
				Database: "db",
				Url:      "postgresql://user:p%40ss%23word@host.com:5432/db?sslmode=require",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := ExtractConnectionFieldsFromUrl(tt.url)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
