package sql_parsing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetLockTimeoutValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sql       string
		wantBool  bool
		wantValue *int64
	}{
		{"empty", "", false, nil},
		{"unrelated query", "SELECT * FROM users", false, nil},
		{"other set query", "SET my_value TO 'test'", false, nil},

		{"set integer", "SET lock_timeout = 5000", true, new(int64(5000))},
		{"set default", "SET lock_timeout = DEFAULT", true, nil},
		{"reset", "reset lock_timeout", true, nil},
		{"reset all", "reset all", true, nil},
		{"set duration string", "SET lock_timeout = '5s'", true, new(int64(5000))},
		{"set quoted integer", "SET lock_timeout = '5000'", true, new(int64(5000))},
		{"set to", "SET lock_timeout TO 5000", true, new(int64(5000))},
		{"set local", "SET LOCAL lock_timeout = 5000", true, new(int64(5000))},
		{"set session", "SET SESSION lock_timeout = 5000", true, new(int64(5000))},
		{"alter system set", "ALTER SYSTEM SET lock_timeout = 5000", true, new(int64(5000))},
		{"set without space", "SET lock_timeout=5000", true, new(int64(5000))},
		{"set with one space", "SET lock_timeout= 5000", true, new(int64(5000))},
		{"set with one space 2", "SET lock_timeout =5000", true, new(int64(5000))},
		{"set with trailing semicolon", "SET lock_timeout = 5000;", true, new(int64(5000))},
		{"set with 1min value", "SET lock_timeout = '1min'", true, new(int64(60000))},
		{"set with 1d value", "SET lock_timeout = '1d'", true, new(int64(86400000))},

		{"invalid duration string", "SET lock_timeout = 'test'", false, nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotBool, gotValue := GetLockTimeoutValue(tc.sql)
			assert.Equal(t, tc.wantBool, gotBool)
			if tc.wantValue != nil {
				assert.NotNil(t, gotValue)
				if gotValue != nil {
					assert.Equal(t, *tc.wantValue, *gotValue)
				}
			} else {
				assert.Nil(t, gotValue)
			}
		})
	}
}
