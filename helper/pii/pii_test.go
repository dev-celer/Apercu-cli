package pii

import "testing"

func TestIsPII(t *testing.T) {
	cases := []struct {
		table  string
		column string
		want   bool
	}{
		// strong signals — column name alone
		{"orders", "email", true},
		{"orders", "Email_Address", true},
		{"x", "first_name", true},
		{"x", "last_name", true},
		{"x", "phone_number", true},
		{"x", "phoneNumber", true}, // lowercased to "phonenumber", matches phone(_?number)
		{"x", "phone", true},
		{"x", "ssn", true},
		{"x", "dob", true},
		{"x", "date_of_birth", true},
		{"x", "iban", true},
		{"x", "credit_card", true},
		{"x", "cvv", true},
		{"x", "password", true},
		{"x", "api_key", true},
		{"x", "access_token", true},
		{"x", "ip_address", true},
		{"x", "mac_address", true},
		{"x", "passport_number", true},

		// ambiguous: needs table context
		{"users", "name", true},
		{"products", "name", false},
		{"customers", "notes", true},
		{"system_logs", "notes", false},

		// non-PII
		{"orders", "id", false},
		{"orders", "created_at", false},
		{"orders", "total_amount", false},
		{"orders", "status", false},
		{"", "", false},
	}

	for _, c := range cases {
		got := IsPII(c.table, c.column)
		if got != c.want {
			t.Errorf("IsPII(%q, %q) = %v; want %v", c.table, c.column, got, c.want)
		}
	}
}
