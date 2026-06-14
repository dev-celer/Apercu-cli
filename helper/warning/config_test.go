package warning

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMissingEnvVarsWarnings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []string
		expected []MissingEnvVarsWarning
	}{
		{
			name:     "one variable",
			input:    []string{"VAR_1"},
			expected: []MissingEnvVarsWarning{{variable: "VAR_1"}},
		},
		{
			name:     "multiple variables",
			input:    []string{"VAR_1", "VAR_2"},
			expected: []MissingEnvVarsWarning{{variable: "VAR_1"}, {variable: "VAR_2"}},
		},
		{
			name:     "no variables",
			input:    []string{},
			expected: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			warnings := NewMissingEnvVarsWarnings(test.input...)
			assert.Equal(t, test.expected, warnings)
		})
	}
}
