package removekeys

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeKey(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{
			name:     "normalizes a key with trailing space",
			key:      "key ",
			expected: "key/",
		},
		{
			name:     "normalizes a key with trailing *",
			key:      "key*",
			expected: "key",
		},
		{
			name:     "normalizes a key with trailing /",
			key:      "key/",
			expected: "key/",
		},
		{
			name:     "normalizes a key without trailing special characters",
			key:      "key",
			expected: "key/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			result := NormalizeKey(tt.key)
			r.Equal(tt.expected, result)
		})
	}
}
