package s3

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetLoggableKey(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		key      string
		expected string
	}{
		{
			name:     "prefix matches start of key",
			prefix:   "/a/",
			key:      "/a/beta/gamma",
			expected: "beta",
		},
		{
			name:     "prefix does not match start of key",
			prefix:   "/a/b",
			key:      "/a/beta/gamma",
			expected: "beta",
		},
		{
			name:     "truncates a long key",
			prefix:   "/a/b",
			key:      "/a/beta-alanin/gamma",
			expected: "beta-ala",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			result := GetLoggableKey(tt.prefix, tt.key, 8)
			r.Equal(tt.expected, result)
		})
	}
}
