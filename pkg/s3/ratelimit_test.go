package s3

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsumes(t *testing.T) {
	r := require.New(t)

	rl := NewPrefixRateLimiter(10, 10)
	r.NotNil(rl)

	b := rl.GetRateLimiter("test")
	r.NotNil(b)
}

func TestGetPrefixLong(t *testing.T) {
	r := require.New(t)

	prefix := getPrefixLong("test")
	r.Equal("", prefix)

	prefix = getPrefixLong("test/key")
	r.Equal("test", prefix)

	prefix = getPrefixLong("test/key/key2")
	r.Equal("test/key", prefix)
}

func TestRatelimit(t *testing.T) {
	r := require.New(t)

	rl := NewPrefixRateLimiter(10, 10)
	r.NotNil(rl)

	b := rl.GetRateLimiter("test")
	r.NotNil(b)

}
