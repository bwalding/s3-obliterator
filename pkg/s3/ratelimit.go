package s3

import (
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

const MAX_LIMITERS = 100

type PrefixRateLimiter struct {
	mtx      sync.Mutex
	rate     int // default rate
	burst    int // default burst
	limiters map[string]*rate.Limiter
}

func NewPrefixRateLimiter(r int, b int) *PrefixRateLimiter {
	return &PrefixRateLimiter{
		limiters: make(map[string]*rate.Limiter),
		rate:     r,
		burst:    b,
	}
}

// Sweep stops runaway creation of new limiters
func (prl *PrefixRateLimiter) Sweep() {
	prl.mtx.Lock()
	defer prl.mtx.Unlock()

	if len(prl.limiters) < MAX_LIMITERS {
		return
	}

	newLimiters := make(map[string]*rate.Limiter)

	// During a sweep, we only keep limiters that are not full
	for k, v := range prl.limiters {
		tokenCount := v.Tokens()
		// logrus.Infof("Sweeping rate limiter: %s -> %f", k, tokenCount)
		if tokenCount >= float64(prl.burst) {
			continue
		}
		newLimiters[k] = v
	}

	logrus.Infof("Sweeping rate limiters: %d -> %d", len(prl.limiters), len(newLimiters))

	prl.limiters = newLimiters
}

func (prl *PrefixRateLimiter) GetRateLimiter(key string) *rate.Limiter {
	prl.Sweep()

	prl.mtx.Lock()
	defer prl.mtx.Unlock()

	prefix := getPrefix(key)
	rl, ok := prl.limiters[prefix]
	if !ok {
		rl = rate.NewLimiter(rate.Limit(prl.rate), prl.burst)
		prl.limiters[prefix] = rl
	}
	return rl
}

func getPrefix(key string) string {
	return getPrefixSingle(key)
}

// https://stackoverflow.com/questions/52443839/s3-what-exactly-is-a-prefix-and-what-ratelimits-apply
// The rate limit key is everything up to the filename
// NOTE: we still got a lot of rate limits with this approach
func getPrefixLong(key string) string {
	pieces := strings.Split(key, "/")
	if len(pieces) == 1 {
		return ""
	}
	return strings.Join(pieces[:len(pieces)-1], "/")
}

func getPrefixSingle(key string) string {
	prefix, _, _ := strings.Cut(key, "/")
	return prefix
}
