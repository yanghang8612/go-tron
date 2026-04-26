package p2p

import (
	"sync"
	"time"
)

// RateLimiter enforces per-message-type token bucket rate limits for a peer.
// Unregistered message types are always permitted.
// Mirrors java-tron's P2pRateLimiter with the same default rates:
//   - MsgSyncBlockChain: 3/s
//   - MsgFetchInvData:   3/s
//   - MsgDisconnect:     1/s
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[byte]*tokenBucket
}

type tokenBucket struct {
	tokens   float64
	rate     float64 // permits per second; also the burst ceiling
	lastTick time.Time
}

// NewRateLimiter returns a rate limiter with java-tron's default rates wired in.
func NewRateLimiter() *RateLimiter {
	rl := &RateLimiter{buckets: make(map[byte]*tokenBucket)}
	rl.register(MsgSyncBlockChain, 3.0)
	rl.register(MsgFetchInvData, 3.0)
	rl.register(MsgDisconnect, 1.0)
	return rl
}

func (rl *RateLimiter) register(msgType byte, rate float64) {
	rl.buckets[msgType] = &tokenBucket{
		tokens:   rate, // start with a full bucket
		rate:     rate,
		lastTick: time.Now(),
	}
}

// Allow returns true if a permit is available for msgType, false if throttled.
func (rl *RateLimiter) Allow(msgType byte) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	b, ok := rl.buckets[msgType]
	if !ok {
		return true
	}
	now := time.Now()
	elapsed := now.Sub(b.lastTick).Seconds()
	b.lastTick = now
	b.tokens += elapsed * b.rate
	if b.tokens > b.rate {
		b.tokens = b.rate
	}
	if b.tokens < 1.0 {
		return false
	}
	b.tokens--
	return true
}
