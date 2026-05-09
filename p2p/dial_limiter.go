package p2p

import (
	"sync"
	"time"
)

// dialLimiter throttles outbound dial attempts on a per-address basis. It
// addresses the thundering-herd pattern in Server.maintainPeers: when any peer
// disconnects, removePeer fires maintainCh, which retries every seed in
// parallel — without this gate, a flaky seed list can hammer remote nodes
// faster than their per-source rate limits tolerate, cascading into a
// session-wide ban (memory: TRON mainnet seeds + rate limiting).
type dialLimiter struct {
	interval time.Duration
	mu       sync.Mutex
	last     map[string]time.Time
}

func newDialLimiter(interval time.Duration) *dialLimiter {
	return &dialLimiter{interval: interval, last: make(map[string]time.Time)}
}

// Allow records and admits a dial attempt for addr if no attempt has been made
// in the past interval; otherwise returns false. Safe for concurrent use; the
// "record" half is atomic with the "admit" half so concurrent callers race for
// a single allowance per window.
func (l *dialLimiter) Allow(addr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if t, ok := l.last[addr]; ok && now.Sub(t) < l.interval {
		return false
	}
	l.last[addr] = now
	return true
}
