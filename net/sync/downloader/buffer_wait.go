package downloader

import (
	"time"
)

// BufferWaitTracker measures how long the importer waits for the next
// contiguous block number to arrive in the raw sync buffer.
type BufferWaitTracker struct {
	start time.Time
	num   uint64
}

// Begin starts or retargets the wait window for next.
func (t *BufferWaitTracker) Begin(next uint64, now time.Time) {
	if t == nil {
		return
	}
	if t.start.IsZero() || t.num != next {
		t.start = now
		t.num = next
	}
}

// End closes the wait window when next matches the active target.
func (t *BufferWaitTracker) End(next uint64, now time.Time) time.Duration {
	if t == nil {
		return 0
	}
	if t.start.IsZero() || t.num != next {
		t.Reset()
		return 0
	}
	elapsed := now.Sub(t.start)
	t.Reset()
	if elapsed < 0 {
		return 0
	}
	return elapsed
}

// Reset clears the active wait window.
func (t *BufferWaitTracker) Reset() {
	if t == nil {
		return
	}
	t.start = time.Time{}
	t.num = 0
}
