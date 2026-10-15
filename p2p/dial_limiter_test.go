package p2p

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestDialLimiter_AllowFirstCall(t *testing.T) {
	l := newDialLimiter(50 * time.Millisecond)
	if !l.Allow("1.1.1.1:1") {
		t.Fatal("first Allow on a fresh limiter must return true")
	}
}

func TestDialLimiter_DenyWithinInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newDialLimiter(50 * time.Millisecond)
		if !l.Allow("1.1.1.1:1") {
			t.Fatal("first Allow must return true")
		}
		if l.Allow("1.1.1.1:1") {
			t.Fatal("immediate second Allow within interval must return false")
		}
	})
}

func TestDialLimiter_AllowAfterInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newDialLimiter(20 * time.Millisecond)
		l.Allow("1.1.1.1:1")
		time.Sleep(30 * time.Millisecond)
		if !l.Allow("1.1.1.1:1") {
			t.Fatal("Allow after interval expired must return true")
		}
	})
}

func TestDialLimiter_IndependentAddrs(t *testing.T) {
	l := newDialLimiter(50 * time.Millisecond)
	if !l.Allow("1.1.1.1:1") {
		t.Fatal("Allow(1.1.1.1) must return true")
	}
	if !l.Allow("2.2.2.2:2") {
		t.Fatal("Allow(2.2.2.2) must return true even though 1.1.1.1 was just allowed")
	}
}

func TestDialLimiter_ConcurrentSameAddrOnlyOneAllowed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newDialLimiter(100 * time.Millisecond)

		const goroutines = 50
		var wg sync.WaitGroup
		var allowed atomic.Int32

		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				if l.Allow("1.1.1.1:1") {
					allowed.Add(1)
				}
			}()
		}
		wg.Wait()

		if got := allowed.Load(); got != 1 {
			t.Fatalf("concurrent Allow on same addr: got %d allowed, want 1", got)
		}
	})
}

// The interval is inclusive at its end; a denial does not move that boundary.
func TestDialLimiter_IntervalBoundary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const interval = 20 * time.Millisecond
		l := newDialLimiter(interval)
		if !l.Allow("1.1.1.1:1") {
			t.Fatal("first Allow must return true")
		}
		time.Sleep(interval - time.Nanosecond)
		if l.Allow("1.1.1.1:1") {
			t.Fatal("Allow one nanosecond before interval ends must return false")
		}
		time.Sleep(time.Nanosecond)
		if !l.Allow("1.1.1.1:1") {
			t.Fatal("Allow at exact interval boundary must return true")
		}
		if l.Allow("1.1.1.1:1") {
			t.Fatal("immediate Allow after a new admission must return false")
		}
		time.Sleep(interval)
		if !l.Allow("1.1.1.1:1") {
			t.Fatal("Allow at the next interval boundary must return true")
		}
	})
}
