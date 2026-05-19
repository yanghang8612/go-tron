package sync

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPauseGate_DefaultNotPaused(t *testing.T) {
	g := NewPauseGate()
	if g.Paused() {
		t.Fatal("freshly-constructed gate should not be paused")
	}
	paused, atNum, at, err := g.Status()
	if paused || atNum != 0 || !at.IsZero() || err != nil {
		t.Fatalf("default Status: paused=%v atNum=%d at=%v err=%v; want zero values", paused, atNum, at, err)
	}
}

func TestPauseGate_EnterMarksPaused(t *testing.T) {
	g := NewPauseGate()
	cause := errors.New("insert failed")
	before := time.Now()
	g.Enter(42, cause)
	after := time.Now()

	if !g.Paused() {
		t.Fatal("Paused() should be true after Enter")
	}
	paused, atNum, at, err := g.Status()
	if !paused {
		t.Fatal("Status.paused should be true after Enter")
	}
	if atNum != 42 {
		t.Fatalf("Status.atNum: got %d, want 42", atNum)
	}
	if err != cause {
		t.Fatalf("Status.err: got %v, want %v", err, cause)
	}
	if at.Before(before) || at.After(after) {
		t.Fatalf("Status.at=%v not in [%v, %v]", at, before, after)
	}
}

func TestPauseGate_FirstReasonWins(t *testing.T) {
	g := NewPauseGate()
	first := errors.New("first cause")
	second := errors.New("second cause")

	g.Enter(100, first)
	firstStampPaused, firstStampNum, firstStampTime, firstStampErr := g.Status()
	if !firstStampPaused || firstStampNum != 100 || firstStampErr != first {
		t.Fatalf("first Enter not captured: paused=%v num=%d err=%v", firstStampPaused, firstStampNum, firstStampErr)
	}

	// Ensure the time advances so we can detect overwrites.
	time.Sleep(2 * time.Millisecond)
	g.Enter(200, second)

	paused, atNum, at, err := g.Status()
	if !paused {
		t.Fatal("still paused expected")
	}
	if atNum != 100 {
		t.Fatalf("atNum: got %d, want 100 (first reason wins)", atNum)
	}
	if err != first {
		t.Fatalf("err: got %v, want %v (first reason wins)", err, first)
	}
	if !at.Equal(firstStampTime) {
		t.Fatalf("at: got %v, want %v (timestamp must not be overwritten)", at, firstStampTime)
	}
}

func TestPauseGate_StatusRoundTrip(t *testing.T) {
	g := NewPauseGate()

	// Pre-Enter: zero values.
	if paused, atNum, at, err := g.Status(); paused || atNum != 0 || !at.IsZero() || err != nil {
		t.Fatalf("pre-Enter Status not zero: paused=%v atNum=%d at=%v err=%v", paused, atNum, at, err)
	}

	cause := errors.New("boom")
	g.Enter(7, cause)

	paused, atNum, at, err := g.Status()
	if !paused || atNum != 7 || at.IsZero() || err != cause {
		t.Fatalf("post-Enter Status: paused=%v atNum=%d at=%v err=%v", paused, atNum, at, err)
	}
	// Calling Status twice must yield identical values.
	paused2, atNum2, at2, err2 := g.Status()
	if paused2 != paused || atNum2 != atNum || !at2.Equal(at) || err2 != err {
		t.Fatalf("Status not stable across calls: 1=(%v,%d,%v,%v) 2=(%v,%d,%v,%v)",
			paused, atNum, at, err, paused2, atNum2, at2, err2)
	}
}

// TestPauseGate_Concurrent exercises the gate from many goroutines at
// once. With -race this catches missing locking; functionally it asserts
// that exactly one Enter wins and the others observe a consistent
// snapshot (Paused implies atNum/err are set and the at-timestamp is
// non-zero).
func TestPauseGate_Concurrent(t *testing.T) {
	g := NewPauseGate()

	const writers = 32
	const readers = 32

	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			g.Enter(uint64(i+1), errors.New("concurrent"))
		}()
	}
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 100; j++ {
				if g.Paused() {
					paused, atNum, at, err := g.Status()
					if !paused {
						t.Errorf("Paused→true then Status.paused→false")
						return
					}
					// Once paused, all fields must be set together.
					if atNum == 0 || at.IsZero() || err == nil {
						t.Errorf("partial state visible: atNum=%d at=%v err=%v", atNum, at, err)
						return
					}
				}
			}
		}()
	}

	close(start)
	wg.Wait()

	// Final state: paused, with one of the writers' values captured.
	paused, atNum, at, err := g.Status()
	if !paused {
		t.Fatal("after all writers ran, gate should be paused")
	}
	if atNum < 1 || atNum > writers {
		t.Fatalf("atNum=%d outside writer range [1,%d]", atNum, writers)
	}
	if at.IsZero() || err == nil {
		t.Fatalf("incomplete state: at=%v err=%v", at, err)
	}
}
