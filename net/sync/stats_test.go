package sync

import (
	"sync"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core"
)

func TestStats_DefaultSnapshotIsZero(t *testing.T) {
	s := NewStats()
	snap := s.CurrentSnapshot()
	if snap.Blocks != 0 || snap.Txs != 0 || snap.TotalBlocks != 0 ||
		snap.ExecElapsed != 0 || snap.BufferWaitElapsed != 0 ||
		!snap.StartTime.IsZero() || !snap.TotalStart.IsZero() ||
		snap.ApplyStats.Total() != 0 {
		t.Fatalf("zero Stats has non-zero snapshot: %+v", snap)
	}
}

func TestStats_InitSessionSetsBothStartTimes(t *testing.T) {
	s := NewStats()
	now := time.Unix(1_700_000_000, 0)
	s.InitSession(now)
	snap := s.CurrentSnapshot()
	if !snap.StartTime.Equal(now) {
		t.Fatalf("StartTime = %v, want %v", snap.StartTime, now)
	}
	if !snap.TotalStart.Equal(now) {
		t.Fatalf("TotalStart = %v, want %v", snap.TotalStart, now)
	}
}

func TestStats_AddBlockAccumulates(t *testing.T) {
	s := NewStats()
	s.InitSession(time.Unix(0, 0))

	s.AddBlock(3, 10*time.Millisecond)
	s.AddBlock(5, 20*time.Millisecond)
	s.AddBlock(2, 7*time.Millisecond)

	snap := s.CurrentSnapshot()
	if snap.Blocks != 3 {
		t.Fatalf("Blocks=%d, want 3", snap.Blocks)
	}
	if snap.TotalBlocks != 3 {
		t.Fatalf("TotalBlocks=%d, want 3", snap.TotalBlocks)
	}
	if snap.Txs != 10 {
		t.Fatalf("Txs=%d, want 10", snap.Txs)
	}
	if snap.ExecElapsed != 37*time.Millisecond {
		t.Fatalf("ExecElapsed=%v, want 37ms", snap.ExecElapsed)
	}
}

func TestStats_AddBufferWaitAccumulates(t *testing.T) {
	s := NewStats()
	s.AddBufferWait(100 * time.Millisecond)
	s.AddBufferWait(50 * time.Millisecond)
	if got := s.CurrentSnapshot().BufferWaitElapsed; got != 150*time.Millisecond {
		t.Fatalf("BufferWaitElapsed=%v, want 150ms", got)
	}
	// Non-positive durations are ignored so a clock-skew protection in
	// drainBufferedBlocks doesn't pollute the window.
	s.AddBufferWait(0)
	s.AddBufferWait(-time.Second)
	if got := s.CurrentSnapshot().BufferWaitElapsed; got != 150*time.Millisecond {
		t.Fatalf("BufferWaitElapsed=%v after no-op adds, want 150ms", got)
	}
}

func TestStats_AddApplyBlockSumsPerPhase(t *testing.T) {
	s := NewStats()
	s.AddApplyBlock(core.ApplyStats{
		Validate:    1 * time.Millisecond,
		Execute:     2 * time.Millisecond,
		Maintenance: 3 * time.Millisecond,
		StateCommit: 4 * time.Millisecond,
		DPUpdate:    5 * time.Millisecond,
		Persist:     6 * time.Millisecond,
		Hooks:       7 * time.Millisecond,
	})
	s.AddApplyBlock(core.ApplyStats{
		Validate:    10 * time.Millisecond,
		Execute:     20 * time.Millisecond,
		Maintenance: 30 * time.Millisecond,
		StateCommit: 40 * time.Millisecond,
		DPUpdate:    50 * time.Millisecond,
		Persist:     60 * time.Millisecond,
		Hooks:       70 * time.Millisecond,
	})
	got := s.CurrentSnapshot().ApplyStats
	want := core.ApplyStats{
		Validate:    11 * time.Millisecond,
		Execute:     22 * time.Millisecond,
		Maintenance: 33 * time.Millisecond,
		StateCommit: 44 * time.Millisecond,
		DPUpdate:    55 * time.Millisecond,
		Persist:     66 * time.Millisecond,
		Hooks:       77 * time.Millisecond,
	}
	if got != want {
		t.Fatalf("ApplyStats=%+v, want %+v", got, want)
	}
}

func TestStats_SnapshotAndResetReturnsThenClears(t *testing.T) {
	s := NewStats()
	start := time.Unix(1_700_000_000, 0)
	s.InitSession(start)
	s.AddBlock(3, 10*time.Millisecond)
	s.AddBufferWait(5 * time.Millisecond)
	s.AddApplyBlock(core.ApplyStats{Validate: 1 * time.Millisecond})

	newStart := start.Add(time.Second)
	snap := s.SnapshotAndReset(newStart)

	// Snapshot reflects pre-reset state.
	if snap.Blocks != 1 {
		t.Fatalf("snap.Blocks=%d, want 1", snap.Blocks)
	}
	if snap.Txs != 3 {
		t.Fatalf("snap.Txs=%d, want 3", snap.Txs)
	}
	if snap.ExecElapsed != 10*time.Millisecond {
		t.Fatalf("snap.ExecElapsed=%v, want 10ms", snap.ExecElapsed)
	}
	if snap.BufferWaitElapsed != 5*time.Millisecond {
		t.Fatalf("snap.BufferWaitElapsed=%v, want 5ms", snap.BufferWaitElapsed)
	}
	if snap.ApplyStats.Validate != 1*time.Millisecond {
		t.Fatalf("snap.ApplyStats.Validate=%v, want 1ms", snap.ApplyStats.Validate)
	}
	if !snap.StartTime.Equal(start) {
		t.Fatalf("snap.StartTime=%v, want %v", snap.StartTime, start)
	}
	if snap.TotalBlocks != 1 {
		t.Fatalf("snap.TotalBlocks=%d, want 1", snap.TotalBlocks)
	}

	// Post-reset: window cleared, session-wide counters preserved.
	now := s.CurrentSnapshot()
	if now.Blocks != 0 || now.Txs != 0 || now.ExecElapsed != 0 ||
		now.BufferWaitElapsed != 0 || now.ApplyStats.Total() != 0 {
		t.Fatalf("window not cleared after SnapshotAndReset: %+v", now)
	}
	if !now.StartTime.Equal(newStart) {
		t.Fatalf("StartTime not advanced: got %v, want %v", now.StartTime, newStart)
	}
	if now.TotalBlocks != 1 {
		t.Fatalf("TotalBlocks lost across reset: got %d, want 1", now.TotalBlocks)
	}
	if !now.TotalStart.Equal(start) {
		t.Fatalf("TotalStart lost across reset: got %v, want %v", now.TotalStart, start)
	}
}

func TestStats_WindowElapsedBoundary(t *testing.T) {
	s := NewStats()
	if got := s.WindowElapsed(time.Now()); got != 0 {
		t.Fatalf("uninitialised WindowElapsed=%v, want 0", got)
	}
	t0 := time.Unix(1_700_000_000, 0)
	s.InitSession(t0)
	if got := s.WindowElapsed(t0); got != 0 {
		t.Fatalf("WindowElapsed at start=%v, want 0", got)
	}
	if got := s.WindowElapsed(t0.Add(500 * time.Millisecond)); got != 500*time.Millisecond {
		t.Fatalf("WindowElapsed mid-window=%v, want 500ms", got)
	}
	// After SnapshotAndReset, WindowElapsed restarts at the new start.
	newStart := t0.Add(2 * time.Second)
	s.SnapshotAndReset(newStart)
	if got := s.WindowElapsed(newStart.Add(100 * time.Millisecond)); got != 100*time.Millisecond {
		t.Fatalf("post-reset WindowElapsed=%v, want 100ms", got)
	}
}

func TestStats_TotalAccessors(t *testing.T) {
	s := NewStats()
	if s.TotalBlocks() != 0 {
		t.Fatal("zero stats: TotalBlocks should be 0")
	}
	if !s.TotalStart().IsZero() {
		t.Fatal("zero stats: TotalStart should be zero")
	}
	start := time.Unix(42, 0)
	s.InitSession(start)
	s.AddBlock(0, 0)
	s.AddBlock(0, 0)
	if s.TotalBlocks() != 2 {
		t.Fatalf("TotalBlocks=%d, want 2", s.TotalBlocks())
	}
	if !s.TotalStart().Equal(start) {
		t.Fatalf("TotalStart=%v, want %v", s.TotalStart(), start)
	}
}

// TestStats_Concurrent stresses the mutex with many writers + readers; the
// invariant is that observed snapshots are internally consistent (cur is
// only read while holding the lock, so partial-write tearing is impossible).
// Run with `go test -race`.
func TestStats_Concurrent(t *testing.T) {
	s := NewStats()
	s.InitSession(time.Now())

	const writers = 32
	const readers = 4
	const writesPer = 200

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < writesPer; j++ {
				s.AddBlock(1, time.Microsecond)
				s.AddBufferWait(time.Microsecond)
				s.AddApplyBlock(core.ApplyStats{Validate: time.Microsecond})
			}
		}()
	}
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 200; j++ {
				_ = s.CurrentSnapshot()
				_ = s.TotalBlocks()
				_ = s.WindowElapsed(time.Now())
			}
		}()
	}

	close(start)
	wg.Wait()

	snap := s.CurrentSnapshot()
	wantBlocks := writers * writesPer
	if snap.Blocks != wantBlocks {
		t.Fatalf("Blocks=%d, want %d", snap.Blocks, wantBlocks)
	}
	if snap.TotalBlocks != wantBlocks {
		t.Fatalf("TotalBlocks=%d, want %d", snap.TotalBlocks, wantBlocks)
	}
	if snap.Txs != wantBlocks {
		t.Fatalf("Txs=%d, want %d", snap.Txs, wantBlocks)
	}
	wantElapsed := time.Duration(wantBlocks) * time.Microsecond
	if snap.ExecElapsed != wantElapsed {
		t.Fatalf("ExecElapsed=%v, want %v", snap.ExecElapsed, wantElapsed)
	}
	if snap.BufferWaitElapsed != wantElapsed {
		t.Fatalf("BufferWaitElapsed=%v, want %v", snap.BufferWaitElapsed, wantElapsed)
	}
	if snap.ApplyStats.Validate != wantElapsed {
		t.Fatalf("ApplyStats.Validate=%v, want %v", snap.ApplyStats.Validate, wantElapsed)
	}
}

// TestStats_RecordBlock pins the emit-boundary contract of RecordBlock —
// the central novel API for slice 3 (combines count++ and the
// window-elapsed snapshot+reset under one Stats.mu critical section so the
// producer-path onApplyStats hook can never observe a half-counted window).
func TestStats_RecordBlock(t *testing.T) {
	const interval = 100 * time.Millisecond
	base := time.Now()

	t.Run("within window counts but does not emit", func(t *testing.T) {
		s := NewStats()
		s.InitSession(base)
		snap, emit := s.RecordBlock(3, 5*time.Millisecond, base.Add(time.Millisecond), interval)
		if emit {
			t.Fatalf("expected no emit within window, got %+v", snap)
		}
		cur := s.CurrentSnapshot()
		if cur.Blocks != 1 || cur.Txs != 3 || cur.TotalBlocks != 1 {
			t.Fatalf("counters after 1 block: Blocks=%d Txs=%d TotalBlocks=%d, want 1/3/1",
				cur.Blocks, cur.Txs, cur.TotalBlocks)
		}
	})

	t.Run("past window emits pre-reset snapshot then resets window", func(t *testing.T) {
		s := NewStats()
		s.InitSession(base)
		s.RecordBlock(2, time.Millisecond, base.Add(time.Millisecond), interval)
		s.RecordBlock(4, 2*time.Millisecond, base.Add(2*time.Millisecond), interval)
		emitAt := base.Add(interval + time.Millisecond)
		snap, emit := s.RecordBlock(1, 3*time.Millisecond, emitAt, interval)
		if !emit {
			t.Fatal("expected emit past window")
		}
		// Snapshot carries the full pre-reset window: 3 blocks, 7 txs.
		if snap.Blocks != 3 || snap.Txs != 7 {
			t.Fatalf("emit snapshot: Blocks=%d Txs=%d, want 3/7", snap.Blocks, snap.Txs)
		}
		cur := s.CurrentSnapshot()
		if cur.Blocks != 0 || cur.Txs != 0 {
			t.Fatalf("window not reset after emit: Blocks=%d Txs=%d", cur.Blocks, cur.Txs)
		}
		if cur.TotalBlocks != 3 {
			t.Fatalf("TotalBlocks must persist across emit: got %d, want 3", cur.TotalBlocks)
		}
		if !cur.StartTime.Equal(emitAt) {
			t.Fatalf("new window StartTime: got %v, want %v", cur.StartTime, emitAt)
		}
	})

	t.Run("zero StartTime never emits", func(t *testing.T) {
		s := NewStats()
		// No InitSession → StartTime is zero; even a far-future now must not emit.
		if _, emit := s.RecordBlock(1, time.Millisecond, base.Add(time.Hour), interval); emit {
			t.Fatal("RecordBlock must not emit when StartTime is zero (no session)")
		}
	})
}
