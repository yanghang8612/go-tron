package sync

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tronprotocol/go-tron/core"
)

// Snapshot is the rolling-window snapshot consumed by the "Imported chain
// segment" formatter. Exported because reportSegment lives in the net
// package (it reads downloader state alongside the Stats data) and needs
// access to every field. Field names mirror the pre-refactor unexported
// shape one-for-one.
type Snapshot struct {
	StartTime         time.Time     // window start
	Blocks            int           // blocks applied in window
	Txs               int           // tx count applied in window
	ExecElapsed       time.Duration // accumulated InsertBlock wall time
	BufferWaitElapsed time.Duration // accumulated time waiting for the next contiguous buffered block
	TotalStart        time.Time     // session start (for "Sync complete" line)
	TotalBlocks       int           // session-wide block count

	// ApplyStats is the per-phase wall-clock breakdown reported by
	// BlockChain.applyBlock via the AddApplyStatsHook callback. Summing across
	// every block applied in the window lets the summary line tell us *which*
	// phase is the bottleneck.
	ApplyStats core.ApplyStats

	// TxKinds counts the transactions applied in the window by contract type
	// (corepb.Transaction_Contract_ContractType name) for the "txTop" summary
	// field — it tells whether a slow window is contract-heavy
	// (TriggerSmartContract) vs transfer-heavy, etc. nil when none recorded.
	TxKinds map[string]int
}

// Stats wraps the rolling-window accumulator behind its own mutex. SyncService
// holds a *Stats and forwards onApplyStats / drain-time bookkeeping into the
// AddX methods. Emission of the throttled "Imported chain segment" line is
// driven from drainBufferedBlocks (which holds the diagnostic state needed by
// the formatter) — Stats owns the accumulator + snapshot+reset only.
//
// Lock order: ss.mu (outer) → Stats.mu (inner) when both are held. The
// onApplyStats path is the only writer that does NOT also hold ss.mu, which is
// safe because Stats serializes its own state.
type Stats struct {
	mu  sync.Mutex
	cur Snapshot
}

// NewStats returns a fresh zero-valued accumulator. Both startTime and
// totalStart are unset; the caller invokes InitSession at sync-start.
func NewStats() *Stats {
	return &Stats{}
}

// InitSession resets the accumulator at the start of a sync session. Mirrors
// the literal `stats = syncStats{startTime: now, totalStart: now}` line that
// initSessionLocked used to run on the SyncService.
func (s *Stats) InitSession(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur = Snapshot{StartTime: now, TotalStart: now}
}

// AddApplyBlock folds one block's per-phase wall-clock breakdown into the
// rolling window. Fires synchronously from applyBlock on the importing
// goroutine — during sync that is drainBufferedBlocks; during normal
// operation it is the broadcast/producer path.
func (s *Stats) AddApplyBlock(a core.ApplyStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur.ApplyStats.Validate += a.Validate
	s.cur.ApplyStats.Execute += a.Execute
	s.cur.ApplyStats.Maintenance += a.Maintenance
	s.cur.ApplyStats.StateCommit += a.StateCommit
	s.cur.ApplyStats.StateCommitDetail.Add(a.StateCommitDetail)
	s.cur.ApplyStats.DPUpdate += a.DPUpdate
	s.cur.ApplyStats.Persist += a.Persist
	s.cur.ApplyStats.Hooks += a.Hooks
}

// AddBlock records one successfully-applied block: bumps the rolling window's
// block/tx counts, the session-wide total, and the cumulative InsertBlock
// wall-clock.
func (s *Stats) AddBlock(txs int, exec time.Duration) {
	s.AddBlocks(1, txs, exec)
}

// AddBlocks records a successfully-applied block range as one window update.
func (s *Stats) AddBlocks(blocks, txs int, exec time.Duration) {
	if blocks <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur.Blocks += blocks
	s.cur.TotalBlocks += blocks
	s.cur.Txs += txs
	s.cur.ExecElapsed += exec
}

// AddTxKinds folds one batch's breakdown of applied transactions by contract
// type into the rolling window. Nil/empty is a no-op. Counts accumulate across
// the window and reset with it (see SnapshotAndReset).
func (s *Stats) AddTxKinds(kinds map[string]int) {
	if len(kinds) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur.TxKinds == nil {
		s.cur.TxKinds = make(map[string]int, len(kinds))
	}
	for k, n := range kinds {
		s.cur.TxKinds[k] += n
	}
}

// TopTxKindsString renders the most frequent transaction contract types in a
// window as a compact "TriggerSmartContract=900,TransferContract=400" string,
// highest count first (ties broken by name asc). limit<=0 (or > distinct kinds)
// emits all; empty input yields "".
func TopTxKindsString(kinds map[string]int, limit int) string {
	if len(kinds) == 0 {
		return ""
	}
	type entry struct {
		name  string
		count int
	}
	entries := make([]entry, 0, len(kinds))
	for k, n := range kinds {
		if n > 0 {
			entries = append(entries, entry{k, n})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count != entries[j].count {
			return entries[i].count > entries[j].count
		}
		return entries[i].name < entries[j].name
	})
	if limit <= 0 || limit > len(entries) {
		limit = len(entries)
	}
	parts := make([]string, 0, limit)
	for _, e := range entries[:limit] {
		parts = append(parts, fmt.Sprintf("%s=%d", e.name, e.count))
	}
	return strings.Join(parts, ",")
}

// AddBufferWait accumulates time spent waiting for the next contiguous
// buffered block during drainBufferedBlocks. Sums into the window's
// BufferWaitElapsed counter.
func (s *Stats) AddBufferWait(d time.Duration) {
	if d <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur.BufferWaitElapsed += d
}

// WindowElapsed reports time since the current window's StartTime. Used by
// drainBufferedBlocks to decide whether the StatsReportInterval has elapsed.
// Returns 0 if StartTime is the zero value.
func (s *Stats) WindowElapsed(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur.StartTime.IsZero() {
		return 0
	}
	return now.Sub(s.cur.StartTime)
}

// SnapshotAndReset returns a copy of the current window's accumulator and
// resets the per-window fields (Blocks/Txs/ExecElapsed/BufferWaitElapsed/
// ApplyStats and StartTime). Session-wide counters (TotalBlocks, TotalStart)
// are preserved. Caller passes `now` so test fixtures can pin the new
// StartTime instead of taking a fresh wall-clock read inside the lock.
func (s *Stats) SnapshotAndReset(now time.Time) Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotAndResetLocked(now)
}

func (s *Stats) snapshotAndResetLocked(now time.Time) Snapshot {
	snap := s.cur
	s.cur.StartTime = now
	s.cur.Blocks = 0
	s.cur.Txs = 0
	s.cur.ExecElapsed = 0
	s.cur.BufferWaitElapsed = 0
	s.cur.ApplyStats = core.ApplyStats{}
	s.cur.TxKinds = nil
	return snap
}

// RecordBlock atomically appends one block's drain-time bookkeeping (txs and
// exec wall-time) into the current window, then — if the window has elapsed
// past `interval` — returns a snapshot of the pre-reset state along with
// `emit=true`. Mirrors the pre-refactor sequence under ss.mu so the producer
// path's onApplyStats hook can never observe a half-counted window.
//
// Caller passes `now` once for both the elapsed-check and the new
// StartTime so a sub-microsecond clock advance can never make the new
// window's startTime earlier than the old window's WindowElapsed reading.
func (s *Stats) RecordBlock(txs int, exec time.Duration, now time.Time, interval time.Duration) (Snapshot, bool) {
	return s.RecordBlocks(1, txs, exec, now, interval)
}

// RecordBlocks atomically appends one contiguous imported range's drain-time
// bookkeeping into the current window, then optionally snapshots and resets.
func (s *Stats) RecordBlocks(blocks, txs int, exec time.Duration, now time.Time, interval time.Duration) (Snapshot, bool) {
	if blocks <= 0 {
		return Snapshot{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur.Blocks += blocks
	s.cur.TotalBlocks += blocks
	s.cur.Txs += txs
	s.cur.ExecElapsed += exec
	if s.cur.StartTime.IsZero() || now.Sub(s.cur.StartTime) < interval {
		return Snapshot{}, false
	}
	return s.snapshotAndResetLocked(now), true
}

// CurrentSnapshot returns a copy of the current accumulator without resetting
// it. Intended for tests and for the finishSync "Sync complete" path which
// needs TotalBlocks + TotalStart while leaving the window untouched.
func (s *Stats) CurrentSnapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur
}

// TotalBlocks returns the session-wide block count.
func (s *Stats) TotalBlocks() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur.TotalBlocks
}

// TotalStart returns the session start time recorded by InitSession.
func (s *Stats) TotalStart() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur.TotalStart
}
