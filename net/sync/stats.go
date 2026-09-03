package sync

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tronprotocol/go-tron/core"
)

// Snapshot is the rolling-window snapshot consumed by the sync import
// progress formatter. Exported because reportSegment lives in the net
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
	TotalTxs          int           // session-wide transaction count
	ApplyBlocks       int           // blocks represented by ApplyStats
	ApplyTxs          int           // transactions represented by ApplyStats

	// ApplyStats is the per-block execution telemetry reported by
	// BlockChain.applyBlock via the AddApplyStatsHook callback. Summing across
	// every block applied in the window lets the summary line expose both phase
	// bottlenecks and work rates such as energy per second.
	ApplyStats core.ApplyStats

	// TxKinds counts the transactions applied in the window by contract type
	// (corepb.Transaction_Contract_ContractType name) for debug diagnostics
	// field — it tells whether a slow window is contract-heavy
	// (TriggerSmartContract) vs transfer-heavy, etc. nil when none recorded.
	TxKinds map[string]int
}

// SpeedHistoryRetention keeps enough completed import windows to produce the
// previous natural day at midnight, with one hour of scheduling margin.
const SpeedHistoryRetention = 25 * time.Hour

// SpeedSummary describes block import speed over an observation window.
// Calendar summaries use natural wall-clock boundaries and expose Coverage so
// an operator can distinguish a full bucket from a process that started partway
// through it. Minimum and Maximum are the slowest and fastest real-time import
// report intervals contributing to the window; idle gaps count as a zero
// minimum.
type SpeedSummary struct {
	Window   time.Duration
	From     time.Time
	To       time.Time
	Coverage time.Duration
	Samples  int
	Blocks   float64
	Average  float64
	Minimum  float64
	Maximum  float64
}

type speedSample struct {
	at      time.Time
	blocks  int
	elapsed time.Duration
}

// Stats wraps the rolling-window accumulator behind its own mutex. SyncService
// holds a *Stats and forwards onApplyStats / drain-time bookkeeping into the
// AddX methods. Emission of the throttled "Sync import progress" line is
// driven from drainBufferedBlocks (which holds the diagnostic state needed by
// the formatter) — Stats owns the accumulator + snapshot+reset only.
//
// Lock order: ss.mu (outer) → Stats.mu (inner) when both are held. The
// onApplyStats path is the only writer that does NOT also hold ss.mu, which is
// safe because Stats serializes its own state.
type Stats struct {
	mu            sync.Mutex
	cur           Snapshot
	speedSamples  []speedSample
	trackingStart time.Time
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
	if s.trackingStart.IsZero() {
		s.trackingStart = now
	}
}

// ObserveSpeed adds one completed report window and returns recent weighted
// average, minimum, and maximum block rates. Samples older than the history
// retention are discarded, while the returned summary is limited to window;
// the current sample is always retained when its elapsed time is positive.
func (s *Stats) ObserveSpeed(now time.Time, blocks int, elapsed, window time.Duration) SpeedSummary {
	if elapsed <= 0 || window <= 0 {
		return SpeedSummary{Window: window}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.trackingStart.IsZero() {
		s.trackingStart = now.Add(-elapsed)
	}

	s.speedSamples = append(s.speedSamples, speedSample{at: now, blocks: blocks, elapsed: elapsed})
	s.pruneSpeedSamplesLocked(now)

	cutoff := now.Add(-window)
	summary := SpeedSummary{Window: window, From: cutoff, To: now}
	var totalElapsed time.Duration
	for _, sample := range s.speedSamples {
		if sample.at.Before(cutoff) {
			continue
		}
		rate := float64(sample.blocks) * float64(time.Second) / float64(sample.elapsed)
		if summary.Samples == 0 || rate < summary.Minimum {
			summary.Minimum = rate
		}
		if summary.Samples == 0 || rate > summary.Maximum {
			summary.Maximum = rate
		}
		summary.Blocks += float64(sample.blocks)
		totalElapsed += sample.elapsed
		summary.Samples++
	}
	if totalElapsed > 0 {
		summary.Coverage = totalElapsed
		summary.Average = summary.Blocks / totalElapsed.Seconds()
	}
	return summary
}

func (s *Stats) pruneSpeedSamplesLocked(now time.Time) {
	cutoff := now.Add(-SpeedHistoryRetention)
	first := 0
	for first < len(s.speedSamples) && s.speedSamples[first].at.Before(cutoff) {
		first++
	}
	if first > 0 {
		copy(s.speedSamples, s.speedSamples[first:])
		s.speedSamples = s.speedSamples[:len(s.speedSamples)-first]
	}
}

// RecordSpeed retains one completed real-time import window for later natural
// time-bucket summaries.
func (s *Stats) RecordSpeed(now time.Time, blocks int, elapsed time.Duration) {
	_ = s.ObserveSpeed(now, blocks, elapsed, SpeedHistoryRetention)
}

// EndSession retains the final partial real-time window before downloader
// state is reset. Without this flush, a sync session that ends between the
// regular eight-second report ticks would disappear from later 30m/1h/1d
// calendar summaries.
func (s *Stats) EndSession(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.cur.StartTime.IsZero() && now.After(s.cur.StartTime) && s.cur.Blocks > 0 {
		s.speedSamples = append(s.speedSamples, speedSample{
			at:      now,
			blocks:  s.cur.Blocks,
			elapsed: now.Sub(s.cur.StartTime),
		})
		s.pruneSpeedSamplesLocked(now)
	}
	s.cur.StartTime = time.Time{}
	s.cur.Blocks = 0
	s.cur.Txs = 0
	s.cur.ApplyBlocks = 0
	s.cur.ApplyTxs = 0
	s.cur.ExecElapsed = 0
	s.cur.BufferWaitElapsed = 0
	s.cur.ApplyStats = core.ApplyStats{}
	s.cur.TxKinds = nil
}

// CalendarSpeedSummary returns speed statistics for a completed wall-clock
// interval. Missing time after tracking started counts as zero throughput;
// time before tracking started is excluded and exposed through Coverage.
func (s *Stats) CalendarSpeedSummary(from, to time.Time) SpeedSummary {
	if !to.After(from) {
		return SpeedSummary{From: from, To: to}
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.trackingStart.IsZero() {
		return SpeedSummary{Window: to.Sub(from), From: from, To: to}
	}
	samples := append([]speedSample(nil), s.speedSamples...)
	if !s.cur.StartTime.IsZero() && to.After(s.cur.StartTime) {
		end := to
		if end.After(s.cur.StartTime) {
			samples = append(samples, speedSample{at: end, blocks: s.cur.Blocks, elapsed: end.Sub(s.cur.StartTime)})
		}
	}
	coverageStart := from
	if !s.trackingStart.IsZero() && s.trackingStart.After(coverageStart) {
		coverageStart = s.trackingStart
	}
	return summarizeSpeedSamples(samples, from, to, coverageStart)
}

func summarizeSpeedSamples(samples []speedSample, from, to, coverageStart time.Time) SpeedSummary {
	summary := SpeedSummary{Window: to.Sub(from), From: from, To: to}
	if coverageStart.Before(to) {
		summary.Coverage = to.Sub(coverageStart)
	}
	type sampleInterval struct{ start, end time.Time }
	intervals := make([]sampleInterval, 0, len(samples))
	for _, sample := range samples {
		if sample.elapsed <= 0 {
			continue
		}
		start := sample.at.Add(-sample.elapsed)
		end := sample.at
		if start.Before(from) {
			start = from
		}
		if start.Before(coverageStart) {
			start = coverageStart
		}
		if end.After(to) {
			end = to
		}
		if !end.After(start) {
			continue
		}
		overlap := end.Sub(start)
		rate := float64(sample.blocks) * float64(time.Second) / float64(sample.elapsed)
		summary.Blocks += rate * overlap.Seconds()
		intervals = append(intervals, sampleInterval{start: start, end: end})
		if summary.Samples == 0 || rate < summary.Minimum {
			summary.Minimum = rate
		}
		if summary.Samples == 0 || rate > summary.Maximum {
			summary.Maximum = rate
		}
		summary.Samples++
	}
	if summary.Coverage > 0 {
		summary.Average = summary.Blocks / summary.Coverage.Seconds()
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].start.Before(intervals[j].start) })
	var observed time.Duration
	if len(intervals) > 0 {
		start, end := intervals[0].start, intervals[0].end
		for _, interval := range intervals[1:] {
			if interval.start.After(end) {
				observed += end.Sub(start)
				start, end = interval.start, interval.end
			} else if interval.end.After(end) {
				end = interval.end
			}
		}
		observed += end.Sub(start)
	}
	if observed < summary.Coverage {
		summary.Minimum = 0
	}
	return summary
}

// AddApplyBlock folds one block's execution telemetry into the rolling window.
// Synchronous apply calls it on the importer; async commitment may call it on
// the commit worker after the foreground and worker timings have been joined.
func (s *Stats) AddApplyBlock(a core.ApplyStats) {
	s.AddApplyBlockWithTxs(0, a)
}

// AddApplyBlockWithTxs folds one completed block sample and its exact
// transaction coverage into the rolling window. Async commitment can deliver
// this callback a few blocks after foreground bookkeeping; the explicit counts
// keep phase normalization honest at a reporting boundary.
func (s *Stats) AddApplyBlockWithTxs(txs int, a core.ApplyStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur.ApplyBlocks++
	if txs > 0 {
		s.cur.ApplyTxs += txs
	}
	s.cur.ApplyStats.Validate += a.Validate
	s.cur.ApplyStats.Execute += a.Execute
	s.cur.ApplyStats.TransactionExecute += a.TransactionExecute
	s.cur.ApplyStats.AccountStateRoot += a.AccountStateRoot
	s.cur.ApplyStats.AdaptiveEnergy += a.AdaptiveEnergy
	s.cur.ApplyStats.Rewards += a.Rewards
	s.cur.ApplyStats.ShieldedFinalize += a.ShieldedFinalize
	s.cur.ApplyStats.WitnessFlush += a.WitnessFlush
	s.cur.ApplyStats.BlockStatistics += a.BlockStatistics
	s.cur.ApplyStats.EnergyUsageTotal += a.EnergyUsageTotal
	s.cur.ApplyStats.VMTransactions += a.VMTransactions
	s.cur.ApplyStats.NativeTransactions += a.NativeTransactions
	s.cur.ApplyStats.VMExecution += a.VMExecution
	s.cur.ApplyStats.VMRawEnergyUsage += a.VMRawEnergyUsage
	s.cur.ApplyStats.Maintenance += a.Maintenance
	s.cur.ApplyStats.StateCommit += a.StateCommit
	s.cur.ApplyStats.StateCommitDetail.Add(a.StateCommitDetail)
	s.cur.ApplyStats.DPUpdate += a.DPUpdate
	s.cur.ApplyStats.Persist += a.Persist
	s.cur.ApplyStats.PersistDetail.Add(a.PersistDetail)
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
	s.cur.TotalTxs += txs
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
	for kind, count := range kinds {
		if count > 0 {
			s.cur.TxKinds[kind] += count
		}
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
	for name, count := range kinds {
		if count > 0 {
			entries = append(entries, entry{name: name, count: count})
		}
	}
	if len(entries) == 0 {
		return ""
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
	for _, entry := range entries[:limit] {
		parts = append(parts, fmt.Sprintf("%s=%d", entry.name, entry.count))
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
// ApplyStats and StartTime). Session-wide counters (TotalBlocks, TotalTxs,
// TotalStart) are preserved. Caller passes `now` so test fixtures can pin the new
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
	s.cur.ApplyBlocks = 0
	s.cur.ApplyTxs = 0
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
	s.cur.TotalTxs += txs
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

// TotalTransactions returns the session-wide transaction count.
func (s *Stats) TotalTransactions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur.TotalTxs
}

// TotalStart returns the session start time recorded by InitSession.
func (s *Stats) TotalStart() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur.TotalStart
}
