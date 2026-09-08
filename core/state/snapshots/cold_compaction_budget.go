package snapshots

import (
	"context"
	"math"
	"os"
	"time"
)

const (
	busyHistoryCompactionInputBytes       = uint64(512 << 20)
	busyHistoryCompactionLogicalBytes     = uint64(2 << 30)
	busyHistoryCompactionInputRecords     = uint64(8_000_000)
	busyHistoryCompactionSources          = uint64(16)
	busyHistoryCompactionNoCandidateRetry = 30 * time.Second
)

// Protected by Runner.passMu. A merge recovery belongs to optional compaction,
// independently of the deadline for publishing and pruning new cold history.
type historyCompactionBudgetState struct {
	notBefore time.Time
	pending   bool
	pendingAt time.Time
}

// Consume one priority opportunity, never a history admission deadline. Without
// this handoff a three-second history recovery could win the shared gate before
// the pending merge on every wake. The caller skips one build opportunity and
// calls compactHistory under the same pass lock; that attempt either makes
// progress or clears/re-arms its own bounded deferral.
func (r *Runner) shouldPrioritizePendingCompaction(now time.Time) bool {
	if r == nil {
		return false
	}
	s := &r.compactionBudget
	if !r.throughputCatchup() || !r.historySyncBudgetActive() {
		s.pending = false
		return false
	}
	if !s.pending || now.Before(s.pendingAt) || now.Before(s.notBefore) || !r.historyLoadPermitsMerge() {
		return false
	}
	s.pending = false
	return true
}

// This readiness hint uses only the already published manifest. No checksum,
// accessor header or compressed body read is allowed before the heavy lease.
// Waiting for the source target can leave a small record/logical-limited run
// pending briefly, but avoids prioritizing an empty merge for every two leaves.
func (r *Runner) hasPotentialBusyCompaction() (bool, error) {
	manifest, err := LoadProductionManifest(r.cfg.Dir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	cfg, ok := DefaultDomainRegistry().Dataset(r.cfg.HistoryDataset)
	if !ok {
		return false, nil
	}
	maxSources := busyHistoryCompactionSources
	if r.cfg.CompactMaxSteps > 0 {
		maxSources = min(maxSources, r.cfg.CompactMaxSteps)
	}
	if maxSources < 2 {
		return false, nil
	}
	var sources, bytes uint64
	var previous SegmentRef
	for _, candidate := range historyCompactionCandidates(manifest, cfg) {
		if candidate.history.effectiveAggregationSteps() != 1 {
			sources, bytes = 0, 0
			continue
		}
		if sources > 0 && !historySegmentsAreContiguous(previous, candidate.history) {
			sources, bytes = 0, 0
		}
		size := candidate.history.Size
		for _, companion := range candidate.companions {
			if companion.Size > math.MaxUint64-size {
				size = math.MaxUint64
				break
			}
			size += companion.Size
		}
		if size > busyHistoryCompactionInputBytes-bytes {
			if sources >= 2 {
				return true, nil
			}
			sources, bytes = 0, 0
		}
		if size > busyHistoryCompactionInputBytes {
			continue
		}
		sources++
		bytes += size
		previous = candidate.history
		if sources >= 2 && (sources >= maxSources || bytes == busyHistoryCompactionInputBytes) {
			return true, nil
		}
	}
	return false, nil
}

func historyCompactionRecovery(work time.Duration, failed bool) time.Duration {
	recovery := time.Minute
	if work <= 0 {
		recovery = 3 * time.Second
	} else if work <= time.Minute/4 {
		recovery = max(3*time.Second, work*4)
	}
	if failed {
		recovery = time.Minute
	}
	return recovery
}

func (r *Runner) compactHistory(ctx context.Context, catchingUp bool) (total HistoryCompactionResult, err error) {
	if r == nil || !r.cfg.Enabled {
		return total, nil
	}
	if err := contextError(ctx); err != nil {
		return total, err
	}
	r.compactionBudget.pending = false
	busy := r.throughputCatchup() && r.historySyncBudgetActive()
	if busy && !r.historyLoadPermitsMerge() {
		return HistoryCompactionResult{Deferred: true, DeferReason: "import-load"}, nil
	}
	now := time.Now()
	if now.Before(r.compactionBudget.notBefore) {
		return HistoryCompactionResult{Deferred: true, DeferReason: "merge-recovery", RetryAfter: r.compactionBudget.notBefore.Sub(now), RetryDeadline: r.compactionBudget.notBefore}, nil
	}
	// Compaction runs after the builder's lease has been released. It must take
	// its own lease instead of overlapping a freezer job that won that window.
	var release func()
	var admitted bool
	if busy {
		release, admitted = r.cfg.HeavyWorkGate.TryAcquireWithCooldown(3 * time.Second)
	} else {
		release, admitted = r.cfg.HeavyWorkGate.TryAcquire()
	}
	if !admitted {
		if busy {
			potential, err := r.hasPotentialBusyCompaction()
			if err != nil {
				return total, err
			}
			if !potential {
				return HistoryCompactionResult{Deferred: true, DeferReason: "leaf-target"}, nil
			}
		}
		retry := r.cfg.HeavyWorkGate.CooldownRemaining()
		if retry <= 0 {
			retry = 3 * time.Second
		}
		deadline := time.Now().Add(retry)
		if busy {
			r.compactionBudget.pending = true
			r.compactionBudget.pendingAt = deadline
		}
		return HistoryCompactionResult{Deferred: true, DeferReason: "heavy-work-gate", RetryAfter: retry, RetryDeadline: deadline}, nil
	}
	defer release()
	started := time.Now()
	defer func() {
		if busy && (total.Merged || err != nil || total.Deferred) {
			total.Recovery = historyCompactionRecovery(time.Since(started), err != nil)
			if err == nil && !total.Merged {
				// The cheap manifest hint cannot see oversized logical values or
				// record counts. Do not prioritize the same empty selection after
				// every newly published leaf when the detailed budget rejects it.
				total.Recovery = busyHistoryCompactionNoCandidateRetry
			}
			r.compactionBudget.notBefore = time.Now().Add(total.Recovery)
			total.RetryAfter = total.Recovery
			total.RetryDeadline = r.compactionBudget.notBefore
		}
	}()
	cfg := CompactionConfig{MaxSteps: r.cfg.CompactMaxSteps, DeleteObsolete: !r.cfg.RetainObsoleteSegments}
	drain := !catchingUp
	if busy {
		cfg.BusyLeafOnly = true
		cfg.MaxInputBytes = busyHistoryCompactionInputBytes
		cfg.MaxInputLogicalBytes = busyHistoryCompactionLogicalBytes
		cfg.MaxInputRecords = busyHistoryCompactionInputRecords
		cfg.MaxSources = busyHistoryCompactionSources
		drain = false
	} else if catchingUp {
		cfg.MinSteps = r.cfg.CompactMaxSteps
	}
	for {
		if err := contextError(ctx); err != nil {
			return total, err
		}
		result, err := CompactHistoryDomainContext(ctx, r.cfg.Dir, r.cfg.HistoryDataset, cfg)
		if err != nil {
			return total, err
		}
		if !result.Merged {
			if !total.Merged {
				total = result
			}
			return total, nil
		}
		mergeHistoryCompactionResult(&total, result)
		if !drain || r.cfg.MaxCompactionPasses > 0 && uint64(total.MergePasses) >= r.cfg.MaxCompactionPasses {
			return total, nil
		}
	}
}
