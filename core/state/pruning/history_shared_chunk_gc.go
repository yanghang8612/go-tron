package pruning

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

const (
	historyChunkGCMetaRows = 64
	historyChunkGCBuckets  = 4
)

// HistorySharedChunkGCStats records this pass's committed metadata retirements,
// not bytes reclaimed by compaction. Deferred buckets remain in the circular
// metadata sweep. Errors retain the previous cursor and do not undo or block
// the normal already-authorized hot-history prune.
type HistorySharedChunkGCStats struct {
	Scanned, Candidates, Retired, NonEmpty, NotCovered, Busy, Errors uint64
	LastError                                                        string
}

type historyChunkGCState struct {
	mu        sync.Mutex
	statsMu   sync.Mutex
	cursor    []byte
	lastError time.Time
	total     HistorySharedChunkGCStats
}

func (s *historyChunkGCState) stats() HistorySharedChunkGCStats {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	return s.total
}

func newHistoryChunkGCMetrics(namespace string) map[string]*metrics.Gauge {
	prefix := namespace + "shared/gc/"
	if namespace == defaultPrunerMetricsNamespace {
		prefix = "state/history/shared/gc/"
	}
	out := make(map[string]*metrics.Gauge)
	for _, name := range []string{"enabled", "scanned_meta", "candidates", "retired", "nonempty", "deferred_coverage", "deferred_busy", "errors"} {
		out[name] = metrics.GetOrRegisterGauge(prefix+name, nil)
	}
	return out
}

func updateHistoryChunkGCMetrics(gauges map[string]*metrics.Gauge, enabled bool, stats HistorySharedChunkGCStats) {
	values := map[string]uint64{"scanned_meta": stats.Scanned, "candidates": stats.Candidates, "retired": stats.Retired, "nonempty": stats.NonEmpty, "deferred_coverage": stats.NotCovered, "deferred_busy": stats.Busy, "errors": stats.Errors}
	if enabled {
		values["enabled"] = 1
	} else {
		values["enabled"] = 0
	}
	for name, value := range values {
		if gauge := gauges[name]; gauge != nil {
			gauge.Update(prunerUintGauge(value))
		}
	}
}

func (w Worker) pruneHistorySharedChunks(ctx context.Context, coverage *snapshotStateDomainCoverageGate, head uint64) (stats HistorySharedChunkGCStats) {
	state := w.historyChunkGC
	if state == nil {
		state = &historyChunkGCState{}
	}
	// Only this bounded optional phase is serialized. There is no maintenance
	// lease and the chain guard is opportunistic, never queued while holding mu.
	state.mu.Lock()
	defer state.mu.Unlock()
	defer func() {
		state.statsMu.Lock()
		defer state.statsMu.Unlock()
		state.total.Scanned += stats.Scanned
		state.total.Candidates += stats.Candidates
		state.total.Retired += stats.Retired
		state.total.NonEmpty += stats.NonEmpty
		state.total.NotCovered += stats.NotCovered
		state.total.Busy += stats.Busy
		state.total.Errors += stats.Errors
		if stats.LastError != "" {
			state.total.LastError = stats.LastError
		}
	}()
	reportError := func(err error) {
		stats.Errors++
		stats.LastError = err.Error()
		if ctx.Err() == nil && time.Since(state.lastError) >= time.Minute {
			log.Warn("History shared chunk GC deferred after error", "err", err)
			state.lastError = time.Now()
		}
	}
	if coverage == nil || len(coverage.segments) == 0 {
		return stats
	}
	page, err := rawdb.ScanStateHistoryChunkGCBuckets(ctx, w.DB, state.cursor, head, historyChunkGCMetaRows, historyChunkGCBuckets)
	if err != nil {
		reportError(err)
		return stats
	}
	stats.Scanned, stats.Candidates = page.Scanned, uint64(len(page.Buckets))
	for _, bucket := range page.Buckets {
		first, last, err := rawdb.StateHistoryChunkBucketBounds(bucket)
		if err != nil {
			reportError(err)
			return stats
		}
		covered, err := w.historyChunkBucketCovered(ctx, coverage, head, first, last)
		if err != nil {
			reportError(err)
			return stats
		}
		if !covered {
			stats.NotCovered++
			continue
		}
		var result rawdb.StateHistoryChunkRetirement
		admitted, err := w.HistoryRangeGuard(ctx, last, func() error {
			var retireErr error
			// w.DB is the fresh committed store. The hot prune's bounded batch
			// was flushed before this phase, and all checks/writes below remain
			// under the canonical proof, writer locks and settled-prefix fence.
			result, retireErr = rawdb.RetireStateHistoryChunkBucket(ctx, w.DB, bucket)
			return retireErr
		})
		if err != nil {
			reportError(err)
			return stats
		}
		if !admitted {
			stats.Busy++
		} else if result.Retired {
			stats.Retired++
		} else if result.NonEmpty {
			stats.NonEmpty++
		}
	}
	// A completed page resets to the beginning. Busy, nonempty and missing-
	// coverage candidates are revisited instead of becoming a leaked prefix.
	state.cursor = append(state.cursor[:0], page.Next...)
	return stats
}

func (w Worker) historyChunkBucketCovered(ctx context.Context, coverage *snapshotStateDomainCoverageGate, head, first, last uint64) (bool, error) {
	if w.Policy.RetainHotHistory(last, head) {
		return false, nil
	}
	// ModeSnap deliberately retains StateTxRange rows after hot pack deletion.
	// Requiring every block provides a bounded, repeatable proof independent of
	// MaxDeletedHistoryBlock. Missing/legacy metadata conservatively retains the
	// bucket; neither current head nor an empty history iterator proves coverage.
	count := uint64(0)
	complete := true
	var previousEnd uint64
	err := rawdb.IterateStateTxRangesByBlockRangeBorrowed(w.DB, first, last, func(row *rawdb.StateTxRange) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if count >= rawdb.StateHistoryChunkBucketBlocks || row.BlockNum != first+count || row.BlockHash == (common.Hash{}) || row.EndTxNum < row.BeginTxNum || (count > 0 && row.BeginTxNum <= previousEnd) {
			complete = false
			return false, nil
		}
		covered, err := coverage.covers(row.BeginTxNum, row.EndTxNum)
		if err != nil {
			return false, err
		}
		if !covered {
			complete = false
			return false, nil
		}
		previousEnd = row.EndTxNum
		count++
		return true, nil
	})
	if err != nil {
		return false, err
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if last-first+1 != rawdb.StateHistoryChunkBucketBlocks {
		return false, errors.New("pruning: invalid shared history bucket span")
	}
	return complete && count == rawdb.StateHistoryChunkBucketBlocks, nil
}
