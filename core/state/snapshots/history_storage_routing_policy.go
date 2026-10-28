package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

const storageRoutingIdleRetry = 30 * time.Second

// Runner.passMu owns this state. IDs/cursors are scheduling hints, not query
// absence authority. In particular, the broad source cache never supplies a
// filter to storageHistoryRoutingReader.
type storageRoutingRefreshState struct {
	dir                string
	broad              [32]byte
	broadLoaded        bool
	broadFrom, broadTo uint64
	broadCovered       map[[32]byte]struct{}
	input              [32]byte
	boundTo, nextTx    uint64
	holes              bool
	failures           uint
	notBefore          time.Time
	last               storageRoutingBuildStats
	metric             *storageRoutingPolicyMetrics
}

type storageRoutingPolicyMetrics struct {
	passes, reads, inputBytes, pages, rows, deferred, noOutput, conflicts *metrics.Counter
	cursor, retry                                                         *metrics.Gauge
}

func (s *storageRoutingRefreshState) initMetrics(namespace string) {
	if s.metric != nil || namespace == "" {
		return
	}
	p := strings.TrimRight(namespace, "/") + "/history/routing/storage/"
	c := func(name string) *metrics.Counter { return metrics.GetOrRegisterCounter(p+name, nil) }
	s.metric = &storageRoutingPolicyMetrics{
		passes: c("passes"), reads: c("dictionary_reads"), inputBytes: c("declared_input_bytes"),
		pages: c("pages_published"), rows: c("proof_rows_published"), deferred: c("backoff_deferred"),
		noOutput: c("no_output"), conflicts: c("publication_conflicts"),
		cursor: metrics.GetOrRegisterGauge(p+"scan_cursor", nil), retry: metrics.GetOrRegisterGauge(p+"retry_remaining_ns", nil),
	}
}

// Scope old identities through the previous horizon. Appended tail sources do
// not defeat a failed-prefix backoff; replacements/retirements and page changes
// within that horizon do. Other lightweight families do not reset this cursor.
func storageRoutingScheduleIdentity(ctx context.Context, m *Manifest, broad [32]byte, bound uint64) ([32]byte, error) {
	canonical, err := systemRoutingInputIdentity(ctx, m, broad, nil, bound)
	if err != nil {
		return [32]byte{}, err
	}
	var pages [][32]byte
	for _, r := range m.Segments {
		if r.Kind == SegmentStorageHistoryRouting && r.FromTxNum <= bound {
			pages = append(pages, systemRoutingRefIdentity(&r))
		}
	}
	sort.Slice(pages, func(i, j int) bool { return bytes.Compare(pages[i][:], pages[j][:]) < 0 })
	h := sha256.New()
	h.Write([]byte("go-tron/storage-routing/schedule/v1\x00"))
	h.Write(canonical[:])
	for _, id := range pages {
		h.Write(id[:])
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result, contextError(ctx)
}

func refreshStorageHistoryRoutingContext(ctx context.Context, dir string, s *storageRoutingRefreshState, now time.Time) (bool, error) {
	return refreshStorageHistoryRoutingWithBudget(ctx, dir, s, now, storageRoutingPageSources, smallSystemRoutingWorkLimit{bytes: 128 << 20, duration: 2 * time.Second})
}

func refreshStorageHistoryRoutingWithBudget(ctx context.Context, dir string, s *storageRoutingRefreshState, now time.Time, maxNew int, work smallSystemRoutingWorkLimit) (bool, error) {
	if err := contextError(ctx); err != nil {
		return false, err
	}
	if s == nil {
		return false, errors.New("snapshots: storage routing requires Runner state")
	}
	if s.dir != dir {
		metric := s.metric
		*s = storageRoutingRefreshState{dir: dir, metric: metric}
	}
	s.last = storageRoutingBuildStats{}
	if !historyReadLeaseGate.TryRLock() {
		return false, nil
	}
	defer historyReadLeaseGate.RUnlock()
	m, err := loadProductionManifestReadOnly(dir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	m = manifestLookupView(m)
	count, indexed, last := 0, false, uint64(0)
	var broad *SegmentRef
	for i := range m.Segments {
		r := &m.Segments[i]
		if r.Kind == SegmentSystemHistoryRouting {
			if broad != nil {
				return false, errors.New("snapshots: multiple system history routing segments")
			}
			broad = r
		}
		if r.Kind == SegmentStorageHistoryRouting {
			indexed = true
		}
		if r.Kind == SegmentHistory && r.normalizedDataset() == SegmentDatasetStateDomainChange {
			count++
			last = max(last, r.ToTxNum)
		}
	}
	if count == 0 || count < systemHistoryRoutingMaxNewSources && !indexed {
		return false, nil
	}
	broadID := systemRoutingRefIdentity(broad)
	input, err := storageRoutingScheduleIdentity(ctx, m, broadID, s.boundTo)
	if err != nil {
		return false, err
	}
	if s.input != ([32]byte{}) && input != s.input {
		s.nextTx, s.failures, s.holes, s.notBefore = 0, 0, false, time.Time{}
	}
	if now.Before(s.notBefore) {
		if s.metric != nil {
			s.metric.deferred.Inc(1)
			s.metric.retry.Update(int64(s.notBefore.Sub(now)))
		}
		return false, nil
	}
	if !s.broadLoaded || s.broad != broadID {
		var covered map[[32]byte]struct{}
		from, to := uint64(0), uint64(0)
		if broad != nil {
			s.last.broadReads++
			data, _, err := readSystemHistoryRoutingBytes(ctx, dir, *broad)
			if err != nil {
				return false, err
			}
			route, err := walkHistoryRoutingFamily(ctx, data, *broad, false, map[string]bool{}, nil)
			if err != nil {
				return false, err
			}
			if route.storagePrefixes {
				covered, from, to = systemRoutingCovered(route.sources), broad.FromTxNum, broad.ToTxNum
			}
		}
		s.broadLoaded, s.broad, s.broadCovered, s.broadFrom, s.broadTo = true, broadID, covered, from, to
	}
	if s.nextTx == 0 {
		s.holes = false // Start a new sweep, retaining the retry exponent.
	}
	s.boundTo = last
	s.input, err = storageRoutingScheduleIdentity(ctx, m, broadID, s.boundTo)
	if err != nil {
		return false, err
	}
	started, next := time.Now(), s.nextTx
	ref, built, err := buildStorageHistoryRoutingWithPolicy(ctx, dir, m, maxNew, s.nextTx, &next, storageRoutingBuildPolicy{
		work: work, broadFrom: s.broadFrom, broadTo: s.broadTo, broadCovered: s.broadCovered, stats: &s.last,
	})
	if s.metric != nil {
		s.metric.passes.Inc(1)
		s.metric.reads.Inc(int64(s.last.sourceReads))
		s.metric.inputBytes.Inc(int64(s.last.inputBytes))
	}
	if err != nil {
		return false, err // Keep the pre-build cursor on hard failure.
	}
	if built {
		latest, publishErr := publishPagedHistoryRoutingContext(ctx, dir, m, ref, SegmentStorageHistoryRouting)
		if errors.Is(publishErr, errMetadataRoutingRefreshConflict) {
			if s.metric != nil {
				s.metric.conflicts.Inc(1)
			}
			return false, nil // Retry this cursor against the new identities.
		}
		if publishErr != nil {
			return false, publishErr
		}
		s.input, err = storageRoutingScheduleIdentity(ctx, manifestLookupView(latest), broadID, s.boundTo)
		if err != nil {
			return true, err
		}
		s.nextTx, s.failures, s.notBefore = next, 0, time.Time{}
		s.holes = s.holes || s.last.unsupported > 0 || s.last.repairDeferred
		if s.metric != nil {
			s.metric.pages.Inc(1)
			s.metric.rows.Inc(int64(ref.Rows))
			s.metric.cursor.Update(int64(next))
			s.metric.retry.Update(0)
		}
		return true, nil
	}
	// Fully attempted unsupported rows may advance the scan cursor without
	// claiming coverage. Otherwise a rejected prefix would block all later rows.
	s.nextTx = next
	s.holes = s.holes || s.last.unsupported > 0 || s.last.repairDeferred
	retry := storageRoutingIdleRetry
	if s.last.sourceReads > 0 || s.holes {
		s.failures = min(s.failures+1, 6)
		retry = min(systemRoutingBudgetRetryMin<<(s.failures-1), systemRoutingBudgetRetryMax)
	} else {
		s.failures = 0
	}
	s.notBefore = now.Add(time.Since(started) + retry)
	if s.metric != nil {
		s.metric.noOutput.Inc(1)
		s.metric.cursor.Update(int64(next))
		s.metric.retry.Update(int64(retry))
	}
	return false, nil
}
