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

const accountRoutingIdleRetry = 30 * time.Second

// Runner.passMu owns this state. Cursors and identity hashes decide when to
// attempt work; query readers never consume them as absence proofs.
type accountRoutingRefreshState struct {
	dir             string
	input           [32]byte
	boundTo, nextTx uint64
	holes           bool
	failures        uint
	notBefore       time.Time
	last            accountDeltaBuildStats
	metric          *accountRoutingPolicyMetrics
}

type accountRoutingPolicyMetrics struct {
	passes, reads, headers, metadataReads, inputBytes, metadataBytes         *metrics.Counter
	pages, rows, deferred, noOutput, conflicts, unsupported, metadataUnknown *metrics.Counter
	cursor, retry                                                            *metrics.Gauge
}

func (s *accountRoutingRefreshState) initMetrics(namespace string) {
	if s.metric != nil || namespace == "" {
		return
	}
	p := strings.TrimRight(namespace, "/") + "/history/routing/account/"
	c := func(n string) *metrics.Counter { return metrics.GetOrRegisterCounter(p+n, nil) }
	s.metric = &accountRoutingPolicyMetrics{
		passes: c("passes"), reads: c("dictionary_reads"), headers: c("dictionary_headers"),
		metadataReads: c("metadata_reads"), inputBytes: c("declared_input_bytes"), metadataBytes: c("metadata_read_bytes"),
		pages: c("pages_published"), rows: c("rows_published"), deferred: c("backoff_deferred"),
		noOutput: c("no_output"), conflicts: c("publication_conflicts"), unsupported: c("unsupported_sources"),
		metadataUnknown: c("metadata_unknown"),
		cursor:          metrics.GetOrRegisterGauge(p+"scan_cursor", nil), retry: metrics.GetOrRegisterGauge(p+"retry_remaining_ns", nil),
	}
}

// Replacements and routing changes inside the last attempted horizon reset the
// scan. New tail sources cannot continually defeat a failed-prefix backoff.
func accountRoutingScheduleIdentity(ctx context.Context, m *Manifest, bound uint64) ([32]byte, error) {
	return accountRoutingScheduleIdentityExcept(ctx, m, bound, SegmentRef{})
}

func accountRoutingScheduleIdentityExcept(ctx context.Context, m *Manifest, bound uint64, exclude SegmentRef) ([32]byte, error) {
	canonical, err := systemRoutingInputIdentity(ctx, m, [32]byte{}, nil, bound)
	if err != nil {
		return [32]byte{}, err
	}
	var routes [][32]byte
	for _, ref := range m.Segments {
		if ref != exclude && isAccountRoutingKind(ref.Kind) && ref.FromTxNum <= bound {
			routes = append(routes, systemRoutingRefIdentity(&ref))
		}
	}
	sort.Slice(routes, func(i, j int) bool { return bytes.Compare(routes[i][:], routes[j][:]) < 0 })
	h := sha256.New()
	h.Write([]byte("go-tron/account-routing/schedule/v1\x00"))
	h.Write(canonical[:])
	for _, id := range routes {
		h.Write(id[:])
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result, contextError(ctx)
}

func refreshAccountHistoryDeltaContext(ctx context.Context, dir string, s *accountRoutingRefreshState, now time.Time) (bool, error) {
	return refreshAccountHistoryDeltaWithBudget(ctx, dir, s, now, systemHistoryRoutingMaxNewSources, smallSystemRoutingWorkLimit{bytes: 128 << 20, duration: 2 * time.Second})
}

func refreshAccountHistoryDeltaWithBudget(ctx context.Context, dir string, s *accountRoutingRefreshState, now time.Time, maxNew int, work smallSystemRoutingWorkLimit) (bool, error) {
	if err := contextError(ctx); err != nil {
		return false, err
	}
	if s == nil {
		return false, errors.New("snapshots: account delta routing requires Runner state")
	}
	if s.dir != dir {
		metric := s.metric
		*s = accountRoutingRefreshState{dir: dir, metric: metric}
	}
	s.last = accountDeltaBuildStats{}
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
	count, last, indexed := 0, uint64(0), false
	for _, ref := range m.Segments {
		indexed = indexed || isAccountRoutingKind(ref.Kind)
		if ref.Kind == SegmentHistory && ref.normalizedDataset() == SegmentDatasetStateDomainChange {
			count++
			last = max(last, ref.ToTxNum)
		}
	}
	// Preserve the existing bootstrap threshold. Building optional tables on
	// every tiny initial leaf can displace ordinary cold merges and churn the
	// manifest before negative cross-source reads are material.
	if count == 0 || count < systemHistoryRoutingMaxNewSources && !indexed {
		return false, nil
	}
	input, err := accountRoutingScheduleIdentity(ctx, m, s.boundTo)
	if err != nil {
		return false, err
	}
	if s.input != ([32]byte{}) && s.input != input {
		s.nextTx, s.failures, s.holes, s.notBefore = 0, 0, false, time.Time{}
	}
	if now.Before(s.notBefore) {
		if s.metric != nil {
			s.metric.deferred.Inc(1)
			s.metric.retry.Update(int64(s.notBefore.Sub(now)))
		}
		return false, nil
	}
	if s.nextTx == 0 {
		s.holes = false
	}
	s.boundTo = last
	s.input, err = accountRoutingScheduleIdentity(ctx, m, s.boundTo)
	if err != nil {
		return false, err
	}
	started := time.Now()
	ref, built, stats, err := buildAccountRoutingDelta(ctx, dir, m, maxNew, s.nextTx, work)
	s.last = stats
	if s.metric != nil {
		s.metric.passes.Inc(1)
		s.metric.reads.Inc(int64(stats.sourceReads))
		s.metric.headers.Inc(int64(stats.dictionaryHeaders))
		s.metric.metadataReads.Inc(int64(stats.metadataReads))
		s.metric.metadataBytes.Inc(int64(stats.metadataBytes))
		s.metric.metadataUnknown.Inc(int64(stats.metadataUnknown))
		s.metric.inputBytes.Inc(int64(stats.inputBytes))
		s.metric.unsupported.Inc(int64(stats.unsupported))
	}
	if errors.Is(err, errSystemHistoryRoutingBudget) {
		// A metadata/row/output budget failure is not permission to rebuild
		// the base. Keep this cursor and retry with bounded backoff.
		s.holes = true
		s.deferRetry(now, time.Since(started))
		return false, nil
	}
	if err != nil {
		return false, err // Hard failures do not commit the attempted cursor.
	}
	if built {
		latest, publishErr := publishAccountHistoryDelta(ctx, dir, m, ref, nil)
		if errors.Is(publishErr, errAccountDeltaPublicationConflict) {
			if s.metric != nil {
				s.metric.conflicts.Inc(1)
			}
			s.holes = true
			s.deferRetry(now, time.Since(started))
			return false, nil // The same cursor is retried against fresh inputs.
		}
		if publishErr != nil {
			return false, publishErr
		}
		// The publisher guards the output envelope. A source already scanned
		// before it can legally change while this batch is built; do not absorb
		// that earlier hole into our saved identity and advance past it.
		otherInput, err := accountRoutingScheduleIdentityExcept(ctx, manifestLookupView(latest), s.boundTo, ref)
		if err != nil {
			return true, err
		}
		rescan := otherInput != s.input
		input, err = accountRoutingScheduleIdentity(ctx, manifestLookupView(latest), s.boundTo)
		if err != nil {
			return true, err // Published, but do not advance a cancelled cursor.
		}
		s.input, s.nextTx = input, stats.nextTx
		s.failures, s.notBefore = 0, time.Time{}
		s.holes = s.holes || stats.unsupported > 0
		if rescan {
			s.nextTx, s.holes = 0, false
		}
		if s.metric != nil {
			s.metric.pages.Inc(1)
			s.metric.rows.Inc(int64(ref.Rows))
			s.metric.cursor.Update(int64(s.nextTx))
			s.metric.retry.Update(0)
		}
		if stats.eof && !rescan {
			s.deferRetry(now, time.Since(started))
		}
		return true, nil
	}
	// A fully attempted unsupported source can advance scheduling without
	// claiming coverage. Otherwise it would permanently pin a valid tail.
	s.nextTx = stats.nextTx
	s.holes = s.holes || stats.unsupported > 0
	if s.metric != nil {
		s.metric.noOutput.Inc(1)
	}
	s.deferRetry(now, time.Since(started))
	return false, nil
}

func (s *accountRoutingRefreshState) deferRetry(now time.Time, elapsed time.Duration) {
	retry := accountRoutingIdleRetry
	if s.holes {
		s.failures = min(s.failures+1, 6)
		retry = min(systemRoutingBudgetRetryMin<<(s.failures-1), systemRoutingBudgetRetryMax)
	} else {
		s.failures = 0
	}
	s.notBefore = now.Add(elapsed + retry)
	if s.metric != nil {
		s.metric.cursor.Update(int64(s.nextTx))
		s.metric.retry.Update(int64(retry))
	}
}
