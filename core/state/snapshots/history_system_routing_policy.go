package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

type systemRoutingBuildLimits struct{ bytes, keys, sources int }

var defaultSystemHistoryRoutingLimits = systemRoutingBuildLimits{
	systemHistoryRoutingMaxBytes, systemHistoryRoutingMaxKeys, systemHistoryRoutingMaxSources,
}

type systemRoutingBuildStats struct {
	attempts, budgetRejects, dictionaryReads, cacheHits int
	limit, selected                                     int
	boundTo, firstTo, nextTx                            uint64
	sources                                             [][32]byte
}

type systemRoutingSourceKeys struct {
	keys        []string
	unsupported bool
}

// This cache exists for one build only. It never attests to query absence.
type systemRoutingSourceCache struct {
	entries map[[32]byte]systemRoutingSourceKeys
	bytes   int
}

func (c *systemRoutingSourceCache) keys(ctx context.Context, dir string, h, a SegmentRef, id [32]byte, stats *systemRoutingBuildStats) ([]string, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if entry, ok := c.entries[id]; ok {
		stats.cacheHits++
		if entry.unsupported {
			return nil, errSystemHistoryRoutingBudget
		}
		return entry.keys, nil
	}
	stats.dictionaryReads++
	keys, err := systemHistorySourceKeys(ctx, dir, h, a)
	if errors.Is(err, errSystemHistoryRoutingBudget) {
		c.entries[id] = systemRoutingSourceKeys{unsupported: true}
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	charge := 0
	for _, key := range keys {
		charge += len(key) + 32
	}
	if charge > systemHistoryRoutingBuildKeyBytes-c.bytes {
		c.entries[id] = systemRoutingSourceKeys{unsupported: true}
		return nil, errSystemHistoryRoutingBudget
	}
	c.bytes += charge
	c.entries[id] = systemRoutingSourceKeys{keys: keys}
	return keys, nil
}

func buildAdaptiveSystemHistoryRouting(ctx context.Context, dir string, manifest *Manifest, old *systemHistoryRouting, maxNew int, startTx uint64, limits systemRoutingBuildLimits) (systemRoutingBuildStats, SegmentRef, bool, error) {
	var total systemRoutingBuildStats
	if manifest == nil || maxNew < 1 {
		return total, SegmentRef{}, false, nil
	}
	manifest = manifestLookupView(manifest)
	cache := systemRoutingSourceCache{entries: make(map[[32]byte]systemRoutingSourceKeys)}
	// A nearly full route often cannot admit even two sources. Probe the
	// minimum immediately after rejection; never rebuild identical prefixes
	// or scan the million-key old object through every power of two.
	for limit := min(maxNew, systemHistoryRoutingMaxNewSources); ; limit = 1 {
		if err := contextError(ctx); err != nil {
			return total, SegmentRef{}, false, err
		}
		current := systemRoutingBuildStats{limit: limit}
		ref, built, err := buildSystemHistoryRoutingCandidate(ctx, dir, manifest, old, limit, startTx, &cache, &current, limits)
		current.attempts = total.attempts + 1
		current.budgetRejects += total.budgetRejects
		current.dictionaryReads += total.dictionaryReads
		current.cacheHits += total.cacheHits
		total = current
		if !errors.Is(err, errSystemHistoryRoutingBudget) {
			return total, ref, built, err
		}
		total.budgetRejects++
		if limit == 1 {
			return total, SegmentRef{}, false, nil
		}
	}
}

const (
	systemRoutingBudgetRetryMin   = time.Minute
	systemRoutingBudgetRetryMax   = 30 * time.Minute
	systemRoutingUnsupportedRetry = 15 * time.Second
)

// Runner.passMu owns this state. It retains source IDs, not decoded keys or
// memberships. All fields are scheduling hints, never coverage authority.
type systemRoutingRefreshState struct {
	dir       string
	route     [32]byte
	covered   map[[32]byte]struct{}
	input     [32]byte
	boundTo   uint64
	nextTx    uint64
	batch     int
	failures  uint
	notBefore time.Time
	metric    *systemRoutingPolicyMetrics
}

type systemRoutingPolicyMetrics struct {
	attempts, dictionaries, budgets, deferred, noProgress, selected *metrics.Counter
	batch, retry                                                    *metrics.Gauge
}

func (s *systemRoutingRefreshState) initMetrics(namespace string) {
	if s.metric != nil || namespace == "" {
		return
	}
	p := strings.TrimRight(namespace, "/") + "/history/routing/system/"
	c := func(n string) *metrics.Counter { return metrics.GetOrRegisterCounter(p+n, nil) }
	s.metric = &systemRoutingPolicyMetrics{
		attempts: c("assembly_attempts"), dictionaries: c("dictionary_reads"), budgets: c("budget_rejections"),
		deferred: c("backoff_deferred"), noProgress: c("no_progress"), selected: c("sources_published"),
		batch: metrics.GetOrRegisterGauge(p+"batch_limit", nil), retry: metrics.GetOrRegisterGauge(p+"retry_remaining_ns", nil),
	}
}

func (s *systemRoutingRefreshState) reset(dir string, route [32]byte) {
	metric := s.metric
	*s = systemRoutingRefreshState{dir: dir, route: route, batch: systemHistoryRoutingMaxNewSources, metric: metric}
}

func systemRoutingRefIdentity(ref *SegmentRef) [32]byte {
	if ref == nil {
		return [32]byte{}
	}
	return systemHistoryRoutingPairID(*ref, *ref)
}

// Include every still-active reusable old source, even beyond the attempted
// prefix. A replacement/retirement can free budget and must cancel deferral.
// New tail pairs beyond bound are excluded so appends cannot defeat backoff.
func systemRoutingInputIdentity(ctx context.Context, manifest *Manifest, route [32]byte, covered map[[32]byte]struct{}, bound uint64) ([32]byte, error) {
	var ids [][32]byte
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for i, h := range manifest.Segments {
		if i%256 == 0 {
			if err := contextError(ctx); err != nil {
				return [32]byte{}, err
			}
		}
		if h.Kind != SegmentHistory || h.normalizedDataset() != SegmentDatasetStateDomainChange {
			continue
		}
		a, ok := cfg.HistoryAccessorRef(manifest, h)
		if !ok {
			return [32]byte{}, errors.New("snapshots: routing source missing accessor")
		}
		id := systemHistoryRoutingPairID(h, a)
		_, reused := covered[id]
		if reused || h.FromTxNum <= bound {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	h := sha256.New()
	h.Write([]byte("go-tron/system-routing/schedule/v1\x00"))
	h.Write(route[:])
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(ids)))
	h.Write(size[:])
	for _, id := range ids {
		h.Write(id[:])
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result, contextError(ctx)
}

func systemRoutingCovered(sources [][32]byte) map[[32]byte]struct{} {
	covered := make(map[[32]byte]struct{}, len(sources))
	for _, id := range sources {
		covered[id] = struct{}{}
	}
	return covered
}

func refreshSystemHistoryRoutingWithStateContext(ctx context.Context, dir string, busy bool, state *systemRoutingRefreshState, now time.Time) (bool, error) {
	return refreshSystemHistoryRoutingWithLimitsContext(ctx, dir, busy, state, now, defaultSystemHistoryRoutingLimits)
}

func refreshSystemHistoryRoutingWithLimitsContext(ctx context.Context, dir string, busy bool, state *systemRoutingRefreshState, now time.Time, limits systemRoutingBuildLimits) (bool, error) {
	started := time.Now()
	if err := contextError(ctx); err != nil {
		return false, err
	}
	if !historyReadLeaseGate.TryRLock() {
		return false, nil
	}
	defer historyReadLeaseGate.RUnlock()
	manifest, err := loadProductionManifestReadOnly(dir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	manifest = manifestLookupView(manifest)
	var routeRef *SegmentRef
	count := 0
	for i := range manifest.Segments {
		r := &manifest.Segments[i]
		if r.Kind == SegmentSystemHistoryRouting {
			if routeRef != nil {
				return false, errors.New("snapshots: multiple system history routing segments")
			}
			routeRef = r
		}
		if r.Kind == SegmentHistory && r.normalizedDataset() == SegmentDatasetStateDomainChange {
			count++
		}
	}
	if count < systemHistoryRoutingMaxNewSources && routeRef == nil {
		return false, nil
	}
	routeID := systemRoutingRefIdentity(routeRef)
	if state != nil {
		if state.dir != dir || state.route != routeID {
			state.reset(dir, routeID)
		}
		if state.covered != nil {
			input, err := systemRoutingInputIdentity(ctx, manifest, routeID, state.covered, state.boundTo)
			if err != nil {
				return false, err
			}
			if input != state.input {
				state.reset(dir, routeID)
			}
		}
		if now.Before(state.notBefore) {
			// Cheap existence/size failures remain visible while skipping the
			// expensive decode. Query/import paths still hash physical bytes.
			if routeRef != nil {
				stat, err := os.Stat(filepath.Join(dir, routeRef.Path))
				if err != nil {
					return false, err
				}
				if stat.Size() != int64(routeRef.Size) {
					return false, errors.New("snapshots: system history routing size mismatch")
				}
			}
			if state.metric != nil {
				state.metric.deferred.Inc(1)
				state.metric.retry.Update(int64(state.notBefore.Sub(now)))
			}
			return false, nil
		}
	}
	old, err := loadSystemHistoryRouting(ctx, dir, manifest)
	if err != nil {
		return false, err
	}
	if old != nil && !old.storagePrefixes {
		old = nil
	}
	limit, startTx := systemHistoryRoutingMaxNewSources, uint64(0)
	if state != nil {
		limit, startTx = max(1, state.batch), state.nextTx
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	uncovered := 0
	for _, h := range manifest.Segments {
		if h.Kind != SegmentHistory || h.normalizedDataset() != SegmentDatasetStateDomainChange {
			continue
		}
		a, ok := cfg.HistoryAccessorRef(manifest, h)
		if !ok {
			return false, errors.New("snapshots: routing source missing accessor")
		}
		if old == nil {
			uncovered++
		} else if _, ok := old.columns[systemHistoryRoutingPairID(h, a)]; !ok {
			uncovered++
		}
	}
	if uncovered == 0 || busy && uncovered < limit {
		return false, nil
	}
	stats, ref, built, err := buildAdaptiveSystemHistoryRouting(ctx, dir, manifest, old, limit, startTx, limits)
	if state != nil && state.metric != nil {
		state.metric.attempts.Inc(int64(stats.attempts))
		state.metric.dictionaries.Inc(int64(stats.dictionaryReads))
		state.metric.budgets.Inc(int64(stats.budgetRejects))
	}
	if err != nil {
		return false, err
	}
	if err := contextError(ctx); err != nil {
		return false, err
	}
	if built {
		if _, err := NewAggregator(dir).Integrate(manifest.VisibleTxStart, manifest.VisibleTxEnd, []SegmentRef{ref}); err != nil {
			return false, err
		}
		if state != nil {
			state.route = systemRoutingRefIdentity(&ref)
			state.covered = systemRoutingCovered(stats.sources)
			state.boundTo, state.nextTx, state.batch, state.failures, state.notBefore = 0, 0, stats.limit, 0, time.Time{}
			state.input, err = systemRoutingInputIdentity(ctx, manifest, state.route, state.covered, 0)
			if state.metric != nil {
				state.metric.selected.Inc(int64(stats.selected))
				state.metric.batch.Update(int64(state.batch))
				state.metric.retry.Update(0)
			}
			if err != nil {
				return true, err
			}
		}
		return true, nil
	}
	if state != nil {
		state.covered = make(map[[32]byte]struct{})
		if old != nil {
			state.covered = systemRoutingCovered(old.sources)
		}
		state.boundTo, state.nextTx = stats.boundTo, stats.nextTx
		retry := systemRoutingUnsupportedRetry
		if stats.budgetRejects > 0 {
			state.batch = 1
			state.failures = min(state.failures+1, 6)
			retry = min(systemRoutingBudgetRetryMin<<(state.failures-1), systemRoutingBudgetRetryMax)
			state.nextTx = stats.firstTo + 1
		}
		if state.boundTo == 0 {
			for _, h := range manifest.Segments {
				if h.Kind == SegmentHistory && h.normalizedDataset() == SegmentDatasetStateDomainChange {
					state.boundTo = max(state.boundTo, h.ToTxNum)
				}
			}
		}
		state.input, err = systemRoutingInputIdentity(ctx, manifest, routeID, state.covered, state.boundTo)
		if err != nil {
			return false, fmt.Errorf("snapshots: routing scheduling identity: %w", err)
		}
		state.notBefore = now.Add(time.Since(started) + retry)
		if state.metric != nil {
			state.metric.noProgress.Inc(1)
			state.metric.batch.Update(int64(state.batch))
			state.metric.retry.Update(int64(retry))
		}
	}
	return false, nil
}

// Heap sort bounds cancellation latency without retaining another key array.
// Sorting may reorder this unpublished candidate, never authoritative data.
func sortSystemRoutingKeysContext(ctx context.Context, keys []string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	steps := 0
	down := func(root, end int) error {
		for root < end/2 {
			child := 2*root + 1
			if child+1 < end && keys[child] < keys[child+1] {
				child++
			}
			if keys[root] >= keys[child] {
				break
			}
			keys[root], keys[child] = keys[child], keys[root]
			root = child
			steps++
			if steps%1024 == 0 {
				if err := contextError(ctx); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for i := len(keys) / 2; i > 0; i-- {
		if err := down(i-1, len(keys)); err != nil {
			return err
		}
	}
	for end := len(keys) - 1; end > 0; end-- {
		keys[0], keys[end] = keys[end], keys[0]
		if err := down(0, end); err != nil {
			return err
		}
		if end%1024 == 0 {
			if err := contextError(ctx); err != nil {
				return err
			}
		}
	}
	return contextError(ctx)
}
