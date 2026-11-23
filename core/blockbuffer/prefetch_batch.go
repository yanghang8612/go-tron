package blockbuffer

import (
	"bytes"
	"errors"
	"slices"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var (
	prefetchBatchCounter         = metrics.NewRegisteredCounter("blockbuffer/state_prefetch/batches", nil)
	prefetchBatchDurableCounter  = metrics.NewRegisteredCounter("blockbuffer/state_prefetch/durable_rows", nil)
	prefetchRecheckAttempts      = metrics.NewRegisteredCounter("blockbuffer/state_prefetch/recheck_attempts", nil)
	prefetchRecheckEligible      = metrics.NewRegisteredCounter("blockbuffer/state_prefetch/recheck_eligible", nil)
	prefetchRecheckCachedPresent = metrics.NewRegisteredCounter("blockbuffer/state_prefetch/recheck_cached_present", nil)
	prefetchRecheckCachedAbsent  = metrics.NewRegisteredCounter("blockbuffer/state_prefetch/recheck_cached_absent", nil)
)

type pendingPrefetch struct {
	index int
	epoch baseReadCacheEpoch
}

// PrefetchBatch resolves overlays and cache first, then sorts only durable
// misses and reads them with one exact-key cursor. Capture EVERY invalidation
// epoch BEFORE opening the snapshot: an epoch sampled after a flush could
// otherwise authorize publishing an older snapshot row into the latest cache.
// Concurrent flushes reject late fills; overlay mutations remain authoritative
// on the subsequent canonical read. No buffer lock is held during disk I/O.
func (b *Buffer) PrefetchBatch(keys [][]byte, visit func(int, []byte, bool, error) error) (result error) {
	factory, capable := b.base.(pointread.Snapshotter)
	view := b.loadReadView()
	cache := view.baseReadCache
	if !capable || cache == nil {
		for i, key := range keys {
			value, err := b.Prefetch(key)
			present := err == nil
			if b.IsKeyNotFound(err) {
				err = nil
			}
			if err := visit(i, value, present, err); err != nil {
				return err
			}
		}
		return nil
	}
	pending := make([]pendingPrefetch, 0, len(keys))
	for i, key := range keys {
		hash := layerBloomHashBytes(key)
		value, found, tomb := lookupLayersNewest(view.inflight, key, hash)
		if !found && !tomb {
			value, found, tomb = lookupLayersNewest(view.layers, key, hash)
		}
		if found || tomb {
			if err := visit(i, value, !tomb, nil); err != nil {
				return err
			}
			continue
		}
		value, cached, epoch := cache.getForPrefetchWithEpoch(key)
		if cached {
			if err := visit(i, value, value != nil, nil); err != nil {
				return err
			}
			continue
		}
		pending = append(pending, pendingPrefetch{index: i, epoch: epoch})
	}
	if len(pending) == 0 {
		return nil
	}
	slices.SortFunc(pending, func(a, c pendingPrefetch) int { return bytes.Compare(keys[a.index], keys[c.index]) })
	// A flush may expose durable writes before publishing their cache version,
	// and a multi-group flush advances the version only once. Require a
	// quiescent flush boundary before the snapshot and again at any reuse.
	stableVersion := b.flushMu.TryLock()
	var version uint64
	if stableVersion {
		version = cache.version.Load()
		b.flushMu.Unlock()
	}
	snapshot, err := factory.NewPointReadSnapshot()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, snapshot.Close()) }()
	cursor, err := snapshot.NewCursor(nil)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, cursor.Close()) }()
	prefetchBatchCounter.Inc(1)
	// Count visited pending rows, including fallbacks. Eligible rows have a
	// captured version and a state-plan key whose durable writes go through
	// the buffer's flush path. A hit also needs matching live metadata.
	var attempts, eligible, cachedPresent, cachedAbsent int64
	defer func() {
		prefetchRecheckAttempts.Inc(attempts)
		prefetchRecheckEligible.Inc(eligible)
		prefetchRecheckCachedPresent.Inc(cachedPresent)
		prefetchRecheckCachedAbsent.Inc(cachedAbsent)
	}()
	for _, request := range pending {
		key := keys[request.index]
		attempts++
		if stableVersion && rawdb.IsStatePrefetchKey(key) {
			eligible++
			if value, cached := b.recheckPrefetch(cache, key, request.epoch, version); cached {
				if value != nil {
					cachedPresent++
				} else {
					cachedAbsent++
				}
				if err := visit(request.index, value, value != nil, nil); err != nil {
					return err
				}
				continue
			}
		}
		prefetchBatchDurableCounter.Inc(1)
		// Run visit inside the cursor callback; neither a Pebble-backed value
		// nor a newly recycled cache allocation may escape its ownership scope.
		var visitErr error
		present, readErr := cursor.View(key, func(value []byte) error {
			cache.prefetchIfEpoch(key, value, request.epoch)
			visitErr = visit(request.index, value, true, nil)
			return nil
		})
		if visitErr != nil {
			return visitErr
		}
		if readErr != nil || !present {
			if readErr != nil {
				// A subsequent cursor View can clear its previous positioning
				// error. After a failed View, preserve the original retry path.
				stableVersion = false
			} else {
				cache.prefetchMissingIfEpoch(key, request.epoch)
			}
			if err := visit(request.index, nil, false, readErr); err != nil {
				return err
			}
		}
	}
	return nil
}

// recheckPrefetch may reuse only a row installed at the captured quiescent
// version and before any subsequent flush or key invalidation. Any contention
// falls back to the original cursor. Acquiring flushMu while holding a shard
// is safe only via TryLock: a flusher can hold flushMu while waiting on it.
func (b *Buffer) recheckPrefetch(cache *baseReadCache, key []byte, epoch baseReadCacheEpoch, version uint64) ([]byte, bool) {
	if cache.version.Load() != version {
		return nil, false
	}
	shard := &cache.shards[baseReadCacheShardIndex(key)]
	if !shard.mu.TryRLock() {
		return nil, false
	}
	defer shard.mu.RUnlock()
	entry, ok := shard.entries[string(key)]
	if !ok || entry.version != version || cache.invalidations[epoch.slot].Load() != epoch.value || !b.flushMu.TryLock() {
		return nil, false
	}
	defer b.flushMu.Unlock()
	view := b.readView.Load()
	if view == nil || view.baseReadCache != cache || cache.version.Load() != version ||
		cache.invalidations[epoch.slot].Load() != epoch.value {
		return nil, false
	}
	entry.reference(baseReadCacheReferencePrefetch)
	value := entry.value
	if value != nil {
		entry.exposed.Store(true)
	}
	return value, true
}
