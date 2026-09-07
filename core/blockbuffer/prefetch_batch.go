package blockbuffer

import (
	"bytes"
	"errors"
	"slices"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/pointread"
)

var (
	prefetchBatchCounter        = metrics.NewRegisteredCounter("blockbuffer/state_prefetch/batches", nil)
	prefetchBatchDurableCounter = metrics.NewRegisteredCounter("blockbuffer/state_prefetch/durable_rows", nil)
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
	for _, request := range pending {
		key := keys[request.index]
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
			if readErr == nil {
				cache.prefetchMissingIfEpoch(key, request.epoch)
			}
			if err := visit(request.index, nil, false, readErr); err != nil {
				return err
			}
		}
	}
	return nil
}
