package blockbuffer

import (
	"errors"
	"sync"
	"time"

	"github.com/tronprotocol/go-tron/core/pointread"
)

// Frozen 833e4a0b session prefetch, callback, capture and lifecycle methods.
// Only receiver/type/pool names and the equivalent context-session pointer cast
// change. Shared foreground logic, cache implementation and guards are unchanged.
// A separate exact-layout context pool avoids contaminating production callbacks
// or charging legacy-only adapter allocations to each measured operation.
type legacyCommitmentOwnershipSession commitmentParentReadSession
type legacyCommitmentOwnershipContext commitmentParentReadContext

var legacyCommitmentOwnershipContextPool = sync.Pool{New: func() any {
	ctx := new(commitmentParentReadContext)
	ctx.callback = (*legacyCommitmentOwnershipContext)(ctx).consume
	return ctx
}}

func (ctx *legacyCommitmentOwnershipContext) cacheFillAllowed(cacheable bool) bool {
	return (*commitmentParentReadContext)(ctx).cacheFillAllowed(cacheable)
}

func (s *legacyCommitmentOwnershipSession) ViewKeyParts(reader int, first, second []byte, fn func([]byte, bool) error) (bool, error) {
	return (*commitmentParentReadSession)(s).ViewKeyParts(reader, first, second, fn)
}

func (s *legacyCommitmentOwnershipSession) readDurable(cursor pointread.Cursor, key []byte, fn func([]byte) error) (bool, error) {
	return (*commitmentParentReadSession)(s).readDurable(cursor, key, fn)
}

func legacyNewCommitmentOwnershipSession(v *LayerView, readers int) (pointread.CommitmentParentSession, error) {
	if readers <= 0 || v == nil || v.b == nil {
		return nil, nil
	}
	b := v.b
	factory, ok := b.base.(pointread.Snapshotter)
	if !ok {
		return nil, nil
	}
	// Pair the durable snapshot with a topology that cannot lose layers before
	// the snapshot's Pebble sequence is fixed. b.mu also excludes a concurrent
	// CommitInflight append, so the parent cut never includes a partial tail.
	b.mu.RLock()
	// publishReadViewLocked already owns an immutable copy of both topology
	// slices. Retain that published backing for the fold instead of copying the
	// committed slice a second time. A later topology publication cannot mutate
	// this view, and the session's layers slice keeps its backing and layer
	// pointers alive after the old view is replaced.
	topology := b.readView.Load()
	var layers []*layer
	var inflight []*layer
	cache := b.baseReadCache
	if topology != nil {
		layers = topology.layers
		inflight = olderInflightLayers(topology, v.l)
		cache = topology.baseReadCache
	} else if len(b.layers) > 0 {
		// Preserve Buffer's supported zero-value fallback. Production buffers are
		// constructed with New and always have a published read view.
		layers = append([]*layer(nil), b.layers...)
	}
	var cacheVersion uint64
	if cache != nil {
		cacheVersion = cache.version.Load()
	}
	var snapshot pointread.Snapshot
	var err error
	if capacityFactory, ok := b.base.(pointread.CapacitySnapshotter); ok {
		snapshot, err = capacityFactory.NewPointReadSnapshotWithCapacity(readers)
	} else {
		snapshot, err = factory.NewPointReadSnapshot()
	}
	b.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	session := &legacyCommitmentOwnershipSession{
		layers:       layers,
		inflight:     inflight,
		cache:        cache,
		cacheVersion: cacheVersion,
		snapshot:     snapshot,
		cursors:      make([]pointread.Cursor, readers),
		keyScratch:   borrowCommitmentParentKeyScratch(readers),
	}
	session.readContexts = legacyBorrowCommitmentOwnershipContexts(session, readers)
	return session, nil
}

func (s *legacyCommitmentOwnershipSession) PrefetchKeyParts(reader int, first, second []byte) (bool, error) {
	if s == nil || s.snapshot == nil || reader < 0 || reader >= len(s.cursors) {
		return false, errors.New("blockbuffer: invalid commitment parent prefetch reader")
	}
	total := len(first) + len(second)
	if total > splitReadKeyStackSize {
		key := make([]byte, 0, total)
		key = append(key, first...)
		key = append(key, second...)
		return s.prefetchKey(reader, first, key)
	}
	start := reader * splitReadKeyStackSize
	key := (*s.keyScratch)[start : start+total]
	n := copy(key, first)
	copy(key[n:], second)
	return s.prefetchKey(reader, first, key)
}

func (s *legacyCommitmentOwnershipSession) prefetchKey(reader int, keyPrefix, key []byte) (bool, error) {
	ctx := s.readContexts[reader]
	ctx.prefetchPlanned++
	depth := len(key) - len(keyPrefix)
	prefetchDepth := commitmentParentPrefetchDepthBucket(depth)
	if prefetchDepth >= 0 {
		ctx.prefetchDepthPlanned[prefetchDepth]++
	}
	keyHash := layerBloomHashBytes(key)
	if _, found, tomb := lookupLayersNewest(s.inflight, key, keyHash); tomb {
		ctx.prefetchOverlay++
		return false, nil
	} else if found {
		ctx.prefetchOverlay++
		return true, nil
	}
	if _, found, tomb := lookupLayersNewest(s.layers, key, keyHash); tomb {
		ctx.prefetchOverlay++
		return false, nil
	} else if found {
		ctx.prefetchOverlay++
		return true, nil
	}
	for {
		cached, present, cacheEpoch, cacheable := s.cache.probeAtVersionForPrefetch(key, s.cacheVersion)
		if cached {
			ctx.prefetchCache++
			if prefetchDepth >= 0 {
				ctx.prefetchDepthCache[prefetchDepth]++
			}
			return present, nil
		}
		ctx.recordCacheMiss(cacheable, depth, true)
		cursor := s.cursors[reader]
		if cursor == nil {
			var err error
			cursor, err = s.snapshot.NewCursor(keyPrefix)
			if err != nil {
				return false, err
			}
			s.cursors[reader] = cursor
		}
		call, leader, share, _ := s.flights.acquire(key, keyHash, true)
		if leader {
			return s.leadCommitmentParentPrefetch(ctx, cursor, key, call, cacheEpoch, cacheable, prefetchDepth)
		}

		ctx.flightWaiters++
		ctx.flightPrefetchWait++
		waitStarted := time.Now()
		found, value, err := s.flights.wait(call)
		ctx.flightWaitNanos += uint64(time.Since(waitStarted))
		if !share || err != nil {
			s.flights.release(call)
			if err != nil {
				return false, err
			}
			// The leader had already left the cursor callback when this caller
			// joined. Its cache publication is now visible; retry without ever
			// starting a concurrent duplicate read.
			continue
		}

		ctx.flightSharedResults++
		ctx.flightSharedPrefetch++
		if found {
			ctx.flightSharedPresent++
		} else {
			ctx.flightSharedMissing++
		}
		func() {
			defer s.flights.release(call)
			cached, present, epoch, canStore := s.cache.probeAtVersionForPrefetch(key, s.cacheVersion)
			if cached {
				found = present
				return
			}
			ctx.recordCacheMiss(canStore, depth, true)
			if ctx.cacheFillAllowed(canStore) {
				if found {
					s.cache.prefetchIfEpoch(key, value, epoch)
				} else {
					s.cache.prefetchMissingIfEpoch(key, epoch)
				}
			}
		}()
		return found, nil
	}
}

func (s *legacyCommitmentOwnershipSession) leadCommitmentParentPrefetch(
	ctx *commitmentParentReadContext,
	cursor pointread.Cursor,
	key []byte,
	call *commitmentParentReadFlight,
	cacheEpoch baseReadCacheEpoch,
	cacheable bool,
	prefetchDepth int,
) (found bool, err error) {
	completed := false
	released := false
	defer func() {
		ctx.key = nil
		ctx.epoch = baseReadCacheEpoch{}
		ctx.cacheable = false
		ctx.prefetch = false
		ctx.flight = nil
		ctx.flightShared = false
		if !completed {
			s.flights.complete(call, false, errCommitmentParentReadAborted)
		}
		if !released {
			s.flights.release(call)
		}
	}()

	ctx.flightLeaders++
	ctx.prefetchDurable++
	if prefetchDepth >= 0 {
		ctx.prefetchDepthDurable[prefetchDepth]++
	}
	ctx.key = key
	ctx.epoch = cacheEpoch
	ctx.cacheable = cacheable
	ctx.prefetch = true
	ctx.flight = call
	found, err = s.readDurable(cursor, key, ctx.callback)
	ctx.flight = nil
	if found {
		ctx.prefetchHits++
	}
	if err == nil && !found && ctx.cacheFillAllowed(cacheable) {
		s.cache.prefetchMissingIfEpoch(key, cacheEpoch)
	}
	if err != nil {
		ctx.flightLeaderErrors++
	}
	s.flights.complete(call, found, err)
	completed = true
	s.flights.release(call)
	released = true
	return found, err
}

func (s *legacyCommitmentOwnershipSession) Close() error {
	if s == nil || s.snapshot == nil {
		return nil
	}
	var firstErr error
	for i, cursor := range s.cursors {
		if cursor != nil {
			if err := cursor.Close(); firstErr == nil && err != nil {
				firstErr = err
			}
			s.cursors[i] = nil
		}
	}
	if err := s.snapshot.Close(); firstErr == nil && err != nil {
		firstErr = err
	}
	s.snapshot = nil
	s.layers = nil
	s.inflight = nil
	cache := s.cache
	s.cache = nil
	var overlayResolved, cacheResolved, durableReads, durableHits, trunkCached, trunkDurable, windowCached uint64
	var cacheNoResident, cacheResidentNewer, cacheFillVersionChanged uint64
	var cacheDeepNoResident [baseReadCacheDiagnosticDepths][2]uint64
	var prefetchPlanned, prefetchOverlay, prefetchCache, prefetchDurable, prefetchHits uint64
	var depthCached, depthDurable [4]uint64
	var exactDepthCached, exactDepthDurable [4]uint64
	var prefetchDepthPlanned, prefetchDepthCache, prefetchDepthDurable, prefetchDepthUseful [2]uint64
	var flightLeaders, flightWaiters, flightSharedResults, flightSharedForeground, flightSharedPrefetch uint64
	var flightSharedPresent, flightSharedMissing uint64
	var flightLeaderErrors, flightWaitNanos, flightForegroundWait, flightPrefetchWait uint64
	for _, ctx := range s.readContexts {
		overlayResolved += ctx.overlayResolved
		cacheResolved += ctx.cacheResolved
		cacheNoResident += ctx.cacheNoResident
		cacheResidentNewer += ctx.cacheResidentNewer
		for depth := range cacheDeepNoResident {
			for source := range cacheDeepNoResident[depth] {
				cacheDeepNoResident[depth][source] += ctx.cacheDeepNoResident[depth][source]
			}
		}
		cacheFillVersionChanged += ctx.cacheFillVersionChanged
		durableReads += ctx.durableReads
		durableHits += ctx.durableHits
		trunkCached += ctx.trunkCached
		trunkDurable += ctx.trunkDurable
		windowCached += ctx.windowCached
		prefetchPlanned += ctx.prefetchPlanned
		prefetchOverlay += ctx.prefetchOverlay
		prefetchCache += ctx.prefetchCache
		prefetchDurable += ctx.prefetchDurable
		prefetchHits += ctx.prefetchHits
		flightLeaders += ctx.flightLeaders
		flightWaiters += ctx.flightWaiters
		flightSharedResults += ctx.flightSharedResults
		flightSharedForeground += ctx.flightSharedForeground
		flightSharedPrefetch += ctx.flightSharedPrefetch
		flightSharedPresent += ctx.flightSharedPresent
		flightSharedMissing += ctx.flightSharedMissing
		flightLeaderErrors += ctx.flightLeaderErrors
		flightWaitNanos += ctx.flightWaitNanos
		flightForegroundWait += ctx.flightForegroundWait
		flightPrefetchWait += ctx.flightPrefetchWait
		for bucket := range prefetchDepthPlanned {
			prefetchDepthPlanned[bucket] += ctx.prefetchDepthPlanned[bucket]
			prefetchDepthCache[bucket] += ctx.prefetchDepthCache[bucket]
			prefetchDepthDurable[bucket] += ctx.prefetchDepthDurable[bucket]
			prefetchDepthUseful[bucket] += ctx.prefetchDepthUseful[bucket]
		}
		for bucket := range depthCached {
			depthCached[bucket] += ctx.depthCached[bucket]
			depthDurable[bucket] += ctx.depthDurable[bucket]
			exactDepthCached[bucket] += ctx.exactDepthCached[bucket]
			exactDepthDurable[bucket] += ctx.exactDepthDurable[bucket]
		}
	}
	commitmentParentOverlayResolvedCounter.Inc(int64(overlayResolved))
	commitmentParentCacheResolvedCounter.Inc(int64(cacheResolved))
	commitmentParentCacheNoResidentCounter.Inc(int64(cacheNoResident))
	commitmentParentCacheResidentNewerCounter.Inc(int64(cacheResidentNewer))
	commitmentParentCacheFillVersionChangedCounter.Inc(int64(cacheFillVersionChanged))
	for depth := range cacheDeepNoResident {
		for source, count := range cacheDeepNoResident[depth] {
			commitmentParentDeepNoResidentCounters[depth][source].Inc(int64(count))
		}
	}
	commitmentParentDurableReadsCounter.Inc(int64(durableReads))
	commitmentParentDurableHitsCounter.Inc(int64(durableHits))
	commitmentParentTrunkCacheCounter.Inc(int64(trunkCached))
	commitmentParentTrunkDurableCounter.Inc(int64(trunkDurable))
	commitmentParentWindowCacheCounter.Inc(int64(windowCached))
	commitmentParentPrefetchPlannedCounter.Inc(int64(prefetchPlanned))
	commitmentParentPrefetchOverlayCounter.Inc(int64(prefetchOverlay))
	commitmentParentPrefetchCacheCounter.Inc(int64(prefetchCache))
	commitmentParentPrefetchDurableCounter.Inc(int64(prefetchDurable))
	commitmentParentPrefetchDurableHitCounter.Inc(int64(prefetchHits))
	commitmentParentSingleflightLeadersCounter.Inc(int64(flightLeaders))
	commitmentParentSingleflightWaitersCounter.Inc(int64(flightWaiters))
	commitmentParentSingleflightSharedCounter.Inc(int64(flightSharedResults))
	commitmentParentSingleflightForegroundSharedCounter.Inc(int64(flightSharedForeground))
	commitmentParentSingleflightPrefetchSharedCounter.Inc(int64(flightSharedPrefetch))
	commitmentParentSingleflightSharedPresentCounter.Inc(int64(flightSharedPresent))
	commitmentParentSingleflightSharedMissingCounter.Inc(int64(flightSharedMissing))
	commitmentParentSingleflightLeaderErrorsCounter.Inc(int64(flightLeaderErrors))
	commitmentParentSingleflightWaitNanosCounter.Inc(int64(flightWaitNanos))
	commitmentParentSingleflightForegroundWaitersCounter.Inc(int64(flightForegroundWait))
	commitmentParentSingleflightPrefetchWaitersCounter.Inc(int64(flightPrefetchWait))
	for bucket := range prefetchDepthPlanned {
		commitmentParentPrefetchDepthPlannedCounters[bucket].Inc(int64(prefetchDepthPlanned[bucket]))
		commitmentParentPrefetchDepthCacheCounters[bucket].Inc(int64(prefetchDepthCache[bucket]))
		commitmentParentPrefetchDepthDurableCounters[bucket].Inc(int64(prefetchDepthDurable[bucket]))
		commitmentParentPrefetchDepthUsefulCounters[bucket].Inc(int64(prefetchDepthUseful[bucket]))
	}
	for bucket := range depthCached {
		commitmentParentDepthCacheCounters[bucket].Inc(int64(depthCached[bucket]))
		commitmentParentDepthDurableCounters[bucket].Inc(int64(depthDurable[bucket]))
		commitmentParentExactDepthCacheCounters[bucket].Inc(int64(exactDepthCached[bucket]))
		commitmentParentExactDepthDurableCounters[bucket].Inc(int64(exactDepthDurable[bucket]))
	}
	if cache != nil {
		cache.maybePublishMetrics()
	}
	legacyReturnCommitmentOwnershipContexts(s.readContexts)
	s.readContexts = nil
	returnCommitmentParentKeyScratch(s.keyScratch)
	s.keyScratch = nil
	return firstErr
}

func (ctx *legacyCommitmentOwnershipContext) consume(value []byte) error {
	s := ctx.session
	if ctx.cacheFillAllowed(ctx.cacheable) {
		if ctx.prefetch {
			s.cache.prefetchIfEpoch(ctx.key, value, ctx.epoch)
		} else {
			s.cache.storeIfEpoch(ctx.key, value, ctx.epoch)
		}
	}
	if ctx.flight != nil && s.flights.capture(ctx.flight, value) {
		ctx.flightShared = true
	}
	if ctx.prefetch {
		return nil
	}
	if !ctx.flightShared {
		ctx.callbackErr = ctx.fn(value, false)
	}
	return nil
}

func legacyBorrowCommitmentOwnershipContexts(session *legacyCommitmentOwnershipSession, readers int) []*commitmentParentReadContext {
	contexts := make([]*commitmentParentReadContext, readers)
	for i := range contexts {
		ctx := legacyCommitmentOwnershipContextPool.Get().(*commitmentParentReadContext)
		ctx.session = (*commitmentParentReadSession)(session)
		contexts[i] = ctx
	}
	return contexts
}

func legacyReturnCommitmentOwnershipContexts(contexts []*commitmentParentReadContext) {
	for i, ctx := range contexts {
		ctx.session = nil
		ctx.key = nil
		ctx.epoch = baseReadCacheEpoch{}
		ctx.cacheable = false
		ctx.prefetch = false
		ctx.fn = nil
		ctx.flight = nil
		ctx.callbackErr = nil
		ctx.flightShared = false
		ctx.overlayResolved = 0
		ctx.cacheResolved = 0
		ctx.cacheNoResident = 0
		ctx.cacheResidentNewer = 0
		ctx.cacheFillVersionChanged = 0
		ctx.cacheDeepNoResident = [baseReadCacheDiagnosticDepths][2]uint64{}
		ctx.durableReads = 0
		ctx.durableHits = 0
		ctx.trunkCached = 0
		ctx.trunkDurable = 0
		ctx.windowCached = 0
		ctx.depthCached = [4]uint64{}
		ctx.depthDurable = [4]uint64{}
		ctx.exactDepthCached = [4]uint64{}
		ctx.exactDepthDurable = [4]uint64{}
		ctx.prefetchPlanned = 0
		ctx.prefetchOverlay = 0
		ctx.prefetchCache = 0
		ctx.prefetchDurable = 0
		ctx.prefetchHits = 0
		ctx.prefetchDepthPlanned = [2]uint64{}
		ctx.prefetchDepthCache = [2]uint64{}
		ctx.prefetchDepthDurable = [2]uint64{}
		ctx.prefetchDepthUseful = [2]uint64{}
		ctx.flightLeaders = 0
		ctx.flightWaiters = 0
		ctx.flightSharedResults = 0
		ctx.flightSharedForeground = 0
		ctx.flightSharedPrefetch = 0
		ctx.flightSharedPresent = 0
		ctx.flightSharedMissing = 0
		ctx.flightLeaderErrors = 0
		ctx.flightWaitNanos = 0
		ctx.flightForegroundWait = 0
		ctx.flightPrefetchWait = 0
		legacyCommitmentOwnershipContextPool.Put(ctx)
		contexts[i] = nil
	}
}
