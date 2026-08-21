package blockbuffer

import (
	"encoding/binary"
	"errors"
	"sort"
	"strings"
	"sync"
	"unsafe"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
)

const splitReadKeyStackSize = 128

const (
	// 17 foreground readers (16 nibble lanes + root) plus 16 independent
	// read-ahead readers. A rotating branch generation doubles both sets for its
	// frozen/legacy fallback keyspace.
	defaultCommitmentParentReaders   = 33
	maxPooledCommitmentParentReaders = 66
)

var splitReadKeyPool = sync.Pool{
	New: func() any { return new([splitReadKeyStackSize]byte) },
}

var commitmentParentKeyScratchPool = sync.Pool{
	New: func() any {
		scratch := make([]byte, 0, defaultCommitmentParentReaders*splitReadKeyStackSize)
		return &scratch
	},
}

func borrowCommitmentParentKeyScratch(readers int) *[]byte {
	scratch := commitmentParentKeyScratchPool.Get().(*[]byte)
	size := readers * splitReadKeyStackSize
	if cap(*scratch) < size {
		*scratch = make([]byte, size)
	} else {
		*scratch = (*scratch)[:size]
	}
	return scratch
}

func returnCommitmentParentKeyScratch(scratch *[]byte) {
	if cap(*scratch) > maxPooledCommitmentParentReaders*splitReadKeyStackSize {
		*scratch = nil
		return
	}
	*scratch = (*scratch)[:0]
	commitmentParentKeyScratchPool.Put(scratch)
}

func borrowSplitReadKey() *[splitReadKeyStackSize]byte {
	return splitReadKeyPool.Get().(*[splitReadKeyStackSize]byte)
}

func returnSplitReadKey(key *[splitReadKeyStackSize]byte) {
	splitReadKeyPool.Put(key)
}

type ownedValueBatchLink struct {
	end  int
	next int // next entry index + 1; zero terminates the shard chain
}

var ownedValueBatchLinksPool = sync.Pool{
	New: func() any {
		links := make([]ownedValueBatchLink, 0, 256)
		return &links
	},
}

func borrowOwnedValueBatchLinks(size int) *[]ownedValueBatchLink {
	linksPtr := ownedValueBatchLinksPool.Get().(*[]ownedValueBatchLink)
	if cap(*linksPtr) < size {
		*linksPtr = make([]ownedValueBatchLink, size)
	} else {
		*linksPtr = (*linksPtr)[:size]
	}
	return linksPtr
}

func returnOwnedValueBatchLinks(linksPtr *[]ownedValueBatchLink) {
	links := *linksPtr
	if cap(links) <= 4096 {
		*linksPtr = links[:0]
		ownedValueBatchLinksPool.Put(linksPtr)
	}
}

// LayerView is a read/write view bound to ONE in-flight layer. Reads resolve
// that layer's own writes/tombstones first, then the committed stack
// (newest-first), then the base reader — it deliberately IGNORES every other
// in-flight layer so there is no forward dependency on a layer the worker has
// not produced yet. Writes target the bound layer only.
//
// The async commit worker uses a LayerView (obtained via Buffer.ViewLayer) as
// the commitment store / account-KV index for block N's fold + publish tail,
// while the foreground writes the newer layer N+1 through the Buffer directly.
// Because both go through Buffer.mu and target disjoint layers, the sharded
// layer maps stay race-free.
//
// A LayerView satisfies ethdb.KeyValueReader + ethdb.KeyValueWriter +
// ethdb.Iteratee, so it drops in anywhere those interfaces (CommitmentDB,
// accountKVIndexStore) are expected.
type LayerView struct {
	b *Buffer
	l *layer
}

type commitmentParentView struct {
	b    *Buffer
	l    *layer
	base pointread.View
}

type commitmentParentReadSession struct {
	layers []*layer
	// inflight contains only layers older than the LayerView bound to this
	// session. Ordered commitment lanes may publish a finished nibble into an
	// older block while other nibbles of that block are still folding; the same
	// nibble in the next block must see those writes without seeing its own or a
	// newer block's layer.
	inflight     []*layer
	cache        *baseReadCache
	cacheVersion uint64
	snapshot     pointread.Snapshot
	cursors      []pointread.Cursor
	readContexts []*commitmentParentReadContext
	// keyScratch is split into one 128-byte region per cursor/reader. The
	// CommitmentParentSession contract gives each reader index one exclusive
	// worker, so split-key assembly needs neither a lock nor a sync.Pool trip on
	// every branch lookup.
	keyScratch *[]byte
}

// commitmentParentReadContext owns the callback state for one session reader.
// The session contract assigns every reader index to one exclusive fold worker,
// so the context may be reused with that reader's cursor without a lock. Keeping
// callback pre-bound avoids allocating the capturing closure that would
// otherwise escape on every durable branch read.
type commitmentParentReadContext struct {
	session   *commitmentParentReadSession
	key       []byte
	epoch     baseReadCacheEpoch
	cacheable bool
	prefetch  bool
	fn        func(value []byte, stable bool) error
	callback  func(value []byte) error

	// One fold worker exclusively owns each context, so these counters stay
	// non-atomic on the read hot path. Close aggregates them into process metrics
	// once after all sibling workers have joined.
	overlayResolved uint64
	cacheResolved   uint64
	durableReads    uint64
	durableHits     uint64
	trunkCached     uint64
	trunkDurable    uint64
	windowCached    uint64
	depthCached     [4]uint64
	depthDurable    [4]uint64
	prefetchPlanned uint64
	prefetchOverlay uint64
	prefetchCache   uint64
	prefetchDurable uint64
	prefetchHits    uint64
}

func newCommitmentParentReadContext() any {
	ctx := new(commitmentParentReadContext)
	ctx.callback = ctx.consume
	return ctx
}

var commitmentParentReadContextPool = sync.Pool{New: newCommitmentParentReadContext}

func borrowCommitmentParentReadContexts(session *commitmentParentReadSession, readers int) []*commitmentParentReadContext {
	contexts := make([]*commitmentParentReadContext, readers)
	for i := range contexts {
		ctx := commitmentParentReadContextPool.Get().(*commitmentParentReadContext)
		ctx.session = session
		contexts[i] = ctx
	}
	return contexts
}

func returnCommitmentParentReadContexts(contexts []*commitmentParentReadContext) {
	for i, ctx := range contexts {
		ctx.session = nil
		ctx.key = nil
		ctx.epoch = baseReadCacheEpoch{}
		ctx.cacheable = false
		ctx.prefetch = false
		ctx.fn = nil
		ctx.overlayResolved = 0
		ctx.cacheResolved = 0
		ctx.durableReads = 0
		ctx.durableHits = 0
		ctx.trunkCached = 0
		ctx.trunkDurable = 0
		ctx.windowCached = 0
		ctx.depthCached = [4]uint64{}
		ctx.depthDurable = [4]uint64{}
		ctx.prefetchPlanned = 0
		ctx.prefetchOverlay = 0
		ctx.prefetchCache = 0
		ctx.prefetchDurable = 0
		ctx.prefetchHits = 0
		commitmentParentReadContextPool.Put(ctx)
		contexts[i] = nil
	}
}

func (ctx *commitmentParentReadContext) consume(value []byte) error {
	s := ctx.session
	if ctx.cacheable && s.cache.version.Load() == s.cacheVersion {
		if ctx.prefetch {
			s.cache.prefetchIfEpoch(ctx.key, value, ctx.epoch)
		} else {
			s.cache.storeIfEpoch(ctx.key, value, ctx.epoch)
		}
	}
	if ctx.prefetch {
		return nil
	}
	return ctx.fn(value, false)
}

var _ pointread.CommitmentParentViewer = (*LayerView)(nil)
var _ pointread.CommitmentParentView = (*commitmentParentView)(nil)
var _ pointread.CommitmentParentSessioner = (*LayerView)(nil)
var _ pointread.CommitmentParentSession = (*commitmentParentReadSession)(nil)
var _ pointread.CommitmentParentPrefetchSession = (*commitmentParentReadSession)(nil)

// BeginDurableCommitmentRebuild leases the durable base for the exceptional
// full commitment-branch bootstrap. Branch rows are a derived index, so the
// rebuild may stream them directly to disk instead of retaining the complete
// table in one rewindable block layer. The latest-domain root and the current
// block's incremental branch mutations still go through this LayerView.
//
// Direct streaming is safe only at the oldest in-flight layer with no
// committed overlays. Holding flushMu keeps that empty committed cut stable
// until release. A crash can leave an incomplete legacy branch table, but not
// a published root/marker; the next fold detects the root mismatch and repeats
// the rebuild from authoritative latest-domain rows.
//
// This method is intentionally discovered through a narrow structural
// interface in state/domains; ordinary blockbuffer callers must not bypass
// the rewindable layer.
func (v *LayerView) BeginDurableCommitmentRebuild() (ethdb.KeyValueStore, func(), bool) {
	if v == nil || v.b == nil || v.l == nil {
		return nil, nil, false
	}
	base, ok := v.b.base.(ethdb.KeyValueStore)
	if !ok {
		return nil, nil, false
	}

	v.b.flushMu.Lock()
	v.b.mu.RLock()
	eligible := len(v.b.layers) == 0 && len(v.b.inflight) > 0 &&
		v.b.inflight[0] == v.l && v.l.state == layerInflight
	v.b.mu.RUnlock()
	if !eligible {
		v.b.flushMu.Unlock()
		return nil, nil, false
	}

	var once sync.Once
	release := func() {
		once.Do(func() {
			v.b.clearBaseReadCache()
			v.b.flushMu.Unlock()
		})
	}
	return base, release, true
}

// NewCommitmentParentView holds the durable engine's lifecycle read lease for
// one fold. It deliberately does not pin a Pebble snapshot or reuse iterators:
// each exact-key lookup retains DB.Get's point-read/Bloom-filter behaviour.
func (v *LayerView) NewCommitmentParentView() (pointread.CommitmentParentView, error) {
	if v == nil || v.b == nil || v.b.base == nil {
		return nil, nil
	}
	factory, ok := v.b.base.(pointread.Viewer)
	if !ok {
		return nil, nil
	}
	base, err := factory.NewPointReadView()
	if err != nil {
		return nil, err
	}
	return &commitmentParentView{b: v.b, l: v.l, base: base}, nil
}

func (v *commitmentParentView) GetKeyParts(first, second []byte, fn func(value []byte, stable bool) error) (bool, error) {
	if v == nil || v.b == nil || v.base == nil {
		return false, errors.New("blockbuffer: closed commitment parent view")
	}
	total := len(first) + len(second)
	if total > splitReadKeyStackSize {
		key := make([]byte, 0, total)
		key = append(key, first...)
		key = append(key, second...)
		return v.get(key, fn)
	}
	keyBuf := borrowSplitReadKey()
	defer returnSplitReadKey(keyBuf)
	key := keyBuf[:total]
	n := copy(key, first)
	copy(key[n:], second)
	return v.get(key, fn)
}

func (v *commitmentParentView) get(key []byte, fn func(value []byte, stable bool) error) (bool, error) {
	view := v.b.loadReadView()
	keyHash := layerBloomHashBytes(key)
	if value, found, tomb := lookupLayersNewest(olderInflightLayers(view, v.l), key, keyHash); tomb {
		return false, nil
	} else if found {
		return true, fn(value, true)
	}
	if value, found, tomb := lookupLayersNewest(view.layers, key, keyHash); tomb {
		return false, nil
	} else if found {
		return true, fn(value, true)
	}
	cache := view.baseReadCache
	var epoch baseReadCacheEpoch
	if cache != nil {
		cached, present, observed, err := cache.viewWithEpoch(key, fn)
		if cached {
			return present, err
		} else {
			epoch = observed
		}
	}
	ctx := borrowScopedBaseViewContext(cache, key, epoch, fn)
	err := v.base.Get(key, ctx.callback)
	called := ctx.called
	callbackErr := ctx.callbackErr
	returnScopedBaseViewContext(ctx)
	if called {
		if callbackErr != nil {
			return true, callbackErr
		}
		return true, err
	}
	if isKeyNotFound(v.b.base, err) {
		if cache != nil {
			cache.setMissingIfEpoch(key, epoch)
		}
		return false, nil
	}
	return false, err
}

func (v *commitmentParentView) Close() error {
	if v == nil || v.base == nil {
		return nil
	}
	err := v.base.Close()
	v.base = nil
	v.b = nil
	v.l = nil
	return err
}

// NewCommitmentParentReadSession captures the older in-flight layers, committed
// overlay topology, and a durable Pebble snapshot as one parent-state cut.
// Holding b.mu.RLock across
// both captures is sufficient: FlushUpTo cannot drop a layer until it acquires
// b.mu.Lock. If its durable batch lands before the snapshot, the retained layer
// merely overlays the same final values/tombstones; if it lands afterwards, the
// retained layer supplies the writes absent from the older snapshot. Avoiding
// flushMu here lets the commitment worker begin its fold while a background
// flush is doing disk I/O instead of serializing the two independent paths.
//
// The returned session deliberately excludes this LayerView's bound in-flight
// layer and all newer layers. Same-nibble lane ordering makes live writes in
// retained older layers the exact parent version. Unsupported durable stores
// return (nil, nil), and rawdb falls back to the ordinary point-read view.
func (v *LayerView) NewCommitmentParentReadSession(readers int) (pointread.CommitmentParentSession, error) {
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
	session := &commitmentParentReadSession{
		layers:       layers,
		inflight:     inflight,
		cache:        cache,
		cacheVersion: cacheVersion,
		snapshot:     snapshot,
		cursors:      make([]pointread.Cursor, readers),
		keyScratch:   borrowCommitmentParentKeyScratch(readers),
	}
	session.readContexts = borrowCommitmentParentReadContexts(session, readers)
	return session, nil
}

func (s *commitmentParentReadSession) ViewKeyParts(reader int, first, second []byte, fn func(value []byte, stable bool) error) (bool, error) {
	if s == nil || s.snapshot == nil || reader < 0 || reader >= len(s.cursors) {
		return false, errors.New("blockbuffer: invalid commitment parent reader")
	}
	total := len(first) + len(second)
	if total > splitReadKeyStackSize {
		key := make([]byte, 0, total)
		key = append(key, first...)
		key = append(key, second...)
		return s.view(reader, first, key, fn)
	}
	start := reader * splitReadKeyStackSize
	key := (*s.keyScratch)[start : start+total]
	n := copy(key, first)
	copy(key[n:], second)
	return s.view(reader, first, key, fn)
}

// PrefetchKeyParts resolves a predicted parent branch without decoding it. A
// durable result is force-admitted through the bounded cache with a prefetch
// marker; the foreground ViewKeyParts hit clears that marker and supplies the
// ordinary CLOCK credit. Reader ownership and snapshot semantics are identical
// to ViewKeyParts.
func (s *commitmentParentReadSession) PrefetchKeyParts(reader int, first, second []byte) (bool, error) {
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

func (s *commitmentParentReadSession) prefetchKey(reader int, keyPrefix, key []byte) (bool, error) {
	ctx := s.readContexts[reader]
	ctx.prefetchPlanned++
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
	cached, present, cacheEpoch, cacheable := s.cache.probeAtVersionForPrefetch(key, s.cacheVersion)
	if cached {
		ctx.prefetchCache++
		return present, nil
	}
	cursor := s.cursors[reader]
	if cursor == nil {
		var err error
		cursor, err = s.snapshot.NewCursor(keyPrefix)
		if err != nil {
			return false, err
		}
		s.cursors[reader] = cursor
	}
	ctx.key = key
	ctx.epoch = cacheEpoch
	ctx.cacheable = cacheable
	ctx.prefetch = true
	ctx.prefetchDurable++
	found, err := cursor.View(key, ctx.callback)
	if found {
		ctx.prefetchHits++
	}
	ctx.key = nil
	ctx.epoch = baseReadCacheEpoch{}
	ctx.cacheable = false
	ctx.prefetch = false
	if err == nil && !found && cacheable && s.cache.version.Load() == s.cacheVersion {
		s.cache.prefetchMissingIfEpoch(key, cacheEpoch)
	}
	return found, err
}

func (s *commitmentParentReadSession) view(reader int, keyPrefix, key []byte, fn func(value []byte, stable bool) error) (bool, error) {
	ctx := s.readContexts[reader]
	depth := len(key) - len(keyPrefix)
	trunk := depth >= 0 && depth <= baseReadCacheTrunkDepth
	depthBucket := commitmentParentDeepDepthBucket(depth)
	keyHash := layerBloomHashBytes(key)
	if value, found, tomb := lookupLayersNewest(s.inflight, key, keyHash); tomb {
		ctx.overlayResolved++
		return false, nil
	} else if found {
		ctx.overlayResolved++
		return true, fn(value, true)
	}
	if value, found, tomb := lookupLayersNewest(s.layers, key, keyHash); tomb {
		ctx.overlayResolved++
		return false, nil
	} else if found {
		ctx.overlayResolved++
		return true, fn(value, true)
	}
	cached, present, windowHit, cacheEpoch, cacheable, err := s.cache.viewAtVersion(key, s.cacheVersion, fn)
	if cached {
		ctx.cacheResolved++
		if trunk {
			ctx.trunkCached++
		}
		if windowHit {
			ctx.windowCached++
		}
		if depthBucket >= 0 {
			ctx.depthCached[depthBucket]++
		}
		return present, err
	}
	cursor := s.cursors[reader]
	if cursor == nil {
		var err error
		cursor, err = s.snapshot.NewCursor(keyPrefix)
		if err != nil {
			return false, err
		}
		s.cursors[reader] = cursor
	}
	ctx.key = key
	ctx.epoch = cacheEpoch
	ctx.cacheable = cacheable
	ctx.fn = fn
	ctx.durableReads++
	if trunk {
		ctx.trunkDurable++
	}
	if depthBucket >= 0 {
		ctx.depthDurable[depthBucket]++
	}
	found, err := cursor.View(key, ctx.callback)
	if found {
		ctx.durableHits++
	}
	ctx.key = nil
	ctx.epoch = baseReadCacheEpoch{}
	ctx.cacheable = false
	ctx.fn = nil
	if err == nil && !found && cacheable && s.cache.version.Load() == s.cacheVersion {
		s.cache.setMissingIfEpoch(key, cacheEpoch)
	}
	return found, err
}

func (s *commitmentParentReadSession) Close() error {
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
	s.cache = nil
	var overlayResolved, cacheResolved, durableReads, durableHits, trunkCached, trunkDurable, windowCached uint64
	var prefetchPlanned, prefetchOverlay, prefetchCache, prefetchDurable, prefetchHits uint64
	var depthCached, depthDurable [4]uint64
	for _, ctx := range s.readContexts {
		overlayResolved += ctx.overlayResolved
		cacheResolved += ctx.cacheResolved
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
		for bucket := range depthCached {
			depthCached[bucket] += ctx.depthCached[bucket]
			depthDurable[bucket] += ctx.depthDurable[bucket]
		}
	}
	commitmentParentOverlayResolvedCounter.Inc(int64(overlayResolved))
	commitmentParentCacheResolvedCounter.Inc(int64(cacheResolved))
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
	for bucket := range depthCached {
		commitmentParentDepthCacheCounters[bucket].Inc(int64(depthCached[bucket]))
		commitmentParentDepthDurableCounters[bucket].Inc(int64(depthDurable[bucket]))
	}
	returnCommitmentParentReadContexts(s.readContexts)
	s.readContexts = nil
	returnCommitmentParentKeyScratch(s.keyScratch)
	s.keyScratch = nil
	return firstErr
}

func commitmentParentDeepDepthBucket(depth int) int {
	switch {
	case depth < 5:
		return -1
	case depth <= 8:
		return 0
	case depth <= 16:
		return 1
	case depth <= 32:
		return 2
	default:
		return 3
	}
}

// ConcurrentReadWriteSafe is the LayerView counterpart of Buffer's structural
// marker. Every write targets this fixed layer, while reads resolve the fixed
// layer and committed topology; both paths take the selected key shard's lock.
func (*LayerView) ConcurrentReadWriteSafe() {}

// ViewLayer returns a read/write view bound to the in-flight layer referenced
// by h. The handle must still be in flight; a view over a no-longer-in-flight
// layer reads/writes a detached layer (its writes never reach the committed
// stack), which the caller avoids by draining the worker before discarding.
func (b *Buffer) ViewLayer(h InflightHandle) *LayerView {
	return &LayerView{b: b, l: h.l}
}

// ViewLayerInto binds dst to h without allocating a separate view object. The
// caller must keep dst alive for as long as any reader or writer retains it.
// This is useful when a longer-lived job already owns suitable storage.
func (b *Buffer) ViewLayerInto(h InflightHandle, dst *LayerView) {
	if dst == nil {
		panic("blockbuffer: ViewLayerInto called with nil destination")
	}
	*dst = LayerView{b: b, l: h.l}
}

// LayerWriter returns just the write half of a LayerView (an
// ethdb.KeyValueWriter) bound to h's layer. Convenience for tail writers that
// only Put/Delete (dynProps.Flush, WriteHeadBlockHash, …).
func (b *Buffer) LayerWriter(h InflightHandle) ethdb.KeyValueWriter {
	return &LayerView{b: b, l: h.l}
}

// putInto writes (k,v) into a specific layer under the key's shard lock. Used
// by the layer-bound writer so the worker can target an older in-flight layer
// (concurrently with the foreground writing the newest one — disjoint layers,
// disjoint locks).
func (b *Buffer) putInto(l *layer, key, value []byte) {
	k := string(key)
	b.putIntoString(l, k, value)
}

// joinKeyParts constructs the immutable map key in one allocation. Building an
// intermediate []byte and then converting it to string would allocate twice.
func joinKeyParts(first, second []byte) string {
	var key strings.Builder
	key.Grow(len(first) + len(second))
	_, _ = key.Write(first)
	_, _ = key.Write(second)
	return key.String()
}

func joinKeyPartsString(first []byte, second string) string {
	var key strings.Builder
	key.Grow(len(first) + len(second))
	_, _ = key.Write(first)
	_, _ = key.WriteString(second)
	return key.String()
}

// appendStateKVLatestKey assembles rawdb's flat-latest physical key into dst.
// The schema prefix is passed by rawdb through a structural interface so this
// package does not depend on rawdb (and therefore does not create a cycle).
func appendStateKVLatestKey(dst, prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey []byte) []byte {
	dst = append(dst, prefix...)
	dst = append(dst, accountID[:]...)
	var numeric [10]byte
	binary.BigEndian.PutUint64(numeric[:8], generation)
	binary.BigEndian.PutUint16(numeric[8:], domain)
	dst = append(dst, numeric[:]...)
	return append(dst, logicalKey...)
}

// joinStateKVLatestKey constructs the immutable layer-map key in one
// allocation, avoiding rawdb's temporary physical []byte allocation.
func joinStateKVLatestKey(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey []byte) string {
	var key strings.Builder
	key.Grow(len(prefix) + common.AccountIDLength + 10 + len(logicalKey))
	_, _ = key.Write(prefix)
	_, _ = key.Write(accountID[:])
	var numeric [10]byte
	binary.BigEndian.PutUint64(numeric[:8], generation)
	binary.BigEndian.PutUint16(numeric[8:], domain)
	_, _ = key.Write(numeric[:])
	_, _ = key.Write(logicalKey)
	return key.String()
}

func (b *Buffer) putIntoKeyParts(l *layer, first, second, value []byte) {
	b.putIntoString(l, joinKeyParts(first, second), value)
}

func (b *Buffer) putIntoKeyPartsOwnedValue(l *layer, first, second, value []byte) {
	b.putIntoStringOwnedValue(l, joinKeyParts(first, second), value)
}

func (b *Buffer) putIntoKeyPartsStringOwnedValue(l *layer, first []byte, second string, value []byte) {
	b.putIntoStringOwnedValue(l, joinKeyPartsString(first, second), value)
}

// putIntoKeyPartsStringsOwnedValues publishes a batch of caller-owned values
// while packing all immutable physical map keys into one exact-size arena.
// Each unsafe string keeps that shared arena alive until its layer is dropped;
// the arena is never mutated after publication.
func (b *Buffer) putIntoKeyPartsStringsOwnedValues(l *layer, first []byte, seconds []string, values [][]byte, reserveBatches int) {
	if reserveBatches < 1 {
		reserveBatches = 1
	}
	totalSize := len(first) * len(seconds)
	for _, second := range seconds {
		totalSize += len(second)
	}
	keyArena := l.reserveOwnedKeyBytes(totalSize, reserveBatches)
	linksPtr := borrowOwnedValueBatchLinks(len(seconds))
	defer returnOwnedValueBatchLinks(linksPtr)
	links := *linksPtr
	var heads, tails, counts [layerShardCount]int
	offset := 0
	for i, second := range seconds {
		start := offset
		offset += copy(keyArena[offset:], first)
		offset += copy(keyArena[offset:], second)
		shard := layerShardIndexBytes(keyArena[start:offset])
		links[i] = ownedValueBatchLink{end: offset}
		entry := i + 1
		if tails[shard] == 0 {
			heads[shard] = entry
		} else {
			links[tails[shard]-1].next = entry
		}
		tails[shard] = entry
		counts[shard]++
	}
	for shard, head := range heads {
		if head == 0 {
			continue
		}
		s := &l.shards[shard]
		s.mu.Lock()
		// The packed branch writer can introduce many generic keys at once.
		// Invalidate a previously built sorted iterator index once per shard,
		// rather than checking membership for every emitted branch.
		if s.prefixBucketIndex != nil {
			s.prefixBucketIndex.invalidateIteratorKeys()
		}
		if !s.commitmentReserved {
			// Reserve for the number of root-sibling batches the commitment fold
			// actually started. Historical mainnet blocks commonly touch only one
			// or two siblings; assuming the maximum fan-out of 16 made every sparse
			// block allocate a mostly empty map. A dense fold still supplies 16 and
			// preserves the single-allocation behaviour of the original fast path.
			capacity := len(s.writes) + counts[shard]*reserveBatches + int(s.pendingOwnedPuts)
			reserved := make(map[string][]byte, capacity)
			for key, value := range s.writes {
				reserved[key] = value
			}
			s.writes = reserved
			s.commitmentReserved = true
		}
		for entry := head; entry != 0; entry = links[entry-1].next {
			i := entry - 1
			start := 0
			if i > 0 {
				start = links[i-1].end
			}
			end := links[i].end
			var key string
			if end > start {
				key = unsafe.String(unsafe.SliceData(keyArena[start:end]), end-start)
			}
			l.addBloomString(key)
			// This bulk path is exclusively the CommitmentBranch structured
			// writer. It cannot match the account-KV schema prefix, so avoid an
			// index-prefix check on every branch emitted by the hot fold path.
			delete(s.deletes, key)
			s.writes[key] = values[i]
		}
		s.mu.Unlock()
	}
}

func (b *Buffer) putIntoStateKVLatest(l *layer, prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey, value []byte) {
	b.putIntoString(l, joinStateKVLatestKey(prefix, accountID, generation, domain, logicalKey), value)
}

func (b *Buffer) putIntoStateKVLatestOwnedValue(l *layer, prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey, value []byte) {
	b.putIntoStringOwnedValue(l, joinStateKVLatestKey(prefix, accountID, generation, domain, logicalKey), value)
}

func (b *Buffer) putIntoString(l *layer, key string, value []byte) {
	v := append([]byte(nil), value...)
	b.putIntoStringOwnedValue(l, key, v)
}

// putIntoStringOwnedValue publishes a caller-transferred immutable value
// without copying it. Keep this separate from putIntoString so ordinary
// ethdb.Put semantics remain defensive even if the caller mutates its slice.
func (b *Buffer) putIntoStringOwnedValue(l *layer, key string, value []byte) {
	s := l.shardForString(key)
	s.mu.Lock()
	l.addBloomString(key)
	s.trackPrefixBucketKeyBeforeMutation(key)
	s.trackIteratorKeyBeforeMutation(key)
	delete(s.deletes, key)
	if s.writes == nil {
		s.writes = make(map[string][]byte)
	}
	s.writes[key] = value
	s.mu.Unlock()
}

// deleteInto tombstones key in a specific layer under the key's shard lock.
func (b *Buffer) deleteInto(l *layer, key []byte) {
	b.deleteIntoString(l, string(key))
}

func (b *Buffer) deleteIntoKeyParts(l *layer, first, second []byte) {
	b.deleteIntoString(l, joinKeyParts(first, second))
}

func (b *Buffer) deleteIntoStateKVLatest(l *layer, prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey []byte) {
	b.deleteIntoString(l, joinStateKVLatestKey(prefix, accountID, generation, domain, logicalKey))
}

func (b *Buffer) deleteIntoString(l *layer, key string) {
	s := l.shardForString(key)
	s.mu.Lock()
	l.addBloomString(key)
	s.trackPrefixBucketKeyBeforeMutation(key)
	s.trackIteratorKeyBeforeMutation(key)
	delete(s.writes, key)
	if s.deletes == nil {
		s.deletes = make(map[string]struct{})
	}
	s.deletes[key] = struct{}{}
	s.mu.Unlock()
}

func (b *Buffer) deleteRangeInto(l *layer, start, end []byte) error {
	if l == nil {
		return errors.New("blockbuffer: range delete with no target layer")
	}
	next := layerRangeDelete{start: string(start), end: string(end)}
	if next.end == "" || next.start >= next.end {
		return errors.New("blockbuffer: invalid range delete bounds")
	}

	// DeleteRange is a structural layer mutation. Publish the immutable range
	// first, then remove earlier point operations inside it. Concurrent reads
	// may observe the pre-delete point value until this method returns, but can
	// never fall through to a now-masked durable value in between the two steps.
	l.rangeDeleteMu.Lock()
	ranges := append([]layerRangeDelete(nil), l.loadRangeDeletes()...)
	ranges = append(ranges, next)
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].start < ranges[j].start })
	merged := make([]layerRangeDelete, 0, len(ranges))
	for _, candidate := range ranges {
		if len(merged) == 0 || merged[len(merged)-1].end < candidate.start {
			merged = append(merged, candidate)
			continue
		}
		last := &merged[len(merged)-1]
		if candidate.end > last.end {
			last.end = candidate.end
		}
	}
	published := append([]layerRangeDelete(nil), merged...)
	l.rangeDeletes.Store(&published)
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		for key := range s.writes {
			if next.containsString(key) {
				delete(s.writes, key)
				delete(s.durableWrites, key)
			}
		}
		for key := range s.deletes {
			if next.containsString(key) {
				delete(s.deletes, key)
			}
		}
		if s.prefixBucketIndex != nil {
			s.prefixBucketIndex.invalidateIteratorKeys()
			s.prefixBucketIndex.buckets = nil
			s.prefixBucketIndex.built = [4]uint64{}
		}
		s.mu.Unlock()
	}
	l.rangeDeleteMu.Unlock()

	// A snapshot captured before the range publication may still populate the
	// durable cache while this layer is live. Clearing here and again after the
	// successful flush keeps old base rows from reappearing after layer drop.
	b.clearBaseReadCache()
	return nil
}

func (v *LayerView) Put(key, value []byte) error {
	v.b.putInto(v.l, key, value)
	return nil
}

// PutOwnedValue is the layer-bound ownership-taking write path. The caller
// keeps value immutable; the layer may retain its backing bytes directly.
func (v *LayerView) PutOwnedValue(key, value []byte) error {
	v.b.putIntoStringOwnedValue(v.l, string(key), value)
	return nil
}

// PutStringOwnedValue is the layer-bound fixed-key write path. Both key and
// value are immutable caller-owned storage and may be retained directly.
func (v *LayerView) PutStringOwnedValue(key string, value []byte) error {
	v.b.putIntoStringOwnedValue(v.l, key, value)
	return nil
}

func (v *LayerView) Delete(key []byte) error {
	v.b.deleteInto(v.l, key)
	return nil
}

// DeleteRange is the layer-bound counterpart of Buffer.DeleteRange.
func (v *LayerView) DeleteRange(start, end []byte) error {
	return v.b.deleteRangeInto(v.l, start, end)
}

// PutKeyParts implements rawdb's optional split-key writer path. It is public
// only so a structural interface in rawdb can discover it without introducing
// a package dependency; callers should otherwise use Put.
func (v *LayerView) PutKeyParts(first, second, value []byte) error {
	v.b.putIntoKeyParts(v.l, first, second, value)
	return nil
}

// PutKeyPartsOwnedValue is the split-key write path for a freshly encoded
// immutable value. The caller transfers value ownership to the layer.
func (v *LayerView) PutKeyPartsOwnedValue(first, second, value []byte) error {
	v.b.putIntoKeyPartsOwnedValue(v.l, first, second, value)
	return nil
}

// PutKeyPartsStringOwnedValue is the branch-batch counterpart of
// PutKeyPartsOwnedValue. The caller already owns the logical suffix as an
// immutable map string, so joining it directly avoids a temporary []byte.
func (v *LayerView) PutKeyPartsStringOwnedValue(first []byte, second string, value []byte) error {
	v.b.putIntoKeyPartsStringOwnedValue(v.l, first, second, value)
	return nil
}

// PutKeyPartsStringsOwnedValues is the sibling-fold batch counterpart of
// PutKeyPartsStringOwnedValue. The caller transfers immutable values; this
// layer owns one shared arena containing every joined physical key.
func (v *LayerView) PutKeyPartsStringsOwnedValues(first []byte, seconds []string, values [][]byte) error {
	return v.PutKeyPartsStringsOwnedValuesWithBatchCount(first, seconds, values, 1)
}

// PutKeyPartsStringsOwnedValuesWithBatchCount additionally supplies the
// number of sibling batches expected to publish into this layer. It lets the
// first batch reserve accurately for sparse and dense commitment folds alike.
func (v *LayerView) PutKeyPartsStringsOwnedValuesWithBatchCount(first []byte, seconds []string, values [][]byte, batchCount int) error {
	if len(seconds) != len(values) {
		return errors.New("blockbuffer: key/value batch length mismatch")
	}
	v.b.putIntoKeyPartsStringsOwnedValues(v.l, first, seconds, values, batchCount)
	return nil
}

// DeleteKeyParts is the delete counterpart of PutKeyParts.
func (v *LayerView) DeleteKeyParts(first, second []byte) error {
	v.b.deleteIntoKeyParts(v.l, first, second)
	return nil
}

// PutStateKVLatest implements rawdb's structured flat-latest writer path.
// The value keeps ordinary Put's defensive-copy ownership contract.
func (v *LayerView) PutStateKVLatest(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey, value []byte) error {
	v.b.putIntoStateKVLatest(v.l, prefix, accountID, generation, domain, logicalKey, value)
	return nil
}

// PutStateKVLatestOwnedValue is the structured ownership-taking write path.
func (v *LayerView) PutStateKVLatestOwnedValue(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey, value []byte) error {
	v.b.putIntoStateKVLatestOwnedValue(v.l, prefix, accountID, generation, domain, logicalKey, value)
	return nil
}

// DeleteStateKVLatest is the structured flat-latest delete counterpart.
func (v *LayerView) DeleteStateKVLatest(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey []byte) error {
	v.b.deleteIntoStateKVLatest(v.l, prefix, accountID, generation, domain, logicalKey)
	return nil
}

// Get resolves key over [bound layer, committed stack newest-first, base].
// One immutable read view keeps the committed slice stable; each layer's
// matching map shard (including the bound in-flight layer, which the worker
// writes via putInto) is read under its own shard lock via lookup.
func (v *LayerView) Get(key []byte) ([]byte, error) {
	value, present, err := v.GetWithPresence(key)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, ErrNotFound
	}
	return value, nil
}

// GetWithPresence is the layer-bound atomic point-read counterpart of Get.
// It keeps the bound layer and one published committed topology together for
// rawdb metadata readers used by asynchronous block execution.
func (v *LayerView) GetWithPresence(key []byte) ([]byte, bool, error) {
	b := v.b
	view := b.loadReadView()
	keyHash := layerBloomHashBytes(key)
	val, found, tomb := v.l.lookupHash(key, keyHash)
	if tomb {
		return nil, false, nil
	}
	if found {
		out := append([]byte(nil), val...)
		return out, true, nil
	}
	if val, found, tomb = lookupLayersNewest(view.layers, key, keyHash); tomb {
		return nil, false, nil
	} else if found {
		return append([]byte(nil), val...), true, nil
	}
	return readBaseWithPresence(b.base, key)
}

func (v *LayerView) IsKeyNotFound(err error) bool {
	if errors.Is(err, ErrNotFound) {
		return true
	}
	return v != nil && v.b != nil && v.b.IsKeyNotFound(err)
}

// GetNoCopy is Get without the defensive value copy for hits in the bound or
// committed layers. It deliberately has the same visibility as Get — the bound
// layer first, then committed layers newest-first, never another in-flight
// layer — and falls back to the base reader unchanged.
//
// The returned slice aliases immutable-by-replacement layer storage and must
// not be mutated. Replacement never changes the old backing bytes, so the
// commitment fold can borrow decoded leaf-key fields until its synchronous
// descent finishes. Implementing this optional rawdb fast-path on LayerView
// matters for async commit, where every fold is bound to a specific in-flight
// layer rather than reading through Buffer.GetNoCopy directly.
func (v *LayerView) GetNoCopy(key []byte) ([]byte, error) {
	return v.getNoCopy(key, false)
}

// GetNoCopyCached is GetNoCopy plus the Buffer's bounded durable-base cache.
// It is consumed by rawdb flat-latest and commitment branch reads; the bound
// and committed overlays still take precedence and are never inserted into the
// base cache.
func (v *LayerView) GetNoCopyCached(key []byte) ([]byte, error) {
	return v.getNoCopy(key, true)
}

func (v *LayerView) getNoCopy(key []byte, cacheBase bool) ([]byte, error) {
	b := v.b
	view := b.loadReadView()
	keyHash := layerBloomHashBytes(key)
	val, found, tomb := v.l.lookupHash(key, keyHash)
	if tomb {
		return nil, ErrNotFound
	}
	if found {
		return val, nil
	}
	if val, found, tomb = lookupLayersNewest(view.layers, key, keyHash); tomb {
		return nil, ErrNotFound
	} else if found {
		return val, nil
	}
	cache := view.baseReadCache
	if b.base == nil {
		return nil, ErrNotFound
	}
	var cacheEpoch baseReadCacheEpoch
	if cacheBase && cache != nil {
		if value, ok, epoch := cache.getWithEpoch(key); ok {
			if value == nil {
				return nil, ErrNotFound
			}
			return value, nil
		} else {
			cacheEpoch = epoch
		}
	}
	if !cacheBase || cache == nil {
		return readBaseValue(b.base, key)
	}
	return readBaseIntoCache(b.base, cache, key, cacheEpoch)
}

// GetNoCopyCachedKeyParts is the split-key counterpart of GetNoCopyCached for
// the async commit worker's layer-bound view.
func (v *LayerView) GetNoCopyCachedKeyParts(first, second []byte) ([]byte, error) {
	total := len(first) + len(second)
	if total > splitReadKeyStackSize {
		key := make([]byte, 0, total)
		key = append(key, first...)
		key = append(key, second...)
		return v.getNoCopy(key, true)
	}

	var stack [splitReadKeyStackSize]byte
	key := stack[:total]
	n := copy(key, first)
	copy(key[n:], second)
	return v.getNoCopyCachedStackKey(key)
}

// GetNoCopyCachedStateAccountLatest is the layer-bound typed account-latest
// read path. See Buffer.GetNoCopyCachedStateAccountLatest.
func (v *LayerView) GetNoCopyCachedStateAccountLatest(prefix []byte, accountID common.AccountID) ([]byte, error) {
	total := len(prefix) + common.AccountIDLength
	if total > splitReadKeyStackSize {
		key := make([]byte, 0, total)
		key = append(key, prefix...)
		key = append(key, accountID[:]...)
		return v.getNoCopy(key, true)
	}

	var stack [splitReadKeyStackSize]byte
	key := stack[:total]
	n := copy(key, prefix)
	copy(key[n:], accountID[:])
	return v.getNoCopyCachedStackKey(key)
}

// ViewNoCopyCachedKeyParts is the layer-bound callback counterpart of
// GetNoCopyCachedKeyParts. See Buffer.ViewNoCopyCachedKeyParts for the stable
// lifetime contract.
func (v *LayerView) ViewNoCopyCachedKeyParts(first, second []byte, fn func(value []byte, stable bool) error) (bool, error) {
	total := len(first) + len(second)
	if total > splitReadKeyStackSize {
		key := make([]byte, 0, total)
		key = append(key, first...)
		key = append(key, second...)
		return v.viewNoCopyCachedKey(key, fn)
	}

	keyBuf := borrowSplitReadKey()
	defer returnSplitReadKey(keyBuf)
	key := keyBuf[:total]
	n := copy(key, first)
	copy(key[n:], second)
	return v.viewNoCopyCachedKey(key, fn)
}

// ViewCommitmentParentKeyParts is the async commitment fold's parent-state
// reader. It deliberately skips this view's own and newer in-flight layers and
// resolves older in-flight commitment writes, committed layers, then the durable
// base. The committing block's layer contains its flat-state writes but no
// pre-block commitment branches. Same-nibble ordered folds may publish the exact
// parent branch into an older in-flight layer before that whole block is ready
// to promote; all other in-flight layers remain invisible.
//
// This narrow method is discovered structurally by rawdb and must not replace
// ordinary LayerView reads: callers that need read-your-own-writes continue to
// use ViewNoCopyCachedKeyParts.
func (v *LayerView) ViewCommitmentParentKeyParts(first, second []byte, fn func(value []byte, stable bool) error) (bool, error) {
	total := len(first) + len(second)
	if total > splitReadKeyStackSize {
		key := make([]byte, 0, total)
		key = append(key, first...)
		key = append(key, second...)
		return v.viewCommitmentParentKey(key, fn)
	}

	keyBuf := borrowSplitReadKey()
	defer returnSplitReadKey(keyBuf)
	key := keyBuf[:total]
	n := copy(key, first)
	copy(key[n:], second)
	return v.viewCommitmentParentKey(key, fn)
}

func (v *LayerView) viewCommitmentParentKey(key []byte, fn func(value []byte, stable bool) error) (bool, error) {
	b := v.b
	view := b.loadReadView()
	keyHash := layerBloomHashBytes(key)
	if value, found, tomb := lookupLayersNewest(olderInflightLayers(view, v.l), key, keyHash); tomb {
		return false, nil
	} else if found {
		return true, fn(value, true)
	}
	if value, found, tomb := lookupLayersNewest(view.layers, key, keyHash); tomb {
		return false, nil
	} else if found {
		return true, fn(value, true)
	}
	if b.base == nil {
		return false, nil
	}
	cache := view.baseReadCache
	var cacheEpoch baseReadCacheEpoch
	if cache != nil {
		cached, present, epoch, err := cache.viewWithEpoch(key, fn)
		if cached {
			return present, err
		} else {
			cacheEpoch = epoch
		}
	}
	return viewBaseIntoCache(b.base, cache, key, cacheEpoch, fn)
}

func olderInflightLayers(view *bufferReadView, bound *layer) []*layer {
	if view == nil || bound == nil {
		return nil
	}
	for i, candidate := range view.inflight {
		if candidate == bound {
			return view.inflight[:i]
		}
	}
	return nil
}

func (v *LayerView) viewNoCopyCachedKey(key []byte, fn func(value []byte, stable bool) error) (bool, error) {
	b := v.b
	view := b.loadReadView()
	keyHash := layerBloomHashBytes(key)
	value, found, tomb := v.l.lookupHash(key, keyHash)
	if tomb {
		return false, nil
	}
	if found {
		return true, fn(value, true)
	}
	if value, found, tomb = lookupLayersNewest(view.layers, key, keyHash); tomb {
		return false, nil
	} else if found {
		return true, fn(value, true)
	}
	if b.base == nil {
		return false, nil
	}
	cache := view.baseReadCache
	var cacheEpoch baseReadCacheEpoch
	if cache != nil {
		cached, present, epoch, err := cache.viewWithEpoch(key, fn)
		if cached {
			return present, err
		} else {
			cacheEpoch = epoch
		}
	}
	return viewBaseIntoCache(b.base, cache, key, cacheEpoch, fn)
}

// GetNoCopyCachedStateKVLatest implements rawdb's structured flat-latest read
// path. Normal storage keys fit in the fixed stack buffer; only genuinely long
// logical keys allocate, preserving the generic reader's behaviour.
func (v *LayerView) GetNoCopyCachedStateKVLatest(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey []byte) ([]byte, error) {
	total := len(prefix) + common.AccountIDLength + 10 + len(logicalKey)
	if total > splitReadKeyStackSize {
		key := make([]byte, 0, total)
		key = appendStateKVLatestKey(key, prefix, accountID, generation, domain, logicalKey)
		return v.getNoCopy(key, true)
	}

	var stack [splitReadKeyStackSize]byte
	key := appendStateKVLatestKey(stack[:0], prefix, accountID, generation, domain, logicalKey)
	return v.getNoCopyCachedStackKey(key)
}

// getNoCopyCachedStackKey resolves a key backed by caller stack storage. A
// durable miss uses the shared pooled scratch-key path, avoiding both escape of
// the caller's fixed array and one temporary heap object per base read.
func (v *LayerView) getNoCopyCachedStackKey(key []byte) ([]byte, error) {
	b := v.b
	view := b.loadReadView()
	keyHash := layerBloomHashBytes(key)
	value, found, tomb := v.l.lookupHash(key, keyHash)
	if tomb {
		return nil, ErrNotFound
	}
	if found {
		return value, nil
	}
	if value, found, tomb = lookupLayersNewest(view.layers, key, keyHash); tomb {
		return nil, ErrNotFound
	} else if found {
		return value, nil
	}
	if b.base == nil {
		return nil, ErrNotFound
	}
	cache := view.baseReadCache
	var cacheEpoch baseReadCacheEpoch
	if cache != nil {
		if cached, ok, epoch := cache.getWithEpoch(key); ok {
			if cached == nil {
				return nil, ErrNotFound
			}
			return cached, nil
		} else {
			cacheEpoch = epoch
		}
	}
	return readBaseIntoCachePooledKey(b.base, cache, key, cacheEpoch)
}

// Has reports existence over [bound layer, committed stack, base].
func (v *LayerView) Has(key []byte) (bool, error) {
	b := v.b
	view := b.loadReadView()
	keyHash := layerBloomHashBytes(key)
	if _, found, tomb := v.l.lookupHash(key, keyHash); tomb {
		return false, nil
	} else if found {
		return true, nil
	}
	if _, found, tomb := lookupLayersNewest(view.layers, key, keyHash); tomb {
		return false, nil
	} else if found {
		return true, nil
	}
	if b.base == nil {
		return false, nil
	}
	return b.base.Has(key)
}

// NewIterator iterates [bound layer, committed stack newest-first, base],
// skipping all other in-flight layers. Reuses the Buffer's overlay+base merge.
func (v *LayerView) NewIterator(prefix, start []byte) ethdb.Iterator {
	b := v.b
	view := b.loadReadView()
	overlay := newOverlayState()
	overlay.walk(v.l, prefix, start)
	for i := len(view.layers) - 1; i >= 0; i-- {
		overlay.walk(view.layers[i], prefix, start)
	}
	return b.finishIterator(overlay, prefix, start)
}

// NewStateKVLatestIterator is the layer-bound form of Buffer's structured
// account-KV iterator. It includes the bound worker layer and committed stack,
// but excludes other in-flight layers exactly like LayerView.NewIterator.
func (v *LayerView) NewStateKVLatestIterator(schemaPrefix []byte, accountID common.AccountID, physicalPrefix []byte) ethdb.Iterator {
	b := v.b
	view := b.loadReadView()
	overlay := newOverlayState()
	schema := string(schemaPrefix)
	physical := unsafe.String(unsafe.SliceData(physicalPrefix), len(physicalPrefix))
	overlay.walkPrefixBucket(v.l, schema, accountID[0], physical)
	for i := len(view.layers) - 1; i >= 0; i-- {
		overlay.walkPrefixBucket(view.layers[i], schema, accountID[0], physical)
	}
	return b.finishIterator(overlay, physicalPrefix, nil)
}
