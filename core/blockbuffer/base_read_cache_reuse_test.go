package blockbuffer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
	"unsafe"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func reuseObserverKey(depth int, delta bool, id uint32) []byte {
	prefix := []byte(rawdb.CommitmentBranchKeyPrefix)
	if delta {
		prefix = make([]byte, len(rawdb.CommitmentBranchDeltaKeyPrefix)+8)
		copy(prefix, rawdb.CommitmentBranchDeltaKeyPrefix)
		binary.BigEndian.PutUint64(prefix[len(prefix)-8:], 23)
	}
	key := append(prefix, make([]byte, depth)...)
	for nibble := 0; nibble < depth; nibble++ {
		key[len(key)-1-nibble] = byte(id>>(4*nibble)) & 15
	}
	return key
}

func sampledReuseObserverKey(t testing.TB, depth int, delta bool, accept func([]byte, uint32) bool) []byte {
	t.Helper()
	for id := uint32(0); id < 1<<22; id++ {
		key := reuseObserverKey(depth, delta, id)
		index, sampled := baseReadCacheReuseIndex(string(key))
		if sampled && (accept == nil || accept(key, index)) {
			return key
		}
	}
	t.Fatal("could not find a deterministic sampled physical key")
	return nil
}

func enrollReuseObserverKey(t testing.TB, cache *baseReadCache, key []byte, cohort uint8) *baseReadCacheEntry {
	t.Helper()
	_, cached, epoch := cache.getWithEpoch(key)
	if cached {
		t.Fatal("enrollment fixture unexpectedly resident")
	}
	stored := cache.storeIfEpoch(key, []byte("old"), epoch)
	if stored != (cohort == baseReadCacheReuseWindowFlush) {
		t.Fatalf("first admission=%v for cohort %d", stored, cohort)
	}
	cache.setFlushed(string(key), []byte("new"))
	shard := &cache.shards[baseReadCacheShardIndex(key)]
	if cohort == baseReadCacheReuseWindowFlush {
		shard.mu.Lock()
		if !shard.evictWindowOne() {
			t.Fatal("FIFO did not consume its real token")
		}
		shard.mu.Unlock()
	}
	entry := shard.entries[string(key)]
	if entry == nil || entry.window {
		t.Fatal("flush cohort did not reach tail")
	}
	return entry
}

func assertReuseConservation(t testing.TB, cache *baseReadCache) baseReadCacheReuseStats {
	t.Helper()
	stats := cache.stats().reuse
	for depth := range stats.cohorts {
		for cohort, got := range stats.cohorts[depth] {
			censored, completedSources, liveSources := uint64(0), uint64(0), uint64(0)
			for _, count := range got.censored {
				censored += count
			}
			for source, count := range got.completedSources {
				completedSources += count
				liveSources += got.liveSources[source]
			}
			if got.samples != got.completed+censored+got.live || got.completed != completedSources || got.live != liveSources+got.liveMissing || got.liveMissing != 0 {
				t.Fatalf("depth %d cohort %d conservation failed: %+v", depth+6, cohort, got)
			}
			if got.sampledInitialCharge != got.completedInitialCharge+got.censoredInitialCharge+got.liveInitialCharge {
				t.Fatalf("depth %d cohort %d initial-charge conservation failed: %+v", depth+6, cohort, got)
			}
		}
	}
	return stats
}

func TestBaseReadCacheReuseObserverOptInAndMemoryBound(t *testing.T) {
	t.Setenv("GTRON_BASE_CACHE_REUSE_OBSERVER", "")
	off := newBaseReadCache(1 << 20)
	t.Setenv("GTRON_BASE_CACHE_REUSE_OBSERVER", "1")
	on := newBaseReadCache(1 << 20)
	t.Setenv("GTRON_BASE_CACHE_REUSE_OBSERVER", "0")
	other := newBaseReadCache(1 << 20)
	if off.reuse != nil || on.reuse == nil || other.reuse != nil || off.reuseOwnerID == 0 || off.reuseOwnerID == on.reuseOwnerID || on.reuseOwnerID == other.reuseOwnerID {
		t.Fatal("constructor opt-in or owner identity is incorrect")
	}
	if on.stats().reuse.enabled != true || off.stats().reuse.enabled {
		t.Fatal("environment was re-read after owner construction")
	}
	if unsafe.Sizeof(baseReadCacheEntry{}) != 80 || baseReadCacheReuseMetadataBytes() > baseReadCacheReuseMemoryLimit {
		t.Fatalf("entry=%d observer metadata=%d", unsafe.Sizeof(baseReadCacheEntry{}), baseReadCacheReuseMetadataBytes())
	}
	var containsPointers func(reflect.Type) bool
	containsPointers = func(typ reflect.Type) bool {
		switch typ.Kind() {
		case reflect.Array:
			return containsPointers(typ.Elem())
		case reflect.Struct:
			for i := 0; i < typ.NumField(); i++ {
				if containsPointers(typ.Field(i).Type) {
					return true
				}
			}
		case reflect.Pointer, reflect.UnsafePointer, reflect.String, reflect.Slice, reflect.Map, reflect.Interface, reflect.Func, reflect.Chan:
			return true
		}
		return false
	}
	if containsPointers(reflect.TypeFor[baseReadCacheReuseObserver]()) {
		t.Fatal("observer retains pointer-bearing metadata")
	}
	t.Logf("entry_bytes=%d slot_bytes=%d observer_metadata_bytes=%d total_slots=%d", unsafe.Sizeof(baseReadCacheEntry{}), unsafe.Sizeof(baseReadCacheReuseSlot{}), baseReadCacheReuseMetadataBytes(), baseReadCacheShardCount*baseReadCacheReuseSlots)
}

func TestBaseReadCacheReuseObserverCohortsAndSources(t *testing.T) {
	t.Setenv("GTRON_BASE_CACHE_REUSE_OBSERVER", "1")
	for _, cohort := range []uint8{baseReadCacheReuseWindowFlush, baseReadCacheReuseFlushAdmission} {
		for _, depth := range []int{6, 7} {
			for _, delta := range []bool{false, true} {
				for source := uint32(0); source < baseReadCacheReuseSources; source++ {
					t.Run(fmt.Sprintf("cohort_%d/depth_%d/delta_%v/source_%d", cohort, depth, delta, source), func(t *testing.T) {
						trunk := -1
						if cohort == baseReadCacheReuseWindowFlush {
							trunk = 4
						}
						cache := newBaseReadCacheWithTrunk(4<<20, trunk, rawdb.CommitmentBranchKeyPrefix)
						key := sampledReuseObserverKey(t, depth, delta, nil)
						entry := enrollReuseObserverKey(t, cache, key, cohort)
						if source&1 != 0 {
							cache.getWithEpoch(key)
						}
						if source&2 != 0 {
							cache.probeAtVersionForPrefetch(key, cache.version.Load())
						}
						live := assertReuseConservation(t, cache).cohorts[depth-6][cohort]
						if live.samples != 1 || live.live != 1 || live.completed != 0 || live.liveSources[source] != 1 {
							t.Fatalf("unretired cohort misclassified: %+v", live)
						}
						shard := &cache.shards[baseReadCacheShardIndex(key)]
						for entry.live {
							shard.mu.Lock()
							if !shard.evictOne(false) {
								t.Fatal("tail lost its token")
							}
							shard.mu.Unlock()
							got := assertReuseConservation(t, cache).cohorts[depth-6][cohort]
							if entry.live && got.completed != 0 {
								t.Fatal("CLOCK second chance was counted as completion")
							}
						}
						completed := assertReuseConservation(t, cache).cohorts[depth-6][cohort]
						if completed.completed != 1 || completed.completedSources[source] != 1 || completed.live != 0 {
							t.Fatalf("capacity completion sources=%+v", completed)
						}
					})
				}
			}
		}
	}
}

func TestBaseReadCacheReuseObserverCensorPaths(t *testing.T) {
	t.Setenv("GTRON_BASE_CACHE_REUSE_OBSERVER", "1")
	for _, reason := range []int{baseReadCacheReuseDeleted, baseReadCacheReuseOversized, baseReadCacheReuseCleared, baseReadCacheReuseReplaced} {
		t.Run(fmt.Sprint(reason), func(t *testing.T) {
			cache := newBaseReadCache(4<<20, rawdb.CommitmentBranchKeyPrefix)
			key := sampledReuseObserverKey(t, 7, true, nil)
			entry := enrollReuseObserverKey(t, cache, key, baseReadCacheReuseFlushAdmission)
			shard := &cache.shards[baseReadCacheShardIndex(key)]
			switch reason {
			case baseReadCacheReuseDeleted:
				cache.del(key)
			case baseReadCacheReuseOversized:
				cache.setFlushed(string(key), make([]byte, shard.limit+1))
			case baseReadCacheReuseCleared:
				cache.clear()
			case baseReadCacheReuseReplaced:
				shard.mu.Lock()
				shard.observeReuseAdmission(entry, baseReadCacheReuseFlushAdmission)
				shard.mu.Unlock()
			}
			got := assertReuseConservation(t, cache).cohorts[1][baseReadCacheReuseFlushAdmission]
			if got.censored[reason] != 1 || got.completed != 0 {
				t.Fatalf("censor reason %d = %+v", reason, got)
			}
			if reason == baseReadCacheReuseReplaced {
				if got.samples != 2 || got.live != 1 {
					t.Fatal("same-key new episode did not replace the previous sample")
				}
				return
			}
			if got.samples != 1 || got.live != 0 {
				t.Fatal("censored sample remains live")
			}
			shard.mu.Lock()
			shard.evictOne(false) // any old token is stale, not another completion
			shard.mu.Unlock()
			if assertReuseConservation(t, cache).cohorts[1][baseReadCacheReuseFlushAdmission] != got {
				t.Fatal("stale token recycling changed completed/censored samples")
			}
		})
	}
}

func TestBaseReadCacheReuseObserverCompleteKeyCollision(t *testing.T) {
	t.Setenv("GTRON_BASE_CACHE_REUSE_OBSERVER", "1")
	cache := newBaseReadCache(4<<20, rawdb.CommitmentBranchKeyPrefix)
	first := sampledReuseObserverKey(t, 7, true, nil)
	index, _ := baseReadCacheReuseIndex(string(first))
	shardID := baseReadCacheShardIndex(first)
	second := sampledReuseObserverKey(t, 7, true, func(key []byte, candidate uint32) bool {
		return candidate == index && baseReadCacheShardIndex(key) == shardID && !bytes.Equal(first, key)
	})
	enrollReuseObserverKey(t, cache, first, baseReadCacheReuseFlushAdmission)
	enrollReuseObserverKey(t, cache, second, baseReadCacheReuseFlushAdmission)
	got := assertReuseConservation(t, cache).cohorts[1][baseReadCacheReuseFlushAdmission]
	if got.samples != 2 || got.censored[baseReadCacheReuseCollision] != 1 || got.live != 1 {
		t.Fatalf("collision accounting: %+v", got)
	}
	// The old entry is still live in the cache. Its retirement must not close
	// the different complete key now occupying the same observer slot.
	shard := &cache.shards[shardID]
	shard.mu.Lock()
	shard.evictOne(false)
	shard.mu.Unlock()
	if assertReuseConservation(t, cache).cohorts[1][baseReadCacheReuseFlushAdmission] != got {
		t.Fatal("retirement confused a colliding physical key with the sample")
	}
	// Inline comparison also includes all eight generation bytes. A different
	// physical generation of the same path must not match this live record.
	otherGeneration := bytes.Clone(second)
	otherGeneration[len(rawdb.CommitmentBranchDeltaKeyPrefix)+7]++
	if shard.reuse.slots[index].matches(string(otherGeneration)) {
		t.Fatal("generation bytes were omitted from full physical key equality")
	}
	cache.del(second)
	assertReuseConservation(t, cache)
}

func TestBaseReadCacheReuseObserverSamplingAndLiveCharge(t *testing.T) {
	t.Setenv("GTRON_BASE_CACHE_REUSE_OBSERVER", "1")
	cache := newBaseReadCache(4<<20, rawdb.CommitmentBranchKeyPrefix)
	seen := make(map[uint32]struct{})
	sampled := 0
	for id := uint32(0); id < 1<<16; id++ {
		key := reuseObserverKey(7, true, id)
		index, ok := baseReadCacheReuseIndex(string(key))
		if ok {
			seen[index] = struct{}{}
			sampled++
		}
	}
	if sampled < 800 || sampled > 1250 || len(seen) != baseReadCacheReuseSlots {
		t.Fatalf("sampling gate/index correlation: samples=%d distinct slots=%d", sampled, len(seen))
	}
	key := sampledReuseObserverKey(t, 6, false, nil)
	enrollReuseObserverKey(t, cache, key, baseReadCacheReuseFlushAdmission)
	before := assertReuseConservation(t, cache).cohorts[0][baseReadCacheReuseFlushAdmission]
	cache.setFlushed(string(key), bytes.Repeat([]byte{4}, 1024))
	after := assertReuseConservation(t, cache).cohorts[0][baseReadCacheReuseFlushAdmission]
	if after.samples != 1 || after.sampledInitialCharge != before.sampledInitialCharge || after.liveCurrentCharge <= before.liveCurrentCharge {
		t.Fatal("a refresh was treated as a new sample or lost current charge")
	}
	cache.clear()
	cleared := assertReuseConservation(t, cache).cohorts[0][baseReadCacheReuseFlushAdmission]
	if cleared.samples != 1 || cleared.censored[baseReadCacheReuseCleared] != 1 {
		t.Fatal("clear reset owner-lifetime counters")
	}
}

type reusePolicyEntry struct {
	Key                   string
	Value                 []byte
	Charge, ValueCapacity int
	KeyCapacity           uint32
	Version               uint64
	References            uint32
	Live, Other, Trunk    bool
	Window, Exposed       bool
}

type reusePolicyShard struct {
	Entries, Queue, OtherQueue, WindowQueue, Free []reusePolicyEntry
	Admission, OtherAdmission                     []uint64
	Integers                                      [16]int
	WindowIntegers                                [8]uint64
	Diagnostics                                   [baseReadCacheDiagnosticDepths]baseReadCacheDepthDiagnostics
}

type reusePolicyState struct {
	Version, PublishSequence uint64
	Invalidations            []uint64
	Shards                   [baseReadCacheShardCount]reusePolicyShard
}

func snapshotReusePolicy(cache *baseReadCache) reusePolicyState {
	state := reusePolicyState{Version: cache.version.Load(), PublishSequence: cache.metricsPublishSequence.Load()}
	for i := range cache.invalidations {
		state.Invalidations = append(state.Invalidations, cache.invalidations[i].Load())
	}
	entryState := func(entry *baseReadCacheEntry) reusePolicyEntry {
		if entry == nil {
			return reusePolicyEntry{}
		}
		return reusePolicyEntry{Key: entry.key, Value: bytes.Clone(entry.value), Charge: entry.charge, ValueCapacity: cap(entry.value), KeyCapacity: entry.keyCapacity, Version: entry.version, References: entry.references.Load(), Live: entry.live, Other: entry.nonCommitment, Trunk: entry.trunk, Window: entry.window, Exposed: entry.exposed.Load()}
	}
	for i := range cache.shards {
		s := &cache.shards[i]
		dst := &state.Shards[i]
		for _, entry := range s.entries {
			dst.Entries = append(dst.Entries, entryState(entry))
		}
		sort.Slice(dst.Entries, func(i, j int) bool { return dst.Entries[i].Key < dst.Entries[j].Key })
		for _, entry := range s.queue {
			dst.Queue = append(dst.Queue, entryState(entry))
		}
		for _, entry := range s.nonCommitmentQueue {
			dst.OtherQueue = append(dst.OtherQueue, entryState(entry))
		}
		for _, entry := range s.windowQueue {
			dst.WindowQueue = append(dst.WindowQueue, entryState(entry))
		}
		for entry := s.freeEntries; entry != nil; entry = entry.nextFree {
			dst.Free = append(dst.Free, entryState(entry))
		}
		dst.Admission = append([]uint64(nil), s.admission...)
		dst.OtherAdmission = append([]uint64(nil), s.nonCommitmentAdmission...)
		dst.Integers = [16]int{s.head, s.nonCommitmentHead, s.windowHead, s.used, s.nonCommitmentUsed, s.trunkUsed, s.windowUsed, s.nonCommitmentEntries, s.trunkEntries, s.windowEntries, s.limit, s.nonCommitmentLimit, s.trunkLimit, s.windowLimit, s.freeEntryCount, s.freeValueBytes}
		dst.WindowIntegers = [8]uint64{s.windowAdmissions, uint64(s.windowAdmissionCounter), uint64(s.windowProbeCandidates), uint64(s.windowProbeAdmissions), uint64(s.windowOutcomeCount), uint64(s.windowPromotions), uint64(s.windowAdmissionShift), uint64(s.windowHitEvents.Load())}
		dst.Diagnostics = s.diagnostics
	}
	return state
}

func TestBaseReadCacheReuseObserverTransparentTrace(t *testing.T) {
	t.Setenv("GTRON_BASE_CACHE_REUSE_OBSERVER", "0")
	off := newBaseReadCacheWithTrunk(256<<10, 4, rawdb.CommitmentBranchKeyPrefix)
	t.Setenv("GTRON_BASE_CACHE_REUSE_OBSERVER", "1")
	on := newBaseReadCacheWithTrunk(256<<10, 4, rawdb.CommitmentBranchKeyPrefix)
	const keys = 8192
	physicalKeys := make([][]byte, keys)
	values := make([][]byte, keys)
	for i := range physicalKeys {
		physicalKeys[i] = reuseObserverKey(6+i%2, i%3 == 0, uint32(i))
		if i%11 == 0 {
			physicalKeys[i] = []byte(fmt.Sprintf("other-state-%d", i))
		} else if i%17 == 0 {
			physicalKeys[i] = reuseObserverKey(4, false, uint32(i))
		}
		values[i] = bytes.Repeat([]byte{byte(i)}, 32+i%5*32)
	}
	// Establish real sampled cohorts before ordinary unsampled traffic begins;
	// both owners execute identical reads, flushes and FIFO pressure.
	for id, enrolled := uint32(keys), 0; enrolled < 256; id++ {
		key := reuseObserverKey(6+int(id%2), id%3 == 0, id)
		if _, sampled := baseReadCacheReuseIndex(string(key)); !sampled {
			continue
		}
		physicalKeys[enrolled] = key
		for _, cache := range []*baseReadCache{off, on} {
			_, _, epoch := cache.getWithEpoch(key)
			cache.storeIfEpoch(key, values[enrolled], epoch)
			cache.setFlushed(string(key), values[enrolled])
		}
		enrolled++
	}
	random := rand.New(rand.NewSource(31503))
	for step := 0; step < 32768; step++ {
		id := random.Intn(keys)
		key := physicalKeys[id]
		operation := random.Intn(10)
		if operation >= 5 && operation <= 8 {
			values[id] = bytes.Repeat([]byte{byte(step)}, 32+step%5*32)
			if operation == 8 {
				values[id] = nil
			}
		}
		for _, cache := range []*baseReadCache{off, on} {
			switch {
			case operation < 5:
				cached, present, _, _, epoch, cacheable, err := cache.viewAtVersion(key, cache.version.Load(), func(got []byte, _ bool) error {
					if !bytes.Equal(got, values[id]) {
						return fmt.Errorf("wrong value at step %d", step)
					}
					return nil
				})
				if err != nil || cached && present != (values[id] != nil) {
					t.Fatalf("step %d cached=%v present=%v err=%v", step, cached, present, err)
				}
				if !cached && cacheable {
					if values[id] == nil {
						cache.setMissingIfEpoch(key, epoch)
					} else {
						cache.storeIfEpoch(key, values[id], epoch)
					}
				}
			case operation == 8:
				cache.advanceVersion()
				cache.del(key)
			case operation < 8:
				cache.advanceVersion()
				cache.setFlushed(string(key), values[id])
			default:
				cached, _, epoch, cacheable := cache.probeAtVersionForPrefetch(key, cache.version.Load())
				if !cached && cacheable {
					if values[id] == nil {
						cache.prefetchMissingIfEpoch(key, epoch)
					} else {
						cache.prefetchIfEpoch(key, values[id], epoch)
					}
				}
			}
		}
		if step%1024 == 1023 {
			if !reflect.DeepEqual(snapshotReusePolicy(off), snapshotReusePolicy(on)) {
				t.Fatalf("observer changed queue/credit/epoch/budget state at step %d", step)
			}
			assertReuseConservation(t, on)
		}
		if step%8192 == 8191 {
			off.clear()
			on.clear()
			assertReuseConservation(t, on)
		}
	}
	var samples uint64
	for _, depth := range assertReuseConservation(t, on).cohorts {
		for _, cohort := range depth {
			samples += cohort.samples
		}
	}
	if samples == 0 {
		t.Fatal("transparency fixture enrolled no observer samples")
	}
	t.Logf("32768 mixed operations: exact cache policy state equal; enrolled samples=%d", samples)
}

func TestBaseReadCacheReuseObserverConcurrentSourcesClearAndPublish(t *testing.T) {
	t.Setenv("GTRON_BASE_CACHE_REUSE_OBSERVER", "1")
	cache := newBaseReadCache(4<<20, rawdb.CommitmentBranchKeyPrefix)
	key := sampledReuseObserverKey(t, 6, false, nil)
	enrollReuseObserverKey(t, cache, key, baseReadCacheReuseFlushAdmission)
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 1000; i++ {
				cache.getWithEpoch(key)
				cache.probeAtVersionForPrefetch(key, cache.version.Load())
			}
		}()
	}
	workers.Add(2)
	go func() {
		defer workers.Done()
		for i := 0; i < 500; i++ {
			cache.advanceVersion()
			cache.setFlushed(string(key), []byte("new"))
			if i%50 == 0 {
				cache.clear()
				_, _, epoch := cache.getWithEpoch(key)
				cache.storeIfEpoch(key, []byte("new"), epoch)
				cache.setFlushed(string(key), []byte("new"))
			}
		}
	}()
	go func() {
		defer workers.Done()
		for i := 0; i < 500; i++ {
			cache.publishMetrics()
			assertReuseConservation(t, cache)
		}
	}()
	workers.Wait()
	assertReuseConservation(t, cache)
	cache.clear()
	cache.publishMetrics()
	if baseReadCacheReuseOwnerGauge.Snapshot().Value() != int64(cache.reuseOwnerID) || baseReadCacheReuseEnabledGauge.Snapshot().Value() != 1 || baseReadCacheReuseMetadataGauge.Snapshot().Value() != int64(baseReadCacheReuseMetadataBytes()) {
		t.Fatal("published owner/enabled/metadata does not match the cache")
	}
}

func TestBaseReadCacheReuseObserverPriorReferencesExcluded(t *testing.T) {
	t.Setenv("GTRON_BASE_CACHE_REUSE_OBSERVER", "1")
	for _, sources := range []uint32{1, 2, 3} {
		cache := newBaseReadCacheWithTrunk(4<<20, 4, rawdb.CommitmentBranchKeyPrefix)
		key := sampledReuseObserverKey(t, 7, true, nil)
		_, _, epoch := cache.getWithEpoch(key)
		cache.storeIfEpoch(key, []byte("old"), epoch)
		if sources&1 != 0 {
			cache.getWithEpoch(key)
		}
		if sources&2 != 0 {
			cache.probeAtVersionForPrefetch(key, cache.version.Load())
		}
		cache.setFlushed(string(key), []byte("new"))
		shard := &cache.shards[baseReadCacheShardIndex(key)]
		shard.mu.Lock()
		shard.evictWindowOne()
		shard.mu.Unlock()
		got := assertReuseConservation(t, cache).cohorts[1][baseReadCacheReuseWindowFlush]
		if got.eligible != 0 || got.samples != 0 {
			t.Fatalf("pre-enrollment reference source %d entered flush-only cohort", sources)
		}
	}
}

func TestBaseReadCacheReuseObserverConcurrentOwnerPublication(t *testing.T) {
	t.Setenv("GTRON_BASE_CACHE_REUSE_OBSERVER", "1")
	first := newBaseReadCache(4<<20, rawdb.CommitmentBranchKeyPrefix)
	second := newBaseReadCache(4<<20, rawdb.CommitmentBranchKeyPrefix)
	key := sampledReuseObserverKey(t, 6, false, nil)
	enrollReuseObserverKey(t, first, key, baseReadCacheReuseFlushAdmission)
	first.clear()
	var workers sync.WaitGroup
	for _, cache := range []*baseReadCache{first, second} {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 200; i++ {
				cache.publishMetrics()
			}
		}()
	}
	for i := 0; i < 200; i++ {
		// A real scrape has no such lock and remains eventually consistent.
		// Verify only that publishers do not interleave their identity/values.
		baseReadCacheReusePublishMu.Lock()
		owner := baseReadCacheReuseOwnerGauge.Snapshot().Value()
		value := baseReadCacheReuseGauges[0][baseReadCacheReuseFlushAdmission].samples.Snapshot().Value()
		valid := owner == int64(first.reuseOwnerID) && value == 1 || owner == int64(second.reuseOwnerID) && value == 0
		baseReadCacheReusePublishMu.Unlock()
		if !valid {
			t.Errorf("owner/cohort publication interleaved: owner=%d samples=%d", owner, value)
			break
		}
	}
	workers.Wait()
}
