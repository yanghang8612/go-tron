package blockbuffer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func cacheDiagnosticKey(depth int, delta bool) (prefix, path, key []byte) {
	prefix = []byte(rawdb.CommitmentBranchKeyPrefix)
	if delta {
		prefix = make([]byte, len(rawdb.CommitmentBranchDeltaKeyPrefix)+8)
		copy(prefix, rawdb.CommitmentBranchDeltaKeyPrefix)
		binary.BigEndian.PutUint64(prefix[len(prefix)-8:], 987)
	}
	path = bytes.Repeat([]byte{3}, depth)
	key = append(bytes.Clone(prefix), path...)
	return prefix, path, key
}

func TestBaseReadCacheDiagnosticReferenceCreditAndOutcomes(t *testing.T) {
	for sources := uint32(0); sources < baseReadCacheDiagnosticSources; sources++ {
		t.Run(baseReadCacheDiagnosticSourceNames[sources], func(t *testing.T) {
			c := newBaseReadCacheWithTrunk(1<<20, 4, rawdb.CommitmentBranchKeyPrefix)
			_, _, key := cacheDiagnosticKey(6, false)
			_, _, epoch := c.getWithEpoch(key)
			if !c.storeIfEpoch(key, []byte("value"), epoch) {
				t.Fatal("first read did not enter window")
			}
			s := &c.shards[baseReadCacheShardIndex(key)]
			e := s.entries[string(key)]
			if e.references.Load()&^baseReadCacheDiagnosticDepthMask != 0 || !e.window {
				t.Fatal("initial window admission acquired reference credit/source")
			}
			if sources != 0 {
				// Saturate with the first source, then add the other sources after
				// saturation. New diagnostic bits must not add a fourth credit.
				first := sources & -sources
				for range 5 {
					e.reference(first << baseReadCacheReferenceSourceShift)
				}
				for source := uint32(1); source < baseReadCacheDiagnosticSources; source <<= 1 {
					if sources&source != 0 {
						e.reference(source << baseReadCacheReferenceSourceShift)
					}
				}
				if got := e.references.Load(); got != sources<<baseReadCacheReferenceSourceShift|baseReadCacheMaxReferenceCredit|1<<baseReadCacheDiagnosticDepthShift {
					t.Fatalf("reference state=%#x", got)
				}
			}
			if !s.evictWindowOne() {
				t.Fatal("window did not consume its token")
			}
			if sources == 0 {
				if len(s.entries) != 0 || s.diagnostics[0].outcomes[baseReadCacheDiagnosticWindowEvicted][0] != 1 {
					t.Fatal("untouched entry was not evicted")
				}
				return
			}
			if e.window || s.diagnostics[0].outcomes[baseReadCacheDiagnosticWindowPromoted][sources] != 1 {
				t.Fatal("referenced window entry was not promoted with its exact source set")
			}
			// Original policy: promotion spends one of three credits. Two tail
			// sweeps grant second chances; the third evicts. Source bits survive
			// credit exhaustion but must not extend residency by another sweep.
			for sweep := 0; sweep < 3; sweep++ {
				if !s.evictOne(false) {
					t.Fatal("tail lost its token")
				}
				_, live := s.entries[string(key)]
				if live != (sweep < 2) {
					t.Fatalf("source set changed CLOCK policy at sweep %d", sweep)
				}
			}
			if s.diagnostics[0].outcomes[baseReadCacheDiagnosticTailEvicted][sources] != 1 || e.references.Load() != 0 {
				t.Fatal("eviction source accounting or metadata reset failed")
			}
		})
	}
}

func TestBaseReadCacheDiagnosticConcurrentSourcesAndPrefetchMarker(t *testing.T) {
	var e baseReadCacheEntry
	e.references.Store(baseReadCachePrefetchedReference)
	var wg sync.WaitGroup
	for worker := range 60 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.reference(uint32(1<<(worker%3)) << baseReadCacheReferenceSourceShift)
		}()
	}
	wg.Wait()
	want := baseReadCacheReferenceSourceMask | baseReadCachePrefetchedReference | baseReadCacheMaxReferenceCredit
	if got := e.references.Load(); got != want {
		t.Fatalf("concurrent source/credit merge=%#x, want %#x", got, want)
	}
	if !e.recordUsefulPrefetch() || e.recordUsefulPrefetch() {
		t.Fatal("prefetched marker must be consumed exactly once")
	}
	for range baseReadCacheMaxReferenceCredit {
		if !e.consumeReference() {
			t.Fatal("lost credit")
		}
	}
	if e.consumeReference() || e.references.Load() != baseReadCacheReferenceSourceMask {
		t.Fatal("source bits became credit or were removed by decay")
	}
}

func TestBaseReadCacheDiagnosticReferencePaths(t *testing.T) {
	for _, test := range []struct {
		name   string
		source uint32
		read   func(*testing.T, *baseReadCache, []byte, baseReadCacheEpoch)
	}{
		{"get", baseReadCacheReferenceForeground, func(t *testing.T, c *baseReadCache, k []byte, _ baseReadCacheEpoch) { c.getWithEpoch(k) }},
		{"snapshot_get", baseReadCacheReferenceForeground, func(t *testing.T, c *baseReadCache, k []byte, _ baseReadCacheEpoch) { c.getAtVersion(k, 0) }},
		{"view", baseReadCacheReferenceForeground, func(t *testing.T, c *baseReadCache, k []byte, _ baseReadCacheEpoch) {
			if _, _, _, err := c.viewWithEpoch(k, func([]byte, bool) error { return nil }); err != nil {
				t.Fatal(err)
			}
		}},
		{"snapshot_view", baseReadCacheReferenceForeground, func(t *testing.T, c *baseReadCache, k []byte, _ baseReadCacheEpoch) {
			if _, _, _, _, _, _, err := c.viewAtVersion(k, 0, func([]byte, bool) error { return nil }); err != nil {
				t.Fatal(err)
			}
		}},
		{"prefetch_get", baseReadCacheReferencePrefetch, func(t *testing.T, c *baseReadCache, k []byte, _ baseReadCacheEpoch) { c.getForPrefetchWithEpoch(k) }},
		{"prefetch_probe", baseReadCacheReferencePrefetch, func(t *testing.T, c *baseReadCache, k []byte, _ baseReadCacheEpoch) {
			c.probeAtVersionForPrefetch(k, 0)
		}},
		{"foreground_publish_race", baseReadCacheReferenceForeground, func(t *testing.T, c *baseReadCache, k []byte, e baseReadCacheEpoch) {
			c.storeIfEpoch(k, []byte("value"), e)
		}},
		{"prefetch_publish_race", baseReadCacheReferencePrefetch, func(t *testing.T, c *baseReadCache, k []byte, e baseReadCacheEpoch) {
			c.prefetchIfEpoch(k, []byte("value"), e)
		}},
		{"flush_equal", baseReadCacheReferenceFlush, func(t *testing.T, c *baseReadCache, k []byte, _ baseReadCacheEpoch) {
			c.setFlushed(string(k), []byte("value"))
		}},
		{"flush_reuse", baseReadCacheReferenceFlush, func(t *testing.T, c *baseReadCache, k []byte, _ baseReadCacheEpoch) {
			c.setFlushed(string(k), []byte("new"))
		}},
		{"flush_replace", baseReadCacheReferenceFlush, func(t *testing.T, c *baseReadCache, k []byte, _ baseReadCacheEpoch) {
			c.setFlushed(string(k), []byte("longer replacement"))
		}},
		{"future_snapshot", 0, func(t *testing.T, c *baseReadCache, k []byte, _ baseReadCacheEpoch) {
			c.shards[baseReadCacheShardIndex(k)].entries[string(k)].version = 1
			c.getAtVersion(k, 0)
			c.probeAtVersionForPrefetch(k, 0)
			if _, _, _, _, _, _, err := c.viewAtVersion(k, 0, func([]byte, bool) error { t.Fatal("future value exposed"); return nil }); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newBaseReadCacheWithTrunk(1<<20, 4, rawdb.CommitmentBranchKeyPrefix)
			_, _, key := cacheDiagnosticKey(7, true)
			_, _, epoch := c.getWithEpoch(key)
			if !c.storeIfEpoch(key, []byte("value"), epoch) {
				t.Fatal("window admission failed")
			}
			test.read(t, c, key, epoch)
			e := c.shards[baseReadCacheShardIndex(key)].entries[string(key)]
			if got := e.references.Load() & baseReadCacheReferenceSourceMask; got != test.source {
				t.Fatalf("source=%#x, want %#x", got, test.source)
			}
		})
	}
}

func TestBaseReadCacheDiagnosticAdmissionAndLifecycle(t *testing.T) {
	for _, delta := range []bool{false, true} {
		for _, depth := range []int{5, 6, 7, 8} {
			t.Run(fmt.Sprintf("depth_%d/delta_%v", depth, delta), func(t *testing.T) {
				c := newBaseReadCache(1<<20, rawdb.CommitmentBranchKeyPrefix)
				_, _, key := cacheDiagnosticKey(depth, delta)
				_, _, epoch := c.getWithEpoch(key)
				if c.storeIfEpoch(key, []byte("value"), epoch) || !c.storeIfEpoch(key, []byte("value"), epoch) {
					t.Fatal("two-hit admission changed")
				}
				s := &c.shards[baseReadCacheShardIndex(key)]
				e := s.entries[string(key)]
				c.getForPrefetchWithEpoch(key)
				c.setFlushed(string(key), []byte("value"))
				if e.references.Load()&baseReadCacheReferenceFlush != 0 {
					t.Fatal("ordinary tail flush gained reference credit")
				}
				if c.storeIfEpoch(key, []byte("old"), epoch) {
					t.Fatal("late pre-flush fill was admitted")
				}
				c.del(key)
				if !s.evictOne(false) || e.references.Load() != 0 {
					t.Fatal("stale token retained diagnostic state")
				}
				// A new first observation followed by canonical flush is admission,
				// not a resident reference. It must not manufacture a flush source.
				_, _, epoch = c.getWithEpoch(key)
				if c.storeIfEpoch(key, []byte("next"), epoch) {
					t.Fatal("delete did not clear probation")
				}
				c.setFlushed(string(key), []byte("next"))
				if got := s.entries[string(key)].references.Load() &^ baseReadCacheDiagnosticDepthMask; got != 0 {
					t.Fatalf("new resident inherited references %#x", got)
				}
				want := [baseReadCacheDiagnosticDepths]baseReadCacheDepthDiagnostics{}
				if depth == 6 || depth == 7 {
					r := &want[depth-6].reasons
					r[baseReadCacheDiagnosticProbationFirst] = 2
					r[baseReadCacheDiagnosticProbationPassed] = 1
					r[baseReadCacheDiagnosticProbationRejected] = 2
					r[baseReadCacheDiagnosticEpochRejected] = 1
					r[baseReadCacheDiagnosticFlushAdmission] = 1
				}
				before := c.stats().diagnostics
				if before != want {
					t.Fatalf("branch totals=%v, want %v", before, want)
				}
				c.clear()
				if got := c.stats().diagnostics; got != before {
					t.Fatal("clear reset lifetime totals or counted capacity evictions")
				}
				publishBaseReadCacheMetrics(c.stats())
				if depth == 6 || depth == 7 {
					if got := baseReadCacheDiagnosticReasonGauges[depth-6][baseReadCacheDiagnosticEpochRejected].Snapshot().Value(); got != 1 {
						t.Fatalf("published epoch rejection=%d", got)
					}
				}
			})
		}
	}
}

func TestBaseReadCacheDiagnosticWindowRejections(t *testing.T) {
	for _, capacity := range []bool{false, true} {
		c := newBaseReadCacheWithTrunk(1<<20, 4, rawdb.CommitmentBranchKeyPrefix)
		_, _, key := cacheDiagnosticKey(6, false)
		s := &c.shards[baseReadCacheShardIndex(key)]
		reason := baseReadCacheDiagnosticWindowBypassed
		if capacity {
			s.windowLimit = 1
			reason = baseReadCacheDiagnosticWindowCapacityRejected
		} else {
			s.windowAdmissionShift = baseReadCacheWindowMaxAdmissionShift
		}
		_, _, epoch := c.getWithEpoch(key)
		if c.storeIfEpoch(key, []byte("value"), epoch) || len(s.entries) != 0 {
			t.Fatal("rejected window row entered cache")
		}
		if s.diagnostics[0].reasons[reason] != 1 || s.diagnostics[0].reasons[baseReadCacheDiagnosticProbationFirst] != 1 {
			t.Fatal("window rejection was not classified at its existing branch")
		}
	}
}

func TestBaseReadCacheDiagnosticPublicationSkipsBusyPublisher(t *testing.T) {
	c := newBaseReadCache(1<<20, rawdb.CommitmentBranchKeyPrefix)
	c.publishMetrics()
	c.metricsPublishMu.Lock()
	c.shards[0].diagnostics[0].reasons[baseReadCacheDiagnosticEpochRejected] = 2
	// This synchronous call must skip a busy publisher rather than block the
	// fold close; it cannot publish an unsequenced snapshot of the new totals.
	c.maybePublishMetrics()
	if got := baseReadCacheDiagnosticReasonGauges[0][baseReadCacheDiagnosticEpochRejected].Snapshot().Value(); got != 0 {
		t.Fatalf("busy publisher was bypassed: %d", got)
	}
	if c.metricsPublishSequence.Load() != 0 {
		t.Fatal("busy publication was not scheduled for the next close")
	}
	c.metricsPublishMu.Unlock()
	c.maybePublishMetrics()
	if got := baseReadCacheDiagnosticReasonGauges[0][baseReadCacheDiagnosticEpochRejected].Snapshot().Value(); got != 2 {
		t.Fatalf("next close did not publish updated totals: %d", got)
	}
}

func TestCommitmentParentDeepNoResidentDiagnostics(t *testing.T) {
	for _, delta := range []bool{false, true} {
		for _, depth := range []int{6, 7} {
			for _, prefetch := range []bool{false, true} {
				t.Run(fmt.Sprintf("depth_%d/delta_%v/prefetch_%v", depth, delta, prefetch), func(t *testing.T) {
					cache := newBaseReadCache(1<<20, rawdb.CommitmentBranchKeyPrefix)
					session := &commitmentParentReadSession{
						cache: cache, snapshot: benchmarkCommitmentSnapshot{},
						cursors:    []pointread.Cursor{cacheMetricsCursor{value: []byte("value")}},
						keyScratch: borrowCommitmentParentKeyScratch(1),
					}
					session.readContexts = borrowCommitmentParentReadContexts(session, 1)
					ctx := session.readContexts[0]
					prefix, path, _ := cacheDiagnosticKey(depth, delta)
					source, want := 0, int64(2)
					if prefetch {
						source, want = 1, 1
					}
					counter := commitmentParentDeepNoResidentCounters[depth-6][source]
					before := counter.Snapshot().Count()
					for range 3 {
						var found bool
						var err error
						if prefetch {
							found, err = session.PrefetchKeyParts(0, prefix, path)
						} else {
							found, err = session.ViewKeyParts(0, prefix, path, func([]byte, bool) error { return nil })
						}
						if !found || err != nil {
							t.Fatalf("view found=%v err=%v", found, err)
						}
					}
					if counter.Snapshot().Count() != before {
						t.Fatal("per-read publication bypassed session batching")
					}
					if err := session.Close(); err != nil {
						t.Fatal(err)
					}
					if got := counter.Snapshot().Count() - before; got != want {
						t.Fatalf("depth/source no_resident=%d, want %d", got, want)
					}
					if ctx.cacheDeepNoResident != ([baseReadCacheDiagnosticDepths][2]uint64{}) {
						t.Fatal("pooled context retained counters")
					}
					if err := session.Close(); err != nil || counter.Snapshot().Count()-before != want {
						t.Fatal("second Close repeated counters")
					}
				})
			}
		}
	}
}
