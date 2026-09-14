package blockbuffer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type commitmentOwnershipSession interface {
	pointread.CommitmentParentSession
	pointread.CommitmentParentPrefetchSession
}

func newCommitmentOwnershipSession(view *LayerView, candidate bool) (commitmentOwnershipSession, error) {
	var session pointread.CommitmentParentSession
	var err error
	if candidate {
		session, err = view.NewCommitmentParentReadSession(2)
	} else {
		session, err = legacyNewCommitmentOwnershipSession(view, 2)
	}
	if err != nil || session == nil {
		return nil, fmt.Errorf("ownership session unavailable: %w", err)
	}
	return session.(commitmentOwnershipSession), nil
}

func commitmentOwnershipConcrete(session commitmentOwnershipSession) *commitmentParentReadSession {
	if old, ok := session.(*legacyCommitmentOwnershipSession); ok {
		return (*commitmentParentReadSession)(old)
	}
	return session.(*commitmentParentReadSession)
}

type commitmentOwnershipKeyCapture struct{ key []byte }

func (c *commitmentOwnershipKeyCapture) Put(key, _ []byte) error {
	c.key = bytes.Clone(key)
	return nil
}
func (*commitmentOwnershipKeyCapture) Delete([]byte) error {
	return errors.New("unexpected key deletion")
}

func commitmentOwnershipPrefix(t testing.TB, kind string) []byte {
	t.Helper()
	if kind == "generic" {
		return []byte("generic-ownership/")
	}
	keyspace := rawdb.LegacyCommitmentBranchKeyspace()
	if kind == "delta" {
		var err error
		keyspace, err = rawdb.NewCommitmentBranchDeltaKeyspace(20260914)
		if err != nil {
			t.Fatal(err)
		}
	}
	capture := new(commitmentOwnershipKeyCapture)
	if err := keyspace.Write(capture, nil, nil); err != nil {
		t.Fatal(err)
	}
	return capture.key
}

func checkCommitmentOwnershipBudget(t testing.TB, cache *baseReadCache) {
	t.Helper()
	for i := range cache.shards {
		s := &cache.shards[i]
		s.mu.RLock()
		used, trunk, window, other := 0, 0, 0, 0
		owners := make(map[*baseReadCacheEntry]int)
		for _, queue := range [][]*baseReadCacheEntry{s.queue[s.head:], s.windowQueue[s.windowHead:], s.nonCommitmentQueue[s.nonCommitmentHead:]} {
			for _, entry := range queue {
				if entry != nil && entry.live {
					owners[entry]++
				}
			}
		}
		for key, entry := range s.entries {
			if !entry.live || entry.key != key || entry.charge != int(entry.keyCapacity)+cap(entry.value)+baseReadCacheEntryOverhead {
				t.Fatalf("shard %d invalid resident charge/identity", i)
			}
			used += entry.charge
			if entry.trunk {
				trunk += entry.charge
				if owners[entry] != 0 {
					t.Fatal("trunk entry has a CLOCK owner")
				}
			} else if owners[entry] != 1 {
				t.Fatalf("live entry has %d queue owners", owners[entry])
			}
			if entry.window {
				window += entry.charge
			}
			if entry.nonCommitment {
				other += entry.charge
			}
		}
		freeCount, freeBytes := 0, 0
		for entry := s.freeEntries; entry != nil; entry = entry.nextFree {
			freeCount++
			freeBytes += cap(entry.value)
			if entry.live || entry.exposed.Load() || freeCount > baseReadCacheMaxFreeEntries {
				t.Fatal("invalid/beyond-budget reusable entry")
			}
		}
		if used != s.used || trunk != s.trunkUsed || window != s.windowUsed || other != s.nonCommitmentUsed || used > s.limit || trunk > s.trunkLimit || window > s.windowLimit || freeBytes != s.freeValueBytes || freeCount != s.freeEntryCount || freeBytes > s.limit/baseReadCacheFreeValueBudgetDivisor {
			t.Fatalf("shard %d budget mismatch used=%d/%d limit=%d free=%d/%d", i, used, s.used, s.limit, freeBytes, s.freeValueBytes)
		}
		s.mu.RUnlock()
	}
}

// Real Buffer topology capture and canonical FlushUpTo operate on a real Pebble
// snapshot. Only the legacy session's two prefetch admission sites differ.
func TestCommitmentPrefetchOwnershipSessionFlushOracle(t *testing.T) {
	for _, kind := range []string{"legacy", "delta", "generic"} {
		for _, size := range []int{0, 64, 256, 1024, 4096} {
			for _, direct := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/size_%d/direct_%t", kind, size, direct), func(t *testing.T) {
					var results [2][][]byte
					for variant, candidate := range []bool{false, true} {
						disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
						if err != nil {
							t.Fatal(err)
						}
						func() {
							defer func() { _ = disk.Close() }()
							prefix, path := commitmentOwnershipPrefix(t, kind), []byte{1, 2, 3, 4, 5, 6}
							key := append(bytes.Clone(prefix), path...)
							oldValue := bytes.Repeat([]byte{0x31}, 1024)
							newValue := bytes.Repeat([]byte{0x72}, size)
							if err := disk.Put(key, oldValue); err != nil {
								t.Fatal(err)
							}
							buf := New(disk)
							buf.SetBaseReadCacheSizeWithTrunk(1<<20, 4, rawdb.CommitmentBranchKeyPrefix)
							buf.BeginBlock(bufHash(1), 1)
							h, _ := buf.NewestInflight()
							session, err := newCommitmentOwnershipSession(buf.ViewLayer(h), candidate)
							if err != nil {
								t.Fatal(err)
							}
							defer func() { _ = session.Close() }()
							if found, err := session.PrefetchKeyParts(0, prefix, path); err != nil || !found {
								t.Fatalf("prefetch: %v %v", found, err)
							}
							cache := buf.baseReadCache
							entry := cache.shards[baseReadCacheShardIndex(key)].entries[string(key)]
							if entry == nil || entry.exposed.Load() == candidate {
								t.Fatal("prefetch admission exposure does not match ownership")
							}
							before := unsafe.SliceData(entry.value)
							var retained []byte
							if direct {
								retained, err = buf.Prefetch(key)
								if err != nil || !bytes.Equal(retained, oldValue) {
									t.Fatalf("direct prefetch: %v", err)
								}
							}
							read := func() []byte {
								var out []byte
								found, err := session.ViewKeyParts(1, prefix, path, func(value []byte, stable bool) error {
									out = bytes.Clone(value)
									if stable != (kind == "generic" && entry.version <= commitmentOwnershipConcrete(session).cacheVersion) {
										t.Fatal("callback ownership changed")
									}
									return nil
								})
								if err != nil || !found {
									t.Fatalf("parent read: %v %v", found, err)
								}
								return out
							}
							results[variant] = append(results[variant], read())
							if err := buf.ViewLayer(h).Put(key, newValue); err != nil {
								t.Fatal(err)
							}
							if err := buf.CommitInflight(h); err != nil {
								t.Fatal(err)
							}
							if err := buf.FlushUpTo(1, disk); err != nil {
								t.Fatal(err)
							}
							results[variant] = append(results[variant], read()) // old pinned snapshot survives refresh
							latest, err := disk.Get(key)
							if err != nil || !bytes.Equal(latest, newValue) || !bytes.Equal(results[variant][0], oldValue) || !bytes.Equal(results[variant][1], oldValue) {
								t.Fatal("snapshot/physical bytes changed")
							}
							results[variant] = append(results[variant], latest)
							if direct && !bytes.Equal(retained, oldValue) {
								t.Fatal("flush mutated publicly retained bytes")
							}
							entry = cache.shards[baseReadCacheShardIndex(key)].entries[string(key)]
							reuse := candidate && !direct && kind != "generic" && size <= len(oldValue)
							if (unsafe.SliceData(entry.value) == before) != reuse {
								t.Fatalf("flush backing reuse differs: candidate=%v size=%d direct=%v kind=%s", candidate, size, direct, kind)
							}
							checkCommitmentOwnershipBudget(t, cache)
						}()
					}
					if !reflect.DeepEqual(results[0], results[1]) {
						t.Fatal("full session legacy/candidate byte traces differ")
					}
				})
			}
		}
	}
}

func TestCommitmentPrefetchOwnershipSharedFollowerAndCursorBytes(t *testing.T) {
	for _, candidate := range []bool{false, true} {
		for _, prefetchLeader := range []bool{false, true} {
			for _, kind := range []string{"present", "empty", "missing", "error"} {
				t.Run(fmt.Sprintf("candidate_%t/prefetchLeader_%t/%s", candidate, prefetchLeader, kind), func(t *testing.T) {
					state := &blockingCommitmentCursorState{started: make(chan struct{}), release: make(chan struct{}), present: kind == "present" || kind == "empty", overwrite: true}
					if kind == "present" {
						state.value = []byte("private-cache-cursor-value")
					} else if kind == "empty" {
						state.value = []byte{}
					} else if kind == "error" {
						state.readErr = errors.New("injected read error")
					}
					want := bytes.Clone(state.value)
					cache := newBaseReadCacheWithTrunk(1<<20, -1, rawdb.CommitmentBranchKeyPrefix) // foreground first read remains in probation
					concrete := &commitmentParentReadSession{cache: cache, cacheVersion: cache.version.Load(), snapshot: blockingCommitmentSnapshot{state: state}, cursors: make([]pointread.Cursor, 2), keyScratch: borrowCommitmentParentKeyScratch(2)}
					var session commitmentOwnershipSession = concrete
					if candidate {
						concrete.readContexts = borrowCommitmentParentReadContexts(concrete, 2)
					} else {
						old := (*legacyCommitmentOwnershipSession)(concrete)
						old.readContexts = legacyBorrowCommitmentOwnershipContexts(old, 2)
						session = old
					}
					defer func() { _ = session.Close() }()
					prefix, path := []byte(rawdb.CommitmentBranchKeyPrefix), []byte{6, 1, 2, 3, 4, 5}
					key := append(bytes.Clone(prefix), path...)
					var found [2]bool
					var errs [2]error
					var read []byte
					var wg sync.WaitGroup
					launch := func(reader int, prefetch bool) {
						wg.Add(1)
						go func() {
							defer wg.Done()
							if prefetch {
								found[reader], errs[reader] = session.PrefetchKeyParts(reader, prefix, path)
							} else {
								found[reader], errs[reader] = session.ViewKeyParts(reader, prefix, path, func(value []byte, stable bool) error {
									if stable {
										t.Error("durable/scoped callback unexpectedly stable")
									}
									read = bytes.Clone(value)
									return nil
								})
							}
						}()
					}
					launch(0, prefetchLeader)
					<-state.started
					launch(1, !prefetchLeader)
					waitForCommitmentParentFlightFollowers(t, concrete, key, 1)
					close(state.release)
					wg.Wait()
					for i := range found {
						if found[i] != state.present || !errors.Is(errs[i], state.readErr) {
							t.Fatalf("shared reader %d result=%v/%v", i, found[i], errs[i])
						}
					}
					if !bytes.Equal(read, want) || (kind != "error" && state.calls.Load() != 1) {
						t.Fatal("shared flight changed bytes/number of reads")
					}
					if state.present {
						entry := cache.shards[baseReadCacheShardIndex(key)].entries[string(key)]
						if entry == nil || !reflect.DeepEqual(entry.value, want) || entry.exposed.Load() == candidate {
							t.Fatal("prefetch leader/follower exposed or retained cursor-owned input")
						}
						cache.setFlushed(string(key), bytes.Repeat([]byte{0x42}, len(want)))
						if !reflect.DeepEqual(read, want) {
							t.Fatal("cache refresh mutated shared flight output")
						}
					}
					checkCommitmentOwnershipBudget(t, cache)
				})
			}
		}
	}
}

func TestCommitmentPrefetchOwnershipCallbackBlocksFlush(t *testing.T) {
	cache := newBaseReadCache(1<<20, rawdb.CommitmentBranchKeyPrefix)
	key := []byte(rawdb.CommitmentBranchKeyPrefix + "callback-owner")
	_, _, epoch := cache.getForPrefetchWithEpoch(key)
	cache.storePrefetchIfEpoch(key, []byte("old"), epoch)
	entered, release, readDone, flushDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(readDone)
		_, _, _, _, _, _, err := cache.viewAtVersion(key, cache.version.Load(), func(value []byte, stable bool) error {
			close(entered)
			<-release
			if stable || string(value) != "old" {
				t.Error("flush mutated callback-owned value")
			}
			return errors.New("callback-only error")
		})
		if err == nil || err.Error() != "callback-only error" {
			t.Error("callback error was lost")
		}
	}()
	<-entered
	go func() { cache.setFlushed(string(key), []byte("new")); close(flushDone) }()
	select {
	case <-flushDone:
		t.Fatal("flush passed active callback's read lock")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	<-readDone
	<-flushDone
}

func TestCommitmentPrefetchOwnershipPressurePreservesBudget(t *testing.T) {
	for _, kind := range []string{"legacy", "delta", "generic"} {
		for _, candidate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/candidate_%t", kind, candidate), func(t *testing.T) {
				cache := newBaseReadCacheWithTrunk(256<<10, 4, rawdb.CommitmentBranchKeyPrefix)
				prefix := commitmentOwnershipPrefix(t, kind)
				for round := 0; round < 6; round++ {
					for i := 0; i < 256; i++ {
						key := append(bytes.Clone(prefix), make([]byte, 8)...)
						binary.BigEndian.PutUint64(key[len(prefix):], uint64(i))
						_, _, epoch := cache.getForPrefetchWithEpoch(key)
						value := bytes.Repeat([]byte{byte(round)}, []int{64, 256, 1024, 4096}[i%4])
						if candidate {
							cache.storePrefetchIfEpoch(key, value, epoch)
						} else {
							cache.prefetchIfEpoch(key, value, epoch)
						}
						// Mix larger, smaller, empty and oversized canonical refreshes.
						cache.setFlushed(string(key), bytes.Repeat([]byte{byte(round + 1)}, []int{0, 32, 2048, 20000}[(round+i)%4]))
						if i%31 == 0 {
							checkCommitmentOwnershipBudget(t, cache)
						}
					}
					checkCommitmentOwnershipBudget(t, cache)
				}
			})
		}
	}
}

func TestCommitmentPrefetchOwnershipAlreadyExposedAndEpoch(t *testing.T) {
	cache := newBaseReadCache(1<<20, rawdb.CommitmentBranchKeyPrefix)
	key := []byte(rawdb.CommitmentBranchKeyPrefix + "public-value")
	_, _, epoch := cache.getForPrefetchWithEpoch(key)
	retained, stored := cache.prefetchIfEpoch(key, []byte("original"), epoch)
	if !stored {
		t.Fatal("fixture admission failed")
	}
	cache.storePrefetchIfEpoch(key, []byte("original"), epoch)
	entry := cache.shards[baseReadCacheShardIndex(key)].entries[string(key)]
	if !entry.exposed.Load() {
		t.Fatal("private prefetch downgraded already exposed entry")
	}
	cache.setFlushed(string(key), []byte("replaced"))
	if string(retained) != "original" {
		t.Fatal("public prefetch result was mutated")
	}
	if cache.storePrefetchIfEpoch(key, []byte("obsolete"), epoch) || string(entry.value) != "replaced" {
		t.Fatal("stale prefetch bypassed invalidation epoch")
	}
}

func TestCommitmentPrefetchOwnershipFullSessionReplayOracle(t *testing.T) {
	t.Setenv("GTRON_PEBBLE_BOUNDED_POINT_READ", "get")
	for _, kind := range []string{"legacy", "delta"} {
		for _, mode := range []string{"cold_same", "cold_shrink", "cold_grow", "hot_same", "hot_shrink_grow", "no_flush", "direct_get"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				var outputs [2][][]byte
				var reads [2][]uint64
				for variant, candidate := range []bool{false, true} {
					r := newCommitmentOwnershipReplay(t, candidate, kind, mode, 32)
					for i := 0; i < 4; i++ {
						count, err := r.run()
						if err != nil {
							t.Fatal(err)
						}
						reads[variant] = append(reads[variant], count)
						for _, key := range r.keys {
							value, err := r.disk.Get(key)
							if err != nil || !bytes.Equal(value, r.current) {
								t.Fatalf("canonical replay output changed: %v", err)
							}
							outputs[variant] = append(outputs[variant], value)
						}
						checkCommitmentOwnershipBudget(t, r.buffer.baseReadCache)
					}
				}
				if !reflect.DeepEqual(outputs[0], outputs[1]) || !reflect.DeepEqual(reads[0], reads[1]) {
					t.Fatal("full canonical operation changed bytes or durable reads")
				}
			})
		}
	}
}

type commitmentOwnershipReplay struct {
	disk      ethdb.KeyValueStore
	buffer    *Buffer
	prefix    []byte
	paths     [][]byte
	keys      [][]byte
	current   []byte
	initial   []byte
	small     []byte
	large     []byte
	one, two  []byte
	candidate bool
	mode      string
	step      uint64
}

func newCommitmentOwnershipReplay(t testing.TB, candidate bool, kind, mode string, count int) *commitmentOwnershipReplay {
	t.Helper()
	disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	r := &commitmentOwnershipReplay{disk: disk, buffer: New(disk), prefix: commitmentOwnershipPrefix(t, kind), candidate: candidate, mode: mode,
		initial: bytes.Repeat([]byte{0x11}, 1024), small: bytes.Repeat([]byte{0x22}, 256), large: bytes.Repeat([]byte{0x33}, 4096), one: bytes.Repeat([]byte{0x44}, 1024), two: bytes.Repeat([]byte{0x55}, 1024)}
	r.current = r.initial
	r.buffer.SetBaseReadCacheSizeWithTrunk(8<<20, 4, rawdb.CommitmentBranchKeyPrefix)
	for i := 0; i < count; i++ {
		path := []byte{byte(i % 16), byte(i / 16), 1, 2, 3, 4}
		key := append(bytes.Clone(r.prefix), path...)
		r.paths, r.keys = append(r.paths, path), append(r.keys, key)
		if err := disk.Put(key, r.current); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

func (r *commitmentOwnershipReplay) run() (uint64, error) {
	r.step++
	cold := r.mode == "cold_same" || r.mode == "cold_shrink" || r.mode == "cold_grow" || r.mode == "no_flush" || r.mode == "direct_get"
	if cold {
		for _, key := range r.keys {
			r.buffer.baseReadCache.del(key)
			if err := r.disk.Put(key, r.initial); err != nil {
				return 0, err
			}
		}
		r.current = r.initial
	}
	var hash common.Hash
	binary.BigEndian.PutUint64(hash[:8], r.step)
	r.buffer.BeginBlock(hash, r.step)
	h, _ := r.buffer.NewestInflight()
	view := r.buffer.ViewLayer(h)
	session, err := newCommitmentOwnershipSession(view, r.candidate)
	if err != nil {
		return 0, err
	}
	defer func() { _ = session.Close() }()
	consume := func(value []byte, stable bool) error {
		if stable || !bytes.Equal(value, r.current) {
			return errors.New("replay parent bytes/ownership changed")
		}
		return nil
	}
	for i, path := range r.paths {
		if found, err := session.PrefetchKeyParts(0, r.prefix, path); err != nil || !found {
			return 0, fmt.Errorf("replay prefetch found=%v: %w", found, err)
		}
		if r.mode == "direct_get" {
			value, err := r.buffer.Prefetch(r.keys[i])
			if err != nil || !bytes.Equal(value, r.current) {
				return 0, errors.New("direct-get control bytes differ")
			}
		}
		if found, err := session.ViewKeyParts(1, r.prefix, path, consume); err != nil || !found {
			return 0, fmt.Errorf("replay view found=%v: %w", found, err)
		}
	}
	var reads uint64
	for _, ctx := range commitmentOwnershipConcrete(session).readContexts {
		reads += ctx.prefetchDurable + ctx.durableReads
	}
	if err := session.Close(); err != nil {
		return reads, err
	}
	if r.mode == "no_flush" {
		r.buffer.DiscardActive()
		return reads, nil
	}
	next := r.one
	if bytes.Equal(r.current, r.one) {
		next = r.two
	}
	switch r.mode {
	case "cold_shrink":
		next = r.small
	case "cold_grow":
		next = r.large
	case "hot_shrink_grow":
		next = r.small
		if len(r.current) == len(r.small) {
			next = r.large
		}
	}
	for _, key := range r.keys {
		if err := view.Put(key, next); err != nil {
			return reads, err
		}
	}
	if err := r.buffer.CommitInflight(h); err != nil {
		return reads, err
	}
	if err := r.buffer.FlushUpTo(r.step, r.disk); err != nil {
		return reads, err
	}
	r.current = next
	return reads, nil
}
