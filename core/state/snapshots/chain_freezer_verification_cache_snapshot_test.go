package snapshots

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func pinnedVerificationFixture(t *testing.T) (string, *Manager, SegmentRef, SegmentRef) {
	t.Helper()
	dir := t.TempDir()
	row := eventLogV3TestRow(1, 0, 0, common.BytesToAddress(eventLogTestAddress(0x73)),
		common.Hash{0x31}, common.Hash{0x41}, common.Hash{0xe3}, []byte{0x33})
	ref, err := BuildEventLogV4SegmentFromReader(eventLogRowsReader{rows: []EventLog{row}}, dir, "log/pinned-proof.seg", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	index, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{ref}, "log/pinned-proof.idx")
	if err != nil {
		t.Fatal(err)
	}
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{ref, index})); err != nil {
		t.Fatal(err)
	}
	// Start without a builder-seeded advisory proof so the test can distinguish
	// semantic verification, persistent authentication, and memory reuse.
	if err := os.Remove(filepath.Join(dir, chainFreezerVerificationCacheFile)); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	manager, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, route, err := manager.chainVerificationCache.verifyEventLogIndex(dir, index, []SegmentRef{ref}); err != nil || route != chainFreezerVerificationFull {
		t.Fatalf("seed complete proof: route=%v err=%v", route, err)
	}
	return dir, manager, ref, index
}

func TestPinnedVerificationSnapshotRehashesBeforeMemoryReuse(t *testing.T) {
	dir, manager, _, _ := pinnedVerificationFixture(t)
	parent := manager.chainVerificationCache
	if !parent.dirty {
		t.Fatal("fixture must have a completed proof pending persistence")
	}
	// Pinning must use the parent's authenticated records rather than decoding
	// the sidecar again. A standalone pinned manager keeps its old fallback.
	if err := os.WriteFile(filepath.Join(dir, chainFreezerVerificationCacheFile), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	pinned, release, err := manager.PinHistoryReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	cache := pinned.chainVerificationCache
	if cache == parent || cache.LoadError() != nil || cache.dirty || !parent.dirty || len(cache.eventVerified) != 0 || len(cache.eventFlights) != 0 {
		t.Fatalf("pin inherited transient state: stats=%+v dirty=%t", cache.Stats(), cache.dirty)
	}
	for i := 0; i < 2; i++ {
		rows := 0
		covered, err := pinned.IterateCoveredEventLogs(1, 1, EventLogFilter{}, func(EventLog) (bool, error) { rows++; return true, nil })
		if err != nil || !covered || rows != 1 {
			t.Fatalf("query %d: covered=%t rows=%d err=%v", i, covered, rows, err)
		}
		stats := cache.Stats()
		if stats.EventPersistentHits != 1 || stats.EventMemoryHits != uint64(i) || stats.EventFullVerified != 0 {
			t.Fatalf("query %d verification route: %+v", i, stats)
		}
	}
	standalone, err := OpenPinnedManager(dir, manager.Manifest())
	if err != nil || standalone.chainVerificationCache.LoadError() == nil {
		t.Fatalf("standalone pin did not retain disk-load fallback: %v", err)
	}
}

func TestPinnedVerificationSnapshotRejectsSameIdentityCorruption(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprintf("replace_%t", replace), func(t *testing.T) {
			dir, manager, ref, index := pinnedVerificationFixture(t)
			path := filepath.Join(dir, ref.Path)
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data[len(data)-1] ^= 0xff
			if replace {
				if err := os.Rename(path, path+".original"); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
				t.Fatal(err)
			}
			after, err := os.Stat(path)
			if err != nil || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
				t.Fatalf("fixture changed weak identity: %v", err)
			}
			pinned, release, err := manager.PinHistoryReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			cache := pinned.chainVerificationCache
			if _, _, err := cache.verifyEventLogIndex(dir, index, []SegmentRef{ref}); err == nil {
				t.Fatal("new pin accepted same-size/same-mtime corruption through parent memory proof")
			}
			if stats := cache.Stats(); stats.EventMemoryHits != 0 || stats.EventPersistentHits != 0 {
				t.Fatalf("failed authentication populated proof hits: %+v", stats)
			}
		})
	}
}

func TestPinnedVerificationSnapshotChainProofRoutes(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()
	appendChainFreezerTestRows(t, store, 0, 4)
	dir := filepath.Join(root, "snapshot")
	built, err := NewAggregator(dir).BuildChainFreezer(store, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	parent := NewChainFreezerVerificationCache(dir)
	if _, err := verifiedChainFreezerSnapshotHeadWithCache(dir, built.Manifest, parent); err != nil {
		t.Fatal(err)
	}
	clone := parent.snapshotPersistent(dir)
	for i := 0; i < 2; i++ {
		head, err := verifiedChainFreezerSnapshotHeadWithCache(dir, built.Manifest, clone)
		if err != nil || head != 5 {
			t.Fatalf("chain snapshot query %d: head=%d err=%v", i, head, err)
		}
		if stats := clone.Stats(); stats.PersistentHits != 1 || stats.MemoryHits != uint64(i) || stats.FullVerified != 0 {
			t.Fatalf("chain snapshot query %d stats: %+v", i, stats)
		}
	}
	path := filepath.Join(dir, chainIndexRefs(built.Manifest)[0].Path)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
		t.Fatal(err)
	}
	if _, err := verifiedChainFreezerSnapshotHeadWithCache(dir, built.Manifest, parent.snapshotPersistent(dir)); err == nil {
		t.Fatal("chain snapshot accepted same-size/same-mtime corruption")
	}
}

func TestVerificationSnapshotIndependentStateAndFallback(t *testing.T) {
	dir, manager, _, _ := pinnedVerificationFixture(t)
	parent := manager.chainVerificationCache
	chainRecord := chainFreezerVerificationRecord{Freezer: SegmentRef{Path: "chain/example.seg"}}
	parent.persistent[chainRecord] = struct{}{}
	parent.eventPersistWarned = true
	parent.eventFlights[eventLogVerificationKey{}] = &eventLogVerificationFlight{done: make(chan struct{})}
	clone := parent.snapshotPersistent(dir)
	if len(clone.persistent) != 1 || len(clone.eventPersistent) != 1 || len(clone.verified) != 0 || len(clone.eventVerified) != 0 || len(clone.eventFlights) != 0 || clone.eventPersistWarned || clone.dirty {
		t.Fatalf("unexpected snapshot state: %+v", clone.Stats())
	}
	delete(clone.persistent, chainRecord)
	for key, record := range clone.eventPersistent {
		record.Events[0].Path = "changed"
		clone.eventPersistent[key] = record
		if parent.eventPersistent[key].Events[0].Path == "changed" {
			t.Fatal("snapshot shares the event companion slice")
		}
		delete(clone.eventPersistent, key)
	}
	if len(parent.persistent) != 1 || len(parent.eventPersistent) != 1 || !parent.dirty {
		t.Fatal("snapshot mutations changed parent records or dirty state")
	}
	otherDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(otherDir, chainFreezerVerificationCacheFile), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, cache := range map[string]*ChainFreezerVerificationCache{
		"nil":                 nil,
		"different_directory": parent,
		"failed_parent":       {dir: otherDir, loadErr: fmt.Errorf("load failed")},
	} {
		t.Run(name, func(t *testing.T) {
			got := cache.snapshotPersistent(otherDir)
			if got.LoadError() == nil || got.Stats().LoadErrors != 1 || len(got.eventPersistent) != 0 {
				t.Fatal("fallback did not load the requested directory independently")
			}
		})
	}
}

func TestVerificationSnapshotConcurrentVerificationAndPersistence(t *testing.T) {
	dir, manager, ref, index := pinnedVerificationFixture(t)
	parent := manager.chainVerificationCache
	var wg sync.WaitGroup
	errors := make(chan error, 4)
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 12; i++ {
				cache := parent
				if worker != 0 {
					cache = parent.snapshotPersistent(dir)
				}
				if _, _, err := cache.verifyEventLogIndex(dir, index, []SegmentRef{ref}); err != nil {
					errors <- err
					return
				}
				if worker == 0 {
					if err := parent.persistPending(); err != nil {
						errors <- err
						return
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

func BenchmarkVerificationCachePinnedSnapshot(b *testing.B) {
	// Synthetic valid proof metadata at two observed production record counts
	// (2026-09-15 and 2026-10-10). This measures constructor cost, not archival
	// throughput or the still-required companion SHA authentication.
	for _, count := range []int{39_069, 70_015} {
		b.Run(fmt.Sprintf("records_%d", count), func(b *testing.B) {
			disk := verificationCacheEncodingFixture(count)
			data, err := json.Marshal(disk)
			if err != nil {
				b.Fatal(err)
			}
			dir := b.TempDir()
			if err := os.WriteFile(filepath.Join(dir, chainFreezerVerificationCacheFile), data, 0o600); err != nil {
				b.Fatal(err)
			}
			parent := NewChainFreezerVerificationCache(dir)
			if err := parent.LoadError(); err != nil {
				b.Fatal(err)
			}
			for _, snapshot := range []bool{false, true} {
				b.Run(fmt.Sprintf("snapshot_%t", snapshot), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						cache := parent
						if snapshot {
							cache = parent.snapshotPersistent(dir)
						} else {
							cache = NewChainFreezerVerificationCache(dir)
						}
						if cache.LoadError() != nil || len(cache.eventPersistent) != len(disk.EventEntries) {
							b.Fatal("constructor changed proof records")
						}
					}
				})
			}
		})
	}
}
