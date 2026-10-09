package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type cleanupFakeRoutes struct {
	route   rawdb.HistoryStagingRoute
	claim   *rawdb.HistoryStagingClaim
	binding rawdb.HistoryStagingColdBinding
	reset   bool
}

func (f cleanupFakeRoutes) CurrentEpoch() (uint64, error) { return 1, nil }
func (f cleanupFakeRoutes) ReadResetIntent() (rawdb.HistoryStagingResetIntent, bool, error) {
	return rawdb.HistoryStagingResetIntent{}, f.reset, nil
}
func (f cleanupFakeRoutes) ReadHistoryStagingRouteBarrier() (rawdb.HistoryStagingRouteBarrier, bool, error) {
	return rawdb.HistoryStagingRouteBarrier{Version: 1, Epoch: 1, ThroughBucket: 2, EligibleThrough: 1,
		PlanDigest: [32]byte{1}, CandidateSHA: [32]byte{2}, RouteDigest: [32]byte{3}}, true, nil
}
func (f cleanupFakeRoutes) ReadRoute(bucket uint64) (rawdb.HistoryStagingRoute, bool, error) {
	return f.route, f.route.Bucket == bucket, nil
}
func (f cleanupFakeRoutes) ReadClaim(bucket uint64) (rawdb.HistoryStagingClaim, bool, error) {
	if f.claim == nil {
		return rawdb.HistoryStagingClaim{}, false, nil
	}
	return *f.claim, true, nil
}
func (f cleanupFakeRoutes) ReadColdBindingAt(epoch, bucket uint64) (rawdb.HistoryStagingColdBinding, bool, error) {
	return f.binding, f.binding.Epoch == epoch && f.binding.Bucket == bucket, nil
}

type cleanupFakeAudit struct{ verified int }

func (a *cleanupFakeAudit) VerifyBinding(context.Context, rawdb.HistoryStagingColdBinding) error {
	a.verified++
	return nil
}
func (*cleanupFakeAudit) RecheckAll(context.Context) error { return nil }
func (*cleanupFakeAudit) AuthenticatedTrios() int          { return 1 }

func TestCleanupColdPrefixRequiresCurrentRouteNoClaimAndWholeBinding(t *testing.T) {
	fixture := cleanupFakeRoutes{route: rawdb.HistoryStagingRoute{Version: 1, Bucket: 1, Epoch: 1,
		Owner: rawdb.HistoryStagingOwnerCold, ColdBindingEpoch: 1},
		binding: rawdb.HistoryStagingColdBinding{Version: 1, Bucket: 1, Epoch: 1, BindingEpoch: 1, ManifestEpoch: 1,
			Spans: []rawdb.HistoryStagingColdSpan{{From: 1024, To: 2047, ContentID: [32]byte{1}, SemanticHash: [32]byte{2}, TxRangeDigest: [32]byte{3}}}}}
	for _, test := range []struct {
		name string
		edit func(*cleanupFakeRoutes)
		ok   bool
	}{
		{"durable cold-only binding needs no target receipt", func(*cleanupFakeRoutes) {}, true},
		{"source owner", func(f *cleanupFakeRoutes) { f.route.Owner = rawdb.HistoryStagingOwnerSource }, false},
		{"missing route", func(f *cleanupFakeRoutes) { f.route.Bucket = 2 }, false},
		{"current claim", func(f *cleanupFakeRoutes) { f.claim = &rawdb.HistoryStagingClaim{Epoch: 1} }, false},
		{"stale claim harmless", func(f *cleanupFakeRoutes) { f.claim = &rawdb.HistoryStagingClaim{Epoch: 0} }, true},
		{"partial cold binding", func(f *cleanupFakeRoutes) { f.binding.Spans[0].To = 2046 }, false},
		{"binding epoch differs", func(f *cleanupFakeRoutes) { f.binding.BindingEpoch = 2 }, false},
		{"reset intent", func(f *cleanupFakeRoutes) { f.reset = true }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := fixture
			f.binding.Spans = append([]rawdb.HistoryStagingColdSpan(nil), fixture.binding.Spans...)
			test.edit(&f)
			audit := &cleanupFakeAudit{}
			_, err := proveCleanupPrefix(context.Background(), f, audit, 2047, nil)
			if (err == nil) != test.ok {
				t.Fatalf("proof result=%v, wanted ok=%t", err, test.ok)
			}
			if !test.ok && audit.verified != 0 {
				t.Fatal("rejected route nevertheless authenticated its cold binding")
			}
		})
	}
}

func TestCleanupBloomCanOnlyRetainOnCollision(t *testing.T) {
	b, err := newCleanupBloom(1, 7)
	if err != nil {
		t.Fatal(err)
	}
	var inserted [][32]byte
	for i := byte(0); i < 64; i++ {
		var hash [32]byte
		hash[0], hash[8] = i, i*3
		b.Add(hash)
		inserted = append(inserted, hash)
	}
	for _, hash := range inserted {
		if !b.MayContain(hash) {
			t.Fatal("Bloom saturation introduced a false negative")
		}
	}
}

type cleanupFaultSyncStore struct {
	ethdb.KeyValueStore
	err error
}

func (s cleanupFaultSyncStore) SyncKeyValue() error { return s.err }

func TestCleanupSyncFailureDoesNotCountCommittedDelete(t *testing.T) {
	db := cleanupFaultSyncStore{KeyValueStore: rawdb.NewMemoryDatabase(), err: errors.New("sync failed")}
	defer db.Close()
	stats := &cleanupDeleteStats{}
	w, err := newCleanupBatchWriter(db, t.TempDir(), stats)
	if err != nil {
		t.Fatal(err)
	}
	defer w.batch.Close()
	w.minFree = 0
	if err := w.Delete([]byte("posting-key"), 11, false); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err == nil || !stats.DurabilityUncertainTail || stats.PostingDeleted != 0 || stats.CommittedLogicalBytes != 0 {
		t.Fatalf("ambiguous sync was reported committed: stats=%+v err=%v", stats, err)
	}
}

type cleanupFaultBatch struct {
	ethdb.Batch
	err error
}

func (b cleanupFaultBatch) Write() error { return b.err }

type cleanupFaultWriteStore struct {
	ethdb.KeyValueStore
	err error
}

func (s cleanupFaultWriteStore) NewBatch() ethdb.Batch {
	return cleanupFaultBatch{Batch: s.KeyValueStore.NewBatch(), err: s.err}
}
func (s cleanupFaultWriteStore) SyncKeyValue() error { return nil }

func TestCleanupWriteFailureDoesNotCountCommittedDelete(t *testing.T) {
	db := cleanupFaultWriteStore{KeyValueStore: rawdb.NewMemoryDatabase(), err: errors.New("write failed")}
	defer db.Close()
	stats := &cleanupDeleteStats{}
	w, err := newCleanupBatchWriter(db, t.TempDir(), stats)
	if err != nil {
		t.Fatal(err)
	}
	defer w.batch.Close()
	w.minFree = 0
	if err := w.Delete([]byte("posting-key"), 11, false); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err == nil || !stats.DurabilityUncertainTail || stats.PostingDeleted != 0 {
		t.Fatalf("ambiguous write was reported committed: stats=%+v err=%v", stats, err)
	}
}

func TestCleanupAdmissionRefusesSpaceAndBudgetBeforeWrite(t *testing.T) {
	db := cleanupFaultSyncStore{KeyValueStore: rawdb.NewMemoryDatabase()}
	defer db.Close()
	w, err := newCleanupBatchWriter(db, t.TempDir(), &cleanupDeleteStats{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.batch.Close()
	w.maxBytes = 4
	if err := w.Delete([]byte("posting-key"), 11, false); err == nil {
		t.Fatal("logical byte budget allowed oversized key")
	}
	w.maxBytes = 100
	w.minFree = ^uint64(0)
	if err := db.Put([]byte("posting-key"), []byte("must remain")); err != nil {
		t.Fatal(err)
	}
	if err := w.Delete([]byte("posting-key"), 11, false); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err == nil {
		t.Fatal("free-space refusal did not stop write")
	}
	if value, err := db.Get([]byte("posting-key")); err != nil || string(value) != "must remain" {
		t.Fatalf("free-space rejection changed value: value=%q err=%v", value, err)
	}
}

func TestCleanupEmptyHoldReplacedIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offline-maintenance.hold")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	hold, err := readCleanupHold(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := hold.Recheck(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := hold.Recheck(); err == nil {
		t.Fatal("replaced hold inode accepted")
	}
}

// This exercises the production page scanner and delete batches against a
// real Pebble store. The only physical keys are made by schema-owned writers;
// the two packed frames replace writer-created singletons using the persisted
// posting value codec (version, count, positive delta).
func cleanupPostingFixture(t *testing.T) (ethdb.KeyValueStore, string, map[uint64][]byte, [32]byte, [32]byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hot")
	db, err := rawdb.NewPebbleDB(path, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	for _, block := range []uint64{0, 1023, 1024, 2047, 2048} {
		if err := rawdb.WriteStateDomainChangePostingIndex(db, &rawdb.StateDomainChange{
			BlockNum: block, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0x41, 1},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStateDomainChangePostingIndex(db, &rawdb.StateDomainChange{
		BlockNum: 1200, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0x41, 2},
	}); err != nil {
		t.Fatal(err)
	}
	keys := make(map[uint64][]byte)
	var sharedHash, orphanHash [32]byte
	if err := rawdb.ScanStateChangePostingFrames(context.Background(), db, func(frame rawdb.StateChangePostingFrameCursor) error {
		keys[frame.FirstBlock] = append([]byte(nil), frame.Key...)
		if frame.FirstBlock == 0 {
			sharedHash = frame.Hash
		}
		if frame.FirstBlock == 1200 {
			orphanHash = frame.Hash
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 6 || sharedHash == orphanHash {
		t.Fatalf("invalid posting fixture: keys=%d shared=%x orphan=%x", len(keys), sharedHash, orphanHash)
	}
	for _, first := range []uint64{1023, 2047} {
		if err := db.Put(keys[first], []byte{1, 2, 1}); err != nil {
			t.Fatal(err)
		}
	}
	return db, path, keys, sharedHash, orphanHash
}

func TestCleanupRealPebbleWholeFramesAndDirectoryOrder(t *testing.T) {
	db, path, keys, sharedHash, orphanHash := cleanupPostingFixture(t)
	defer db.Close()
	stats := &cleanupDeleteStats{}
	if err := runCleanupDeletesWithOptions(context.Background(), db, path, 2047, nil, nil, "", stats, 8, 2, 0); err != nil {
		t.Fatal(err)
	}
	if stats.Phase != "complete" || stats.PostingScanned != 6 || stats.PostingDeleted != 2 || stats.PostingRetained != 4 ||
		stats.DirectoryScanned != 2 || stats.DirectoryDeleted != 1 || stats.DirectoryRetained != 1 {
		t.Fatalf("unexpected cleanup sequence: %+v", stats)
	}
	for first, key := range keys {
		present, err := db.Has(key)
		if err != nil {
			t.Fatal(err)
		}
		if want := first != 1024 && first != 1200; present != want {
			t.Fatalf("frame %d presence=%t, want %t", first, present, want)
		}
	}
	var hashes [][32]byte
	if err := rawdb.ScanStateChangeKeyDirectory(context.Background(), db, func(row rawdb.StateChangeDirectoryCursor) error {
		hashes = append(hashes, row.Hash)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(hashes) != 1 || hashes[0] != sharedHash || hashes[0] == orphanHash {
		t.Fatalf("directory retained wrong hashes: %x", hashes)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := rawdb.NewPebbleDBReadOnly(path, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if present, err := reopened.Has(keys[1023]); err != nil || !present {
		t.Fatalf("retained crossing frame lost after reopen: present=%t err=%v", present, err)
	}
	if present, err := reopened.Has(keys[1200]); err != nil || present {
		t.Fatalf("removed frame survived reopen: present=%t err=%v", present, err)
	}
}

func TestCleanupRealPebblePostingSyncFailureSkipsDirectory(t *testing.T) {
	base, path, keys, _, _ := cleanupPostingFixture(t)
	defer base.Close()
	db := cleanupFaultSyncStore{KeyValueStore: base, err: errors.New("injected posting sync failure")}
	stats := &cleanupDeleteStats{}
	if err := runCleanupDeletesWithOptions(context.Background(), db, path, 2047, nil, nil, "", stats, 8, 2, 0); err == nil {
		t.Fatal("posting sync failure was ignored")
	}
	if stats.Phase != "posting" || stats.DirectoryScanned != 0 || stats.DirectoryDeleted != 0 || !stats.DurabilityUncertainTail {
		t.Fatalf("directory phase started after ambiguous posting sync: %+v", stats)
	}
	if present, err := base.Has(keys[1023]); err != nil || !present {
		t.Fatalf("crossing frame changed after sync failure: present=%t err=%v", present, err)
	}
}

func TestCleanupRealPebbleMalformedPostingSkipsDirectory(t *testing.T) {
	db, path, keys, _, _ := cleanupPostingFixture(t)
	defer db.Close()
	if err := db.Put(keys[2048], []byte{1, 2, 0}); err != nil {
		t.Fatal(err)
	}
	stats := &cleanupDeleteStats{}
	if err := runCleanupDeletesWithOptions(context.Background(), db, path, 2047, nil, nil, "", stats, 8, 2, 0); err == nil {
		t.Fatal("malformed posting frame was accepted")
	}
	if stats.Phase != "posting" || stats.DirectoryScanned != 0 || stats.DirectoryDeleted != 0 {
		t.Fatalf("directory phase started after malformed posting: %+v", stats)
	}
}

func TestCleanupInheritedOperationLock(t *testing.T) {
	const childEnv = "GTRON_CLEANUP_LOCK_TEST_CHILD"
	if os.Getenv(childEnv) == "1" {
		path := os.Getenv("GTRON_CLEANUP_LOCK_TEST_PATH")
		lock, err := acquireCleanupOperationLock(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkCleanupOperationLock(lock, path); err != nil {
			t.Fatal(err)
		}
		// Close only our duplicate. LOCK_UN here would also unlock the parent
		// shell's shared open-file description.
		if err := lock.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}
	path := filepath.Join(t.TempDir(), "start.lock")
	parent, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	if err := syscall.Flock(int(parent.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	// ExtraFiles occupies descriptors 3..9 in the child; 9 is the actual
	// inherited locked parent descriptor, not a newly opened lookalike.
	var extras []*os.File
	for i := 3; i < 9; i++ {
		f, err := os.Open(os.DevNull)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		extras = append(extras, f)
	}
	extras = append(extras, parent)
	cmd := exec.Command(os.Args[0], "-test.run=^TestCleanupInheritedOperationLock$")
	cmd.Env = append(os.Environ(), childEnv+"=1", "GTRON_CLEANUP_LOCK_TEST_PATH="+path)
	cmd.ExtraFiles = extras
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("inherited lock child failed: %v: %s", err, out)
	}
	independent, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer independent.Close()
	if err := syscall.Flock(int(independent.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
		_ = syscall.Flock(int(independent.Fd()), syscall.LOCK_UN)
		t.Fatal("child close released parent-held operation lock")
	}
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkCleanupOperationLock(parent, path); err == nil {
		t.Fatal("replaced start.lock path accepted")
	}
}
