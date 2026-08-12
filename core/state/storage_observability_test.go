package state

import (
	"encoding/binary"
	"errors"
	"testing"
	"unsafe"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestStorageReadMetricsBatchOnSuccessfulCommit(t *testing.T) {
	sdb, reader, addr := stateWithStorageReader(t)
	beforeCalls := storageReadCallsCounter.Snapshot().Count()
	beforeHits := storageReadObjectCacheHitCounter.Snapshot().Count()
	beforeCreated := storageReadCreatedZeroCounter.Snapshot().Count()
	beforeAccountMissing := storageReadAccountMissingZeroCounter.Snapshot().Count()
	beforeCold := storageReadColdCounter.Snapshot().Count()
	beforeFound := storageReadColdFoundCounter.Snapshot().Count()
	beforeMissing := storageReadColdMissingCounter.Snapshot().Count()
	beforeErrors := storageReadColdErrorCounter.Snapshot().Count()

	reader.value = []byte{0x11}
	reader.found = true
	foundKey := tcommon.Hash{0x01}
	if got, exists := sdb.GetStateWithExist(addr, foundKey); !exists || got[31] != 0x11 {
		t.Fatalf("cold found = (%x,%v), want (...11,true)", got, exists)
	}
	if got, exists := sdb.GetStateWithExist(addr, foundKey); !exists || got[31] != 0x11 {
		t.Fatalf("object-cache hit = (%x,%v), want (...11,true)", got, exists)
	}

	reader.value = nil
	reader.found = false
	if got, exists := sdb.GetStateWithExist(addr, tcommon.Hash{0x02}); exists || got != (tcommon.Hash{}) {
		t.Fatalf("cold missing = (%x,%v), want (zero,false)", got, exists)
	}

	createdAddr := testAddr(0xb1)
	sdb.CreateAccount(createdAddr, corepb.AccountType_Contract)
	if got, exists := sdb.GetStateWithExist(createdAddr, tcommon.Hash{0x04}); exists || got != (tcommon.Hash{}) {
		t.Fatalf("created account storage = (%x,%v), want (zero,false)", got, exists)
	}
	if got, exists := sdb.GetStateWithExist(testAddr(0xb2), tcommon.Hash{0x05}); exists || got != (tcommon.Hash{}) {
		t.Fatalf("missing account storage = (%x,%v), want (zero,false)", got, exists)
	}

	m := sdb.storageObservability
	if m.calls() != 5 || m.objectCacheHits != 1 || m.createdZero != 1 || m.accountMissingZero != 1 || m.coldReads != 2 {
		t.Fatalf("read outcomes = %+v calls=%d, want calls/hit/created/account-missing/cold 5/1/1/1/2", m, m.calls())
	}
	if m.coldFound != 1 || m.coldMissing != 1 || m.coldErrors != 0 {
		t.Fatalf("cold outcomes found/missing/error = %d/%d/%d, want 1/1/0", m.coldFound, m.coldMissing, m.coldErrors)
	}
	// Registered counters remain untouched until the successful commit boundary.
	if got := storageReadCallsCounter.Snapshot().Count() - beforeCalls; got != 0 {
		t.Fatalf("pre-commit registered calls delta = %d, want 0", got)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if sdb.storageObservability != (storageObservabilityBatch{}) {
		t.Fatalf("successful commit retained local batch: %+v", sdb.storageObservability)
	}
	checks := []struct {
		name string
		got  int64
		want int64
	}{
		{"calls", storageReadCallsCounter.Snapshot().Count() - beforeCalls, 5},
		{"object hits", storageReadObjectCacheHitCounter.Snapshot().Count() - beforeHits, 1},
		{"created zero", storageReadCreatedZeroCounter.Snapshot().Count() - beforeCreated, 1},
		{"account missing zero", storageReadAccountMissingZeroCounter.Snapshot().Count() - beforeAccountMissing, 1},
		{"cold", storageReadColdCounter.Snapshot().Count() - beforeCold, 2},
		{"cold found", storageReadColdFoundCounter.Snapshot().Count() - beforeFound, 1},
		{"cold missing", storageReadColdMissingCounter.Snapshot().Count() - beforeMissing, 1},
		{"cold errors", storageReadColdErrorCounter.Snapshot().Count() - beforeErrors, 0},
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s delta = %d, want %d", check.name, check.got, check.want)
		}
	}
}

func TestStorageReadErrorPoisonsCommit(t *testing.T) {
	sdb, reader, addr := stateWithStorageReader(t)
	reader.err = errors.New("temporary")
	if got, exists := sdb.GetStateWithExist(addr, tcommon.Hash{0x03}); exists || got != (tcommon.Hash{}) {
		t.Fatalf("cold error = (%x,%v), want fail-closed zero,false", got, exists)
	}
	if sdb.Error() == nil {
		t.Fatal("storage read error did not poison StateDB")
	}
	reader.err = nil
	if _, err := sdb.Commit(); err == nil {
		t.Fatal("commit succeeded after rooted storage read failure")
	}
}

func TestStorageColdReadCountsCommitScopePendingResolution(t *testing.T) {
	sdb, _, addr := stateWithStorageReader(t)
	scope := sdb.NewCommitScope()
	defer scope.Discard()
	obj := sdb.stateObjects[addr]
	slot := tcommon.Hash{0x31}
	rowKey := sdb.storageRowKey(addr, slot)
	lookup := accountKVLatestPendingLookupKey(addr, obj.accountKVGeneration, kvdomains.ContractStorage, rowKey[:])
	scope.latestWriter.pending = map[accountKVLatestPendingMapKey]accountKVLatestPending{
		lookup: {value: []byte{0x77}},
	}
	if got, exists := sdb.GetStateWithExist(addr, slot); !exists || got[31] != 0x77 {
		t.Fatalf("pending storage = (%x,%v), want (...77,true)", got, exists)
	}
	if sdb.storageObservability.coldReads != 1 || sdb.storageObservability.coldPendingResolved != 1 || sdb.storageObservability.coldFound != 1 {
		t.Fatalf("pending cold metrics = %+v, want cold/pending/found 1/1/1", sdb.storageObservability)
	}
}

func fillOversizedStorage(obj *stateObject) {
	obj.ensureStorage()
	for i := 0; i <= maxStateObjectCachedStorageSlots; i++ {
		var key tcommon.Hash
		binary.BigEndian.PutUint64(key[24:], uint64(i+1))
		obj.storage[key] = storageSlot{value: tcommon.Hash{byte(i + 1)}, exists: true}
	}
}

func TestOversizedStorageExactSampleReloadAgesAndExpiry(t *testing.T) {
	sdb, reader, addr := stateWithStorageReader(t)
	obj := sdb.stateObjects[addr]
	obj.dirty = false
	obj.accountDirty = false
	fillOversizedStorage(obj)

	sdb.rotateStateObjectWorkingSet()
	if obj.storage != nil {
		t.Fatal("oversized cache was not released")
	}
	sample := sdb.oversizedStorageSamples[addr]
	if sample == nil || sample.keyCount != oversizedStorageSampleKeyLimit {
		t.Fatalf("sample = %#v count=%d, want exact 128-key sample", sample, func() uint8 {
			if sample == nil {
				return 0
			}
			return sample.keyCount
		}())
	}
	if got := sdb.storageObservability.oversizedReleases; got != 1 {
		t.Fatalf("oversized releases = %d, want 1", got)
	}
	if got := sdb.storageObservability.oversizedReleasedSlots; got != maxStateObjectCachedStorageSlots+1 {
		t.Fatalf("released slots = %d, want %d", got, maxStateObjectCachedStorageSlots+1)
	}
	if got := sdb.storageObservability.oversizedReleaseMax; got != maxStateObjectCachedStorageSlots+1 {
		t.Fatalf("release max = %d, want %d", got, maxStateObjectCachedStorageSlots+1)
	}

	keys := [oversizedStorageSampleMaxAge]tcommon.Hash{}
	copy(keys[:], sample.keys[:oversizedStorageSampleMaxAge])
	reader.value = []byte{0x55}
	reader.found = true
	// A failed cold read must stay sampled so the successful retry is counted.
	reader.err = errors.New("retry")
	sdb.GetStateWithExist(addr, keys[0])
	if sample.keyCount != oversizedStorageSampleKeyLimit {
		t.Fatalf("failed read removed sample: count=%d", sample.keyCount)
	}
	reader.err = nil

	for age, key := range keys {
		if got, exists := sdb.GetStateWithExist(addr, key); !exists || got[31] != 0x55 {
			t.Fatalf("age %d reload = (%x,%v), want (...55,true)", age+1, got, exists)
		}
		if age+1 < len(keys) {
			sdb.rotateStateObjectWorkingSet()
		}
	}
	if got := sdb.storageObservability.oversizedReloaded; got != oversizedStorageSampleMaxAge {
		t.Fatalf("reloaded = %d, want %d", got, oversizedStorageSampleMaxAge)
	}
	for age, count := range sdb.storageObservability.oversizedReloadedByAge {
		if count != 1 {
			t.Errorf("reloaded age %d = %d, want 1", age+1, count)
		}
	}
	// Complete age four, then settle every exact key that remained unseen.
	sdb.rotateStateObjectWorkingSet()
	if len(sdb.oversizedStorageSamples) != 0 {
		t.Fatalf("expired samples = %d, want 0", len(sdb.oversizedStorageSamples))
	}
	if got := sdb.storageObservability.oversizedUnreloaded; got != oversizedStorageSampleKeyLimit-oversizedStorageSampleMaxAge {
		t.Fatalf("unreloaded = %d, want %d", got, oversizedStorageSampleKeyLimit-oversizedStorageSampleMaxAge)
	}
}

func TestOversizedStorageSampleContractAndMemoryBounds(t *testing.T) {
	sdb := newTestStateDB(t)
	sharedStorage := make(map[tcommon.Hash]storageSlot, maxStateObjectCachedStorageSlots+1)
	for i := 0; i <= maxStateObjectCachedStorageSlots; i++ {
		var key tcommon.Hash
		binary.BigEndian.PutUint64(key[24:], uint64(i+1))
		sharedStorage[key] = storageSlot{exists: true}
	}
	for i := 0; i <= oversizedStorageSampleContractLimit; i++ {
		addr := testAddr(byte(i + 1))
		obj := sdb.newStateObject(addr, nil)
		obj.storage = sharedStorage
		sdb.stateObjects[addr] = obj
		sdb.recordOversizedStorageRelease(obj)
	}
	if got := len(sdb.oversizedStorageSamples); got != oversizedStorageSampleContractLimit {
		t.Fatalf("sampled contracts = %d, want %d", got, oversizedStorageSampleContractLimit)
	}
	if got := sdb.storageObservability.oversizedSampled; got != oversizedStorageSampleContractLimit*oversizedStorageSampleKeyLimit {
		t.Fatalf("sampled keys = %d, want %d", got, oversizedStorageSampleContractLimit*oversizedStorageSampleKeyLimit)
	}
	if got := sdb.storageObservability.oversizedSampleSkipped; got != 1 {
		t.Fatalf("skipped contracts = %d, want 1", got)
	}
	if retained := uintptr(oversizedStorageSampleContractLimit) * unsafe.Sizeof(oversizedStorageSample{}); retained >= 512<<10 {
		t.Fatalf("fixed sample records retain %d bytes, want < 0.5 MiB", retained)
	}
}

func TestOversizedStorageSampleSettlesOnEvictionAndReplacement(t *testing.T) {
	t.Run("eviction", func(t *testing.T) {
		sdb := newTestStateDB(t)
		addr := testAddr(0xd8)
		sdb.CreateAccount(addr, corepb.AccountType_Contract)
		obj := sdb.stateObjects[addr]
		obj.dirty = false
		obj.accountDirty = false
		obj.created = false
		sdb.oversizedStorageSamples = map[tcommon.Address]*oversizedStorageSample{
			addr: {object: obj, accountGeneration: obj.accountKVGeneration, keyCount: 2},
		}
		obj.cacheTouched = false
		obj.cacheGeneration = 0
		sdb.stateObjectWorkingGeneration = 4
		sdb.touchedStateObjects = nil
		sdb.ancientRetainedStateObjects = []tcommon.Address{addr}
		sdb.rotateStateObjectWorkingSet()
		if sdb.stateObjects[addr] != nil || len(sdb.oversizedStorageSamples) != 0 {
			t.Fatal("eviction retained account or exact sample")
		}
		if got := sdb.storageObservability.oversizedUnreloaded; got != 2 {
			t.Fatalf("eviction unreloaded = %d, want 2", got)
		}
	})

	t.Run("replacement", func(t *testing.T) {
		sdb := newTestStateDB(t)
		addr := testAddr(0xd9)
		sdb.CreateAccount(addr, corepb.AccountType_Contract)
		old := sdb.stateObjects[addr]
		old.deleted = true
		sdb.oversizedStorageSamples = map[tcommon.Address]*oversizedStorageSample{
			addr: {object: old, accountGeneration: old.accountKVGeneration, keyCount: 3},
		}
		if next := sdb.getOrCreateAccount(addr); next == old {
			t.Fatal("deleted account was not replaced")
		}
		if len(sdb.oversizedStorageSamples) != 0 {
			t.Fatal("replacement retained old exact sample")
		}
		if got := sdb.storageObservability.oversizedUnreloaded; got != 3 {
			t.Fatalf("replacement unreloaded = %d, want 3", got)
		}
	})
}

func BenchmarkGetStateCachedStorageObserved(b *testing.B) {
	sdb, err := New(tcommon.Hash{}, NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		b.Fatal(err)
	}
	addr := testAddr(0xef)
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	obj := sdb.stateObjects[addr]
	slot := tcommon.Hash{0x01}
	obj.cacheStorageSlot(slot, storageSlot{value: tcommon.Hash{0x02}, exists: true})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sdb.GetStateWithExist(addr, slot)
	}
}

func BenchmarkOversizedStorageSampleColdLookupMiss(b *testing.B) {
	sdb := &StateDB{stateObjectWorkingGeneration: 1}
	obj := &stateObject{address: testAddr(0xee)}
	sample := &oversizedStorageSample{object: obj, releaseGeneration: 0, keyCount: oversizedStorageSampleKeyLimit}
	for i := range sample.keys {
		sample.keys[i][31] = byte(i)
	}
	sdb.oversizedStorageSamples = map[tcommon.Address]*oversizedStorageSample{obj.address: sample}
	miss := tcommon.Hash{0xff, 0xff}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sdb.recordOversizedStorageReload(obj, miss)
	}
}
