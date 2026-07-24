package state

import (
	"encoding/binary"
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type countingStorageReader struct {
	storageReads int
	value        []byte
	found        bool
	err          error
}

func (r *countingStorageReader) GetLatest(_ tcommon.Address, domain kvdomains.KVDomain, _ []byte) ([]byte, bool, error) {
	if domain == kvdomains.ContractStorage {
		r.storageReads++
	}
	return append([]byte(nil), r.value...), r.found, r.err
}

func stateWithStorageReader(t *testing.T) (*StateDB, *countingStorageReader, tcommon.Address) {
	t.Helper()
	sdb := newTestStateDB(t)
	addr := testAddr(0xa1)
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	obj := sdb.getStateObject(addr)
	obj.created = false
	// Avoid metadata reads in this focused storage-row benchmark. A dirty nil
	// metadata value is the in-memory representation of a known absent row.
	obj.contractMetaDirty = true
	reader := new(countingStorageReader)
	sdb.setAccountKVLatestView(reader, nil)
	return sdb, reader, addr
}

func TestGetStateCachesMissingStorageRow(t *testing.T) {
	sdb, reader, addr := stateWithStorageReader(t)
	slot := tcommon.Hash{0x01}
	for i := 0; i < 100; i++ {
		if got, exists := sdb.GetStateWithExist(addr, slot); got != (tcommon.Hash{}) || exists {
			t.Fatalf("missing slot read %d = (%x,%v), want zero,false", i, got, exists)
		}
	}
	if reader.storageReads != 1 {
		t.Fatalf("durable storage reads = %d, want 1", reader.storageReads)
	}
}

func TestGetStateCachesDurableZeroAsMissing(t *testing.T) {
	sdb, reader, addr := stateWithStorageReader(t)
	reader.value = make([]byte, len(tcommon.Hash{}))
	reader.found = true
	slot := tcommon.Hash{0x02}
	for i := 0; i < 3; i++ {
		if got, exists := sdb.GetStateWithExist(addr, slot); got != (tcommon.Hash{}) || exists {
			t.Fatalf("zero slot read %d = (%x,%v), want zero,false", i, got, exists)
		}
	}
	if reader.storageReads != 1 {
		t.Fatalf("durable storage reads = %d, want 1", reader.storageReads)
	}
}

func TestGetStateDoesNotCacheReadError(t *testing.T) {
	sdb, reader, addr := stateWithStorageReader(t)
	reader.err = errors.New("temporary storage read failure")
	slot := tcommon.Hash{0x03}
	for i := 0; i < 3; i++ {
		if got, exists := sdb.GetStateWithExist(addr, slot); got != (tcommon.Hash{}) || exists {
			t.Fatalf("failed slot read %d = (%x,%v), want zero,false", i, got, exists)
		}
	}
	if reader.storageReads != 3 {
		t.Fatalf("durable storage reads = %d, want 3 retries", reader.storageReads)
	}
	if _, cached := sdb.getStateObject(addr).storage[slot]; cached {
		t.Fatal("transient read error populated the storage cache")
	}
}

func TestSetStateUsesCachedMissingOrigin(t *testing.T) {
	sdb, reader, addr := stateWithStorageReader(t)
	slot := tcommon.Hash{0x04}
	value := tcommon.Hash{0x99}
	if _, exists := sdb.GetStateWithExist(addr, slot); exists {
		t.Fatal("slot unexpectedly exists")
	}
	sdb.SetState(addr, slot, value)
	if got, exists := sdb.GetStateWithExist(addr, slot); got != value || !exists {
		t.Fatalf("written slot = (%x,%v), want (%x,true)", got, exists, value)
	}
	if reader.storageReads != 1 {
		t.Fatalf("durable storage reads = %d, want 1", reader.storageReads)
	}
	origin := sdb.getStateObject(addr).dirtyStorage[slot]
	if !origin.loaded || origin.exists || origin.value != (tcommon.Hash{}) {
		t.Fatalf("dirty origin = %+v, want loaded absent zero", origin)
	}
}

func TestReadCachedMissingStorageDoesNotCommitAsDirty(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xa3)
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	index := &countingKVIndexStore{KeyValueStore: reopened.db.DiskDB()}
	reopened.SetAccountKVIndexStore(index)
	reopened.SetAccountKVIndexReads(true)
	if got, exists := reopened.GetStateWithExist(addr, tcommon.Hash{0x05}); got != (tcommon.Hash{}) || exists {
		t.Fatalf("missing storage = (%x,%v), want zero,false", got, exists)
	}
	reopened.AddBalance(addr, 1)

	index.resetCounts()
	if _, err := reopened.Commit(); err != nil {
		t.Fatal(err)
	}
	if index.puts != 0 || index.deletes != 0 {
		t.Fatalf("read-cached miss touched latest index: puts=%d deletes=%d", index.puts, index.deletes)
	}
}

func BenchmarkGetStateMissingStorageRow(b *testing.B) {
	// newTestStateDB only needs testing.TB behavior, but its legacy helper is
	// typed to *testing.T. Build the equivalent minimal state directly here.
	sdb, err := New(tcommon.Hash{}, NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		b.Fatal(err)
	}
	addr := testAddr(0xa1)
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	obj := sdb.getStateObject(addr)
	obj.created = false
	obj.contractMetaDirty = true
	reader := new(countingStorageReader)
	sdb.setAccountKVLatestView(reader, nil)
	slot := tcommon.Hash{0x01}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sdb.GetStateWithExist(addr, slot)
	}
}

func BenchmarkGetStateUniqueMissingStorageRows(b *testing.B) {
	sdb, err := New(tcommon.Hash{}, NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		b.Fatal(err)
	}
	addr := testAddr(0xa2)
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	obj := sdb.getStateObject(addr)
	obj.created = false
	obj.contractMetaDirty = true
	reader := new(countingStorageReader)
	sdb.setAccountKVLatestView(reader, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var slot tcommon.Hash
		binary.BigEndian.PutUint64(slot[24:], uint64(i))
		sdb.GetStateWithExist(addr, slot)
	}
}

func BenchmarkSetStateCachedDirtySlot(b *testing.B) {
	sdb, err := New(tcommon.Hash{}, NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		b.Fatal(err)
	}
	addr := testAddr(0xa4)
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	obj := sdb.getStateObject(addr)
	obj.contractMetaDirty = true
	slot := tcommon.Hash{0x01}
	values := [2]tcommon.Hash{{0x11}, {0x22}}
	sdb.SetState(addr, slot, values[0])
	// Retain one journal backing slot and warm the recyclable change pool so the
	// benchmark isolates steady-state SetState work.
	sdb.journal.reset()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sdb.journal.reset()
		sdb.SetState(addr, slot, values[(i+1)&1])
	}
}
