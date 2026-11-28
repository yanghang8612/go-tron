package state

import (
	"bytes"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestTVMStorageMigrationModes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		protect bool
	}{
		{"proposal 99", false},
		{"proposal 102 only", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStateDB(t)
			addr := testAddr(1)
			key := tcommon.Hash{31: 7}
			value := tcommon.Hash{31: 42}
			var empty tcommon.Hash
			legacy := LegacyStorageRowKey(addr, key, nil)
			newKey := OptimizedStorageRowKey(addr, key, nil)
			if err := s.SetAccountKV(addr, kvdomains.ContractStorage, legacy[:], value[:]); err != nil {
				t.Fatal(err)
			}

			snap := s.Snapshot()
			storage := &TVMStorage{Optimize: true, ProtectLegacyReads: tc.protect}
			got, exists, alias, err := storage.Read(s, addr, key)
			if err != nil || alias || !exists || got != value {
				t.Fatalf("initial read: value=%x, exists=%v, alias=%v, err=%v", got, exists, alias, err)
			}
			if _, found, err := s.GetAccountKV(addr, kvdomains.ContractStorage, legacy[:]); err != nil || found != tc.protect {
				t.Fatalf("legacy presence after read = %v, err=%v", found, err)
			}
			if err := storage.Write(s, addr, key, tcommon.Hash{}); err != nil {
				t.Fatal(err)
			}
			if raw, found, err := s.GetAccountKV(addr, kvdomains.ContractStorage, newKey); err != nil || !found || !bytes.Equal(raw, empty[:]) {
				t.Fatalf("optimized zero row: value=%x, found=%v, err=%v", raw, found, err)
			}
			if _, found, err := s.GetAccountKV(addr, kvdomains.ContractStorage, legacy[:]); err != nil || found {
				t.Fatalf("legacy presence after write = %v, err=%v", found, err)
			}
			next := &TVMStorage{Optimize: true, ProtectLegacyReads: tc.protect}
			zero, exists, alias, err := next.Read(s, addr, key)
			if err != nil || alias || exists || zero != (tcommon.Hash{}) {
				t.Fatalf("next transaction zero read: value=%x, exists=%v, alias=%v, err=%v", zero, exists, alias, err)
			}

			s.RevertToSnapshot(snap)
			if _, found, err := s.GetAccountKV(addr, kvdomains.ContractStorage, newKey); err != nil || found {
				t.Fatalf("optimized presence after rollback = %v, err=%v", found, err)
			}
			restored, exists, alias, err := storage.Read(s, addr, key)
			if err != nil || alias || !exists || restored != value {
				t.Fatalf("rollback read: value=%x, exists=%v, alias=%v, err=%v", restored, exists, alias, err)
			}
			if err := storage.Write(s, addr, key, tcommon.Hash{}); err != nil {
				t.Fatal(err)
			}
			root, err := s.Commit()
			if err != nil {
				t.Fatal(err)
			}
			reopened, err := New(root, s.db)
			if err != nil {
				t.Fatal(err)
			}
			if value, err := reopened.GetStateStrict(addr, key); err != nil || value != (tcommon.Hash{}) {
				t.Fatalf("persisted zero row: value=%x, err=%v", value, err)
			}
			if _, found, err := reopened.GetAccountKV(addr, kvdomains.ContractStorage, legacy[:]); err != nil || found {
				t.Fatalf("persisted legacy row after migration: found=%v, err=%v", found, err)
			}
		})
	}
}

func TestLegacyStoragePhysicalOverlayWinsOverCleanLogicalCache(t *testing.T) {
	s := newTestStateDB(t)
	addr := testAddr(1)
	slot := tcommon.Hash{31: 5}
	initial, next := tcommon.Hash{31: 7}, tcommon.Hash{31: 8}
	s.SetState(addr, slot, initial)
	root, err := s.Commit()
	if err != nil {
		t.Fatal(err)
	}
	s, err = New(root, s.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.GetState(addr, slot); got != initial {
		t.Fatalf("prewarm storage = %x, want %x", got, initial)
	}
	row := s.LegacyTVMStorageRowKey(addr, slot)
	snap := s.Snapshot()
	if err := s.WriteLegacyTVMStorageRow(addr, row, next, nil); err != nil {
		t.Fatal(err)
	}
	s.FinalizeTransaction()
	if got, exists := s.GetStateWithExist(addr, slot); got != next || !exists {
		t.Fatalf("logical read after publication = %x, exists=%v", got, exists)
	}
	if got, err := s.GetStateStrict(addr, slot); err != nil || got != next {
		t.Fatalf("strict read after publication = %x, err=%v", got, err)
	}
	if err := s.WriteLegacyTVMStorageRow(addr, row, tcommon.Hash{}, nil); err != nil {
		t.Fatal(err)
	}
	if got, exists := s.GetStateWithExist(addr, slot); got != (tcommon.Hash{}) || exists {
		t.Fatalf("logical read after delete = %x, exists=%v", got, exists)
	}
	s.RevertToSnapshot(snap)
	if got := s.GetState(addr, slot); got != initial {
		t.Fatalf("logical cache after rollback = %x, want %x", got, initial)
	}
	if err := s.WriteLegacyTVMStorageRow(addr, row, next, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := s.GetState(addr, slot); got != next {
		t.Fatalf("logical cache after block commit = %x, want %x", got, next)
	}
}

func TestLegacyStorageRejectsMixedTransactionWriteFamilies(t *testing.T) {
	addr := testAddr(1)
	slot := tcommon.Hash{31: 5}
	row := LegacyStorageRowKey(addr, slot, nil)
	value := tcommon.Hash{31: 7}
	writes := TransactionWriteSet{
		{Kind: TransactionAccessStorage, Address: addr, StorageKey: slot}: {
			Exists: true, Value: value[:],
		},
		{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.ContractStorage, LogicalKey: string(row[:])}: {
			Exists: true, Value: value[:],
		},
	}
	if err := validateTransactionWritePhysicalAliases(writes); err == nil {
		t.Fatal("mixed writes to the same storage row were accepted")
	}
}

func TestReadLegacyTVMStorageRowBeforeImages(t *testing.T) {
	s := newTestStateDB(t)
	addr := testAddr(0x55)
	slot32 := tcommon.Hash{31: 1}
	slotShort := tcommon.Hash{31: 2}
	val32 := tcommon.Hash{31: 99}
	shortBytes := []byte{0x07, 0x08}

	key32 := s.LegacyTVMStorageRowKey(addr, slot32)
	keyShort := s.LegacyTVMStorageRowKey(addr, slotShort)
	if err := s.SetAccountKV(addr, kvdomains.ContractStorage, key32[:], val32[:]); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAccountKV(addr, kvdomains.ContractStorage, keyShort[:], shortBytes); err != nil {
		t.Fatal(err)
	}
	root, err := s.Commit()
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := New(root, s.db)
	if err != nil {
		t.Fatal(err)
	}
	got32, beforeNon32, hasBefore, before32, exists, err := reopened.ReadLegacyTVMStorageRow(addr, slot32, key32)
	if err != nil || !exists || !hasBefore || !before32 || beforeNon32 != nil || got32 != val32 {
		t.Fatalf("32-byte row = (%x, %x, hasBefore=%v, before32=%v, exists=%v, err=%v)", got32, beforeNon32, hasBefore, before32, exists, err)
	}
	gotShort, beforeShort, hasShort, isShort32, existsShort, err := reopened.ReadLegacyTVMStorageRow(addr, slotShort, keyShort)
	if err != nil || !existsShort || !hasShort || isShort32 || !bytes.Equal(beforeShort, shortBytes) || gotShort != (tcommon.Hash{30: 0x07, 31: 0x08}) {
		t.Fatalf("short row = (%x, %x, hasBefore=%v, before32=%v, exists=%v, err=%v)", gotShort, beforeShort, hasShort, isShort32, existsShort, err)
	}

	// Verify 32-byte SLOAD commits properly with reconstructed value[:].
	next := tcommon.Hash{31: 123}
	if err := reopened.WriteLegacyTVMStorageRow(addr, key32, next, got32[:]); err != nil {
		t.Fatal(err)
	}
	if got := reopened.GetState(addr, slot32); got != next {
		t.Fatalf("updated state = %x, want %x", got, next)
	}
}

var (
	benchLegacyStorageHashSink tcommon.Hash
	benchLegacyStorageBoolSink bool
)

func BenchmarkReadLegacyTVMStorageRow(b *testing.B) {
	s := newTestStateDB(b)
	addr := testAddr(0x56)
	slot := tcommon.Hash{31: 11}
	val := tcommon.Hash{0: 1, 31: 9}
	key := s.LegacyTVMStorageRowKey(addr, slot)
	if err := s.SetAccountKV(addr, kvdomains.ContractStorage, key[:], val[:]); err != nil {
		b.Fatal(err)
	}
	root, err := s.Commit()
	if err != nil {
		b.Fatal(err)
	}
	reopened, err := New(root, s.db)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		v, _, hasBefore, _, exists, err := reopened.ReadLegacyTVMStorageRow(addr, slot, key)
		if err != nil || !exists || !hasBefore {
			b.Fatalf("read failed: exists=%v hasBefore=%v err=%v", exists, hasBefore, err)
		}
		benchLegacyStorageHashSink = v
		benchLegacyStorageBoolSink = exists
	}
}
