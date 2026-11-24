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
