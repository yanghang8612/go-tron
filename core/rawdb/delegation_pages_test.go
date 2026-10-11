package rawdb

import (
	"bytes"
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestDelegationPageKeysAreFramedRootedAndProtected(t *testing.T) {
	seen := make(map[string]bool)
	for _, account := range [][]byte{nil, {0}, {0, 0}, {1, 2}, []byte("drap-\x00\x01")} {
		for _, incoming := range []bool{false, true} {
			for _, index := range []uint64{0, 1, ^uint64(0)} {
				key := DrAccountIndexLegacyPageStateKey(account, incoming, index)
				if seen[string(key)] {
					t.Fatalf("page key collision: %x", key)
				}
				seen[string(key)] = true
				got, ok := DrAccountIndexLegacyPageAccount(key)
				if !ok || !bytes.Equal(got, account) {
					t.Fatalf("page account parse: %x %v", got, ok)
				}
				rooted, ok := LookupRootedStateKey(key)
				if !ok || rooted.Owner != common.SystemAccountAddress || rooted.Domain != kvdomains.SystemDelegation || !bytes.Equal(rooted.Key, key) || !IsProtectedStateMutationKey(key) {
					t.Fatal("page key escaped rooted state protection")
				}
				for _, bad := range [][]byte{key[:len(key)-1], append(bytes.Clone(key), 0)} {
					if _, ok := DrAccountIndexLegacyPageAccount(bad); ok {
						t.Fatalf("accepted malformed page key %x", bad)
					}
				}
			}
		}
	}
	invalid := DrAccountIndexLegacyPageStateKey(nil, false, 0)
	invalid[len(invalid)-9] = 2
	if _, ok := DrAccountIndexLegacyPageAccount(invalid); ok {
		t.Fatal("accepted unknown page direction")
	}
}

func TestPagedDelegationLayoutMarker(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	defer db.Close()
	if yes, err := ReadPagedDelegationLayout(db); err != nil || yes {
		t.Fatalf("unmarked database: %v %v", yes, err)
	}
	if err := MarkPagedDelegationLayout(db); err != nil {
		t.Fatal(err)
	}
	if yes, err := ReadPagedDelegationLayout(db); err != nil || !yes {
		t.Fatalf("marked database: %v %v", yes, err)
	}
	if err := db.Put(pagedDelegationLayoutKey, []byte{2}); err != nil {
		t.Fatal(err)
	}
	if yes, err := ReadPagedDelegationLayout(db); err == nil || yes {
		t.Fatalf("unknown layout accepted: %v %v", yes, err)
	}
}

type delegationLayoutSyncWriter struct {
	ethdb.KeyValueWriter
	reader ethdb.KeyValueReader
	err    error
	synced bool
}

func (w *delegationLayoutSyncWriter) SyncKeyValue() error {
	w.synced = true
	if admitted, err := ReadPagedDelegationLayout(w.reader); err == nil || admitted {
		return errors.New("rejection marker was not written before sync")
	}
	return w.err
}

func TestInvalidatePagedDelegationLayoutPersistsBeforeImport(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	defer db.Close()
	want := errors.New("sync failure")
	w := &delegationLayoutSyncWriter{KeyValueWriter: db, reader: db, err: want}
	if err := InvalidatePagedDelegationLayout(w); !errors.Is(err, want) || !w.synced {
		t.Fatalf("sync failure not propagated: synced=%v err=%v", w.synced, err)
	}
	w.err, w.synced = nil, false
	if err := InvalidatePagedDelegationLayout(w); err != nil || !w.synced {
		t.Fatalf("rejection marker not persisted: synced=%v err=%v", w.synced, err)
	}
}
