package state

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func assertFrozenSupply(t *testing.T, got []*corepb.Account_Frozen, want ...*corepb.Account_Frozen) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("frozen-supply length = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Fatalf("frozen-supply[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestAccountFrozenSupplyPersistsOutsideAccountEnvelope(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x9d)
	sdb.CreateAccount(addr, corepb.AccountType_AssetIssue)
	first := &corepb.Account_Frozen{FrozenBalance: 11, ExpireTime: 10}
	second := &corepb.Account_Frozen{FrozenBalance: 22, ExpireTime: 30}
	third := &corepb.Account_Frozen{FrozenBalance: 33, ExpireTime: 20}
	sdb.AddFrozenSupply(addr, []*corepb.Account_Frozen{first, second, third})

	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	raw, ok, err := rawdb.ReadStateAccountLatest(sdb.db.DiskDB(), addr)
	if err != nil || !ok {
		t.Fatalf("read account latest: ok=%v err=%v", ok, err)
	}
	envelope, err := DecodeStateAccountV3(raw)
	if err != nil {
		t.Fatal(err)
	}
	var stored corepb.Account
	if err := proto.Unmarshal(envelope.AccountProto, &stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.FrozenSupply) != 0 {
		t.Fatalf("split frozen supply leaked into account envelope: %+v", stored.FrozenSupply)
	}
	for index, want := range []*corepb.Account_Frozen{first, second, third} {
		value, exists, readErr := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.AccountFrozenSupplyAux, accountFrozenSupplyKey(uint32(index)))
		if readErr != nil || !exists {
			t.Fatalf("read frozen-supply row %d: exists=%v err=%v", index, exists, readErr)
		}
		row, decodeErr := decodeAccountFrozenSupplyRow(accountFrozenSupplyKey(uint32(index)), value)
		if decodeErr != nil || !proto.Equal(row.entry, want) {
			t.Fatalf("frozen-supply row %d = %+v err=%v, want %+v", index, row.entry, decodeErr, want)
		}
	}

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	account := reopened.GetAccount(addr)
	if account == nil {
		t.Fatal("materialized account missing")
	}
	assertFrozenSupply(t, account.Proto().FrozenSupply, first, second, third)
}

func TestAccountFrozenSupplySnapshotRevertInvalidatesMaterializedCache(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x9e)
	sdb.CreateAccount(addr, corepb.AccountType_AssetIssue)
	first := &corepb.Account_Frozen{FrozenBalance: 11, ExpireTime: 10}
	second := &corepb.Account_Frozen{FrozenBalance: 22, ExpireTime: 30}
	sdb.AddFrozenSupply(addr, []*corepb.Account_Frozen{first, second})
	if got := sdb.GetAccount(addr); got == nil || len(got.Proto().FrozenSupply) != 2 {
		t.Fatalf("initial frozen supply = %+v", got)
	}

	snapshot := sdb.Snapshot()
	if amount := sdb.RemoveExpiredFrozenSupply(addr, 20); amount != 11 {
		t.Fatalf("removed amount = %d, want 11", amount)
	}
	if got := sdb.GetAccount(addr); got == nil {
		t.Fatal("account missing after removal")
	} else {
		assertFrozenSupply(t, got.Proto().FrozenSupply, second)
	}
	sdb.RevertToSnapshot(snapshot)
	if got := sdb.GetAccount(addr); got == nil {
		t.Fatal("account missing after revert")
	} else {
		assertFrozenSupply(t, got.Proto().FrozenSupply, first, second)
	}
}

func TestAccountFrozenSupplyExpiryWritesOnlyExpiredHistoryRows(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x9f)
	first := &corepb.Account_Frozen{FrozenBalance: 11, ExpireTime: 10}
	second := &corepb.Account_Frozen{FrozenBalance: 22, ExpireTime: 30}
	third := &corepb.Account_Frozen{FrozenBalance: 33, ExpireTime: 20}
	f.applyBlock([32]byte{0x61}, func(s *StateDB) {
		s.AddBalance(addr, 1)
		s.AddFrozenSupply(addr, []*corepb.Account_Frozen{first, second, third})
	})
	f.applyBlock([32]byte{0x62}, func(s *StateDB) {
		if amount := s.RemoveExpiredFrozenSupply(addr, 20); amount != 44 {
			t.Fatalf("removed amount = %d, want 44", amount)
		}
	})

	changes := collectStateDomainChanges(t, f.disk, 2)
	var frozenChanges []*rawdb.StateDomainChange
	for _, change := range changes {
		if change.Owner != addr {
			continue
		}
		if change.FlatDomain == rawdb.StateFlatDomainAccountLatest {
			t.Fatalf("frozen-supply expiry rewrote account envelope: %+v", change)
		}
		if change.FlatDomain == rawdb.StateFlatDomainKVLatest && change.Domain == kvdomains.AccountFrozenSupplyAux {
			frozenChanges = append(frozenChanges, change)
		}
	}
	if len(frozenChanges) != 2 || frozenChanges[0].NextExists || frozenChanges[1].NextExists {
		t.Fatalf("frozen-supply history changes = %+v, want two deletes", frozenChanges)
	}

	at1, err := f.reader().AccountAt(addr, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertFrozenSupply(t, at1.Proto().FrozenSupply, first, second, third)
	at2, err := f.reader().AccountAt(addr, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertFrozenSupply(t, at2.Proto().FrozenSupply, second)
}
