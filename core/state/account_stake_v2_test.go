package state

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func assertUnfrozenV2(t *testing.T, got []*corepb.Account_UnFreezeV2, want ...*corepb.Account_UnFreezeV2) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("unfrozen-v2 length = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Fatalf("unfrozen-v2[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestAccountFrozenV2SingleResourceUpdateWritesOneHistoryRow(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x9c)
	f.applyBlock([32]byte{0x51}, func(s *StateDB) {
		s.AddBalance(addr, 1)
		s.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 100)
		s.AddFreezeV2(addr, corepb.ResourceCode_ENERGY, 200)
	})
	f.applyBlock([32]byte{0x52}, func(s *StateDB) {
		s.ReduceFreezeV2(addr, corepb.ResourceCode_ENERGY, 50)
	})

	changes := collectStateDomainChanges(t, f.disk, 2)
	var matching []*rawdb.StateDomainChange
	for _, change := range changes {
		if change.Owner != addr {
			continue
		}
		if change.FlatDomain == rawdb.StateFlatDomainAccountLatest {
			t.Fatalf("single FrozenV2 update rewrote account envelope: %+v", change)
		}
		if change.FlatDomain == rawdb.StateFlatDomainKVLatest && change.Domain == kvdomains.AccountFrozenV2Aux {
			matching = append(matching, change)
		}
	}
	if len(matching) != 1 || !bytes.Equal(matching[0].Key, accountFrozenV2Key(corepb.ResourceCode_ENERGY)) {
		t.Fatalf("FrozenV2 history rows = %+v, want one ENERGY row", matching)
	}
}

func TestAccountStakeV2PersistsOutsideAccountEnvelope(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x99)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	// Insert ENERGY first to prove materialization preserves first-insertion
	// order rather than sorting by ResourceCode key.
	sdb.AddFreezeV2(addr, corepb.ResourceCode_ENERGY, 100)
	sdb.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 200)
	sdb.AddUnfreezeV2(addr, corepb.ResourceCode_ENERGY, 11, 1_000)
	sdb.AddUnfreezeV2(addr, corepb.ResourceCode_ENERGY, 22, 1_000)

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
	if len(stored.FrozenV2) != 0 || len(stored.UnfrozenV2) != 0 {
		t.Fatalf("split Stake V2 leaked into account envelope: frozen=%+v unfrozen=%+v", stored.FrozenV2, stored.UnfrozenV2)
	}

	energyValue, exists, err := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.AccountFrozenV2Aux, accountFrozenV2Key(corepb.ResourceCode_ENERGY))
	if err != nil || !exists {
		t.Fatalf("read ENERGY frozen-v2: exists=%v err=%v", exists, err)
	}
	energy, err := decodeAccountFrozenV2Row(accountFrozenV2Key(corepb.ResourceCode_ENERGY), energyValue)
	if err != nil || energy.ordinal != 0 || energy.amount != 100 {
		t.Fatalf("ENERGY row = %+v err=%v, want ordinal=0 amount=100", energy, err)
	}
	bandwidthValue, exists, err := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.AccountFrozenV2Aux, accountFrozenV2Key(corepb.ResourceCode_BANDWIDTH))
	if err != nil || !exists {
		t.Fatalf("read BANDWIDTH frozen-v2: exists=%v err=%v", exists, err)
	}
	bandwidth, err := decodeAccountFrozenV2Row(accountFrozenV2Key(corepb.ResourceCode_BANDWIDTH), bandwidthValue)
	if err != nil || bandwidth.ordinal != 1 || bandwidth.amount != 200 {
		t.Fatalf("BANDWIDTH row = %+v err=%v, want ordinal=1 amount=200", bandwidth, err)
	}

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	account := reopened.GetAccount(addr)
	if account == nil {
		t.Fatal("materialized account missing")
	}
	frozen := account.FrozenV2()
	if len(frozen) != 2 || frozen[0].GetType() != corepb.ResourceCode_ENERGY || frozen[0].GetAmount() != 100 || frozen[1].GetType() != corepb.ResourceCode_BANDWIDTH || frozen[1].GetAmount() != 200 {
		t.Fatalf("materialized frozen-v2 order/value = %+v", frozen)
	}
	want1 := &corepb.Account_UnFreezeV2{Type: corepb.ResourceCode_ENERGY, UnfreezeAmount: 11, UnfreezeExpireTime: 1_000}
	want2 := &corepb.Account_UnFreezeV2{Type: corepb.ResourceCode_ENERGY, UnfreezeAmount: 22, UnfreezeExpireTime: 1_000}
	assertUnfrozenV2(t, account.UnfrozenV2(), want1, want2)
	if got := reopened.GetFrozenV2Amount(addr, corepb.ResourceCode_ENERGY); got != 100 {
		t.Fatalf("typed ENERGY amount = %d, want 100", got)
	}
}

func TestAccountStakeV2SnapshotRevertInvalidatesMaterializedCache(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x9a)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 100)
	sdb.AddUnfreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 11, 10)
	sdb.AddUnfreezeV2(addr, corepb.ResourceCode_ENERGY, 22, 30)
	if got := sdb.GetAccount(addr); got == nil || len(got.FrozenV2()) != 1 || len(got.UnfrozenV2()) != 2 {
		t.Fatalf("initial materialized Stake V2 = %+v", got)
	}

	snapshot := sdb.Snapshot()
	sdb.ReduceFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 40)
	sdb.AddFreezeV2(addr, corepb.ResourceCode_ENERGY, 20)
	if withdrawn := sdb.RemoveExpiredUnfreezeV2(addr, 20); withdrawn != 11 {
		t.Fatalf("withdrawn = %d, want 11", withdrawn)
	}
	sdb.AddUnfreezeV2(addr, corepb.ResourceCode_TRON_POWER, 33, 40)
	updated := sdb.GetAccount(addr)
	if updated == nil || len(updated.FrozenV2()) != 2 || updated.FrozenV2()[0].GetAmount() != 60 || updated.FrozenV2()[1].GetType() != corepb.ResourceCode_ENERGY {
		t.Fatalf("updated frozen-v2 = %+v", updated)
	}
	assertUnfrozenV2(t, updated.UnfrozenV2(),
		&corepb.Account_UnFreezeV2{Type: corepb.ResourceCode_ENERGY, UnfreezeAmount: 22, UnfreezeExpireTime: 30},
		&corepb.Account_UnFreezeV2{Type: corepb.ResourceCode_TRON_POWER, UnfreezeAmount: 33, UnfreezeExpireTime: 40},
	)

	sdb.RevertToSnapshot(snapshot)
	reverted := sdb.GetAccount(addr)
	if reverted == nil || len(reverted.FrozenV2()) != 1 || reverted.FrozenV2()[0].GetType() != corepb.ResourceCode_BANDWIDTH || reverted.FrozenV2()[0].GetAmount() != 100 {
		t.Fatalf("frozen-v2 after revert = %+v", reverted)
	}
	assertUnfrozenV2(t, reverted.UnfrozenV2(),
		&corepb.Account_UnFreezeV2{Type: corepb.ResourceCode_BANDWIDTH, UnfreezeAmount: 11, UnfreezeExpireTime: 10},
		&corepb.Account_UnFreezeV2{Type: corepb.ResourceCode_ENERGY, UnfreezeAmount: 22, UnfreezeExpireTime: 30},
	)
}

func TestAccountStakeV2ClearDeletesPhysicalRows(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x9b)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 100)
	sdb.AddUnfreezeV2(addr, corepb.ResourceCode_ENERGY, 22, 30)
	root1, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	reopened.ClearV2Freeze(addr)
	root2, err := reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if _, exists, err := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.AccountFrozenV2Aux, accountFrozenV2Key(corepb.ResourceCode_BANDWIDTH)); err != nil || exists {
		t.Fatalf("frozen-v2 row after clear: exists=%v err=%v", exists, err)
	}
	if _, exists, err := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.AccountUnfrozenV2Aux, accountUnfrozenV2Key(0)); err != nil || exists {
		t.Fatalf("unfrozen-v2 row after clear: exists=%v err=%v", exists, err)
	}
	reopenedAgain, err := New(root2, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if account := reopenedAgain.GetAccount(addr); account == nil || len(account.FrozenV2()) != 0 || len(account.UnfrozenV2()) != 0 {
		t.Fatalf("Stake V2 after clear = %+v", account)
	}
}
