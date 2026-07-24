package state

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestAccountResourcePersistsOutsideAccountEnvelopeAndLoadsLazily(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xa0)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.SetEnergyUsage(addr, 10)
	sdb.SetLatestConsumeTimeForEnergy(addr, 20)
	sdb.SetEnergyWindow(addr, 30, true)
	sdb.FreezeV1Energy(addr, 40, 50)
	sdb.AddDelegatedFrozenV2(addr, corepb.ResourceCode_ENERGY, 60)
	sdb.AddAcquiredDelegatedFrozenV2(addr, corepb.ResourceCode_ENERGY, 70)

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
	if stored.AccountResource != nil {
		t.Fatalf("split AccountResource leaked into account envelope: %+v", stored.AccountResource)
	}
	value, exists, err := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.AccountResourceAux, accountResourceKey)
	if err != nil || !exists {
		t.Fatalf("read AccountResource row: exists=%v err=%v", exists, err)
	}
	var resource corepb.Account_AccountResource
	if err := proto.Unmarshal(value, &resource); err != nil {
		t.Fatal(err)
	}
	if resource.EnergyUsage != 10 || resource.LatestConsumeTimeForEnergy != 20 || resource.EnergyWindowSize != 30 || !resource.EnergyWindowOptimized {
		t.Fatalf("stored hot resource fields = %+v", &resource)
	}
	if resource.GetFrozenBalanceForEnergy().GetFrozenBalance() != 40 || resource.GetFrozenBalanceForEnergy().GetExpireTime() != 50 {
		t.Fatalf("stored V1 energy freeze = %+v", resource.FrozenBalanceForEnergy)
	}
	if resource.DelegatedFrozenV2BalanceForEnergy != 60 || resource.AcquiredDelegatedFrozenV2BalanceForEnergy != 70 {
		t.Fatalf("stored V2 energy delegation = %+v", &resource)
	}

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.GetBalance(addr); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
	obj := reopened.stateObjects[addr]
	if obj == nil || obj.accountResourceLoaded || obj.account.Proto().AccountResource != nil {
		t.Fatalf("ordinary balance read eagerly loaded AccountResource: %+v", obj)
	}
	if got := reopened.GetEnergyUsage(addr); got != 10 {
		t.Fatalf("lazy energy usage = %d, want 10", got)
	}
	if !obj.accountResourceLoaded || obj.account.Proto().AccountResource == nil {
		t.Fatal("typed resource read did not materialize AccountResource")
	}
}

func TestAccountResourcePreservesPresentEmptyMessage(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xa1)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.SetEnergyUsage(addr, 0)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	account := reopened.GetAccount(addr)
	if account == nil || account.Proto().AccountResource == nil {
		t.Fatalf("present empty AccountResource was lost: %+v", account)
	}
}

func TestAccountResourceSnapshotRevertInvalidatesMaterializedCache(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xa2)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.SetEnergyUsage(addr, 10)
	sdb.SetEnergyWindow(addr, 30, false)
	sdb.FreezeV1Energy(addr, 40, 50)
	if account := sdb.GetAccount(addr); account == nil || account.EnergyUsage() != 10 {
		t.Fatalf("initial AccountResource = %+v", account)
	}

	snapshot := sdb.Snapshot()
	sdb.SetEnergyUsage(addr, 20)
	sdb.SetEnergyWindow(addr, 60, true)
	sdb.FreezeV1Energy(addr, 5, 70)
	updated := sdb.GetAccount(addr)
	if updated == nil || updated.EnergyUsage() != 20 || updated.RawEnergyWindowSize() != 60 || !updated.EnergyWindowOptimized() || updated.FrozenEnergyAmount() != 45 || updated.FrozenEnergyExpireTime() != 70 {
		t.Fatalf("updated AccountResource = %+v", updated)
	}

	sdb.RevertToSnapshot(snapshot)
	reverted := sdb.GetAccount(addr)
	if reverted == nil || reverted.EnergyUsage() != 10 || reverted.RawEnergyWindowSize() != 30 || reverted.EnergyWindowOptimized() || reverted.FrozenEnergyAmount() != 40 || reverted.FrozenEnergyExpireTime() != 50 {
		t.Fatalf("AccountResource after revert = %+v", reverted)
	}
}

func TestAccountResourceHotFieldWritesOneHistoryRow(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0xa3)
	f.applyBlock([32]byte{0x71}, func(s *StateDB) {
		s.AddBalance(addr, 1)
		s.SetEnergyUsage(addr, 10)
		s.SetLatestConsumeTimeForEnergy(addr, 20)
	})
	f.applyBlock([32]byte{0x72}, func(s *StateDB) {
		s.SetEnergyUsage(addr, 30)
	})

	changes := collectStateDomainChanges(t, f.disk, 2)
	var resourceChanges []*rawdb.StateDomainChange
	for _, change := range changes {
		if change.Owner != addr {
			continue
		}
		if change.FlatDomain == rawdb.StateFlatDomainAccountLatest {
			t.Fatalf("energy usage update rewrote account envelope: %+v", change)
		}
		if change.FlatDomain == rawdb.StateFlatDomainKVLatest && change.Domain == kvdomains.AccountResourceAux {
			resourceChanges = append(resourceChanges, change)
		}
	}
	if len(resourceChanges) != 1 || !resourceChanges[0].PrevExists || !resourceChanges[0].NextExists {
		t.Fatalf("AccountResource history changes = %+v, want one update", resourceChanges)
	}

	at1, err := f.reader().AccountAt(addr, 1)
	if err != nil {
		t.Fatal(err)
	}
	if at1 == nil || at1.EnergyUsage() != 10 || at1.LatestConsumeTimeForEnergy() != 20 {
		t.Fatalf("block 1 AccountResource = %+v", at1)
	}
	at2, err := f.reader().AccountAt(addr, 2)
	if err != nil {
		t.Fatal(err)
	}
	if at2 == nil || at2.EnergyUsage() != 30 || at2.LatestConsumeTimeForEnergy() != 20 {
		t.Fatalf("block 2 AccountResource = %+v", at2)
	}
}
