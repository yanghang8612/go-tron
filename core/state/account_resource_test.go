package state

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
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

func TestGetAccountFrozenResourceTotalsLoadsOnlyResourceDomains(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xa3)
	delegator := testAddr(0xa4)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.CreateAccount(delegator, corepb.AccountType_Normal)
	sdb.FreezeV1Bandwidth(addr, 11, 100)
	sdb.FreezeV1Energy(addr, 12, 100)
	sdb.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 21)
	sdb.AddFreezeV2(addr, corepb.ResourceCode_ENERGY, 22)
	sdb.FreezeV1DelegatedBandwidth(delegator, addr, 31)
	sdb.FreezeV1DelegatedEnergy(delegator, addr, 32)
	sdb.AddAcquiredDelegatedFrozenV2(addr, corepb.ResourceCode_BANDWIDTH, 41)
	sdb.AddAcquiredDelegatedFrozenV2(addr, corepb.ResourceCode_ENERGY, 42)
	sdb.AddUnfreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 51, 200)
	for tokenID := int64(1_000_000); tokenID < 1_000_128; tokenID++ {
		sdb.SetTRC10Balance(addr, tokenID, tokenID)
	}
	sdb.SetVotes(addr, []*corepb.Vote{{VoteAddress: testAddr(0xb0).Bytes(), VoteCount: 7}})
	sdb.SetPermissions(addr, types.MakeDefaultOwnerPermission(addr), nil, nil)

	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	bandwidth, energy, err := reopened.GetAccountFrozenResourceTotalsV1(addr)
	if err != nil {
		t.Fatal(err)
	}
	if bandwidth != 42 || energy != 44 {
		t.Fatalf("pre-Stake-2.0 resource totals = (%d,%d), want (42,44)", bandwidth, energy)
	}
	if obj := reopened.stateObjects[addr]; obj == nil || obj.accountFrozenV2PointLoaded != 0 || obj.accountStakeV2Loaded {
		t.Fatalf("pre-Stake-2.0 resource read loaded Stake V2: %+v", obj)
	}

	reopened, err = New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	store := &frozenBandwidthPointReadStore{Database: sdb.db.DiskDB()}
	reopened.SetAccountKVIndexStore(store)

	bandwidth, err = reopened.GetAccountFrozenBandwidth(addr)
	if err != nil {
		t.Fatal(err)
	}
	if bandwidth != 104 {
		t.Fatalf("frozen bandwidth total = %d, want 104", bandwidth)
	}
	if store.iteratorCalls != 0 {
		t.Fatalf("bandwidth point read opened %d iterators, want 0", store.iteratorCalls)
	}
	obj := reopened.stateObjects[addr]
	if obj == nil {
		t.Fatal("bandwidth read did not load account envelope")
	}
	if obj.accountResourceLoaded || obj.accountMapsLoaded || obj.accountPermissionsLoaded ||
		obj.accountVotesLoaded || obj.accountStakeV2Loaded || obj.accountFrozenSupplyLoaded ||
		!obj.accountFrozenBandwidthLoaded || obj.accountTronPowerLoaded {
		t.Fatalf("bandwidth read materialized split domains: resource=%t maps=%t permissions=%t votes=%t stakeV2=%t frozenSupply=%t frozenBandwidth=%t tronPower=%t",
			obj.accountResourceLoaded, obj.accountMapsLoaded, obj.accountPermissionsLoaded,
			obj.accountVotesLoaded, obj.accountStakeV2Loaded, obj.accountFrozenSupplyLoaded,
			obj.accountFrozenBandwidthLoaded, obj.accountTronPowerLoaded)
	}

	bandwidth, energy, err = reopened.GetAccountFrozenResourceTotals(addr)
	if err != nil {
		t.Fatal(err)
	}
	if bandwidth != 104 {
		t.Fatalf("frozen bandwidth total = %d, want 104", bandwidth)
	}
	if energy != 108 {
		t.Fatalf("frozen energy total = %d, want 108", energy)
	}
	if store.iteratorCalls != 0 {
		t.Fatalf("resource totals opened %d iterators, want 0", store.iteratorCalls)
	}

	if !obj.accountResourceLoaded {
		t.Fatal("resource read did not load AccountResource")
	}
	if obj.accountMapsLoaded || obj.accountPermissionsLoaded || obj.accountVotesLoaded ||
		obj.accountStakeV2Loaded || obj.accountFrozenSupplyLoaded || obj.accountTronPowerLoaded {
		t.Fatalf("resource read materialized unrelated domains: maps=%t permissions=%t votes=%t stakeV2=%t frozenSupply=%t tronPower=%t",
			obj.accountMapsLoaded, obj.accountPermissionsLoaded, obj.accountVotesLoaded,
			obj.accountStakeV2Loaded, obj.accountFrozenSupplyLoaded, obj.accountTronPowerLoaded)
	}
	pb := obj.account.Proto()
	if len(pb.AssetV2) != 0 || pb.OwnerPermission != nil || len(pb.Votes) != 0 ||
		len(pb.FrozenV2) != 0 || len(pb.UnfrozenV2) != 0 {
		t.Fatalf("resource read leaked unrelated split fields into account proto: %+v", pb)
	}
}

func TestGetAccountFrozenEnergyLoadsOnlyEnergyRows(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xa5)
	delegator := testAddr(0xa6)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.CreateAccount(delegator, corepb.AccountType_Normal)
	sdb.FreezeV1Bandwidth(addr, 11, 100)
	sdb.FreezeV1Energy(addr, 12, 100)
	sdb.AddFreezeV2(addr, corepb.ResourceCode_ENERGY, 22)
	sdb.FreezeV1DelegatedEnergy(delegator, addr, 32)
	sdb.AddAcquiredDelegatedFrozenV2(addr, corepb.ResourceCode_ENERGY, 42)
	sdb.AddUnfreezeV2(addr, corepb.ResourceCode_ENERGY, 51, 200)
	sdb.SetTRC10Balance(addr, 1_000_001, 77)
	sdb.SetVotes(addr, []*corepb.Vote{{VoteAddress: testAddr(0xb1).Bytes(), VoteCount: 7}})
	sdb.SetPermissions(addr, types.MakeDefaultOwnerPermission(addr), nil, nil)

	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}

	energy, err := reopened.GetAccountFrozenEnergy(addr)
	if err != nil {
		t.Fatal(err)
	}
	if energy != 108 {
		t.Fatalf("frozen energy total = %d, want 108", energy)
	}
	obj := reopened.stateObjects[addr]
	if obj == nil || !obj.accountResourceLoaded {
		t.Fatal("energy read did not load envelope and AccountResource")
	}
	if obj.accountFrozenBandwidthLoaded || obj.accountMapsLoaded || obj.accountPermissionsLoaded ||
		obj.accountVotesLoaded || obj.accountStakeV2Loaded || obj.accountFrozenSupplyLoaded ||
		obj.accountTronPowerLoaded {
		t.Fatalf("energy read materialized unrelated domains: bandwidth=%t maps=%t permissions=%t votes=%t stakeV2=%t frozenSupply=%t tronPower=%t",
			obj.accountFrozenBandwidthLoaded, obj.accountMapsLoaded, obj.accountPermissionsLoaded,
			obj.accountVotesLoaded, obj.accountStakeV2Loaded, obj.accountFrozenSupplyLoaded,
			obj.accountTronPowerLoaded)
	}
	pb := obj.account.Proto()
	if len(pb.AssetV2) != 0 || pb.OwnerPermission != nil || len(pb.Votes) != 0 ||
		len(pb.Frozen) != 0 || len(pb.FrozenV2) != 0 || len(pb.UnfrozenV2) != 0 {
		t.Fatalf("energy read leaked unrelated split fields into account proto: %+v", pb)
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
