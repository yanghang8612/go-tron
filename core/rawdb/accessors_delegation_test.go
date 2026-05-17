package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
)

func TestDelegatedResourceWriteRead(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	dr := &DelegatedResource{
		From:                      from,
		To:                        to,
		FrozenBalanceForBandwidth: 1000000,
		FrozenBalanceForEnergy:    500000,
	}
	if err := WriteDelegatedResource(db, from, to, dr); err != nil {
		t.Fatal(err)
	}
	got := ReadDelegatedResource(db, from, to)
	if got == nil {
		t.Fatal("expected delegation record")
	}
	if got.FrozenBalanceForBandwidth != 1000000 || got.FrozenBalanceForEnergy != 500000 {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestDelegatedResourceDelete(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	dr := &DelegatedResource{From: from, To: to, FrozenBalanceForBandwidth: 100}
	WriteDelegatedResource(db, from, to, dr)
	WriteDelegatedResourceV2(db, from, to, true, &DelegatedResource{
		From: from, To: to, FrozenBalanceForEnergy: 200, ExpireTimeForEnergy: 10,
	})
	DeleteDelegatedResource(db, from, to)
	if ReadDelegatedResource(db, from, to) != nil {
		t.Fatal("expected nil after delete")
	}
	if ReadDelegatedResourceV2(db, from, to, false) != nil || ReadDelegatedResourceV2(db, from, to, true) != nil {
		t.Fatal("expected both V2 buckets deleted")
	}
}

func TestDelegatedResourceV2BucketsAndAggregate(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}

	WriteDelegatedResourceV2(db, from, to, false, &DelegatedResource{
		From: from, To: to, FrozenBalanceForBandwidth: 100,
	})
	WriteDelegatedResourceV2(db, from, to, true, &DelegatedResource{
		From: from, To: to, FrozenBalanceForBandwidth: 200, ExpireTimeForBandwidth: 5000,
	})

	if got := ReadDelegatedResourceV2(db, from, to, false); got == nil || got.FrozenBalanceForBandwidth != 100 || got.ExpireTimeForBandwidth != 0 {
		t.Fatalf("unexpected unlocked bucket: %+v", got)
	}
	if got := ReadDelegatedResourceV2(db, from, to, true); got == nil || got.FrozenBalanceForBandwidth != 200 || got.ExpireTimeForBandwidth != 5000 {
		t.Fatalf("unexpected locked bucket: %+v", got)
	}
	agg := ReadDelegatedResource(db, from, to)
	if agg == nil || agg.FrozenBalanceForBandwidth != 300 || agg.ExpireTimeForBandwidth != 5000 {
		t.Fatalf("unexpected aggregate: %+v", agg)
	}
}

func TestUnlockExpiredDelegatedResource(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}

	WriteDelegatedResourceV2(db, from, to, true, &DelegatedResource{
		From: from, To: to,
		FrozenBalanceForBandwidth: 100,
		ExpireTimeForBandwidth:    999,
		FrozenBalanceForEnergy:    200,
		ExpireTimeForEnergy:       2000,
	})

	if err := UnlockExpiredDelegatedResource(db, db, from, to, 1000); err != nil {
		t.Fatal(err)
	}
	unlocked := ReadDelegatedResourceV2(db, from, to, false)
	if unlocked == nil || unlocked.FrozenBalanceForBandwidth != 100 || unlocked.FrozenBalanceForEnergy != 0 {
		t.Fatalf("unexpected unlocked after expiry: %+v", unlocked)
	}
	locked := ReadDelegatedResourceV2(db, from, to, true)
	if locked == nil || locked.FrozenBalanceForBandwidth != 0 || locked.FrozenBalanceForEnergy != 200 {
		t.Fatalf("unexpected locked after partial expiry: %+v", locked)
	}
}

func TestDelegationIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	receivers := []common.Address{{0x41, 0x02}, {0x41, 0x03}}
	if err := WriteDelegationIndex(db, from, receivers); err != nil {
		t.Fatal(err)
	}
	got := ReadDelegationIndex(db, from)
	if len(got) != 2 {
		t.Fatalf("expected 2 receivers, got %d", len(got))
	}
	if got[0] != receivers[0] || got[1] != receivers[1] {
		t.Fatalf("unexpected receivers: %v", got)
	}
}

func TestDelegationNotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	if ReadDelegatedResource(db, from, to) != nil {
		t.Fatal("expected nil")
	}
	if ReadDelegationIndex(db, from) != nil {
		t.Fatal("expected nil")
	}
}
