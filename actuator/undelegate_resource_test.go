package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestUnDelegateResourceValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         500000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnDelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	dr := &rawdb.DelegatedResource{
		From: owner, To: receiver,
		FrozenBalanceForBandwidth: 1000000,
	}
	rawdb.WriteDelegatedResourceV2(db, owner, receiver, true, dr)

	act := &UnDelegateResourceActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestUnDelegateResourceLocked(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         500000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnDelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.BlockTime = 1000
	ctx.PrevBlockTime = 1000
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	dr := &rawdb.DelegatedResource{
		From: owner, To: receiver,
		FrozenBalanceForBandwidth: 1000000,
		ExpireTimeForBandwidth:    999999,
	}
	rawdb.WriteDelegatedResourceV2(db, owner, receiver, true, dr)

	act := &UnDelegateResourceActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for locked delegation")
	}
}

func TestUnDelegateResourceExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         1000000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnDelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	dr := &rawdb.DelegatedResource{
		From: owner, To: receiver,
		FrozenBalanceForBandwidth: 1000000,
	}
	rawdb.WriteDelegatedResourceV2(db, owner, receiver, false, dr)
	rawdb.WriteDelegationIndex(db, owner, []tcommon.Address{receiver})

	act := &UnDelegateResourceActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	// Delegation fully removed
	if rawdb.ReadDelegatedResource(db, owner, receiver) != nil {
		t.Fatal("delegation should be removed")
	}
	// Owner's frozen restored
	if ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH) != 1000000 {
		t.Fatal("frozen balance not restored")
	}
}

func TestUnDelegateResource_AllowsUnlockedWhenLockedBucketStillFuture(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         500000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnDelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.PrevBlockTime = 1000
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)
	ctx.State.AddDelegatedFrozenV2(owner, corepb.ResourceCode_BANDWIDTH, 1_500_000)
	ctx.State.AddAcquiredDelegatedFrozenV2(receiver, corepb.ResourceCode_BANDWIDTH, 1_500_000)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	rawdb.WriteDelegatedResourceV2(db, owner, receiver, false, &rawdb.DelegatedResource{
		From: owner, To: receiver, FrozenBalanceForBandwidth: 1_000_000,
	})
	rawdb.WriteDelegatedResourceV2(db, owner, receiver, true, &rawdb.DelegatedResource{
		From: owner, To: receiver, FrozenBalanceForBandwidth: 500_000, ExpireTimeForBandwidth: 999_999,
	})
	rawdb.WriteDelegationIndex(db, owner, []tcommon.Address{receiver})

	act := &UnDelegateResourceActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should use unlocked bucket without being blocked by future locked bucket: %v", err)
	}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	unlocked := rawdb.ReadDelegatedResourceV2(db, owner, receiver, false)
	if unlocked == nil || unlocked.FrozenBalanceForBandwidth != 500_000 {
		t.Fatalf("unexpected unlocked bucket after undelegate: %+v", unlocked)
	}
	if locked := rawdb.ReadDelegatedResourceV2(db, owner, receiver, true); locked == nil || locked.FrozenBalanceForBandwidth != 500_000 {
		t.Fatalf("future locked bucket should remain: %+v", locked)
	}
	if receivers := rawdb.ReadDelegationIndex(db, owner); len(receivers) != 1 || receivers[0] != receiver {
		t.Fatalf("delegation index should remain while locked bucket exists: %v", receivers)
	}
}

func TestUnDelegateResource_MovesExpiredLockedBucketBeforeSubtract(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         2_500_000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnDelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.PrevBlockTime = 1000
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)
	ctx.State.AddDelegatedFrozenV2(owner, corepb.ResourceCode_BANDWIDTH, 3_000_000)
	ctx.State.AddAcquiredDelegatedFrozenV2(receiver, corepb.ResourceCode_BANDWIDTH, 3_000_000)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	rawdb.WriteDelegatedResourceV2(db, owner, receiver, false, &rawdb.DelegatedResource{
		From: owner, To: receiver, FrozenBalanceForBandwidth: 1_000_000,
	})
	rawdb.WriteDelegatedResourceV2(db, owner, receiver, true, &rawdb.DelegatedResource{
		From: owner, To: receiver, FrozenBalanceForBandwidth: 2_000_000, ExpireTimeForBandwidth: 999,
	})
	rawdb.WriteDelegationIndex(db, owner, []tcommon.Address{receiver})

	act := &UnDelegateResourceActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should count expired locked bucket as available: %v", err)
	}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if locked := rawdb.ReadDelegatedResourceV2(db, owner, receiver, true); locked != nil {
		t.Fatalf("expired locked bucket should be removed: %+v", locked)
	}
	unlocked := rawdb.ReadDelegatedResourceV2(db, owner, receiver, false)
	if unlocked == nil || unlocked.FrozenBalanceForBandwidth != 500_000 {
		t.Fatalf("unexpected unlocked bucket after moving expired lock and subtracting: %+v", unlocked)
	}
}
