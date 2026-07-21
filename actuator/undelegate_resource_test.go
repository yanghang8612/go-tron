package actuator

import (
	"testing"

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

	dr := &rawdb.DelegatedResource{
		From: owner, To: receiver,
		FrozenBalanceForBandwidth: 1000000,
	}
	if err := ctx.State.WriteDelegatedResourceV2(owner, receiver, true, dr); err != nil {
		t.Fatal(err)
	}

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

	dr := &rawdb.DelegatedResource{
		From: owner, To: receiver,
		FrozenBalanceForBandwidth: 1000000,
		ExpireTimeForBandwidth:    999999,
	}
	if err := ctx.State.WriteDelegatedResourceV2(owner, receiver, true, dr); err != nil {
		t.Fatal(err)
	}

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

	dr := &rawdb.DelegatedResource{
		From: owner, To: receiver,
		FrozenBalanceForBandwidth: 1000000,
	}
	if err := ctx.State.WriteDelegatedResourceV2(owner, receiver, false, dr); err != nil {
		t.Fatal(err)
	}
	if err := ctx.State.WriteDrAccountIndexDelegate(true, owner[:], receiver[:], 1); err != nil {
		t.Fatal(err)
	}

	act := &UnDelegateResourceActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	// Delegation fully removed
	if ctx.State.ReadDelegatedResource(owner, receiver) != nil {
		t.Fatal("delegation should be removed")
	}
	// Owner's frozen restored
	if ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH) != 1000000 {
		t.Fatal("frozen balance not restored")
	}
	if got := ctx.State.ReadDrAccountIndexEntry(rawdb.DrAccIdxV2From, owner[:], receiver[:]); got != nil {
		t.Fatalf("V2 from index should be removed: %+v", got)
	}
	if got := ctx.State.ReadDrAccountIndexEntry(rawdb.DrAccIdxV2To, receiver[:], owner[:]); got != nil {
		t.Fatalf("V2 to index should be removed: %+v", got)
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

	if err := ctx.State.WriteDelegatedResourceV2(owner, receiver, false, &rawdb.DelegatedResource{
		From: owner, To: receiver, FrozenBalanceForBandwidth: 1_000_000,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ctx.State.WriteDelegatedResourceV2(owner, receiver, true, &rawdb.DelegatedResource{
		From: owner, To: receiver, FrozenBalanceForBandwidth: 500_000, ExpireTimeForBandwidth: 999_999,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ctx.State.WriteDrAccountIndexDelegate(true, owner[:], receiver[:], 1); err != nil {
		t.Fatal(err)
	}

	act := &UnDelegateResourceActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should use unlocked bucket without being blocked by future locked bucket: %v", err)
	}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	unlocked := ctx.State.ReadDelegatedResourceV2(owner, receiver, false)
	if unlocked == nil || unlocked.FrozenBalanceForBandwidth != 500_000 {
		t.Fatalf("unexpected unlocked bucket after undelegate: %+v", unlocked)
	}
	if locked := ctx.State.ReadDelegatedResourceV2(owner, receiver, true); locked == nil || locked.FrozenBalanceForBandwidth != 500_000 {
		t.Fatalf("future locked bucket should remain: %+v", locked)
	}
	if got := ctx.State.ReadDrAccountIndexEntry(rawdb.DrAccIdxV2From, owner[:], receiver[:]); got == nil {
		t.Fatal("delegation index should remain while locked bucket exists")
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

	if err := ctx.State.WriteDelegatedResourceV2(owner, receiver, false, &rawdb.DelegatedResource{
		From: owner, To: receiver, FrozenBalanceForBandwidth: 1_000_000,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ctx.State.WriteDelegatedResourceV2(owner, receiver, true, &rawdb.DelegatedResource{
		From: owner, To: receiver, FrozenBalanceForBandwidth: 2_000_000, ExpireTimeForBandwidth: 999,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ctx.State.WriteDrAccountIndexDelegate(true, owner[:], receiver[:], 1); err != nil {
		t.Fatal(err)
	}

	act := &UnDelegateResourceActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should count expired locked bucket as available: %v", err)
	}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if locked := ctx.State.ReadDelegatedResourceV2(owner, receiver, true); locked != nil {
		t.Fatalf("expired locked bucket should be removed: %+v", locked)
	}
	unlocked := ctx.State.ReadDelegatedResourceV2(owner, receiver, false)
	if unlocked == nil || unlocked.FrozenBalanceForBandwidth != 500_000 {
		t.Fatalf("unexpected unlocked bucket after moving expired lock and subtracting: %+v", unlocked)
	}
}
