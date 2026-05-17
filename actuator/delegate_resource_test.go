package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestDelegateResourceValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         1000000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	act := &DelegateResourceActuator{}

	// Accounts don't exist
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 5000000)

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestDelegateResourceSelfDelegation(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: owner[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         1000000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &DelegateResourceActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for self-delegation")
	}
}

func TestDelegateResourceExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         1000000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 5000000)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db

	act := &DelegateResourceActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	// Owner's frozen reduced
	if ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH) != 4000000 {
		t.Fatalf("owner frozen not reduced")
	}

	// Delegation record
	dr := rawdb.ReadDelegatedResource(db, owner, receiver)
	if dr == nil || dr.FrozenBalanceForBandwidth != 1000000 {
		t.Fatalf("delegation record wrong: %+v", dr)
	}
	if unlocked := rawdb.ReadDelegatedResourceV2(db, owner, receiver, false); unlocked == nil || unlocked.FrozenBalanceForBandwidth != 1000000 {
		t.Fatalf("unlocked delegation bucket wrong: %+v", unlocked)
	}
	if locked := rawdb.ReadDelegatedResourceV2(db, owner, receiver, true); locked != nil {
		t.Fatalf("locked bucket should not exist for unlocked delegation: %+v", locked)
	}
}

func TestDelegateResourceValidate_UsesRemainingFrozenOnly(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         2_000_000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 2_000_000)
	ctx.State.AddDelegatedFrozenV2(owner, corepb.ResourceCode_BANDWIDTH, 3_000_000)

	if err := (&DelegateResourceActuator{}).Validate(ctx); err != nil {
		t.Fatalf("validate should use remaining frozen balance without subtracting already delegated again: %v", err)
	}
}

func TestDelegateResourceValidate_RejectsConsumedBandwidthV2(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         5_000_000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.DynProps.Set("total_net_limit", 1_000)
	ctx.DynProps.SetTotalNetWeight(10)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 10_000_000)
	ctx.State.SetNetUsage(owner, 600)
	ctx.State.SetLatestConsumeTime(owner, ctx.ResourceTime())

	err := (&DelegateResourceActuator{}).Validate(ctx)
	if err == nil {
		t.Fatal("expected consumed bandwidth to reduce delegatable V2 balance")
	}
	if err.Error() != "delegateBalance must be less than or equal to available FreezeBandwidthV2 balance" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDelegateResourceValidate_AcquiredBandwidthShieldsSelfV2Usage(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         5_000_000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.DynProps.Set("total_net_limit", 1_000)
	ctx.DynProps.SetTotalNetWeight(10)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 10_000_000)
	ctx.State.AddAcquiredDelegatedFrozenV2(owner, corepb.ResourceCode_BANDWIDTH, 3_000_000)
	ctx.State.SetNetUsage(owner, 600)
	ctx.State.SetLatestConsumeTime(owner, ctx.ResourceTime())

	if err := (&DelegateResourceActuator{}).Validate(ctx); err != nil {
		t.Fatalf("acquired delegation should be consumed before self V2 balance: %v", err)
	}
}

func TestDelegateResourceValidate_RejectsConsumedEnergyV2(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_ENERGY,
		Balance:         5_000_000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.DynProps.SetTotalEnergyCurrentLimit(1_000)
	ctx.DynProps.SetTotalEnergyWeight(10)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 10_000_000)
	ctx.State.SetEnergyUsage(owner, 600)
	ctx.State.SetLatestConsumeTimeForEnergy(owner, ctx.ResourceTime())

	err := (&DelegateResourceActuator{}).Validate(ctx)
	if err == nil {
		t.Fatal("expected consumed energy to reduce delegatable V2 balance")
	}
	if err.Error() != "delegateBalance must be less than or equal to available FreezeEnergyV2 balance" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDelegateResourceValidate_RejectsContractReceiver(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         1_000_000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Contract)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 5_000_000)

	if err := (&DelegateResourceActuator{}).Validate(ctx); err == nil {
		t.Fatal("expected contract receiver to be rejected")
	}
}

// setupLockedDelegateCtx is a helper that builds a ready-to-execute
// DelegateResourceContract with Lock=true and the given LockPeriod (in
// java-tron's "blocks" unit). Caller can tweak DynProps before running
// the actuator.
func setupLockedDelegateCtx(t *testing.T, lockPeriodBlocks int64) (*Context, tcommon.Address, tcommon.Address, *contractpb.DelegateResourceContract) {
	t.Helper()
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         1000000,
		Lock:            true,
		LockPeriod:      lockPeriodBlocks,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 5000000)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	return ctx, owner, receiver, c
}

// Pre-fork (default state, MaxDelegateLockPeriod==86400 so
// SupportMaxDelegateLockPeriod is false): the contract's LockPeriod field
// is ignored and lockPeriod forced to DelegatePeriod/BlockProducedInterval
// = 86400 blocks. Mirror java-tron getLockPeriod's `else` branch.
func TestDelegateResource_LockPreFork_ForcesDefault(t *testing.T) {
	ctx, owner, receiver, _ := setupLockedDelegateCtx(t, 99 /* bogus contract value */)
	// MaxDelegateLockPeriod stays at default (86400); SupportMaxDelegateLockPeriod=false.

	act := &DelegateResourceActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	dr := rawdb.ReadDelegatedResource(ctx.DB, owner, receiver)
	wantExpire := ctx.PrevBlockTime + int64(params.DelegatePeriod/params.BlockProducedInterval)*params.BlockProducedInterval
	if dr.ExpireTimeForBandwidth != wantExpire {
		t.Fatalf("pre-fork expire = %d, want %d (PrevBlockTime + 86400*3000ms)", dr.ExpireTimeForBandwidth, wantExpire)
	}
}

// Post-fork (proposal #78 raised MaxDelegateLockPeriod above the default):
// the contract's LockPeriod is honored verbatim, expireTime advances by
// LockPeriod * BlockProducedInterval.
func TestDelegateResource_LockPostFork_HonorsContract(t *testing.T) {
	ctx, owner, receiver, _ := setupLockedDelegateCtx(t, 100 /* blocks */)
	ctx.DynProps.SetMaxDelegateLockPeriod(10_000_000) // > 86400 → SupportMaxDelegateLockPeriod true

	act := &DelegateResourceActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	dr := rawdb.ReadDelegatedResource(ctx.DB, owner, receiver)
	wantExpire := ctx.PrevBlockTime + 100*params.BlockProducedInterval
	if dr.ExpireTimeForBandwidth != wantExpire {
		t.Fatalf("post-fork expire = %d, want %d (PrevBlockTime + 100*3000ms)", dr.ExpireTimeForBandwidth, wantExpire)
	}
}

func TestDelegateResource_LockedAndUnlockedUseSeparateV2Buckets(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	db := ethrawdb.NewMemoryDatabase()
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract,
		&contractpb.DelegateResourceContract{
			OwnerAddress:    owner[:],
			ReceiverAddress: receiver[:],
			Resource:        corepb.ResourceCode_BANDWIDTH,
			Balance:         1_000_000,
		}, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.DynProps.SetMaxDelegateLockPeriod(10_000_000)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 5_000_000)
	ctx.DB = db

	act := &DelegateResourceActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("unlocked execute: %v", err)
	}

	ctx.Tx = newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract,
		&contractpb.DelegateResourceContract{
			OwnerAddress:    owner[:],
			ReceiverAddress: receiver[:],
			Resource:        corepb.ResourceCode_BANDWIDTH,
			Balance:         2_000_000,
			Lock:            true,
			LockPeriod:      100,
		}, 0).Tx
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("locked execute: %v", err)
	}

	unlocked := rawdb.ReadDelegatedResourceV2(db, owner, receiver, false)
	if unlocked == nil || unlocked.FrozenBalanceForBandwidth != 1_000_000 || unlocked.ExpireTimeForBandwidth != 0 {
		t.Fatalf("unexpected unlocked bucket: %+v", unlocked)
	}
	locked := rawdb.ReadDelegatedResourceV2(db, owner, receiver, true)
	wantExpire := ctx.PrevBlockTime + 100*params.BlockProducedInterval
	if locked == nil || locked.FrozenBalanceForBandwidth != 2_000_000 || locked.ExpireTimeForBandwidth != wantExpire {
		t.Fatalf("unexpected locked bucket: %+v want expire %d", locked, wantExpire)
	}
	agg := rawdb.ReadDelegatedResource(db, owner, receiver)
	if agg == nil || agg.FrozenBalanceForBandwidth != 3_000_000 || agg.ExpireTimeForBandwidth != wantExpire {
		t.Fatalf("unexpected aggregate: %+v", agg)
	}
}

// Post-fork Validate rejects lockPeriod outside [0, maxDelegateLockPeriod].
func TestDelegateResource_LockPostFork_RejectsOutOfRange(t *testing.T) {
	// 86402 blocks exceeds the maxDelegateLockPeriod (86401) below.
	ctx, _, _, _ := setupLockedDelegateCtx(t, 86402)
	// Gate true (max=86401 > default 86400, UnfreezeDelayDays already 14)
	// but contract LockPeriod overshoots that ceiling.
	ctx.DynProps.SetMaxDelegateLockPeriod(86401)

	if err := (&DelegateResourceActuator{}).Validate(ctx); err == nil {
		t.Fatal("expected reject for lockPeriod > max")
	}
}

// Post-fork validRemainTime: a new locked delegation can't shorten an
// already-locked entry's remaining time. Mirror java-tron validRemainTime.
func TestDelegateResource_LockPostFork_RejectsShorterRemain(t *testing.T) {
	ctx, owner, receiver, _ := setupLockedDelegateCtx(t, 100 /* 100 blocks = 300_000 ms */)
	ctx.DynProps.SetMaxDelegateLockPeriod(10_000_000)
	// Pre-seed a prior locked delegation whose remaining time exceeds the new
	// lockPeriod's duration: existingExpire - PrevBlockTime > 100*3000.
	rawdb.WriteDelegatedResourceV2(ctx.DB, owner, receiver, true, &rawdb.DelegatedResource{
		From: owner, To: receiver,
		FrozenBalanceForBandwidth: 1,
		ExpireTimeForBandwidth:    ctx.PrevBlockTime + 1_000_000, // remain = 1_000_000ms ≫ 300_000ms
	})

	if err := (&DelegateResourceActuator{}).Validate(ctx); err == nil {
		t.Fatal("expected reject for shorter lockPeriod than remaining time")
	}
}
