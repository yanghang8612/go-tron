package actuator

import (
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestFreezeBalance_WeightIncrementBandwidth verifies the freeze actuator
// grows total_net_weight by frozenBalance/TRX_PRECISION (java-tron's
// FreezeBalanceActuator.addTotalWeight).
func TestFreezeBalance_WeightIncrementBandwidth(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000) // 10 TRX

	tx := makeFreezeBalanceTx(1, 5_000_000, 3, corepb.ResourceCode_BANDWIDTH, nil) // freeze 5 TRX
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	if got := ctx.DynProps.TotalNetWeight(); got != 0 {
		t.Fatalf("pre-freeze net weight: got %d, want 0", got)
	}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got := ctx.DynProps.TotalNetWeight(); got != 5 {
		t.Errorf("post-freeze net weight: got %d, want 5 (= 5_000_000 SUN / TRX_PRECISION)", got)
	}
}

func TestFreezeBalance_WeightIncrementEnergy(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(2)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeFreezeBalanceTx(2, 7_000_000, 3, corepb.ResourceCode_ENERGY, nil)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got := ctx.DynProps.TotalEnergyWeight(); got != 7 {
		t.Errorf("post-freeze energy weight: got %d, want 7", got)
	}
	if got := ctx.DynProps.TotalNetWeight(); got != 0 {
		t.Errorf("net weight must not move when resource=ENERGY: got %d", got)
	}
}

func TestFreezeBalance_RejectedPostFork(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(3)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeFreezeBalanceTx(3, 1_000_000, 3, corepb.ResourceCode_BANDWIDTH, nil)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	// Activate unfreeze-delay; java-tron closes old FreezeBalance only at
	// this gate, not merely at allow_new_resource_model.
	ctx.DynProps.SetUnfreezeDelayDays(14)

	err := act.Validate(ctx)
	if err == nil {
		t.Fatal("expected validate to reject V1 freeze post-fork")
	}
	if err.Error() != "freeze v2 is open, old freeze is closed" {
		t.Errorf("error message drift: got %q", err.Error())
	}
}

// TestUnfreezeBalance_WeightDecrementBandwidth covers the negative half:
// after an expired V1 entry is unfrozen, total_net_weight shrinks by the
// unfrozen TRX amount.
func TestUnfreezeBalance_WeightDecrementBandwidth(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(4)
	seedAccount(statedb, owner, 10_000_000)

	// Freeze 5 TRX, then fast-forward past expiry and unfreeze.
	freezeTx := makeFreezeBalanceTx(4, 5_000_000, 3, corepb.ResourceCode_BANDWIDTH, nil)
	fAct := &FreezeBalanceActuator{}
	fCtx := setupContext(t, statedb, freezeTx)
	if _, err := fAct.Execute(fCtx); err != nil {
		t.Fatalf("freeze execute: %v", err)
	}

	if got := fCtx.DynProps.TotalNetWeight(); got != 5 {
		t.Fatalf("post-freeze weight: got %d, want 5", got)
	}

	unfreezeTx := makeUnfreezeBalanceTx(4, corepb.ResourceCode_BANDWIDTH, nil)
	uAct := &UnfreezeBalanceActuator{}
	uCtx := &Context{
		State:         statedb,
		DynProps:      fCtx.DynProps,
		Tx:            unfreezeTx,
		BlockTime:     fCtx.BlockTime + 4*86_400_000, // 4 days later — past 3d expiry
		PrevBlockTime: fCtx.BlockTime + 4*86_400_000,
		BlockNumber:   2,
	}
	if _, err := uAct.Execute(uCtx); err != nil {
		t.Fatalf("unfreeze execute: %v", err)
	}
	if got := uCtx.DynProps.TotalNetWeight(); got != 0 {
		t.Errorf("post-unfreeze weight: got %d, want 0", got)
	}
}

func TestUnfreezeBalance_WorksPostFork(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(5)
	seedAccount(statedb, owner, 10_000_000)

	// Freeze BEFORE activating the fork.
	freezeTx := makeFreezeBalanceTx(5, 2_000_000, 3, corepb.ResourceCode_ENERGY, nil)
	fAct := &FreezeBalanceActuator{}
	fCtx := setupContext(t, statedb, freezeTx)
	if _, err := fAct.Execute(fCtx); err != nil {
		t.Fatalf("freeze execute: %v", err)
	}

	// Now activate V2 fork. Historical V1 unfreezes must still succeed.
	fCtx.DynProps.Set("allow_new_resource_model", 1)

	unfreezeTx := makeUnfreezeBalanceTx(5, corepb.ResourceCode_ENERGY, nil)
	uAct := &UnfreezeBalanceActuator{}
	uCtx := &Context{
		State:         statedb,
		DynProps:      fCtx.DynProps,
		Tx:            unfreezeTx,
		BlockTime:     fCtx.BlockTime + 4*86_400_000,
		PrevBlockTime: fCtx.BlockTime + 4*86_400_000,
		BlockNumber:   2,
	}
	if err := uAct.Validate(uCtx); err != nil {
		t.Fatalf("unfreeze validate should succeed post-fork: %v", err)
	}
	if _, err := uAct.Execute(uCtx); err != nil {
		t.Fatalf("unfreeze execute: %v", err)
	}
	if got := uCtx.DynProps.TotalEnergyWeight(); got != 0 {
		t.Errorf("post-unfreeze energy weight: got %d, want 0", got)
	}
}
