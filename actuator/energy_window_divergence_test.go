package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Golden tests for the per-account energy recovery window, ported from
// java-tron (see docs/dev/energy-window-divergence-2026-05-20.md). Oracle
// values are taken from / verified against java-tron's own test suite:
//
//   - V1 / V2 settle goldens: org.tron.core.EnergyProcessorTest
//     .testUseEnergyInWindowSizeV2 (CI-enforced).
//   - Path B (contract-tx pre-charge -> reset -> settle) feed-forward golden and
//     the success-vs-failure divergence: verified by driving java-tron's
//     EnergyProcessor through the updateUsage + useEnergy sequence (corretto-17),
//     2026-05-20.

// TestComputeEnergyIncrease_V2MatchesJavaGolden replicates java
// EnergyProcessorTest scenario 2 (supportAllowCancelAllUnfreezeV2 ON): start
// energy_usage=70021176, window=300 (V1, optimized=false), and apply 1001
// settle increments of 2345 with lastTime==now==9999. java result:
// energy_usage=72368521, windowSizeV2=1224919, windowSize=1224, optimized=true.
func TestComputeEnergyIncrease_V2MatchesJavaGolden(t *testing.T) {
	rawWindow, optimized := int64(300), false
	usage := int64(70_021_176)
	const now = int64(9_999)
	const cancelAllV2 = true
	for i := 0; i < 1001; i++ {
		usage, rawWindow, optimized = computeEnergyIncrease(rawWindow, optimized, usage, 2345, now, now, false, cancelAllV2)
	}
	if usage != 72_368_521 {
		t.Fatalf("energy_usage = %d, want 72368521 (java golden)", usage)
	}
	if rawWindow != 1_224_919 {
		t.Fatalf("windowSizeV2 = %d, want 1224919 (java golden)", rawWindow)
	}
	if !optimized {
		t.Fatalf("optimized = false, want true (V2 path sets optimized)")
	}
}

// TestComputeEnergyIncrease_V1MatchesJavaGolden replicates java
// EnergyProcessorTest scenario 1 (supportUnfreezeDelay ON, cancelAllV2 OFF):
// same start, 1001 increments at lastTime==now. java result:
// energy_usage=72368521, windowSize=300, optimized=false.
func TestComputeEnergyIncrease_V1MatchesJavaGolden(t *testing.T) {
	rawWindow, optimized := int64(300), false
	usage := int64(70_021_176)
	const now = int64(9_999)
	const cancelAllV2 = false
	for i := 0; i < 1001; i++ {
		usage, rawWindow, optimized = computeEnergyIncrease(rawWindow, optimized, usage, 2345, now, now, false, cancelAllV2)
	}
	if usage != 72_368_521 {
		t.Fatalf("energy_usage = %d, want 72368521 (java golden)", usage)
	}
	if rawWindow != 300 {
		t.Fatalf("windowSize = %d, want 300 (java golden)", rawWindow)
	}
	if optimized {
		t.Fatalf("optimized = true, want false (V1 path leaves optimized untouched)")
	}
}

// TestUseEnergyForBill_PerAccountWindow_PathBGolden drives the actual settle
// entry point through the contract-tx flow (V2 regime, time elapsed, success).
// Caller starts with energy_usage=1_000_000, energy_window=14_400_000 (V2,
// optimized), last consume slot 9999; the charge happens 7200 slots later
// (slot 17199).
//
// java-verified golden (corretto-17, 2026-05-20): energy_usage=550_000,
// energy_window_size=9_163_637 (V2), optimized, latest_consume_time=17199.
// (go-tron's old global-window settle produced 800_000 and left the window
// stale at 14_400_000 — the divergence this fix closes.)
func TestUseEnergyForBill_PerAccountWindow_PathBGolden(t *testing.T) {
	owner := tcommon.Address{0x41, 0x77, 0x01}
	ctx := newEnergyBillCtx(t, owner)
	ctx.HeadSlot = 17_199
	ctx.DynProps.SetUnfreezeDelayDays(14)          // SupportUnfreezeDelay
	ctx.DynProps.SetAllowCancelAllUnfreezeV2(true) // -> SupportCancelAllUnfreezeV2 (V2 path)

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetEnergyUsage(owner, 1_000_000)
	ctx.State.SetLatestConsumeTimeForEnergy(owner, 9_999)
	ctx.State.GetAccount(owner).SetNewEnergyWindowSizeV2(14_400_000)

	useEnergyForBill(ctx, owner, 50_000, true) // success path

	if got := ctx.State.GetEnergyUsage(owner); got != 550_000 {
		t.Fatalf("energy_usage = %d, want 550000 (java golden)", got)
	}
	after := ctx.State.GetAccount(owner)
	if got := after.RawEnergyWindowSize(); got != 9_163_637 {
		t.Fatalf("energy_window_size = %d, want 9163637 (java golden)", got)
	}
	if !after.EnergyWindowOptimized() {
		t.Fatalf("energy_window_optimized = false, want true")
	}
	if got := ctx.State.GetLatestConsumeTimeForEnergy(owner); got != 17_199 {
		t.Fatalf("latest_consume_time_for_energy = %d, want 17199", got)
	}
}

// TestUseEnergyForBill_FeedForward proves the consensus-fork scenario: tx1
// writes a per-account window; tx2, after the window-shaping recovery, reads
// THAT window (not the global default) to recover usage. With the fix, tx1's
// non-default window must influence tx2's stored energy_usage.
func TestUseEnergyForBill_FeedForward(t *testing.T) {
	owner := tcommon.Address{0x41, 0x77, 0x02}
	ctx := newEnergyBillCtx(t, owner)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.DynProps.SetAllowCancelAllUnfreezeV2(true)

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetEnergyUsage(owner, 1_000_000)
	ctx.State.SetLatestConsumeTimeForEnergy(owner, 9_999)
	ctx.State.GetAccount(owner).SetNewEnergyWindowSizeV2(14_400_000)

	// tx1 at slot 17199 (7200 elapsed): per Path B golden -> usage 550000, window 9163637.
	ctx.HeadSlot = 17_199
	useEnergyForBill(ctx, owner, 50_000, true)
	w1 := ctx.State.GetAccount(owner).RawEnergyWindowSize()
	if w1 != 9_163_637 {
		t.Fatalf("after tx1: window = %d, want 9163637", w1)
	}

	// tx2 must recover tx1's usage against the WRITTEN window (9163), not the
	// global 28800. The window field is live state feeding the next tx.
	ctx.HeadSlot = 17_199 + 4_581 // half of the ~9163-slot written window
	useEnergyForBill(ctx, owner, 0, true)
	w2 := ctx.State.GetAccount(owner).RawEnergyWindowSize()
	if w2 == 14_400_000 {
		t.Fatalf("tx2 left the original ingested window 14400000 — window not being tracked")
	}
	// Recovery used the per-account window, so usage decayed by ~half of 550000,
	// not by 4581/28800. Assert it's well below the global-window result.
	got := ctx.State.GetEnergyUsage(owner)
	globalWindowResult := int64(550_000) * (28_800 - 4_581) / 28_800 // ~462k if global window (wrong)
	if got >= globalWindowResult {
		t.Fatalf("tx2 energy_usage = %d; per-account window must decay faster than global (%d)", got, globalWindowResult)
	}
}

// TestUseEnergyForBill_SuccessVsFailureDiverge pins the java behavior that the
// SETTLE differs between success and failure paths. On success, java commits the
// VMActuator pre-charge and TransactionTrace.resetAccountUsage restores the
// pre-merge state, so useEnergy settles in two steps (recover-then-add at
// lastTime==now). On REVERT/exception/OOE the pre-charge is discarded
// (VMActuator.java:234-250 never commits rootRepository) and resetAccountUsage
// is skipped, so useEnergy settles single-step over the ORIGINAL usage/time.
//
// At delta=7155 these produce DIFFERENT committed state (java-verified,
// corretto-17): success -> (553124, window 9193479); failure -> (553125,
// window 9193462). A single byte of energy_usage / ~17 of window — a real fork.
func TestUseEnergyForBill_SuccessVsFailureDiverge(t *testing.T) {
	setup := func(t *testing.T, owner tcommon.Address) *Context {
		ctx := newEnergyBillCtx(t, owner)
		ctx.HeadSlot = 9_999 + 7_155
		ctx.DynProps.SetUnfreezeDelayDays(14)
		ctx.DynProps.SetAllowCancelAllUnfreezeV2(true)
		ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
		ctx.State.SetEnergyUsage(owner, 1_000_000)
		ctx.State.SetLatestConsumeTimeForEnergy(owner, 9_999)
		ctx.State.GetAccount(owner).SetNewEnergyWindowSizeV2(14_400_000)
		return ctx
	}

	t.Run("success_two_step", func(t *testing.T) {
		owner := tcommon.Address{0x41, 0x77, 0x04}
		ctx := setup(t, owner)
		useEnergyForBill(ctx, owner, 50_000, true)
		if got := ctx.State.GetEnergyUsage(owner); got != 553_124 {
			t.Fatalf("success energy_usage = %d, want 553124 (java two-step)", got)
		}
		if got := ctx.State.GetAccount(owner).RawEnergyWindowSize(); got != 9_193_479 {
			t.Fatalf("success window = %d, want 9193479 (java two-step)", got)
		}
	})

	t.Run("failure_single_step", func(t *testing.T) {
		owner := tcommon.Address{0x41, 0x77, 0x05}
		ctx := setup(t, owner)
		useEnergyForBill(ctx, owner, 50_000, false)
		if got := ctx.State.GetEnergyUsage(owner); got != 553_125 {
			t.Fatalf("failure energy_usage = %d, want 553125 (java single-step)", got)
		}
		if got := ctx.State.GetAccount(owner).RawEnergyWindowSize(); got != 9_193_462 {
			t.Fatalf("failure window = %d, want 9193462 (java single-step)", got)
		}
	})
}

// TestUseEnergyForBill_NonV2_GlobalWindowUnchanged confirms the pre-Stake-2.0
// path is preserved: with SupportUnfreezeDelay off, go-tron keeps using the
// global window and never writes the per-account field (matches java's static
// increase path).
func TestUseEnergyForBill_NonV2_GlobalWindowUnchanged(t *testing.T) {
	owner := tcommon.Address{0x41, 0x77, 0x03}
	ctx := newEnergyBillCtx(t, owner)
	ctx.HeadSlot = 1_000_000 // UnfreezeDelay NOT set -> SupportUnfreezeDelay() == false

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetEnergyUsage(owner, 1_000_000)
	ctx.State.SetLatestConsumeTimeForEnergy(owner, ctx.HeadSlot-7_200)

	useEnergyForBill(ctx, owner, 50_000, true)

	// Global-window precision-averaging recovery then add (java increase x2):
	// recover 1_000_000 over 7200/28800 -> 750000, add 50000 at now==now ->
	// 800000. At these round inputs the precision-averaging and the old truncate
	// coincide; TestComputeEnergyIncreaseGlobal_PreStake2Golden covers a case
	// where they diverge.
	if got := ctx.State.GetEnergyUsage(owner); got != 800_000 {
		t.Fatalf("non-V2 energy_usage = %d, want 800000 (global window preserved)", got)
	}
	if got := ctx.State.GetAccount(owner).RawEnergyWindowSize(); got != 0 {
		t.Fatalf("non-V2 path wrote energy_window_size = %d, want 0 (must stay untouched)", got)
	}
}

// TestComputeEnergyIncreaseGlobal_PreStake2Golden pins the pre-Stake-2.0 energy
// recovery/settle against java-tron ResourceProcessor.increase (the 4-arg,
// global 28800-slot window, precision-averaging: divideCeil*1e6 + round(decay) +
// getUsage). The numbers come from the real Nile fork at block 8,825,873: a busy
// contract owner with energy_usage=852,710,572 recovered one slot later. java's
// increase keeps usage at 852,680,964; go-tron's previous truncate
// (oldUsage*(W-delta)/W) gave 852,680,963 — one unit low. That ~1/recovered-block
// bias compounded over ~8.8M blocks into a ~249,705 drift that flipped an
// OUT_OF_ENERGY into a SUCCESS (see project_pre_stake2_energy_recovery_drift).
func TestComputeEnergyIncreaseGlobal_PreStake2Golden(t *testing.T) {
	const U = int64(852_710_572)

	// Recovery (usage==0), 1 slot elapsed. The whole point of the fix:
	if got := computeEnergyIncreaseGlobal(U, 0, 0, 1, false); got != 852_680_964 {
		t.Fatalf("recovery(delta=1) = %d, want 852680964 (java increase); old truncate gave 852680963", got)
	}
	// Two-step settle: recover, then add a 13818-energy charge at lastTime==now.
	rec := computeEnergyIncreaseGlobal(U, 0, 0, 1, false)
	if got := computeEnergyIncreaseGlobal(rec, 13_818, 1, 1, false); got != 852_694_782 {
		t.Fatalf("settle(delta=1,+13818) = %d, want 852694782 (java two-step)", got)
	}
	// Boundary cases: no decay at lastTime==now; full decay once delta>=window.
	if got := computeEnergyIncreaseGlobal(U, 0, 5, 5, false); got != U {
		t.Fatalf("recovery(delta=0) = %d, want %d (no decay at lastTime==now)", got, U)
	}
	if got := computeEnergyIncreaseGlobal(U, 0, 0, 28_800, false); got != 0 {
		t.Fatalf("recovery(delta>=window) = %d, want 0 (fully decayed)", got)
	}
}

// TestUseEnergyForBill_PreStake2RecoverDrift drives the settle entry point through
// the pre-Stake-2.0 path with the Nile fork's diverging inputs and asserts the
// stored energy_usage matches java's two-step increase (852_694_782), not the old
// truncate result (852_694_781). Guards the consensus path end-to-end.
func TestUseEnergyForBill_PreStake2RecoverDrift(t *testing.T) {
	owner := tcommon.Address{0x41, 0x77, 0x06}
	ctx := newEnergyBillCtx(t, owner)
	ctx.HeadSlot = 1_000_000 // UnfreezeDelay NOT set -> SupportUnfreezeDelay() == false

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetEnergyUsage(owner, 852_710_572)
	ctx.State.SetLatestConsumeTimeForEnergy(owner, ctx.HeadSlot-1) // delta=1 slot

	useEnergyForBill(ctx, owner, 13_818, true)

	if got := ctx.State.GetEnergyUsage(owner); got != 852_694_782 {
		t.Fatalf("pre-Stake-2.0 settle energy_usage = %d, want 852694782 (java); old truncate gave 852694781", got)
	}
	if got := ctx.State.GetAccount(owner).RawEnergyWindowSize(); got != 0 {
		t.Fatalf("pre-Stake-2.0 path wrote energy_window_size = %d, want 0 (global window, untouched)", got)
	}
}

// TestNile8825873CanonicalParentStateLeavesTx3OutOfEnergyBudget pins the static
// /jn vs /gn diagnosis for Nile block 8,825,873. java-tron's parent state leaves
// the origin with 102,680 energy; the first three same-contract deposit() calls
// each consume 27,461, so tx3 must enter the VM with only 20,297 energy and fail
// at SSTORE. A lower parent energy_usage gives the 269,969 limit seen in the
// stale gtron DB and incorrectly turns the tx into SUCCESS.
func TestNile8825873CanonicalParentStateLeavesTx3OutOfEnergyBudget(t *testing.T) {
	caller := tcommon.Address{0x41, 0x95, 0x37, 0x6a, 0x34, 0xfc, 0x88, 0x95, 0xaf, 0xab, 0x95, 0x96, 0x47, 0x2d, 0x5f, 0x65, 0x8b, 0xf9, 0x5d, 0xae, 0x7c}
	origin := tcommon.Address{0x41, 0x84, 0x29, 0x2b, 0x9e, 0xe2, 0xe6, 0x85, 0x59, 0x1a, 0x92, 0x6b, 0x82, 0xf2, 0xed, 0x4d, 0xbc, 0xac, 0x06, 0xe3, 0xc1}
	contractAddr := tcommon.Address{0x41, 0x55, 0x22, 0x30, 0x72, 0xbd, 0x8b, 0x36, 0x89, 0x93, 0x10, 0xda, 0xcc, 0x80, 0x36, 0x7d, 0xed, 0xf8, 0x51, 0xfc, 0x34}

	ctx := newEnergyBillCtx(t, caller)
	ctx.HeadSlot = 533_104_559
	ctx.DynProps.SetLatestBlockHeaderNumber(8_825_872)
	ctx.DynProps.SetTotalEnergyWeight(10_550_584)
	ctx.DynProps.SetTotalEnergyLimit(90_000_000_000)
	ctx.DynProps.SetTotalEnergyCurrentLimit(90_000_000_000)

	ctx.State.CreateAccount(caller, corepb.AccountType_Normal)
	ctx.State.CreateAccount(origin, corepb.AccountType_Normal)
	ctx.State.GetAccount(origin).AddFrozenEnergy(100_000_000_000, 1_591_612_749_000)
	ctx.State.SetEnergyUsage(origin, 852_930_668)
	ctx.State.SetLatestConsumeTimeForEnergy(origin, ctx.HeadSlot)
	installOriginContract(t, ctx, contractAddr, origin, 0, 10_000_000)

	if got := calcAccountEnergyLimit(ctx.State.GetAccount(origin), ctx.DynProps); got != 853_033_348 {
		t.Fatalf("origin energy limit = %d, want 853033348", got)
	}
	if got := availableAccountEnergyForBill(ctx.State, ctx.DynProps, origin, ctx.ResourceTime()); got != 102_680 {
		t.Fatalf("parent origin energy left = %d, want 102680 from java /jn parent state", got)
	}

	for i := 0; i < 3; i++ {
		useEnergyForBill(ctx, origin, 27_461, true)
	}

	if got := ctx.State.GetEnergyUsage(origin); got != 853_013_051 {
		t.Fatalf("origin usage after three deposits = %d, want 853013051", got)
	}
	if got := availableAccountEnergyForBill(ctx.State, ctx.DynProps, origin, ctx.ResourceTime()); got != 20_297 {
		t.Fatalf("origin energy left before tx3 = %d, want 20297", got)
	}
	if got := triggerEnergyLimit(ctx, caller, contractAddr, ctx.Tx.FeeLimit(), 0, &Result{}); got != 20_297 {
		t.Fatalf("tx3 invoke energy limit = %d, want 20297; 27461-energy deposit must be OUT_OF_ENERGY", got)
	}
}
