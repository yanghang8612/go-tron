package core

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// newNetWindowDynProps builds a DynamicProperties for the bandwidth-window tests:
// total_net_weight=1 (so a 1-TRX freeze yields a limit far above the test costs),
// plus the Stake-2.0 gates as requested.
func newNetWindowDynProps(unfreezeDelay, cancelAllV2 bool) *state.DynamicProperties {
	dp := state.NewDynamicProperties()
	dp.SetTotalNetWeight(1)
	if unfreezeDelay {
		dp.SetUnfreezeDelayDays(14) // SupportUnfreezeDelay
	}
	if cancelAllV2 {
		dp.SetAllowCancelAllUnfreezeV2(true) // -> SupportCancelAllUnfreezeV2 (with unfreeze delay)
	}
	return dp
}

// Golden tests for the per-account bandwidth (NET) recovery window, ported from
// java-tron BandwidthProcessor/ResourceProcessor. The window math is shared with
// the ENERGY path (actuator/energy_window.go), so the pinned oracle values are
// the same java-CI-enforced numbers used there:
//   - V1 / V2 settle: org.tron.core.EnergyProcessorTest.testUseEnergyInWindowSizeV2.
//   - single-step settle (the shape bandwidth useAccountNet uses — one
//     increase(ac, BANDWIDTH, usage, bytes, lastTime, now), no VM pre-charge):
//     java-verified, corretto-17, 2026-05-20.

// TestComputeResourceIncrease_V2MatchesJavaGolden replicates java
// EnergyProcessorTest scenario 2 (supportAllowCancelAllUnfreezeV2 ON): start
// usage=70021176, window=300 (V1, optimized=false), apply 1001 settle increments
// of 2345 at lastTime==now==9999. java: usage=72368521, windowSizeV2=1224919,
// optimized=true. Identical to the energy port because the window math is
// resource-agnostic.
func TestComputeResourceIncrease_V2MatchesJavaGolden(t *testing.T) {
	rawWindow, optimized := int64(300), false
	usage := int64(70_021_176)
	const now = int64(9_999)
	const cancelAllV2 = true
	for i := 0; i < 1001; i++ {
		usage, rawWindow, optimized = computeResourceIncrease(rawWindow, optimized, usage, 2345, now, now, false, cancelAllV2)
	}
	if usage != 72_368_521 {
		t.Fatalf("net usage = %d, want 72368521 (java golden)", usage)
	}
	if rawWindow != 1_224_919 {
		t.Fatalf("windowSizeV2 = %d, want 1224919 (java golden)", rawWindow)
	}
	if !optimized {
		t.Fatalf("optimized = false, want true (V2 path sets optimized)")
	}
}

// TestComputeResourceIncrease_V1MatchesJavaGolden — scenario 1 (supportUnfreezeDelay
// ON, cancelAllV2 OFF): same start, 1001 increments. java: usage=72368521,
// windowSize=300, optimized=false.
func TestComputeResourceIncrease_V1MatchesJavaGolden(t *testing.T) {
	rawWindow, optimized := int64(300), false
	usage := int64(70_021_176)
	const now = int64(9_999)
	const cancelAllV2 = false
	for i := 0; i < 1001; i++ {
		usage, rawWindow, optimized = computeResourceIncrease(rawWindow, optimized, usage, 2345, now, now, false, cancelAllV2)
	}
	if usage != 72_368_521 {
		t.Fatalf("net usage = %d, want 72368521 (java golden)", usage)
	}
	if rawWindow != 300 {
		t.Fatalf("windowSize = %d, want 300 (java golden)", rawWindow)
	}
	if optimized {
		t.Fatalf("optimized = true, want false (V1 path leaves optimized untouched)")
	}
}

// TestComputeResourceIncrease_RecoveryEquivalence pins the equivalence the
// bandwidth limit check relies on: computeResourceIncrease(..., usage=0, ...).
// newUsage equals java recovery(ac, BANDWIDTH, usage, lastTime, now) ==
// increase(usage, 0, lastTime, now, oldWindowSize). With usage==0 the V1/V2 window
// branches differ but newUsage (== remainUsage) is identical.
func TestComputeResourceIncrease_RecoveryEquivalence(t *testing.T) {
	// V2 optimized window, 7155 slots elapsed.
	recV2, _, _ := computeResourceIncrease(14_400_000, true, 1_000_000, 0, 9_999, 17_154, false, true)
	recV1, _, _ := computeResourceIncrease(14_400_000, true, 1_000_000, 0, 9_999, 17_154, false, false)
	if recV2 != recV1 {
		t.Fatalf("recovery diverges by window mode: V2=%d V1=%d (usage==0 must match)", recV2, recV1)
	}
	// no decay at lastTime==now; full decay once delta >= window.
	if got, _, _ := computeResourceIncrease(0, false, 852_710_572, 0, 5, 5, false, false); got != 852_710_572 {
		t.Fatalf("recovery(delta=0) = %d, want 852710572 (no decay)", got)
	}
	if got, _, _ := computeResourceIncrease(0, false, 852_710_572, 0, 0, 28_800, false, false); got != 0 {
		t.Fatalf("recovery(delta>=window) = %d, want 0 (fully decayed)", got)
	}
}

// TestChargeStakedNet_PerAccountWindow_SingleStepGolden drives the real bandwidth
// settle entry point through the Stake-2.0 (supportUnfreezeDelay + cancelAllV2)
// regime with a pre-existing non-default V2 window and elapsed time. Bandwidth
// settles single-step (one increase(ac, BANDWIDTH, usage, bytes, lastTime, now)),
// matching java's BandwidthProcessor.useAccountNet — the same arithmetic as the
// energy FAILURE single-step golden: usage 1_000_000 + 50_000 over a 14_400_000
// (V2) window 7155 slots later -> net_usage 553_125, net_window 9_193_462 (V2).
func TestChargeStakedNet_PerAccountWindow_SingleStepGolden(t *testing.T) {
	statedb := newTestState(t)
	dynProps := newNetWindowDynProps(true, true)

	addr := testProcessorAddr(1)
	statedb.CreateAccount(addr, corepb.AccountType_Normal)
	statedb.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 1_000_000) // weight -> limit >> cost
	statedb.SetNetUsage(addr, 1_000_000)
	statedb.SetLatestConsumeTime(addr, 9_999)
	statedb.GetAccount(addr).SetNewNetWindowSizeV2(14_400_000)

	const now = int64(9_999 + 7_155)
	if ok := chargeStakedNet(statedb, dynProps, addr, 50_000, now); !ok {
		t.Fatal("chargeStakedNet returned false; stake should cover the cost")
	}
	if got := statedb.GetNetUsage(addr); got != 553_125 {
		t.Fatalf("net_usage = %d, want 553125 (java single-step golden)", got)
	}
	after := statedb.GetAccount(addr)
	if got := after.RawNetWindowSize(); got != 9_193_462 {
		t.Fatalf("net_window_size = %d, want 9193462 (java single-step golden)", got)
	}
	if !after.NetWindowOptimized() {
		t.Fatalf("net_window_optimized = false, want true")
	}
	if got := statedb.GetLatestConsumeTime(addr); got != now {
		t.Fatalf("latest_consume_time = %d, want %d", got, now)
	}
}

// TestChargeStakedNet_FeedForward proves the consensus-fork scenario: tx1 writes a
// per-account net_window; tx2, after the window-shaping recovery, recovers usage
// against THAT written window (not the global 28800 default). Before this fix the
// window was never tracked, so tx2 would decay over 28800 and store a different
// net_usage — a latent fork in the Stake-2.0 era.
func TestChargeStakedNet_FeedForward(t *testing.T) {
	statedb := newTestState(t)
	dynProps := newNetWindowDynProps(true, true)

	addr := testProcessorAddr(2)
	statedb.CreateAccount(addr, corepb.AccountType_Normal)
	statedb.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 1_000_000)
	statedb.SetNetUsage(addr, 1_000_000)
	statedb.SetLatestConsumeTime(addr, 9_999)
	statedb.GetAccount(addr).SetNewNetWindowSizeV2(14_400_000)

	// tx1 at slot 17154 -> net_usage 553125, window 9_193_462 (per single-step golden).
	if ok := chargeStakedNet(statedb, dynProps, addr, 50_000, 17_154); !ok {
		t.Fatal("tx1 chargeStakedNet false")
	}
	w1 := statedb.GetAccount(addr).RawNetWindowSize()
	if w1 != 9_193_462 {
		t.Fatalf("after tx1: net_window = %d, want 9193462", w1)
	}

	// tx2 recovers tx1's usage against the WRITTEN window (~9193 slots), not 28800.
	now2 := int64(17_154 + 4_581) // ~half the written window
	if ok := chargeStakedNet(statedb, dynProps, addr, 0, now2); !ok {
		t.Fatal("tx2 chargeStakedNet false")
	}
	if w2 := statedb.GetAccount(addr).RawNetWindowSize(); w2 == 14_400_000 {
		t.Fatalf("tx2 left the original ingested window 14400000 — window not tracked")
	}
	got := statedb.GetNetUsage(addr)
	globalWindowResult := int64(553_125) * (28_800 - 4_581) / 28_800 // ~465k if global window (wrong)
	if got >= globalWindowResult {
		t.Fatalf("tx2 net_usage = %d; per-account window must decay faster than global (%d)", got, globalWindowResult)
	}
}

// TestChargeStakedNet_NonV2_GlobalWindowUnchanged confirms the pre-Stake-2.0 path
// is preserved: with SupportUnfreezeDelay off, chargeStakedNet keeps the global
// 28800-slot window recover+add and never writes net_window (matches java's
// static increase path). Starting usage 1_000_000, 7200 slots elapsed, +50_000:
// recover to 750_000 then add -> 800_000 (the round-input case where precision-
// averaging and truncate coincide), and net_window stays 0.
func TestChargeStakedNet_NonV2_GlobalWindowUnchanged(t *testing.T) {
	statedb := newTestState(t)
	dynProps := newNetWindowDynProps(false, false)

	addr := testProcessorAddr(3)
	statedb.CreateAccount(addr, corepb.AccountType_Normal)
	statedb.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 1_000_000)
	statedb.SetNetUsage(addr, 1_000_000)
	statedb.SetLatestConsumeTime(addr, 1_000_000-7_200)

	if ok := chargeStakedNet(statedb, dynProps, addr, 50_000, 1_000_000); !ok {
		t.Fatal("chargeStakedNet false")
	}
	if got := statedb.GetNetUsage(addr); got != 800_000 {
		t.Fatalf("non-V2 net_usage = %d, want 800000 (global window preserved)", got)
	}
	if got := statedb.GetAccount(addr).RawNetWindowSize(); got != 0 {
		t.Fatalf("non-V2 path wrote net_window_size = %d, want 0 (must stay untouched)", got)
	}
}
