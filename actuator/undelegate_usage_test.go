package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/delegation"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

const trxPrecisionTest = 1_000_000

// ctxFor builds a Context usable by the transfer helpers. Only State +
// DynProps + BlockTime are read; BlockTime carries the resource slot here.
func ctxFor(statedb *state.StateDB, dp *state.DynamicProperties, now int64) *Context {
	return &Context{State: statedb, DynProps: dp, BlockTime: now}
}

func TestTransferUsageFromReceiver_ProRata(t *testing.T) {
	statedb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	dp.Set("total_net_limit", 43_200_000_000)
	dp.SetTotalNetWeight(1000) // 1000 TRX total stake network-wide

	receiver := makeTestAddr(0x21)
	seedAccount(statedb, receiver, 0)

	// Receiver has 100 TRX acquired delegation, 0 own frozen. Total = 100 TRX.
	statedb.AddAcquiredDelegatedFrozenV2(receiver, corepb.ResourceCode_BANDWIDTH, 100*trxPrecisionTest)
	// They've consumed 500 bandwidth at an earlier resource slot.
	statedb.SetNetUsage(receiver, 500)
	statedb.SetLatestConsumeTime(receiver, 10)

	ctx := ctxFor(statedb, dp, 10) // no time elapsed → no decay

	// Undelegate 40 TRX — 40% of receiver's pool.
	transfer := delegation.TransferUsageFromReceiver(ctx.State, ctx.DynProps, receiver, corepb.ResourceCode_BANDWIDTH, 40*trxPrecisionTest, ctx.BlockTime)

	// Expected transfer = 500 × 40/100 = 200, capped at
	// maxUsage = 40 × (43_200_000_000 / 1000) = 40 × 43_200_000 = 1_728_000_000 (no cap hit).
	if transfer != 200 {
		t.Fatalf("transfer: got %d, want 200", transfer)
	}
	// Receiver's remaining usage: 500 - 200 = 300.
	if got := statedb.GetNetUsage(receiver); got != 300 {
		t.Fatalf("receiver usage after transfer: got %d, want 300", got)
	}
	if got := statedb.GetLatestConsumeTime(receiver); got != 10 {
		t.Fatalf("receiver consume time: got %d, want 10", got)
	}
}

func TestTransferUsageFromReceiver_CapByMaxUsage(t *testing.T) {
	statedb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	// Tiny total network → unDelegateMaxUsage is very small and dominates.
	dp.Set("total_net_limit", 1_000)
	dp.SetTotalNetWeight(1_000_000) // 1M TRX total weight

	receiver := makeTestAddr(0x21)
	seedAccount(statedb, receiver, 0)
	statedb.AddAcquiredDelegatedFrozenV2(receiver, corepb.ResourceCode_BANDWIDTH, 100*trxPrecisionTest)
	statedb.SetNetUsage(receiver, 1_000_000) // huge usage
	statedb.SetLatestConsumeTime(receiver, 0)

	ctx := ctxFor(statedb, dp, 0)

	// Undelegate 10 TRX.
	// Raw pro-rata = 1_000_000 × 10/100 = 100_000.
	// Max = (10 × 1_000) / 1_000_000 = 0 (integer truncation).
	transfer := delegation.TransferUsageFromReceiver(ctx.State, ctx.DynProps, receiver, corepb.ResourceCode_BANDWIDTH, 10*trxPrecisionTest, ctx.BlockTime)
	if transfer != 0 {
		t.Fatalf("transfer should be clamped to 0: got %d", transfer)
	}
}

func TestTransferUsageFromReceiver_NoUsage(t *testing.T) {
	statedb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	dp.Set("total_net_limit", 43_200_000_000)
	dp.SetTotalNetWeight(1000)

	receiver := makeTestAddr(0x21)
	seedAccount(statedb, receiver, 0)
	statedb.AddAcquiredDelegatedFrozenV2(receiver, corepb.ResourceCode_BANDWIDTH, 100*trxPrecisionTest)
	// No usage.

	ctx := ctxFor(statedb, dp, 1000)
	transfer := delegation.TransferUsageFromReceiver(ctx.State, ctx.DynProps, receiver, corepb.ResourceCode_BANDWIDTH, 40*trxPrecisionTest, ctx.BlockTime)
	if transfer != 0 {
		t.Fatalf("transfer with no usage: got %d, want 0", transfer)
	}
}

func TestTransferUsageFromReceiver_Energy(t *testing.T) {
	statedb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetTotalEnergyCurrentLimit(50_000_000_000)
	dp.SetTotalEnergyWeight(1000)

	receiver := makeTestAddr(0x21)
	seedAccount(statedb, receiver, 0)
	statedb.AddAcquiredDelegatedFrozenV2(receiver, corepb.ResourceCode_ENERGY, 200*trxPrecisionTest)
	statedb.SetEnergyUsage(receiver, 1000)
	statedb.SetLatestConsumeTimeForEnergy(receiver, 5)

	ctx := ctxFor(statedb, dp, 5)

	// Undelegate 50 TRX — 25% of receiver's 200 TRX energy pool.
	transfer := delegation.TransferUsageFromReceiver(ctx.State, ctx.DynProps, receiver, corepb.ResourceCode_ENERGY, 50*trxPrecisionTest, ctx.BlockTime)
	if transfer != 250 {
		t.Fatalf("energy transfer: got %d, want 250", transfer)
	}
	if got := statedb.GetEnergyUsage(receiver); got != 750 {
		t.Fatalf("receiver energy usage: got %d, want 750", got)
	}
}

func TestFoldUsageIntoOwner_AddsOnTopOfRecovered(t *testing.T) {
	statedb := setupStateDB(t)
	dp := state.NewDynamicProperties()

	owner := makeTestAddr(0x11)
	seedAccount(statedb, owner, 0)

	// Owner had 400 usage at slot 0; half a resource window later, recovery
	// leaves ~200.
	statedb.SetNetUsage(owner, 400)
	statedb.SetLatestConsumeTime(owner, 0)

	now := int64(params.WindowSizeSlots / 2)
	ctx := ctxFor(statedb, dp, now)

	delegation.FoldUsageIntoOwner(ctx.State, owner, corepb.ResourceCode_BANDWIDTH, 100, ctx.BlockTime)

	// Recovered = 400 × (window - halfWindow) / window = 200.
	// + transferred 100 = 300.
	if got := statedb.GetNetUsage(owner); got != 300 {
		t.Fatalf("owner usage: got %d, want 300", got)
	}
	if got := statedb.GetLatestConsumeTime(owner); got != now {
		t.Fatalf("owner consume time: got %d, want %d", got, now)
	}
}

// TestUnDelegateResourceExecute_TransfersUsageEndToEnd walks the full
// actuator path: delegate is already in place, receiver has consumed, owner
// undelegates, and the assertion covers (a) receiver usage drops, (b)
// owner usage gains, (c) frozen/delegated balances rebalance, (d)
// delegation record is updated.
func TestUnDelegateResourceExecute_TransfersUsageEndToEnd(t *testing.T) {
	statedb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetAllowDelegateResource(true)
	dp.SetUnfreezeDelayDays(14)
	dp.Set("total_net_limit", 43_200_000_000)
	dp.SetTotalNetWeight(1000)

	owner := makeTestAddr(0x41)
	receiver := makeTestAddr(0x42)
	seedAccount(statedb, owner, 0)
	seedAccount(statedb, receiver, 0)

	// Owner has delegated 100 TRX worth of bandwidth to receiver.
	statedb.AddDelegatedFrozenV2(owner, corepb.ResourceCode_BANDWIDTH, 100*trxPrecisionTest)
	statedb.AddAcquiredDelegatedFrozenV2(receiver, corepb.ResourceCode_BANDWIDTH, 100*trxPrecisionTest)
	if err := statedb.WriteDelegatedResourceV2(owner, receiver, false, &rawdb.DelegatedResource{
		From:                      owner,
		To:                        receiver,
		FrozenBalanceForBandwidth: 100 * trxPrecisionTest,
	}); err != nil {
		t.Fatal(err)
	}

	// Receiver has used 600 bandwidth.
	statedb.SetNetUsage(receiver, 600)
	statedb.SetLatestConsumeTime(receiver, 5)

	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress:    owner.Bytes(),
		ReceiverAddress: receiver.Bytes(),
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         40 * trxPrecisionTest,
	}
	any, _ := anypb.New(c)
	tx := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_UnDelegateResourceContract,
				Parameter: any,
			}},
		},
	})

	act := &UnDelegateResourceActuator{}
	ctx := &Context{
		State:         statedb,
		DynProps:      dp,
		Tx:            tx,
		BlockTime:     5_000, // same time — no decay
		PrevBlockTime: 5_000,
		HeadSlot:      5,
		HasHeadSlot:   true,
		BlockNumber:   100,
	}

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}

	// Receiver usage: 600 × 40/100 = 240 transfer → remaining 360.
	if got := statedb.GetNetUsage(receiver); got != 360 {
		t.Fatalf("receiver usage: got %d, want 360", got)
	}
	// Owner usage: 0 + 240 = 240.
	if got := statedb.GetNetUsage(owner); got != 240 {
		t.Fatalf("owner usage: got %d, want 240", got)
	}

	// Balances rebalanced.
	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH); got != 40*trxPrecisionTest {
		t.Fatalf("owner frozen_v2: got %d, want %d", got, 40*trxPrecisionTest)
	}
	if got := statedb.GetDelegatedFrozenV2(owner, corepb.ResourceCode_BANDWIDTH); got != 60*trxPrecisionTest {
		t.Fatalf("owner delegated_v2: got %d, want %d", got, 60*trxPrecisionTest)
	}
	recvAcct := statedb.GetAccount(receiver)
	if got := recvAcct.AcquiredDelegatedFrozenV2BalanceForBandwidth(); got != 60*trxPrecisionTest {
		t.Fatalf("receiver acquired: got %d, want %d", got, 60*trxPrecisionTest)
	}

	// Delegation record updated.
	dr := statedb.ReadDelegatedResource(owner, receiver)
	if dr == nil || dr.FrozenBalanceForBandwidth != 60*trxPrecisionTest {
		t.Fatalf("delegation record frozen: got %+v", dr)
	}
}

func TestRecoverUsageWindow_EdgeCases(t *testing.T) {
	// Non-positive old usage → 0.
	if got := delegation.RecoverUsageWindow(0, 0, 1000); got != 0 {
		t.Fatalf("zero: got %d", got)
	}
	// Elapsed ≥ window → fully decayed.
	if got := delegation.RecoverUsageWindow(500, 0, int64(params.WindowSizeSlots)); got != 0 {
		t.Fatalf("full window elapsed: got %d", got)
	}
	// Elapsed <= 0 → unchanged.
	if got := delegation.RecoverUsageWindow(500, 2000, 1000); got != 500 {
		t.Fatalf("negative elapsed: got %d", got)
	}
	// Mid-window: half decay.
	if got := delegation.RecoverUsageWindow(1000, 0, int64(params.WindowSizeSlots/2)); got != 500 {
		t.Fatalf("half decay: got %d", got)
	}
}
