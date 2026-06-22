package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

func TestProcessProposals_Approved(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dynProps := state.NewDynamicProperties()

	// Create proposal to change witness_pay_per_block (ID 5) to 32000000
	p := &rawdb.Proposal{
		ID:             0,
		Proposer:       tcommon.Address{0x41, 0x01},
		Parameters:     map[int64]int64{5: 32000000},
		CreateTime:     1000,
		ExpirationTime: 2000,
		Approvals: []tcommon.Address{
			{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03},
		},
		State: rawdb.ProposalStatePending,
	}
	statedb.WriteProposal(0, p)
	statedb.WriteProposalIndex([]int64{0})

	// 3 approvals out of 4 SRs = 75% >= 70%
	active := []tcommon.Address{{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03}, {0x41, 0x04}}
	if err := ProcessProposals(db, statedb, dynProps, active, 3000, nil, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := statedb.ReadProposal(0)
	if got.State != rawdb.ProposalStateApproved {
		t.Fatalf("expected APPROVED, got %d", got.State)
	}
	if dynProps.WitnessPayPerBlock() != 32000000 {
		t.Fatalf("parameter not applied: %d", dynProps.WitnessPayPerBlock())
	}
}

func TestProcessProposals_Canceled(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dynProps := state.NewDynamicProperties()

	p := &rawdb.Proposal{
		ID:             0,
		Parameters:     map[int64]int64{5: 32000000},
		ExpirationTime: 2000,
		Approvals:      []tcommon.Address{{0x41, 0x01}}, // 1 of 4 = 25%
		State:          rawdb.ProposalStatePending,
	}
	statedb.WriteProposal(0, p)
	statedb.WriteProposalIndex([]int64{0})

	active4 := []tcommon.Address{{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03}, {0x41, 0x04}}
	if err := ProcessProposals(db, statedb, dynProps, active4, 3000, nil, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := statedb.ReadProposal(0)
	if got.State != rawdb.ProposalStateCanceled {
		t.Fatalf("expected CANCELED, got %d", got.State)
	}
	// Parameter should NOT have changed (mainnet default is 32000000).
	if dynProps.WitnessPayPerBlock() != 32000000 {
		t.Fatalf("parameter should not change: %d", dynProps.WitnessPayPerBlock())
	}
}

func TestProcessProposals_NotExpired(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dynProps := state.NewDynamicProperties()

	p := &rawdb.Proposal{
		ID:             0,
		Parameters:     map[int64]int64{5: 32000000},
		ExpirationTime: 9999999,
		Approvals:      []tcommon.Address{{0x41, 0x01}},
		State:          rawdb.ProposalStatePending,
	}
	statedb.WriteProposal(0, p)
	statedb.WriteProposalIndex([]int64{0})

	if err := ProcessProposals(db, statedb, dynProps, []tcommon.Address{{0x41, 0x01}}, 1000, nil, nil); err != nil { // maintenance time < expiration
		t.Fatalf("unexpected error: %v", err)
	}

	got := statedb.ReadProposal(0)
	if got.State != rawdb.ProposalStatePending {
		t.Fatalf("expected still PENDING, got %d", got.State)
	}
}

func TestApplyProposalSideEffects_PriceHistory(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()

	runProposal := func(paramID, value, expirationTime int64, active []tcommon.Address) *state.DynamicProperties {
		t.Helper()
		statedb := newTestStateDB(t)
		dp := state.NewDynamicProperties()
		p := &rawdb.Proposal{
			Parameters:     map[int64]int64{paramID: value},
			ExpirationTime: expirationTime,
			Approvals:      active,
			State:          rawdb.ProposalStatePending,
		}
		statedb.WriteProposal(p.ID, p)
		statedb.WriteProposalIndex([]int64{p.ID})
		if err := ProcessProposals(db, statedb, dp, active, expirationTime+1, nil, nil); err != nil {
			t.Fatalf("ProcessProposals: %v", err)
		}
		return dp
	}

	active := []tcommon.Address{{0x41, 0x01}, {0x41, 0x02}}

	// Proposal #3 (TRANSACTION_FEE) appends to bandwidth_price_history
	dp := runProposal(3, 20, 5000, active)
	want := "0:10,5000:20"
	if got := dp.BandwidthPriceHistory(); got != want {
		t.Errorf("BandwidthPriceHistory after proposal #3: got %q, want %q", got, want)
	}

	// Proposal #11 (ENERGY_FEE) appends to energy_price_history
	dp = runProposal(11, 200, 6000, active)
	want = "0:100,6000:200"
	if got := dp.EnergyPriceHistory(); got != want {
		t.Errorf("EnergyPriceHistory after proposal #11: got %q, want %q", got, want)
	}

	// Proposal #68 (MEMO_FEE) appends to memo_fee_history
	dp = runProposal(68, 100, 7000, active)
	want = "0:0,7000:100"
	if got := dp.MemoFeeHistory(); got != want {
		t.Errorf("MemoFeeHistory after proposal #68: got %q, want %q", got, want)
	}
}

// TestProcessProposals_SameCycleDescendingOrder locks the L3 fix: two proposals
// touching the same DP key that expire in the SAME maintenance cycle must be
// applied in DESCENDING id order, matching java ProposalController (latestProposalNum
// down to 1). So the LOW-id proposal wins the final value (last write), and the
// *_price_history string appends high-id-first. Pre-fix go iterated ascending,
// flipping both the committed fee value and the history bytes.
func TestProcessProposals_SameCycleDescendingOrder(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()
	active := []tcommon.Address{{0x41, 0x01}, {0x41, 0x02}}

	// Two ENERGY_FEE (#11) proposals expiring in the same cycle, different values.
	p0 := &rawdb.Proposal{ID: 0, Parameters: map[int64]int64{11: 200}, ExpirationTime: 5000, Approvals: active, State: rawdb.ProposalStatePending}
	p1 := &rawdb.Proposal{ID: 1, Parameters: map[int64]int64{11: 300}, ExpirationTime: 5000, Approvals: active, State: rawdb.ProposalStatePending}
	statedb.WriteProposal(0, p0)
	statedb.WriteProposal(1, p1)
	statedb.WriteProposalIndex([]int64{0, 1})

	if err := ProcessProposals(db, statedb, dp, active, 5001, nil, nil); err != nil {
		t.Fatalf("ProcessProposals: %v", err)
	}

	// Descending: apply id=1 (300) then id=0 (200) -> low id wins.
	if got := dp.EnergyFee(); got != 200 {
		t.Fatalf("energy_fee: got %d, want 200 (low id wins under descending order)", got)
	}
	// History appends high-id-first: "<default>,5000:300,5000:200".
	want := "0:100,5000:300,5000:200"
	if got := dp.EnergyPriceHistory(); got != want {
		t.Fatalf("energy_price_history: got %q, want %q", got, want)
	}
}

// TestApplyProposalSideEffects_AddSystemContract verifies that the proposals
// that gate new system contracts (java-tron addSystemContractAndSetPermission
// call sites) update both AvailableContractType and ActiveDefaultOperations.
func TestApplyProposalSideEffects_AddSystemContract(t *testing.T) {
	active := []tcommon.Address{{0x41, 0x01}, {0x41, 0x02}}

	runProposal := func(paramID, value, expirationTime int64) *state.DynamicProperties {
		t.Helper()
		db := ethrawdb.NewMemoryDatabase()
		statedb := newTestStateDB(t)
		dp := state.NewDynamicProperties()
		p := &rawdb.Proposal{
			Parameters:     map[int64]int64{paramID: value},
			ExpirationTime: expirationTime,
			Approvals:      active,
			State:          rawdb.ProposalStatePending,
		}
		statedb.WriteProposal(p.ID, p)
		statedb.WriteProposalIndex([]int64{p.ID})
		if err := ProcessProposals(db, statedb, dp, active, expirationTime+1, nil, nil); err != nil {
			t.Fatalf("ProcessProposals: %v", err)
		}
		return dp
	}

	check := func(t *testing.T, dp *state.DynamicProperties, ids []int) {
		t.Helper()
		avail := dp.AvailableContractType()
		active := dp.ActiveDefaultOperations()
		for _, id := range ids {
			if avail[id/8]&(1<<(id%8)) == 0 {
				t.Errorf("AvailableContractType: bit %d not set", id)
			}
			if active[id/8]&(1<<(id%8)) == 0 {
				t.Errorf("ActiveDefaultOperations: bit %d not set", id)
			}
		}
	}

	// Proposal 26 (ALLOW_TVM_CONSTANTINOPLE) → bit 48
	check(t, runProposal(26, 1, 1000), []int{48})
	// Proposal 27 (ALLOW_SHIELDED_TRANSACTION) → bit 51 on Nile's historical activation.
	dp := runProposal(27, 1, 1000)
	check(t, dp, []int{51})
	if !dp.AllowShieldedTransaction() {
		t.Fatal("allow_shielded_transaction should be set after proposal 27")
	}
	// Proposal 30 (ALLOW_CHANGE_DELEGATION) → bit 49
	check(t, runProposal(30, 1, 1000), []int{49})
	// Proposal 44 (ALLOW_MARKET_TRANSACTION) → bits 52, 53
	check(t, runProposal(44, 1, 1000), []int{52, 53})
	// Proposal 70 (UNFREEZE_DELAY_DAYS) → bits 54-58
	check(t, runProposal(70, 86400, 1000), []int{54, 55, 56, 57, 58})
	// Proposal 77 (ALLOW_CANCEL_ALL_UNFREEZE_V2) → bit 59
	check(t, runProposal(77, 1, 1000), []int{59})
}
