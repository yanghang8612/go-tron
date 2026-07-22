package actuator

import (
	"math/big"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestWithdrawReward_SkipsWhenChangeDelegationOff(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	addr := makeTestAddr(0x10)
	seedAccount(statedb, addr, 0)

	// ChangeDelegation is false — should be a no-op.
	withdrawReward(db, statedb, dp, addr)

	if got := statedb.GetAllowance(addr); got != 0 {
		t.Fatalf("allowance: got %d, want 0", got)
	}
}

func TestWithdrawReward_SettlesPendingReward(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetChangeDelegation(true)
	dp.SetNewRewardAlgorithmEffectiveCycle(0)
	dp.SetCurrentCycleNumber(10)

	voter := makeTestAddr(0x20)
	witness := makeTestAddr(0x30)
	seedAccount(statedb, voter, 0)

	// Voter currently votes 100 for witness.
	statedb.SetVotes(voter, []*corepb.Vote{
		{VoteAddress: witness.Bytes(), VoteCount: 100},
	})

	// Set up VI spread: VI[0] = 0, VI[9] = 1e18 (delta applies to cycles [1, 10)).
	_ = statedb.WriteWitnessVI(0, witness.Bytes(), new(big.Int))
	_ = statedb.WriteWitnessVI(9, witness.Bytes(), reward.DecimalOfViReward)

	// beginCycle defaults to 0, endCycle defaults to RewardRemark (-1),
	// so the edge-case path is NOT triggered; we fall straight to the
	// currentVotes path with beginCycle=1 (after the defaults-as-zero
	// bump) … actually, beginCycle=0, currentVotes path runs
	// ComputeVoterReward(0, 10).
	withdrawReward(db, statedb, dp, voter)

	// Expected reward: delta (1e18) × 100 / 1e18 = 100.
	if got := statedb.GetAllowance(voter); got != 100 {
		t.Fatalf("settled allowance: got %d, want 100", got)
	}

	// Cursors updated: begin = 10, end = 11.
	if got := statedb.ReadBeginCycle(voter.Bytes()); got != 10 {
		t.Fatalf("beginCycle: got %d, want 10", got)
	}
	if got := statedb.ReadEndCycle(voter.Bytes()); got != 11 {
		t.Fatalf("endCycle: got %d, want 11", got)
	}

	// java-tron snapshots the detached account capsule loaded before reward
	// settlement. The live account has the new allowance, but account-vote must
	// retain the pre-settlement value.
	var snapshot corepb.Account
	if err := proto.Unmarshal(statedb.ReadCycleAccountVote(10, voter.Bytes()), &snapshot); err != nil {
		t.Fatal(err)
	}
	if got := snapshot.GetAllowance(); got != 0 {
		t.Fatalf("snapshot allowance: got %d, want pre-settlement value 0", got)
	}
}

func TestWithdrawReward_NoVotesResetsBegin(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetChangeDelegation(true)
	dp.SetCurrentCycleNumber(5)

	voter := makeTestAddr(0x20)
	seedAccount(statedb, voter, 0)
	// No votes on account.

	withdrawReward(db, statedb, dp, voter)

	// beginCycle should be bumped to currentCycle+1 = 6.
	if got := statedb.ReadBeginCycle(voter.Bytes()); got != 6 {
		t.Fatalf("beginCycle: got %d, want 6", got)
	}
}

func TestWithdrawReward_SkipsWhenBeginInFuture(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetChangeDelegation(true)
	dp.SetCurrentCycleNumber(3)

	voter := makeTestAddr(0x20)
	seedAccount(statedb, voter, 0)
	// Voter's beginCycle is ahead of currentCycle — no-op.
	_ = statedb.WriteBeginCycle(voter.Bytes(), 10)

	withdrawReward(db, statedb, dp, voter)

	if got := statedb.GetAllowance(voter); got != 0 {
		t.Fatalf("allowance: got %d, want 0", got)
	}
	if got := statedb.ReadBeginCycle(voter.Bytes()); got != 10 {
		t.Fatalf("beginCycle should not change: got %d, want 10", got)
	}
}

func TestQueryReward_IncludesPendingAndAllowance(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetChangeDelegation(true)
	dp.SetNewRewardAlgorithmEffectiveCycle(0)
	dp.SetCurrentCycleNumber(10)

	voter := makeTestAddr(0x20)
	witness := makeTestAddr(0x30)
	seedAccount(statedb, voter, 0)
	statedb.SetAllowance(voter, 50)
	statedb.SetVotes(voter, []*corepb.Vote{
		{VoteAddress: witness.Bytes(), VoteCount: 100},
	})

	_ = statedb.WriteWitnessVI(0, witness.Bytes(), new(big.Int))
	_ = statedb.WriteWitnessVI(9, witness.Bytes(), reward.DecimalOfViReward)

	got := queryReward(db, statedb, dp, voter)

	// 100 pending + 50 allowance = 150.
	if got != 150 {
		t.Fatalf("query: got %d, want 150", got)
	}

	// queryReward MUST NOT mutate state.
	if got := statedb.GetAllowance(voter); got != 50 {
		t.Fatalf("allowance mutated: got %d, want 50", got)
	}
	if got := statedb.ReadBeginCycle(voter.Bytes()); got != 0 {
		t.Fatalf("cursor mutated: got %d, want 0", got)
	}
}

func TestQueryReward_SkipsWhenFlagOff(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	voter := makeTestAddr(0x20)
	seedAccount(statedb, voter, 0)
	statedb.SetAllowance(voter, 100)

	got := queryReward(db, statedb, dp, voter)

	// ChangeDelegation off → query returns 0 even if allowance exists.
	if got != 0 {
		t.Fatalf("flag-off query: got %d, want 0", got)
	}
}
