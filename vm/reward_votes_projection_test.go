package vm

import (
	"math/big"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func rewardProjectionTVM(t *testing.T) (*TVM, *state.StateDB, *state.DynamicProperties, tcommon.Address, tcommon.Address) {
	t.Helper()
	s := freshJumpdestState(t)
	owner, witness := stakeAddr(0xe1), stakeAddr(0xe2)
	s.CreateAccount(owner, corepb.AccountType_Normal)
	s.SetAllowance(owner, 50)
	s.SetVotes(owner, []*corepb.Vote{{VoteAddress: witness.Bytes(), VoteCount: 100}})
	dp := state.NewDynamicProperties()
	dp.SetCurrentCycleNumber(10)
	dp.SetChangeDelegation(true)
	dp.SetNewRewardAlgorithmEffectiveCycle(0)
	tvm := NewTVM(s, dp, owner, 10, 1000000, tcommon.Address{}, 1, TVMConfig{Vote: true})
	return tvm, s, dp, owner, witness
}

func TestTVMRewardSnapshotEmptyPresenceAndMissing(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "present_empty"}[existing], func(t *testing.T) {
			tvm, s, _, owner, _ := rewardProjectionTVM(t)
			if err := s.WriteBeginCycle(owner.Bytes(), 10); err != nil {
				t.Fatal(err)
			}
			if err := s.WriteEndCycle(owner.Bytes(), 11); err != nil {
				t.Fatal(err)
			}
			if existing {
				if err := s.WriteCycleAccountVotes(10, owner.Bytes(), nil); err != nil {
					t.Fatal(err)
				}
			}
			tvmWithdrawReward(tvm, owner)
			votes, present, err := s.ReadCycleAccountVotesStrict(10, owner.Bytes())
			if err != nil || !present {
				t.Fatalf("snapshot presence=%v err=%v", present, err)
			}
			if existing && len(votes) != 0 {
				t.Fatal("present empty current snapshot was confused with missing and overwritten")
			}
			if !existing && (len(votes) != 1 || votes[0].VoteCount != 100) {
				t.Fatal("missing current snapshot did not record the exact current Votes")
			}
		})
	}
}

func TestTVMRewardVotesProjectionSettlementRevertAndOlderCycle(t *testing.T) {
	tvm, s, _, owner, witness := rewardProjectionTVM(t)
	if err := s.WriteBeginCycle(owner.Bytes(), 8); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteEndCycle(owner.Bytes(), 9); err != nil {
		t.Fatal(err)
	}
	oldVotes := []*corepb.Vote{{VoteAddress: witness.Bytes(), VoteCount: 20}, {VoteAddress: witness.Bytes(), VoteCount: 20}}
	oldVotes[0].ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1})
	if err := s.WriteCycleAccountVotes(8, owner.Bytes(), oldVotes); err != nil {
		t.Fatal(err)
	}
	for cycle, value := range map[int64]int64{7: 0, 8: 1, 9: 2} {
		if err := s.WriteWitnessVI(cycle, witness.Bytes(), new(big.Int).Mul(big.NewInt(value), reward.DecimalOfViReward)); err != nil {
			t.Fatal(err)
		}
	}
	if got := tvmQueryReward(tvm, owner); got != 190 {
		t.Fatalf("query reward=%d want=190", got)
	}
	cut := s.Snapshot()
	tvmWithdrawReward(tvm, owner)
	if got := s.GetAllowance(owner); got != 190 {
		t.Fatalf("paid allowance=%d want=190", got)
	}
	if old, present, err := s.ReadCycleAccountVotesStrict(8, owner.Bytes()); err != nil || !present || !proto.Equal(&corepb.Account{Votes: old}, &corepb.Account{Votes: oldVotes}) {
		t.Fatalf("older cycle Votes lost: %v %v %v", old, present, err)
	}
	if now, present, err := s.ReadCycleAccountVotesStrict(10, owner.Bytes()); err != nil || !present || len(now) != 1 || now[0].VoteCount != 100 {
		t.Fatalf("new snapshot mismatch: %v %v %v", now, present, err)
	}
	s.RevertToSnapshot(cut)
	if got := s.GetAllowance(owner); got != 50 {
		t.Fatalf("allowance failed to revert: %d", got)
	}
	if _, present, err := s.ReadCycleAccountVotesStrict(10, owner.Bytes()); err != nil || present {
		t.Fatalf("future cycle snapshot leaked through revert: %v %v", present, err)
	}
}
