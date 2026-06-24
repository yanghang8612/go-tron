package vm

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestQueryPrecompilesRejectHighByteOperands pins java DataWord.longValueSafe for
// the getChainParameter (0x0b) param-code and the resourceV2 (0x10) resource-type
// operands: a word with any high byte set saturates to maxInt64 and is rejected,
// NOT truncated to its low bytes. go previously read the low 8 bytes, so a
// high-byte word whose low bytes matched a valid code/type returned a live value
// where java returns 0.
func TestQueryPrecompilesRejectHighByteOperands(t *testing.T) {
	tvm, statedb, dp := newStakeParityTVM(t)
	dp.Set("total_net_limit", 43_200_000_000)
	acct := stakeAddr(0xA1)
	statedb.CreateAccount(acct, corepb.AccountType_Normal)
	statedb.AddFreezeV2(acct, corepb.ResourceCode_BANDWIDTH, 100*tvmTRXPrecision)

	// getChainParameter(0x0b): clean code 1 → TOTAL_NET_LIMIT; high-byte code → 0.
	if out, _, err := (&getChainParameter{}).Run(tvm, tcommon.Address{}, int64ToBytes32(1), 50); err != nil || int64FromWord(out) != 43_200_000_000 {
		t.Fatalf("getChainParameter(1): got %d err=%v, want 43200000000", int64FromWord(out), err)
	}
	badCode := int64ToBytes32(1)
	badCode[0] = 0xff // high byte set, low-8 still == 1
	if out, _, _ := (&getChainParameter{}).Run(tvm, tcommon.Address{}, badCode, 50); int64FromWord(out) != 0 {
		t.Fatalf("getChainParameter(high-byte code): got %d, want 0 (longValueSafe→INVALID)", int64FromWord(out))
	}

	// resourceV2(0x10), from==target: clean type 0 → frozenV2 balance; high-byte type → 0.
	mkRV2 := func(typeWord func([]byte)) []byte {
		in := make([]byte, 96)
		copy(in[12:32], acct[1:]) // target
		copy(in[44:64], acct[1:]) // from == target
		typeWord(in[64:96])
		return in
	}
	if out, _, _ := (&resourceV2{}).Run(tvm, tcommon.Address{}, mkRV2(func(w []byte) {}), 50); int64FromWord(out) != 100*tvmTRXPrecision {
		t.Fatalf("resourceV2(type 0): got %d, want %d", int64FromWord(out), 100*tvmTRXPrecision)
	}
	if out, _, _ := (&resourceV2{}).Run(tvm, tcommon.Address{}, mkRV2(func(w []byte) { w[0] = 0xff }), 50); int64FromWord(out) != 0 {
		t.Fatalf("resourceV2(high-byte type): got %d, want 0 (longValueSafe rejects)", int64FromWord(out))
	}
}

// Bug 1 — getChainParameter must return TOTAL_NET_WEIGHT (code 2) and
// TOTAL_ENERGY_WEIGHT (code 4); go previously handled only 1/3/5 and returned 0.
// java ChainParameterEnum maps 2->getTotalNetWeight, 4->getTotalEnergyWeight.
func TestGetChainParameter_NetAndEnergyWeight(t *testing.T) {
	tvm, _, dp := newProductionWiredTVM(t)
	dp.SetTotalNetWeight(8_739_651_802)
	dp.SetTotalEnergyWeight(331_697_663)
	for _, tc := range []struct {
		code, want int64
	}{{2, 8_739_651_802}, {4, 331_697_663}} {
		out, _, err := (&getChainParameter{}).Run(tvm, tcommon.Address{}, int64ToBytes32(tc.code), 50)
		if err != nil {
			t.Fatalf("code %d: %v", tc.code, err)
		}
		if got := int64FromWord(out); got != tc.want {
			t.Fatalf("getChainParameter(%d) = %d, want %d", tc.code, got, tc.want)
		}
	}
}

// Bug 3 — parseInt64SafeFromWord saturates like java DataWord.longValueSafe().
func TestParseInt64SafeFromWord(t *testing.T) {
	const maxInt64 = int64(^uint64(0) >> 1)
	mk := func(set func([]byte)) []byte { w := make([]byte, 32); set(w); return w }
	cases := []struct {
		name string
		w    []byte
		want int64
	}{
		{"small", mk(func(w []byte) { w[31] = 5 }), 5},
		{"exact_maxint64", mk(func(w []byte) {
			for i := 24; i < 32; i++ {
				w[i] = 0xff
			}
			w[24] = 0x7f
		}), maxInt64},
		{"high24_set", mk(func(w []byte) { w[0] = 1; w[31] = 5 }), maxInt64},
		{"low8_high_bit", mk(func(w []byte) { w[24] = 0x80 }), maxInt64},
	}
	for _, c := range cases {
		if got := parseInt64SafeFromWord(c.w, 0); got != c.want {
			t.Fatalf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

// Bug 4 — DELEGATE/UNDELEGATE must reject any resourceType except
// BANDWIDTH/ENERGY (java *Processor.validate switch default throws); go could
// previously delegate TRON_POWER.
func TestDelegateUndelegateRejectNonBandwidthEnergy(t *testing.T) {
	tvm, statedb, _ := newStakeParityTVM(t)
	owner := stakeAddr(0x41)
	recv := stakeAddr(0x42)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(recv, corepb.AccountType_Normal)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 100*tvmTRXPrecision)

	if ret := callDelegateResource(t, tvm, owner, recv, corepb.ResourceCode_TRON_POWER, 40*tvmTRXPrecision); ret != 0 {
		t.Fatalf("delegate TRON_POWER: got %d, want 0", ret)
	}
	if ret := callUnDelegateResource(t, tvm, owner, recv, corepb.ResourceCode_TRON_POWER, 40*tvmTRXPrecision); ret != 0 {
		t.Fatalf("undelegate TRON_POWER: got %d, want 0", ret)
	}
	// Sanity: ENERGY still delegates (guard does not over-reject).
	if ret := callDelegateResource(t, tvm, owner, recv, corepb.ResourceCode_ENERGY, 40*tvmTRXPrecision); ret != 1 {
		t.Fatalf("delegate ENERGY: got %d, want 1", ret)
	}
}

// Bug 2 — opWithdrawReward must settle the cycle bookkeeping (advance beginCycle)
// even when the net withdrawable is 0; java Program.withdrawReward commits the
// execute() unconditionally. go previously early-returned and left beginCycle stale.
func TestWithdrawRewardSettlesBookkeepingWhenZero(t *testing.T) {
	tvm, statedb, _ := newVoteRewardTVM(t) // currentCycle = 10
	caller := voteRewardAddr(0x31)
	witness := voteRewardAddr(0x32)
	statedb.CreateAccount(caller, corepb.AccountType_Normal)
	statedb.SetVotes(caller, []*corepb.Vote{{VoteAddress: witness.Bytes(), VoteCount: 100}})
	_ = statedb.WriteBeginCycle(caller.Bytes(), 1) // 1 < currentCycle 10
	// No witness VI seeded and allowance 0 -> withdrawable == 0.
	statedb.AddBalance(caller, 1000)

	stack := newStack()
	contract := NewContract(caller, caller, 0, 100000)
	if _, err := opWithdrawReward(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opWithdrawReward: %v", err)
	}
	ret := stack.pop()
	if got := ret.Uint64(); got != 0 {
		t.Fatalf("withdraw amount on 0 reward: got %d, want 0", got)
	}
	if got := statedb.GetBalance(caller); got != 1000 {
		t.Fatalf("balance mutated on 0 reward: got %d, want 1000", got)
	}
	if got := statedb.ReadBeginCycle(caller.Bytes()); got != 10 {
		t.Fatalf("beginCycle not advanced on 0 reward: got %d, want 10 (settle must run unconditionally)", got)
	}
}
