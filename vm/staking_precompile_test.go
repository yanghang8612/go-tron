package vm

import (
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func stakingPrecompileAddr(last byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = last
	return addr
}

func stakingAddrWord(addr tcommon.Address) []byte {
	word := make([]byte, 32)
	copy(word[12:], addr[1:])
	return word
}

func stakingInput(words ...[]byte) []byte {
	out := make([]byte, 0, 32*len(words))
	for _, word := range words {
		out = append(out, word...)
	}
	return out
}

func stakingWordAt(out []byte, idx int) int64 {
	return int64FromWord(out[idx*32 : (idx+1)*32])
}

func stakingOpcodeStack(receiver tcommon.Address, resource corepb.ResourceCode, amount *uint256.Int) *Stack {
	receiverWord := addressToUint256(receiver)
	stack := newStack()
	stack.push(&receiverWord)
	stack.push(uint256.NewInt(uint64(resource)))
	stack.push(amount)
	return stack
}

func TestDelegateResourceOpcodeRejectsAmountOutOfLongRange(t *testing.T) {
	tvm, statedb, _ := newTestTVMForCreate(t, TVMConfig{}, nil)
	caller := stakingPrecompileAddr(0x41)
	receiver := stakingPrecompileAddr(0x42)
	statedb.CreateAccount(caller, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.AddFreezeV2(caller, corepb.ResourceCode_BANDWIDTH, 100_000_000)

	var tooLarge uint256.Int
	tooLarge.SetUint64(uint64(1) << 63)
	stack := stakingOpcodeStack(receiver, corepb.ResourceCode_BANDWIDTH, &tooLarge)
	contract := NewContract(caller, caller, 0, 100_000)

	if _, err := opDelegateResource(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opDelegateResource error: %v", err)
	}
	if got := stack.pop(); !got.IsZero() {
		t.Fatalf("delegate result: got %s, want 0", got.String())
	}
	if got := statedb.GetFrozenV2Amount(caller, corepb.ResourceCode_BANDWIDTH); got != 100_000_000 {
		t.Fatalf("caller frozen changed: got %d", got)
	}
	if got := statedb.GetDelegatedFrozenV2(caller, corepb.ResourceCode_BANDWIDTH); got != 0 {
		t.Fatalf("caller delegated changed: got %d", got)
	}
	if got := statedb.GetAccount(receiver).AcquiredDelegatedFrozenV2BalanceForBandwidth(); got != 0 {
		t.Fatalf("receiver acquired changed: got %d", got)
	}
}

func TestUnDelegateResourceOpcodeRejectsAmountOutOfLongRange(t *testing.T) {
	tvm, statedb, _ := newTestTVMForCreate(t, TVMConfig{}, nil)
	caller := stakingPrecompileAddr(0x43)
	receiver := stakingPrecompileAddr(0x44)
	statedb.CreateAccount(caller, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.AddDelegatedFrozenV2(caller, corepb.ResourceCode_BANDWIDTH, 100_000_000)
	statedb.AddAcquiredDelegatedFrozenV2(receiver, corepb.ResourceCode_BANDWIDTH, 100_000_000)

	var tooLarge uint256.Int
	tooLarge.SetUint64(uint64(1) << 63)
	stack := stakingOpcodeStack(receiver, corepb.ResourceCode_BANDWIDTH, &tooLarge)
	contract := NewContract(caller, caller, 0, 100_000)

	if _, err := opUnDelegateResource(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opUnDelegateResource error: %v", err)
	}
	if got := stack.pop(); !got.IsZero() {
		t.Fatalf("undelegate result: got %s, want 0", got.String())
	}
	if got := statedb.GetDelegatedFrozenV2(caller, corepb.ResourceCode_BANDWIDTH); got != 100_000_000 {
		t.Fatalf("caller delegated changed: got %d", got)
	}
	if got := statedb.GetFrozenV2Amount(caller, corepb.ResourceCode_BANDWIDTH); got != 0 {
		t.Fatalf("caller frozen changed: got %d", got)
	}
	if got := statedb.GetAccount(receiver).AcquiredDelegatedFrozenV2BalanceForBandwidth(); got != 100_000_000 {
		t.Fatalf("receiver acquired changed: got %d", got)
	}
}

func TestStakingV2AvailableUnfreezeCountsOnlyUnexpired(t *testing.T) {
	tvm, statedb, dp := newVoteRewardTVM(t)
	dp.SetLatestBlockHeaderTimestamp(3000)
	addr := stakingPrecompileAddr(0x01)
	statedb.CreateAccount(addr, corepb.AccountType_Normal)
	statedb.AddUnfreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 1, 1000)
	statedb.AddUnfreezeV2(addr, corepb.ResourceCode_ENERGY, 1, 5000)

	out, cost, err := (&availableUnfreezeV2Size{}).Run(tvm, zeroCaller, stakingAddrWord(addr), 50)
	if err != nil {
		t.Fatalf("availableUnfreezeV2Size error: %v", err)
	}
	if cost != 50 {
		t.Fatalf("cost: got %d, want 50", cost)
	}
	if got := int64FromWord(out); got != 31 {
		t.Fatalf("available unfreeze count: got %d, want 31", got)
	}

	missing := stakingPrecompileAddr(0x02)
	out, _, err = (&availableUnfreezeV2Size{}).Run(tvm, zeroCaller, stakingAddrWord(missing), 50)
	if err != nil {
		t.Fatalf("missing account availableUnfreezeV2Size error: %v", err)
	}
	if got := int64FromWord(out); got != 0 {
		t.Fatalf("missing account available unfreeze count: got %d, want 0", got)
	}
}

func TestStakingV2ResourceV2ReadsDelegatedPairBuckets(t *testing.T) {
	tvm, db, _ := newTestTVMWithDB(t)
	statedb := tvm.StateDB
	from := stakingPrecompileAddr(0x11)
	target := stakingPrecompileAddr(0x12)
	statedb.CreateAccount(from, corepb.AccountType_Normal)
	statedb.AddFreezeV2(from, corepb.ResourceCode_TRON_POWER, 99)

	if err := rawdb.WriteDelegatedResourceV2(db, from, target, false, &rawdb.DelegatedResource{
		From:                      from,
		To:                        target,
		FrozenBalanceForBandwidth: 7,
		FrozenBalanceForEnergy:    11,
	}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteDelegatedResourceV2(db, from, target, true, &rawdb.DelegatedResource{
		From:                      from,
		To:                        target,
		FrozenBalanceForBandwidth: 13,
		FrozenBalanceForEnergy:    17,
	}); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		typ  int64
		want int64
	}{
		{name: "bandwidth", typ: 0, want: 20},
		{name: "energy", typ: 1, want: 28},
		{name: "invalid-cross-type", typ: 2, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := stakingInput(stakingAddrWord(target), stakingAddrWord(from), int64ToBytes32(tc.typ))
			out, _, err := (&resourceV2{}).Run(tvm, zeroCaller, input, 50)
			if err != nil {
				t.Fatalf("resourceV2 error: %v", err)
			}
			if got := int64FromWord(out); got != tc.want {
				t.Fatalf("resourceV2 %s: got %d, want %d", tc.name, got, tc.want)
			}
		})
	}

	selfInput := stakingInput(stakingAddrWord(from), stakingAddrWord(from), int64ToBytes32(2))
	out, _, err := (&resourceV2{}).Run(tvm, zeroCaller, selfInput, 50)
	if err != nil {
		t.Fatalf("resourceV2 self error: %v", err)
	}
	if got := int64FromWord(out); got != 99 {
		t.Fatalf("resourceV2 self TRON_POWER: got %d, want 99", got)
	}
}

func TestStakingV2ResourceUsageDelegatableAndCheckUndelegate(t *testing.T) {
	tvm, statedb, dp := newVoteRewardTVM(t)
	dp.SetLatestBlockHeaderTimestamp(3000)
	dp.Set("total_net_limit", 1000)
	dp.SetTotalNetWeight(100)

	addr := stakingPrecompileAddr(0x21)
	statedb.CreateAccount(addr, corepb.AccountType_Normal)
	statedb.FreezeV1Bandwidth(addr, 100_000_000, 0)
	statedb.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 100_000_000)
	statedb.SetNetUsage(addr, 200)
	statedb.SetLatestConsumeTime(addr, 1)

	usageInput := stakingInput(stakingAddrWord(addr), int64ToBytes32(0))
	out, _, err := (&resourceUsage{}).Run(tvm, zeroCaller, usageInput, 50)
	if err != nil {
		t.Fatalf("resourceUsage error: %v", err)
	}
	if got := stakingWordAt(out, 0); got != 20_000_000 {
		t.Fatalf("usage balance: got %d, want 20000000", got)
	}
	if got := stakingWordAt(out, 1); got != 86_400 {
		t.Fatalf("restore seconds: got %d, want 86400", got)
	}

	delegatableInput := stakingInput(stakingAddrWord(addr), int64ToBytes32(0))
	out, _, err = (&delegatableResource{}).Run(tvm, zeroCaller, delegatableInput, 50)
	if err != nil {
		t.Fatalf("delegatableResource error: %v", err)
	}
	if got := int64FromWord(out); got != 100_000_000 {
		t.Fatalf("delegatable resource: got %d, want 100000000", got)
	}

	checkInput := stakingInput(stakingAddrWord(addr), int64ToBytes32(50_000_000), int64ToBytes32(0))
	out, _, err = (&checkUnDelegateResource{}).Run(tvm, zeroCaller, checkInput, 50)
	if err != nil {
		t.Fatalf("checkUnDelegateResource error: %v", err)
	}
	if got := stakingWordAt(out, 0); got != 45_000_000 {
		t.Fatalf("clean undelegatable balance: got %d, want 45000000", got)
	}
	if got := stakingWordAt(out, 1); got != 5_000_000 {
		t.Fatalf("locked undelegatable balance: got %d, want 5000000", got)
	}
	if got := stakingWordAt(out, 2); got != 86_400 {
		t.Fatalf("check restore seconds: got %d, want 86400", got)
	}
}

func TestStakingV2TotalResourceIncludesV1AndV2(t *testing.T) {
	tvm, statedb, _ := newVoteRewardTVM(t)
	receiver := stakingPrecompileAddr(0x31)
	owner := stakingPrecompileAddr(0x32)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)

	statedb.FreezeV1Bandwidth(receiver, 10, 0)
	statedb.AddFreezeV2(receiver, corepb.ResourceCode_BANDWIDTH, 20)
	statedb.AddAcquiredDelegatedFrozenV2(receiver, corepb.ResourceCode_BANDWIDTH, 30)
	statedb.FreezeV1DelegatedBandwidth(owner, receiver, 40)
	statedb.AddDelegatedFrozenV2(owner, corepb.ResourceCode_BANDWIDTH, 50)

	totalInput := stakingInput(stakingAddrWord(receiver), int64ToBytes32(0))
	out, _, err := (&totalResource{}).Run(tvm, zeroCaller, totalInput, 50)
	if err != nil {
		t.Fatalf("totalResource error: %v", err)
	}
	if got := int64FromWord(out); got != 100 {
		t.Fatalf("totalResource: got %d, want 100", got)
	}

	out, _, err = (&totalAcquiredResource{}).Run(tvm, zeroCaller, totalInput, 50)
	if err != nil {
		t.Fatalf("totalAcquiredResource error: %v", err)
	}
	if got := int64FromWord(out); got != 70 {
		t.Fatalf("totalAcquiredResource: got %d, want 70", got)
	}

	delegatedInput := stakingInput(stakingAddrWord(owner), int64ToBytes32(0))
	out, _, err = (&totalDelegatedResource{}).Run(tvm, zeroCaller, delegatedInput, 50)
	if err != nil {
		t.Fatalf("totalDelegatedResource error: %v", err)
	}
	if got := int64FromWord(out); got != 90 {
		t.Fatalf("totalDelegatedResource: got %d, want 90", got)
	}

	invalidInput := stakingInput(stakingAddrWord(receiver), int64ToBytes32(2))
	out, _, err = (&totalResource{}).Run(tvm, zeroCaller, invalidInput, 50)
	if err != nil {
		t.Fatalf("invalid totalResource error: %v", err)
	}
	if got := int64FromWord(out); got != 0 {
		t.Fatalf("invalid totalResource type: got %d, want 0", got)
	}
}
