package vm

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/delegation"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestDelegateResourceUsesActuatorF1NotPrecompileF2 pins the Stake-2.0 dual-impl
// fix for DELEGATERESOURCE (0xDE): java DelegateResourceProcessor.validate uses the
// SAME usage→balance computation as DelegateResourceActuator.validate —
// Bandwidth/EnergyProcessor recover + netUsage = usage*TRX_PRECISION*((double)
// weight/limit) (F1) — NOT the getDelegatableResource precompile's RepositoryImpl
// recover + usage*weight/limit*TRX_PRECISION (F2). go's opDelegateResource wrongly
// gated on delegatableFrozenV2 (F2). The two float evaluation orders diverge once
// usage*weight exceeds 2^53, flipping the accept/reject boundary.
//
// Here usage*weight = 5e9*9e10 > 2^53 makes the available balance differ by 2
// between F1 and F2; an amount in that 2-unit window must follow F1 (the actuator
// helper), not F2 (the precompile helper).
func TestDelegateResourceUsesActuatorF1NotPrecompileF2(t *testing.T) {
	tvm, statedb, dp := newStakeParityTVM(t)
	owner := stakeAddr(0x91)
	receiver := stakeAddr(0x92)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 1_000_000_000_000_000_000) // 1e18
	statedb.SetNetUsage(owner, 5_000_000_000)
	statedb.SetLatestConsumeTime(owner, 0)
	dp.Set("total_net_limit", 43_200_000_000)
	dp.SetTotalNetWeight(90_000_000_000)

	availF1 := delegation.AvailableFrozenV2ForDelegation(statedb, stakingDynamicProperties(tvm), owner, corepb.ResourceCode_BANDWIDTH, stakingNowSlot(tvm))
	availF2 := delegatableFrozenV2(tvm, owner, corepb.ResourceCode_BANDWIDTH)
	if availF1 == availF2 {
		t.Fatalf("setup must make F1 != F2 (got both %d); the boundary test is meaningless otherwise", availF1)
	}

	// amount strictly between the two: the helper with the LARGER available accepts,
	// the smaller rejects — so F1 and F2 disagree.
	lo := availF1
	if availF2 < lo {
		lo = availF2
	}
	amount := lo + 1

	wantAcceptF1 := amount <= availF1 // java validate (F1) accepts iff amount <= availF1
	acceptF2 := amount <= availF2
	if wantAcceptF1 == acceptF2 {
		t.Fatalf("not discriminating: amount=%d availF1=%d availF2=%d", amount, availF1, availF2)
	}

	ret := callDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_BANDWIDTH, amount)
	if got := ret == 1; got != wantAcceptF1 {
		t.Fatalf("opDelegateResource used the wrong (F2/precompile) helper: amount=%d availF1=%d availF2=%d got accept=%v, want %v (F1/actuator)",
			amount, availF1, availF2, got, wantAcceptF1)
	}
}
