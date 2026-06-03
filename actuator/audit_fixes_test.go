package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
)

// TestCalcAccountEnergyLimit_VMPathIsV1Truncate is the regression test for the
// cross-impl audit finding: the IN-VM energy-limit duplicate in java-tron
// (RepositoryImpl.calculateGlobalEnergyLimit) is UNCONDITIONALLY V1
// integer-truncated plain-float — no supportUnfreezeDelay (V2 fractional) branch
// and no harden branch. go-tron's calcAccountEnergyLimit previously took the V2
// fractional branch when unfreeze_delay_days>0 (Stake 2.0, live on Nile), which
// over-grants the in-VM energy limit for non-whole-TRX stakes and can flip
// OUT_OF_ENERGY -> SUCCESS. With frozen=10000.5 TRX, totalLimit=9e10,
// totalWeight=3e9: V1-truncate weight=10000 -> 300000; the old V2 fractional
// (weight=10000.5) gave 300015.
func TestCalcAccountEnergyLimit_VMPathIsV1Truncate(t *testing.T) {
	sdb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetTotalEnergyWeight(3_000_000_000)         // 3e9
	dp.SetTotalEnergyCurrentLimit(90_000_000_000)  // 9e10
	dp.SetUnfreezeDelayDays(14)                    // Stake 2.0 active: pre-fix took the V2 branch

	addr := makeTestAddr(30)
	seedAccount(sdb, addr, 1)
	sdb.FreezeV1Energy(addr, 10_000_500_000, 0) // 10000.5 TRX frozen for energy
	acct := sdb.GetAccount(addr)

	if got := calcAccountEnergyLimit(acct, dp); got != 300000 {
		t.Errorf("in-VM energy limit: got %d, want 300000 (V1-truncate; pre-fix V2 fractional gave 300015)", got)
	}
}
