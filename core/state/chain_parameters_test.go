package state

import "testing"

// javaWalletChainParameterKeys pins the exact key set and emission order of
// java-tron's Wallet.getChainParameters() at GreatVoyage-v4.8.1, cross-checked
// against a live v4.8.1 Nile node's /wallet/getchainparameters response
// (2026-07-13). The wallet API must emit exactly these keys in this order —
// no snake_case DP keys, no internal counters like latest_block_header_number.
var javaWalletChainParameterKeys = []string{
	"getMaintenanceTimeInterval",
	"getAccountUpgradeCost",
	"getCreateAccountFee",
	"getTransactionFee",
	"getAssetIssueFee",
	"getWitnessPayPerBlock",
	"getWitnessStandbyAllowance",
	"getCreateNewAccountFeeInSystemContract",
	"getCreateNewAccountBandwidthRate",
	"getAllowCreationOfContracts",
	"getRemoveThePowerOfTheGr",
	"getEnergyFee",
	"getExchangeCreateFee",
	"getMaxCpuTimeOfOneTx",
	"getAllowUpdateAccountName",
	"getAllowSameTokenName",
	"getAllowDelegateResource",
	"getTotalEnergyLimit",
	"getAllowTvmTransferTrc10",
	"getTotalEnergyCurrentLimit",
	"getAllowMultiSign",
	"getAllowAdaptiveEnergy",
	"getTotalEnergyTargetLimit",
	"getTotalEnergyAverageUsage",
	"getUpdateAccountPermissionFee",
	"getMultiSignFee",
	"getAllowAccountStateRoot",
	"getAllowProtoFilterNum",
	"getAllowTvmConstantinople",
	"getAllowTvmSolidity059",
	"getAllowTvmIstanbul",
	"getAllowShieldedTRC20Transaction",
	"getForbidTransferToContract",
	"getAdaptiveResourceLimitTargetRatio",
	"getAdaptiveResourceLimitMultiplier",
	"getChangeDelegation",
	"getWitness127PayPerBlock",
	"getAllowMarketTransaction",
	"getMarketSellFee",
	"getMarketCancelFee",
	"getAllowPBFT",
	"getAllowTransactionFeePool",
	"getMaxFeeLimit",
	"getAllowOptimizeBlackHole",
	"getAllowNewResourceModel",
	"getAllowTvmFreeze",
	"getAllowTvmVote",
	"getAllowTvmLondon",
	"getAllowTvmCompatibleEvm",
	"getAllowAccountAssetOptimization",
	"getFreeNetLimit",
	"getTotalNetLimit",
	"getAllowHigherLimitForMaxCpuTimeOfOneTx",
	"getAllowAssetOptimization",
	"getAllowNewReward",
	"getMemoFee",
	"getAllowDelegateOptimization",
	"getUnfreezeDelayDays",
	"getAllowOptimizedReturnValueOfChainId",
	"getAllowDynamicEnergy",
	"getDynamicEnergyThreshold",
	"getDynamicEnergyIncreaseFactor",
	"getDynamicEnergyMaxFactor",
	"getAllowTvmShangHai",
	"getAllowCancelAllUnfreezeV2",
	"getMaxDelegateLockPeriod",
	"getAllowOldRewardOpt",
	"getAllowEnergyAdjustment",
	"getMaxCreateAccountTxSize",
	"getAllowStrictMath",
	"getConsensusLogicOptimization",
	"getAllowTvmCancun",
	"getAllowTvmBlob",
	"getAllowTvmSelfdestructRestriction",
	"getProposalExpireTime",
}

func TestChainParameterKeysPinJavaWalletList(t *testing.T) {
	got := ChainParameterKeys()
	if len(got) != len(javaWalletChainParameterKeys) {
		t.Fatalf("ChainParameterKeys returned %d keys, java Wallet.getChainParameters emits %d",
			len(got), len(javaWalletChainParameterKeys))
	}
	for i, want := range javaWalletChainParameterKeys {
		if got[i] != want {
			t.Fatalf("key %d: got %q, java emits %q", i, got[i], want)
		}
	}
}

func TestChainParameterKeysResolveToDefaults(t *testing.T) {
	dp := NewDynamicProperties()
	for _, javaKey := range ChainParameterKeys() {
		goKey := JavaGetterToGoKey(javaKey)
		if goKey == "" {
			t.Errorf("%s has no go DP key in javaGetterToGoKeyMap", javaKey)
			continue
		}
		if _, ok := dp.Get(goKey); !ok {
			t.Errorf("%s -> %s missing from DynamicProperties defaults", javaKey, goKey)
		}
	}
}

func TestJavaGetterMapExtrasAreDocumented(t *testing.T) {
	// Translation entries java-tron v4.8.1 does NOT emit from
	// Wallet.getChainParameters: the v4.8.2 gates (not yet released on the
	// target networks) and MARKET_QUANTITY_LIMIT (governance-settable but
	// never part of the getchainparameters response).
	wantExtras := map[string]bool{
		"getAllowTvmOsaka":                  true,
		"getAllowTvmPrague":                 true,
		"getAllowHardenResourceCalculation": true,
		"getAllowHardenExchangeCalculation": true,
		"getMarketQuantityLimit":            true,
	}
	emitted := make(map[string]bool, len(javaWalletChainParameterKeys))
	for _, k := range ChainParameterKeys() {
		emitted[k] = true
	}
	for k := range javaGetterToGoKeyMap {
		if emitted[k] {
			continue
		}
		if !wantExtras[k] {
			t.Errorf("map entry %s is neither emitted nor a documented extra; if java's Wallet.getChainParameters now includes it, append it to javaChainParameterOrder", k)
		}
		delete(wantExtras, k)
	}
	for k := range wantExtras {
		t.Errorf("documented extra %s missing from javaGetterToGoKeyMap", k)
	}
}
