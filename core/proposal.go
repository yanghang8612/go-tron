package core

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

const version3_6_5 int32 = 9

// ProcessProposals checks all pending proposals and approves or cancels them
// based on the approval count vs active SR count.
// activeWitnesses is the current active super-representative set; only approvals
// from current witnesses are counted (matches java-tron's hasMostApprovals logic).
//
// The proposal records and their index are read/written through statedb — the
// rooted SystemProposal KV (Phase 3d), java-tron's revoking ProposalStore. db
// remains the Reader+Writer for non-rooted chain/runtime data needed by future
// side effects; mutable state writes go through statedb typed stores.
func ProcessProposals(db kvReadWriter, statedb *state.StateDB, dynProps *state.DynamicProperties, activeWitnesses []tcommon.Address, maintenanceTime int64, fc *forks.ForkController, cache *proposalScanCache) error {
	activeCount := len(activeWitnesses)
	ids := statedb.ReadProposalIndex()
	for _, id := range ids {
		// Already-resolved proposals are immutable on a linear chain (see
		// proposalScanCache): skip the SystemKVGet + JSON decode entirely.
		if cache.isTerminal(id) {
			continue
		}
		p := statedb.ReadProposal(id)
		if p == nil {
			continue
		}
		if p.State != rawdb.ProposalStatePending {
			// Reached terminal outside this scan (e.g. a ProposalDelete tx, or
			// the cold-cache first pass): record it so later boundaries skip it.
			cache.markTerminal(id)
			continue
		}
		if p.ExpirationTime > maintenanceTime {
			continue // not yet expired
		}
		if activeCount == 0 {
			continue // cannot compute threshold with zero active witnesses
		}

		// Count only approvals from currently-active witnesses.
		activeApprovals := 0
		for _, approval := range p.Approvals {
			for _, w := range activeWitnesses {
				if approval == w {
					activeApprovals++
					break
				}
			}
		}
		// 70% threshold: matches java-tron's `count >= activeWitnesses.size() * 7 / 10`
		if activeApprovals >= activeCount*7/10 {
			// Snapshot current values for params that java guards with
			// `if (getX() == 0)` — re-approving such a proposal in java is
			// a no-op, including its side-effects. The snapshot lets us
			// reproduce that semantics without re-querying inside the loop.
			oldValues := snapshotGuardedParams(dynProps, p.Parameters)

			// Apply parameters. Guarded params are skipped here when they
			// are already set; applyProposalSideEffects also reads
			// `oldValues` and skips matching side-effect blocks.
			for _, k := range sortedKeys(p.Parameters) {
				name := paramIDToName(k)
				if name == "" {
					continue
				}
				if isGuardedParam(k) && oldValues[k] != 0 {
					continue
				}
				dynProps.Set(name, p.Parameters[k])
			}
			applyProposalSideEffects(db, p, dynProps, fc, maintenanceTime, statedb, oldValues)
			p.State = rawdb.ProposalStateApproved
		} else {
			p.State = rawdb.ProposalStateCanceled
		}
		if err := statedb.WriteProposal(id, p); err != nil {
			return err
		}
		// p is now terminal; cache so subsequent boundaries skip it.
		cache.markTerminal(id)
	}
	return nil
}

// paramIDToName maps a TRON proposal parameter ID to its DynProps key name.
func paramIDToName(id int64) string {
	return forks.ProposalParamKey(id)
}

// guardedParams enumerates the proposal IDs that java-tron's ProposalService
// wraps in `if (getX() == 0) { ... }`. For these, re-approving an already
// active proposal must NOT rewrite the DP or re-run side-effects.
//
//	#10 REMOVE_THE_POWER_OF_THE_GR  — ProposalService.java:77-81
//	#20 ALLOW_MULTI_SIGN            — :123-128
//	#21 ALLOW_ADAPTIVE_ENERGY       — :129-142 (also guards the
//	                                   AdaptiveResourceLimitTargetRatio /
//	                                   Multiplier side-effects)
//	#27 ALLOW_SHIELDED_TRANSACTION  — Nile historical block 1,628,391 only;
//	                                   guarded by getAllowShieldedTransaction()==0
//	#44 ALLOW_MARKET_TRANSACTION    — :220-227 (also guards adding the
//	                                   MarketSell/MarketCancel permission
//	                                   bits 52/53)
//	#77 ALLOW_CANCEL_ALL_UNFREEZE_V2 — :347-354 (also guards adding the
//	                                   CancelAllUnfreezeV2 permission bit 59)
var guardedParams = map[int64]struct{}{
	10: {},
	20: {},
	21: {},
	27: {},
	44: {},
	77: {},
}

func isGuardedParam(id int64) bool {
	_, ok := guardedParams[id]
	return ok
}

// snapshotGuardedParams reads the current DP values for every guarded
// param in `params`. Missing names are skipped. Returns a nil map when
// `params` has no guarded entries (caller treats nil-lookup as 0).
func snapshotGuardedParams(dp *state.DynamicProperties, params map[int64]int64) map[int64]int64 {
	var out map[int64]int64
	for k := range params {
		if !isGuardedParam(k) {
			continue
		}
		name := paramIDToName(k)
		if name == "" {
			continue
		}
		if out == nil {
			out = make(map[int64]int64, len(guardedParams))
		}
		v, _ := dp.Get(name)
		out[k] = v
	}
	return out
}

// applyProposalSideEffects handles java-tron ProposalService-style
// activation hooks that go beyond setting a single DP key.
//
// `oldValues` carries the pre-proposal DP values for guarded paramIDs (see
// `guardedParams`). When oldValues[id] is non-zero, java skips the entire
// case body — both the DP save and the side-effect — so we do the same.
func applyProposalSideEffects(db kvReadWriter, p *rawdb.Proposal, dynProps *state.DynamicProperties, fc *forks.ForkController, maintenanceTime int64, statedb *state.StateDB, oldValues map[int64]int64) {
	wasZero := func(id int64) bool {
		return oldValues[id] == 0
	}
	for paramID, value := range p.Parameters {
		switch paramID {
		case 3: // TRANSACTION_FEE — append entry to bandwidth price history
			dynProps.SetBandwidthPriceHistory(
				dynProps.BandwidthPriceHistory() + fmt.Sprintf(",%d:%d", p.ExpirationTime, value))
		case 11: // ENERGY_FEE — append entry to energy price history
			dynProps.SetEnergyPriceHistory(
				dynProps.EnergyPriceHistory() + fmt.Sprintf(",%d:%d", p.ExpirationTime, value))
		case 17: // TOTAL_ENERGY_LIMIT — v1 setter (deprecated; fork-locked
			// on mainnet by VERSION_3_2_2 but exposed for early replay).
			// Recomputes target_limit only; current_limit is NOT touched.
			dynProps.SetTotalEnergyLimitV1(value)
		case 19: // TOTAL_CURRENT_ENERGY_LIMIT — v2 setter. Recomputes
			// target_limit and (if AllowAdaptiveEnergy==0) current_limit.
			dynProps.SetTotalEnergyLimit(value)
		case 21: // ALLOW_ADAPTIVE_ENERGY
			// java guards the entire block on getAllowAdaptiveEnergy()==0;
			// re-approving the same proposal must not re-overwrite the
			// adaptive ratio/multiplier (which may have moved via #33).
			if !wasZero(21) {
				break
			}
			if fc != nil && fc.Pass(version3_6_5, maintenanceTime, dynProps.MaintenanceTimeInterval()) {
				dynProps.SetAdaptiveResourceLimitTargetRatio(2880)
				dynProps.SetAdaptiveResourceLimitMultiplier(50)
				totalEnergyLimit := dynProps.TotalEnergyLimit()
				dynProps.SetTotalEnergyTargetLimit(totalEnergyLimit / 2880)
			}
		case 33: // ADAPTIVE_RESOURCE_LIMIT_TARGET_RATIO
			// Mirrors java-tron ProposalService.ADAPTIVE_RESOURCE_LIMIT_TARGET_RATIO:
			//   ratio = 24 * 60 * value
			//   saveAdaptiveResourceLimitTargetRatio(ratio)
			//   saveTotalEnergyTargetLimit(totalEnergyLimit / ratio)
			// The generic Set above already wrote `value` to the same key —
			// this overwrites with `1440 * value` so the final flushed state
			// matches java. value is validated to [1, 1000] so ratio is in
			// [1440, 1_440_000] and never zero.
			ratio := 24 * 60 * value
			dynProps.SetAdaptiveResourceLimitTargetRatio(ratio)
			if ratio > 0 {
				dynProps.SetTotalEnergyTargetLimit(dynProps.TotalEnergyLimit() / ratio)
			}
		case 59: // ALLOW_TVM_VOTE — also locks the new-reward effective cycle
			// Mirrors java-tron ProposalService.ALLOW_TVM_VOTE: calls
			// saveNewRewardAlgorithmEffectiveCycle() before saving the flag.
			// On mainnet #59 activated before #67, so the lock fires here
			// and #67 below is a no-op via the SaveX guard.
			if value != 0 {
				dynProps.SaveNewRewardAlgorithmEffectiveCycle()
			}
		case 67: // ALLOW_NEW_REWARD — set effective cycle so VI path starts
			if value != 0 {
				// Mirrors java-tron ProposalService.ALLOW_NEW_REWARD:
				// saveNewRewardAlgorithmEffectiveCycle() is idempotent — if
				// #59 already locked the cycle, this call is a no-op.
				dynProps.SaveNewRewardAlgorithmEffectiveCycle()
			}
		case 68: // MEMO_FEE — append entry to memo fee history
			dynProps.SetMemoFeeHistory(
				dynProps.MemoFeeHistory() + fmt.Sprintf(",%d:%d", p.ExpirationTime, value))
		case 26: // ALLOW_TVM_CONSTANTINOPLE → enables ClearABIContract (48)
			if value != 0 {
				dynProps.AddSystemContractAndSetPermission(48)
			}
		case 27: // ALLOW_SHIELDED_TRANSACTION → enables ShieldedTransferContract (51)
			if !wasZero(27) {
				break
			}
			if value != 0 {
				dynProps.AddSystemContractAndSetPermission(51)
			}
		case 30: // ALLOW_CHANGE_DELEGATION → enables UpdateBrokerageContract (49)
			// java's ProposalService.ALLOW_CHANGE_DELEGATION saves the flag
			// AND adds permission 49 unconditionally — even when the proposal
			// value is 0 (the validator accepts 0|1). Mirror that.
			dynProps.AddSystemContractAndSetPermission(49)
		case 44: // ALLOW_MARKET_TRANSACTION → enables MarketSellAsset (52), MarketCancelOrder (53)
			// java guards on getAllowMarketTransaction()==0 and adds the
			// permission bits regardless of `value` (validator forces 1
			// anyway). Mirror that: only run on the first activation.
			if !wasZero(44) {
				break
			}
			dynProps.AddSystemContractAndSetPermission(52)
			dynProps.AddSystemContractAndSetPermission(53)
		case 70: // UNFREEZE_DELAY_DAYS → enables FreezeBalanceV2 (54), UnfreezeBalanceV2 (55),
			// WithdrawExpireUnfreeze (56), DelegateResource (57), UnDelegateResource (58)
			if value != 0 {
				dynProps.AddSystemContractAndSetPermission(54)
				dynProps.AddSystemContractAndSetPermission(55)
				dynProps.AddSystemContractAndSetPermission(56)
				dynProps.AddSystemContractAndSetPermission(57)
				dynProps.AddSystemContractAndSetPermission(58)
			}
		case 77: // ALLOW_CANCEL_ALL_UNFREEZE_V2 → enables CancelAllUnfreezeV2 (59)
			// java guards on getAllowCancelAllUnfreezeV2()==0; validator
			// forces value=1 so no separate value check is needed.
			if !wasZero(77) {
				break
			}
			dynProps.AddSystemContractAndSetPermission(59)
		case 95: // ALLOW_TVM_PRAGUE → deploy TIP-2935 BlockHashHistory contract
			if value != 0 {
				deployHistoryBlockHash(statedb, dynProps)
			}
		}
	}
}

func sortedKeys(m map[int64]int64) []int64 {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Simple insertion sort for small maps
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
