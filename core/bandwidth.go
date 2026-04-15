package core

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// trxPrecision is the SUN-per-TRX conversion used by resource weight math.
const trxPrecision = 1_000_000

// BandwidthResult captures bandwidth consumption details.
type BandwidthResult struct {
	NetUsage int64
	NetFee   int64
}

// availableAccountNet returns this account's share of the global bandwidth
// pool, mirroring java-tron's BandwidthProcessor.calculateGlobalNetLimit
// (chainbase/.../BandwidthProcessor.java:432). The returned value is the
// maximum net usage the account is entitled to given its frozen stake.
//
// Frozen sources summed here match java's AccountCapsule.getAllFrozenBalanceForBandwidth:
//   - own V1 frozen bandwidth list
//   - V1 delegation acquired in (not delegated-out)
//   - own V2 frozen-for-bandwidth
//   - V2 delegation acquired in
//
// Returns 0 when the account has no weight or global total_net_weight is <= 0.
func availableAccountNet(acct *types.Account, dp *state.DynamicProperties) int64 {
	if acct == nil {
		return 0
	}
	frozen := acct.TotalFrozenBandwidth()
	frozen += acct.AcquiredDelegatedFrozenBandwidth()
	frozen += acct.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH)
	frozen += acct.AcquiredDelegatedFrozenV2BalanceForBandwidth()

	totalWeight := dp.TotalNetWeight()
	if totalWeight <= 0 {
		return 0
	}
	totalLimit := dp.TotalNetLimit()

	// V2 formula (float-precision) is active once the unfreeze-delay proposal
	// is set (proposal #70 on mainnet); otherwise fall back to V1 integer math
	// which rejects sub-TRX balances.
	if dp.UnfreezeDelayDays() > 0 {
		netWeight := float64(frozen) / float64(trxPrecision)
		return int64(netWeight * (float64(totalLimit) / float64(totalWeight)))
	}
	if frozen < trxPrecision {
		return 0
	}
	netWeight := frozen / trxPrecision
	return int64(float64(netWeight) * (float64(totalLimit) / float64(totalWeight)))
}

// consumeBandwidth charges bandwidth for a transaction.
// Priority: staked bandwidth (V1+V2 mixed) -> free bandwidth -> burn TRX.
func consumeBandwidth(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, blockTime int64) (*BandwidthResult, error) {
	sender := extractSender(tx)
	if sender == (tcommon.Address{}) {
		return nil, fmt.Errorf("cannot determine sender")
	}

	txSize := int64(tx.Size())

	acct := statedb.GetAccount(sender)
	netLimit := availableAccountNet(acct, dynProps)
	if netLimit > 0 {
		recoveredUsage := recoverUsage(statedb.GetNetUsage(sender), statedb.GetLatestConsumeTime(sender), blockTime)
		if recoveredUsage+txSize <= netLimit {
			statedb.SetNetUsage(sender, recoveredUsage+txSize)
			statedb.SetLatestConsumeTime(sender, blockTime)
			return &BandwidthResult{NetUsage: txSize}, nil
		}
	}

	// Try free bandwidth
	freeLimit := dynProps.FreeNetLimit()
	recoveredFreeUsage := recoverUsage(statedb.GetFreeNetUsage(sender), statedb.GetLatestConsumeFreeTime(sender), blockTime)
	if recoveredFreeUsage+txSize <= freeLimit {
		statedb.SetFreeNetUsage(sender, recoveredFreeUsage+txSize)
		statedb.SetLatestConsumeFreeTime(sender, blockTime)
		return &BandwidthResult{NetUsage: txSize}, nil
	}

	// Burn TRX
	cost := txSize * dynProps.TransactionFee()
	if err := statedb.SubBalance(sender, cost); err != nil {
		return nil, fmt.Errorf("insufficient balance to pay bandwidth: need %d sun", cost)
	}
	return &BandwidthResult{NetFee: cost}, nil
}

// extractSender extracts the owner address from the first contract of a transaction.
func extractSender(tx *types.Transaction) tcommon.Address {
	contract := tx.Contract()
	if contract == nil {
		return tcommon.Address{}
	}
	msg, err := contract.Parameter.UnmarshalNew()
	if err != nil {
		return tcommon.Address{}
	}
	type ownerAddressGetter interface {
		GetOwnerAddress() []byte
	}
	if oag, ok := msg.(ownerAddressGetter); ok {
		return tcommon.BytesToAddress(oag.GetOwnerAddress())
	}
	return tcommon.Address{}
}
