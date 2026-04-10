package core

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// BandwidthResult captures bandwidth consumption details.
type BandwidthResult struct {
	NetUsage int64
	NetFee   int64
}

// consumeBandwidth charges bandwidth for a transaction.
// Priority: frozen bandwidth -> free bandwidth -> burn TRX.
func consumeBandwidth(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, blockTime int64) (*BandwidthResult, error) {
	sender := extractSender(tx)
	if sender == (tcommon.Address{}) {
		return nil, fmt.Errorf("cannot determine sender")
	}

	txSize := int64(tx.Size())

	// Try frozen bandwidth first
	frozenBW := statedb.GetFrozenV2Amount(sender, corepb.ResourceCode_BANDWIDTH)
	if frozenBW > 0 {
		recoveredUsage := recoverUsage(statedb.GetNetUsage(sender), statedb.GetLatestConsumeTime(sender), blockTime)
		if recoveredUsage+txSize <= frozenBW {
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
