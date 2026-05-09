package core

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// trxPrecision is the SUN-per-TRX conversion used by resource weight math.
const trxPrecision = 1_000_000

// maxResultSizeInTx mirrors java-tron `Constant.MAX_RESULT_SIZE_IN_TX`
// (= 64). Post-fork (supportVM), bandwidth charges add this constant to the
// transaction's serialized size for every non-shielded contract, replacing
// the size of the actual `ret` field stripped from the tx before sizing.
const maxResultSizeInTx int64 = 64

// txBandwidthSize returns the byte count java-tron charges as bandwidth for
// `tx`, mirroring `BandwidthProcessor.consume` (chainbase/.../db/BandwidthProcessor.java:114-128).
//
// Pre-supportVM: full serialized size including the `ret` field (legacy).
// Post-supportVM: serialized size with `ret` stripped, plus 64 bytes per
// non-shielded contract. The asymmetry is what made gtron's pre-fix
// VoteWitnessContract net_usage=239 vs nileex's 299: the empty `ret` slot
// is 4 bytes on the wire, so stripping it (-4) and adding 64 (+64) yields
// the +60 byte delta seen on every Nile non-shielded tx.
func txBandwidthSize(tx *types.Transaction, supportVM bool) int64 {
	if !supportVM {
		return int64(tx.Size())
	}
	stripped := proto.Clone(tx.Proto()).(*corepb.Transaction)
	stripped.Ret = nil
	size := int64(proto.Size(stripped))
	if tx.Proto().RawData != nil {
		for _, c := range tx.Proto().RawData.Contract {
			if c.Type != corepb.Transaction_Contract_ShieldedTransferContract {
				size += maxResultSizeInTx
			}
		}
	}
	return size
}

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
//
// Special case (mirrors java-tron `BandwidthProcessor.consumeForCreateNewAccount`):
// when the contract creates a new on-chain account (TransferContract /
// TransferAssetContract / AccountCreateContract whose target doesn't yet exist),
// only staked bandwidth is consulted. On insufficient stake the path falls
// back to the `create_account_fee` (default 100_000 SUN), bypassing the
// free-bandwidth daily quota entirely.
func consumeBandwidth(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, blockTime int64) (*BandwidthResult, error) {
	sender := extractSender(tx)
	if sender == (tcommon.Address{}) {
		return nil, fmt.Errorf("cannot determine sender")
	}

	txSize := txBandwidthSize(tx, dynProps.AllowCreationOfContracts())

	if contractCreatesNewAccount(statedb, tx) {
		return consumeBandwidthForCreateNewAccount(statedb, dynProps, sender, txSize, blockTime)
	}

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

// contractCreatesNewAccount mirrors java-tron's
// `BandwidthProcessor.contractCreateNewAccount`: returns true when the
// transaction's first contract type is one that materializes a new on-chain
// account. For Transfer/TransferAsset, this depends on whether the recipient
// already exists in state.
func contractCreatesNewAccount(statedb *state.StateDB, tx *types.Transaction) bool {
	contract := tx.Contract()
	if contract == nil || contract.Parameter == nil {
		return false
	}
	switch contract.Type {
	case corepb.Transaction_Contract_AccountCreateContract:
		return true
	case corepb.Transaction_Contract_TransferContract:
		msg, err := contract.Parameter.UnmarshalNew()
		if err != nil {
			return false
		}
		type toGetter interface{ GetToAddress() []byte }
		if g, ok := msg.(toGetter); ok {
			return !statedb.AccountExists(tcommon.BytesToAddress(g.GetToAddress()))
		}
	case corepb.Transaction_Contract_TransferAssetContract:
		msg, err := contract.Parameter.UnmarshalNew()
		if err != nil {
			return false
		}
		type toGetter interface{ GetToAddress() []byte }
		if g, ok := msg.(toGetter); ok {
			return !statedb.AccountExists(tcommon.BytesToAddress(g.GetToAddress()))
		}
	}
	return false
}

// consumeBandwidthForCreateNewAccount charges bandwidth for txs that
// materialize a new account. java-tron `BandwidthProcessor` line 192-206:
// only personal staked bandwidth is consulted (`createNewAccountBandwidthRate`
// applied per byte); on shortage the `create_account_fee` is taken from the
// owner balance and either burned or sent to the blackhole — and
// `total_create_account_cost` is incremented.
func consumeBandwidthForCreateNewAccount(statedb *state.StateDB, dynProps *state.DynamicProperties, sender tcommon.Address, txSize, blockTime int64) (*BandwidthResult, error) {
	ratio := dynProps.CreateNewAccountBandwidthRate()
	if ratio <= 0 {
		ratio = 1
	}
	netCost := txSize * ratio

	acct := statedb.GetAccount(sender)
	netLimit := availableAccountNet(acct, dynProps)
	if netLimit > 0 {
		recoveredUsage := recoverUsage(statedb.GetNetUsage(sender), statedb.GetLatestConsumeTime(sender), blockTime)
		if recoveredUsage+netCost <= netLimit {
			statedb.SetNetUsage(sender, recoveredUsage+netCost)
			statedb.SetLatestConsumeTime(sender, blockTime)
			return &BandwidthResult{NetUsage: netCost}, nil
		}
	}

	fee := dynProps.CreateAccountFee()
	if fee <= 0 {
		// Some private chains may run with zero fee; allow the tx through
		// rather than failing it on a zero-cost path.
		return &BandwidthResult{}, nil
	}
	if err := statedb.SubBalance(sender, fee); err != nil {
		return nil, fmt.Errorf("insufficient balance for create_account_fee: need %d sun", fee)
	}
	if dynProps.AllowBlackHoleOptimization() {
		dynProps.AddBurnTrx(fee)
	} else {
		statedb.AddBalance(params.BlackholeAddress, fee)
	}
	dynProps.AddTotalCreateAccountCost(fee)
	return &BandwidthResult{NetFee: fee}, nil
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
