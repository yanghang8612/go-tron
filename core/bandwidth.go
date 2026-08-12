package core

import (
	"fmt"
	"strconv"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto/pq"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
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
	return txBandwidthSizeFromWireSizes(tx, supportVM, measureTransactionWireSizes(tx))
}

func txBandwidthSizeFromWireSizes(tx *types.Transaction, supportVM bool, sizes transactionWireSizes) int64 {
	if !supportVM {
		return int64(sizes.full)
	}
	size := int64(sizes.withoutRet)
	if tx.Proto().RawData != nil {
		for _, c := range tx.Proto().RawData.Contract {
			if c.Type != corepb.Transaction_Contract_ShieldedTransferContract {
				size += maxResultSizeInTx
			}
		}
	}
	return size
}

// transactionSizeWithoutRet computes the serialized size used by java-tron's
// bandwidth accounting without cloning the full transaction object graph.
// Protobuf message fields are independently length-delimited, so subtracting
// each field-5 Ret entry's encoded size from the complete transaction size is
// exactly equivalent to clearing Ret and sizing again. Unknown fields and every
// other transaction field remain included unchanged.
func transactionSizeWithoutRet(tx *types.Transaction) int {
	return measureTransactionWireSizes(tx).withoutRet
}

// transactionWireSizes holds the protobuf size facts used by validation and
// bandwidth accounting. Immutable block transactions reuse types.Transaction's
// preprocessed facts; mutable standalone transactions are measured per call.
type transactionWireSizes struct {
	full       int
	withoutRet int
	results    int
}

// measureTransactionWireSizes computes the complete serialized size, the size
// with every field-5 Ret entry omitted, and the serialized payload size of all
// Ret entries. Keeping the results together lets applyTransaction share one
// protobuf walk across common validation, the result-size guard, and bandwidth
// accounting.
func measureTransactionWireSizes(tx *types.Transaction) transactionWireSizes {
	if tx == nil {
		return transactionWireSizes{}
	}
	precomputed := tx.SerializedSizes()
	return transactionWireSizes{
		full:       precomputed.Full,
		withoutRet: precomputed.WithoutRet,
		results:    precomputed.Results,
	}
}

// transactionSizes preserves the small tuple helper used by focused tests and
// call sites that need no result-size total.
func transactionSizes(tx *types.Transaction) (size, sizeWithoutRet int) {
	sizes := measureTransactionWireSizes(tx)
	return sizes.full, sizes.withoutRet
}

// BandwidthResult captures bandwidth consumption details.
type BandwidthResult struct {
	NetUsage           int64
	NetFee             int64
	NetFeeForBandwidth bool
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
	return availableAccountNetForFrozen(frozenForNet(acct), dp)
}

// availableAccountNetForFrozen applies the global bandwidth formula to an
// already-computed frozen balance. Keeping this separate lets hot paths use
// StateDB's targeted resource reads without materializing a full Account.
func availableAccountNetForFrozen(frozen int64, dp *state.DynamicProperties) int64 {

	totalWeight := dp.TotalNetWeight()
	if totalWeight <= 0 {
		return 0
	}
	totalLimit := dp.TotalNetLimit()
	harden := dp.AllowHardenResourceCalculation()

	// V2 formula (float-precision) is active once the unfreeze-delay proposal
	// is set (proposal #70 on mainnet); otherwise fall back to V1 integer math
	// which rejects sub-TRX balances.
	if dp.UnfreezeDelayDays() > 0 {
		return calculateGlobalResourceLimitV2(frozen, totalLimit, totalWeight, harden)
	}
	if frozen < trxPrecision {
		return 0
	}
	return calculateGlobalResourceLimitV1(frozen, totalLimit, totalWeight, harden)
}

// chargeStakedNet tries to charge `cost` bytes of net usage to addr's staked
// bandwidth. It returns true (and persists net_usage, the per-account window when
// active, and latest_consume_time on addr) when the stake covers the cost; the
// caller is responsible for latest_operation_time (java sets it on the tx sender,
// which is not always the charged account — e.g. asset issuer billing).
//
// Mirrors java-tron BandwidthProcessor.useAccountNet's two regimes:
//   - !supportUnfreezeDelay (Stake 1.0): global 28800-slot window recover + add.
//   - supportUnfreezeDelay (Stake 2.0): per-account window — recovery() over the
//     account's stored net_window for the limit check, then a single
//     increase(ac, BANDWIDTH, usage, cost, latestConsumeTime, now) that
//     renormalizes and persists the window (V2/optimized under
//     supportCancelAllUnfreezeV2).
func chargeStakedNet(statedb *state.StateDB, dynProps *state.DynamicProperties, addr tcommon.Address, cost, now int64) bool {
	netUsage := statedb.GetNetUsage(addr)
	lastTime := statedb.GetLatestConsumeTime(addr)
	var frozen int64
	var err error
	if dynProps.SupportUnfreezeDelay() {
		frozen, err = statedb.GetAccountFrozenBandwidth(addr)
	} else {
		frozen, err = statedb.GetAccountFrozenBandwidthV1(addr)
	}
	if err != nil {
		return false
	}
	netLimit := availableAccountNetForFrozen(frozen, dynProps)

	if !dynProps.SupportUnfreezeDelay() {
		recovered := recoverUsageForDP(netUsage, lastTime, now, dynProps)
		if recovered+cost > netLimit {
			return false
		}
		statedb.SetNetUsage(addr, recovered+cost)
		statedb.SetLatestConsumeTime(addr, now)
		return true
	}

	harden := dynProps.AllowHardenResourceCalculation()
	cancelAllV2 := dynProps.SupportCancelAllUnfreezeV2()
	rawWindow, optimized := statedb.GetNetWindow(addr)
	// recovery(ac, BANDWIDTH, usage, lastTime, now) for the limit check — usage==0
	// degenerates computeResourceIncrease to java's recovery (newUsage == remainUsage).
	recovered, _, _ := computeResourceIncrease(rawWindow, optimized, netUsage, 0, lastTime, now, harden, cancelAllV2)
	if recovered+cost > netLimit {
		return false
	}
	// single increase(ac, BANDWIDTH, usage, cost, lastTime, now): recompute from the
	// original usage/time, add cost, renormalize + persist the window.
	newUsage, newRaw, newOpt := computeResourceIncrease(rawWindow, optimized, netUsage, cost, lastTime, now, harden, cancelAllV2)
	statedb.SetNetUsage(addr, newUsage)
	statedb.SetNetWindow(addr, newRaw, newOpt)
	statedb.SetLatestConsumeTime(addr, now)
	return true
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
func consumeBandwidth(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, prevBlockTime int64) (BandwidthResult, error) {
	return consumeBandwidthWithResourceTime(statedb, dynProps, tx, prevBlockTime, HeadSlot(prevBlockTime, 0))
}

func consumeBandwidthWithResourceTime(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, prevBlockTime, resourceTime int64) (BandwidthResult, error) {
	if tx.ContractType() == corepb.Transaction_Contract_ShieldedTransferContract {
		return BandwidthResult{}, nil
	}
	return consumeBandwidthWithResourceTimeAndSizes(statedb, dynProps, tx, prevBlockTime, resourceTime, measureTransactionWireSizes(tx))
}

func consumeBandwidthWithResourceTimeAndSizes(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, prevBlockTime, resourceTime int64, sizes transactionWireSizes) (BandwidthResult, error) {
	if tx.ContractType() == corepb.Transaction_Contract_ShieldedTransferContract {
		return BandwidthResult{}, nil
	}
	sender := extractSender(tx)
	if sender == (tcommon.Address{}) {
		return BandwidthResult{}, fmt.Errorf("cannot determine sender")
	}

	txSize := txBandwidthSizeFromWireSizes(tx, dynProps.AllowCreationOfContracts(), sizes)

	if contractCreatesNewAccount(statedb, tx) {
		if dynProps.ConsensusLogicOptimization() {
			// java subtracts signature payload reservations before applying
			// max_create_account_tx_size: 65 bytes per ECDSA signature and the
			// scheme-specific maximum embedded PQAuthSig wire size.
			pb := tx.Proto()
			createSize := int64(sizes.withoutRet - 65*len(pb.GetSignature()))
			if dynProps.AllowPQSignatures() {
				for _, auth := range pb.GetPqAuthSig() {
					wireSize, ok := pq.AuthSigWireSizeUpperBound(auth.GetScheme())
					if !ok {
						return BandwidthResult{}, fmt.Errorf("unknown pq signature scheme %s", auth.GetScheme())
					}
					createSize -= int64(wireSize)
				}
			}
			if createSize > dynProps.MaxCreateAccountTxSize() {
				return BandwidthResult{}, fmt.Errorf("create account transaction size %d exceeds maximum %d", createSize, dynProps.MaxCreateAccountTxSize())
			}
		}
		return consumeBandwidthForCreateNewAccount(statedb, dynProps, sender, txSize, prevBlockTime, resourceTime)
	}

	if tx.ContractType() == corepb.Transaction_Contract_TransferAssetContract {
		ok, err := useAssetAccountNet(statedb, dynProps, tx, sender, txSize, prevBlockTime, resourceTime)
		if err != nil {
			return BandwidthResult{}, err
		}
		if ok {
			return BandwidthResult{NetUsage: txSize}, nil
		}
	}

	if chargeStakedNet(statedb, dynProps, sender, txSize, resourceTime) {
		statedb.SetLatestOperationTime(sender, prevBlockTime)
		return BandwidthResult{NetUsage: txSize}, nil
	}

	// Try free bandwidth
	freeLimit := dynProps.FreeNetLimit()
	recoveredFreeUsage := recoverUsageForDP(statedb.GetFreeNetUsage(sender), statedb.GetLatestConsumeFreeTime(sender), resourceTime, dynProps)
	publicLimit := dynProps.PublicNetLimit()
	publicUsage := dynProps.PublicNetUsage()
	publicTime := dynProps.PublicNetTime()
	recoveredPublicUsage := recoverUsageForDP(publicUsage, publicTime, resourceTime, dynProps)
	// Match java-tron's bytes <= limit-recovered form. Besides preserving the
	// exact admission order, this avoids overflowing recovered+txSize at the
	// boundary of an int64 configuration.
	if txSize <= freeLimit-recoveredFreeUsage && txSize <= publicLimit-recoveredPublicUsage {
		statedb.SetFreeNetUsage(sender, recoveredFreeUsage+txSize)
		statedb.SetLatestConsumeFreeTime(sender, resourceTime)
		statedb.SetLatestOperationTime(sender, prevBlockTime)
		dynProps.RecordPublicNetReservation(state.PublicNetReservation{
			StartUsage:     publicUsage,
			StartTime:      publicTime,
			RecoveredUsage: recoveredPublicUsage,
			ResourceTime:   resourceTime,
			Delta:          txSize,
			Limit:          publicLimit,
		})
		dynProps.SetPublicNetUsage(recoveredPublicUsage + txSize)
		dynProps.SetPublicNetTime(resourceTime)
		return BandwidthResult{NetUsage: txSize}, nil
	}

	// Burn TRX
	cost := txSize * dynProps.TransactionFee()
	if err := statedb.SubBalance(sender, cost); err != nil {
		return BandwidthResult{}, fmt.Errorf("insufficient balance to pay bandwidth: need %d sun", cost)
	}
	statedb.SetLatestOperationTime(sender, prevBlockTime)
	routeBandwidthFee(statedb, dynProps, cost)
	dynProps.AddTotalTransactionCost(cost)
	return BandwidthResult{NetFee: cost, NetFeeForBandwidth: true}, nil
}

func routeBandwidthFee(statedb *state.StateDB, dynProps *state.DynamicProperties, fee int64) {
	if fee <= 0 {
		return
	}
	if dynProps.AllowTransactionFeePool() {
		dynProps.AddTransactionFeePool(fee)
		return
	}
	if dynProps.AllowBlackHoleOptimization() {
		dynProps.AddBurnTrx(fee)
		return
	}
	statedb.AddSettlementBalance(statedb.BlackholeAddress(), fee)
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
		msg, err := tx.DecodedContract()
		if err != nil {
			return false
		}
		type toGetter interface{ GetToAddress() []byte }
		if g, ok := msg.(toGetter); ok {
			return !statedb.AccountExists(tcommon.BytesToAddress(g.GetToAddress()))
		}
	case corepb.Transaction_Contract_TransferAssetContract:
		msg, err := tx.DecodedContract()
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

func useAssetAccountNet(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, sender tcommon.Address, txSize, prevBlockTime, resourceTime int64) (bool, error) {
	contract := tx.Contract()
	if contract == nil || contract.Parameter == nil {
		return false, nil
	}
	msg, err := tx.DecodedContract()
	if err != nil {
		return false, fmt.Errorf("failed to unmarshal TransferAssetContract: %w", err)
	}
	c, ok := msg.(*contractpb.TransferAssetContract)
	if !ok {
		return false, fmt.Errorf("failed to unmarshal TransferAssetContract: unexpected parameter type %T", msg)
	}

	asset, tokenID, err := resolveBandwidthAsset(statedb, dynProps, c.AssetName)
	if err != nil {
		return false, err
	}
	tokenIDStr := strconv.FormatInt(tokenID, 10)

	recoveredPublicUsage := recoverUsageForDP(asset.PublicFreeAssetNetUsage, asset.PublicLatestFreeNetTime, resourceTime, dynProps)
	if txSize > asset.PublicFreeAssetNetLimit-recoveredPublicUsage {
		return false, nil
	}

	var freeUsage, latestAssetOperationTime int64
	if dynProps.AllowSameTokenName() {
		freeUsage = statedb.GetFreeAssetNetUsageV2(sender, tokenIDStr)
		latestAssetOperationTime = statedb.GetLatestAssetOperationTimeV2(sender, tokenIDStr)
	} else {
		tokenName := string(c.AssetName)
		freeUsage = statedb.GetFreeAssetNetUsage(sender, tokenName)
		latestAssetOperationTime = statedb.GetLatestAssetOperationTime(sender, tokenName)
	}
	recoveredFreeAssetUsage := recoverUsageForDP(freeUsage, latestAssetOperationTime, resourceTime, dynProps)
	if txSize > asset.FreeAssetNetLimit-recoveredFreeAssetUsage {
		return false, nil
	}

	issuer := tcommon.BytesToAddress(asset.OwnerAddress)
	if !statedb.AccountExists(issuer) {
		return false, nil
	}
	// java useAssetAccountNet charges the issuer's own staked net via the same
	// recovery + per-account-window increase path as useAccountNet. The asset
	// free/public usages below stay on the global window (java uses the base
	// increase() for those).
	if !chargeStakedNet(statedb, dynProps, issuer, txSize, resourceTime) {
		return false, nil
	}
	statedb.SetLatestOperationTime(sender, prevBlockTime)

	newFreeAssetUsage := recoveredFreeAssetUsage + txSize
	if dynProps.AllowSameTokenName() {
		statedb.SetFreeAssetNetUsageV2(sender, tokenIDStr, newFreeAssetUsage)
		statedb.SetLatestAssetOperationTimeV2(sender, tokenIDStr, resourceTime)
	} else {
		tokenName := string(c.AssetName)
		statedb.SetFreeAssetNetUsage(sender, tokenName, newFreeAssetUsage)
		statedb.SetLatestAssetOperationTime(sender, tokenName, resourceTime)
		statedb.SetFreeAssetNetUsageV2(sender, tokenIDStr, newFreeAssetUsage)
		statedb.SetLatestAssetOperationTimeV2(sender, tokenIDStr, resourceTime)
	}

	newPublicUsage := recoveredPublicUsage + txSize
	if dynProps.AllowSameTokenName() {
		if err := statedb.WriteAssetIssueBandwidth(tokenID, newPublicUsage, resourceTime); err != nil {
			return false, err
		}
	} else {
		// Before AllowSameTokenName java-tron keeps the mandatory legacy name
		// row and, once the migration mirror exists, the ID-keyed V2 row in
		// lockstep. Their mutable public-bandwidth counters are separate fixed
		// rows now, so charging no longer decodes and rewrites either metadata
		// object.
		if statedb.HasAssetIssueByName(c.AssetName) {
			if err := statedb.WriteAssetIssueBandwidthByName(c.AssetName, newPublicUsage, resourceTime); err != nil {
				return false, err
			}
		}
		if statedb.HasAssetIssue(tokenID) {
			if err := statedb.WriteAssetIssueBandwidth(tokenID, newPublicUsage, resourceTime); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func resolveBandwidthAsset(statedb *state.StateDB, dynProps *state.DynamicProperties, assetName []byte) (*contractpb.AssetIssueContract, int64, error) {
	if dynProps.AllowSameTokenName() {
		tokenID, err := strconv.ParseInt(string(assetName), 10, 64)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid asset_name: not a numeric ID")
		}
		asset := statedb.ReadAssetIssue(tokenID)
		if asset == nil {
			return nil, 0, fmt.Errorf("asset [%s] does not exist", assetName)
		}
		return asset, tokenID, nil
	}
	if asset := statedb.ReadAssetIssueByName(assetName); asset != nil {
		tokenID, err := strconv.ParseInt(asset.Id, 10, 64)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid legacy asset ID")
		}
		return asset, tokenID, nil
	}
	if tokenID, ok := statedb.ReadAssetNameIndex(assetName); ok {
		asset := statedb.ReadAssetIssue(tokenID)
		if asset != nil {
			return asset, tokenID, nil
		}
	}
	return nil, 0, fmt.Errorf("asset [%s] does not exist", assetName)
}

// consumeBandwidthForCreateNewAccount charges bandwidth for txs that
// materialize a new account. java-tron `BandwidthProcessor` line 192-206:
// only personal staked bandwidth is consulted (`createNewAccountBandwidthRate`
// applied per byte); on shortage the `create_account_fee` is taken from the
// owner balance and either burned or sent to the blackhole — and
// `total_create_account_cost` is incremented.
func consumeBandwidthForCreateNewAccount(statedb *state.StateDB, dynProps *state.DynamicProperties, sender tcommon.Address, txSize, prevBlockTime, resourceTime int64) (BandwidthResult, error) {
	ratio := dynProps.CreateNewAccountBandwidthRate()
	netCost := txSize * ratio

	if chargeStakedNet(statedb, dynProps, sender, netCost, resourceTime) {
		statedb.SetLatestOperationTime(sender, prevBlockTime)
		return BandwidthResult{NetUsage: netCost}, nil
	}

	fee := dynProps.CreateAccountFee()
	if err := statedb.SubBalance(sender, fee); err != nil {
		return BandwidthResult{}, fmt.Errorf("insufficient balance for create_account_fee: need %d sun", fee)
	}
	statedb.SetLatestOperationTime(sender, prevBlockTime)
	if dynProps.AllowBlackHoleOptimization() {
		dynProps.AddBurnTrx(fee)
	} else {
		statedb.AddSettlementBalance(statedb.BlackholeAddress(), fee)
	}
	dynProps.AddTotalCreateAccountCost(fee)
	return BandwidthResult{NetFee: fee}, nil
}

// extractSender extracts the bandwidth payer from the first contract.
func extractSender(tx *types.Transaction) tcommon.Address {
	owner, _, err := extractContractOwner(tx)
	if err != nil {
		return tcommon.Address{}
	}
	if len(owner) == 0 {
		return tcommon.Address{}
	}
	return tcommon.BytesToAddress(owner)
}
