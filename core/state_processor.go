package core

import (
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// ErrExchangeRejected is returned by ApplyTransaction when an
// ExchangeTransactionContract reaches the block-apply path after the
// VERSION_4_8_0_1 (block version 33) fork has activated. Mirrors java-tron
// Manager.rejectExchangeTransaction (PR #6507, master commit 45e3bf88ca).
// Same error string as core/txpool.ErrExchangeRejected so log-grep
// consumers see one wire-format value across both paths.
var ErrExchangeRejected = errors.New("ExchangeTransactionContract is rejected")

// ApplyTransaction executes a single transaction against the given state.
// Returns the full actuator Result including fee, energy, net, and contract details.
//
// Validation flags:
//   - validate         → run actuator.Validate before Execute (state preconditions)
//   - validateEnvelope → run ValidateTxEnvelope (signature + permission) before
//     anything mutates state. Runs inside the per-tx position rather than as a
//     pre-block sweep so that the statedb reflects prior intra-block effects,
//     matching java-tron Manager.processBlock's interleaved validation. The
//     concrete case it covers: a single block holding an
//     AccountPermissionUpdateContract followed by a TransferContract signed
//     with the post-rotation keys — the transfer's signer is only present in
//     the post-update permission set, so envelope check must see the
//     just-mutated state.
//
// Both flags are independent so tests can keep validate=true / envelope=false
// for unsigned-tx fixtures without sacrificing actuator coverage.
//
// The db parameter accepts either an `ethdb.KeyValueStore` (BuildBlock path)
// or `core/blockbuffer.Buffer` (applyBlock path) — slice 3 of the fork-rewind
// fix widened the type so actuator-side rawdb-direct writes are rewindable.
func ApplyTransaction(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, prevBlockTime, blockTime int64, blockNum uint64, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, validate, validateEnvelope bool) (*actuator.Result, error) {
	return applyTransaction(statedb, dynProps, tx, prevBlockTime, true, HeadSlot(prevBlockTime, 0), blockTime, blockNum, db, activeWitnesses, params.DefaultBlockNumForEnergyLimit, tcommon.Hash{}, tcommon.Address{}, validate, validateEnvelope, false)
}

// ApplyTransactionWithResourceSlot executes a transaction with java-tron's
// resource-window time (`head slot`) separated from millisecond timestamps.
func ApplyTransactionWithResourceSlot(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, prevBlockTime, headSlot, blockTime int64, blockNum uint64, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, validate, validateEnvelope bool) (*actuator.Result, error) {
	return applyTransaction(statedb, dynProps, tx, prevBlockTime, true, headSlot, blockTime, blockNum, db, activeWitnesses, params.DefaultBlockNumForEnergyLimit, tcommon.Hash{}, tcommon.Address{}, validate, validateEnvelope, false)
}

func ApplyTransactionWithResourceSlotAndEnergyFork(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, prevBlockTime, headSlot, blockTime int64, blockNum uint64, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, energyLimitForkBlockNum int64, validate, validateEnvelope bool) (*actuator.Result, error) {
	return applyTransaction(statedb, dynProps, tx, prevBlockTime, true, headSlot, blockTime, blockNum, db, activeWitnesses, energyLimitForkBlockNum, tcommon.Hash{}, tcommon.Address{}, validate, validateEnvelope, false)
}

func applyTransaction(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, prevBlockTime int64, hasHeadSlot bool, headSlot, blockTime int64, blockNum uint64, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, energyLimitForkBlockNum int64, genesisHash tcommon.Hash, coinbase tcommon.Address, validate, validateEnvelope bool, trustTransactionRet bool) (*actuator.Result, error) {
	if err := ValidateContractCount(tx); err != nil {
		return nil, err
	}

	// Block-apply reject for ExchangeTransactionContract once VERSION_4_8_0_1
	// activates. Mirrors java-tron Manager.processBlock's per-tx
	// rejectExchangeTransaction call (master 45e3bf88ca). Pre-fork blocks
	// retain replay safety because PassVersion returns false until the
	// version-bitmap quorum is met. java-tron evaluates this gate against
	// the prev block's timestamp (the DP value during processTransaction),
	// so we pass prevBlockTime here for parity.
	if tx.ContractType() == corepb.Transaction_Contract_ExchangeTransactionContract &&
		forks.PassVersionFromStore(statedb, 33, prevBlockTime, dynProps.MaintenanceTimeInterval()) {
		return nil, ErrExchangeRejected
	}
	// java-tron Manager.validateCommon applies the synthetic "clear ret +
	// two MAX_RESULT_SIZE_IN_TX slots" size gate to pending transactions,
	// but only applies it to in-block transactions after
	// consensus_logic_optimization. Older mainnet blocks can otherwise fail
	// replay even though their actual protobuf bytes are below 500 KiB.
	validateResultSize := !trustTransactionRet || dynProps.ConsensusLogicOptimization()
	if err := validateTxCommon(tx, prevBlockTime, validateResultSize); err != nil {
		return nil, err
	}
	// java Manager.validateCommon adds an in-block expiration LOWER bound once
	// consensus_logic_optimization is active: the tx must not already be expired
	// as of the next block slot. nextSlotTime = latestBlockHeaderTimestamp +
	// slotCount*BLOCK_INTERVAL, slotCount = 1 (+ MaintenanceSkipSlots when the head
	// was a maintenance block, StateFlag==1). During in-block validation both impls
	// read the head's (prev block's) timestamp + state flag, so prevBlockTime +
	// dynProps.StateFlag() here match java's getNextBlockSlotTime. Canonical blocks
	// never contain a sub-slot-expiration tx (java rejects at produce), so this only
	// adds java's reject of a non-canonical block.
	if dynProps.ConsensusLogicOptimization() {
		slotCount := int64(1)
		if dynProps.StateFlag() == 1 {
			slotCount += int64(params.MaintenanceSkipSlots)
		}
		if tx.Expiration() < prevBlockTime+slotCount*params.BlockProducedInterval {
			return nil, ErrTransactionExpiration
		}
	}

	act, err := actuator.CreateActuator(tx)
	if err != nil {
		return nil, fmt.Errorf("create actuator: %w", err)
	}

	ctx := &actuator.Context{
		State:                      statedb,
		DynProps:                   dynProps,
		Tx:                         tx,
		BlockTime:                  blockTime,
		PrevBlockTime:              prevBlockTime,
		HeadSlot:                   headSlot,
		HasHeadSlot:                hasHeadSlot,
		BlockNumber:                blockNum,
		Coinbase:                   coinbase,
		GenesisHash:                genesisHash,
		EnergyLimitForkBlockNum:    energyLimitForkBlockNum,
		HasEnergyLimitForkBlockNum: true,
		DB:                         db,
		ActiveWitnesses:            activeWitnesses,
		TrustTransactionRet:        trustTransactionRet,
	}

	if validateEnvelope {
		// VERSION_4_7_1 (value 27): java-tron swapped the multi-sig dedup
		// key from raw signature bytes to recovered address. We mirror by
		// passing the fork-pass result through.
		multiSigByAddress := forks.PassVersionFromStore(statedb, 27, prevBlockTime, dynProps.MaintenanceTimeInterval())
		if err := ValidateTxEnvelope(tx, statedb, multiSigByAddress); err != nil {
			return nil, fmt.Errorf("validate envelope: %w", err)
		}
		// TAPOS read goes through the same buffered db that landed
		// previous-block writes. When applyBlock is mid-replay for block
		// N, this sees the ring as of block N-1 — which is exactly what
		// java-tron uses for the tapos check (ref blocks must precede the
		// referencing block).
		if err := ValidateTAPOS(tx, db); err != nil {
			return nil, fmt.Errorf("validate tapos: %w", err)
		}
	}

	if validate {
		if err := act.Validate(ctx); err != nil {
			return nil, fmt.Errorf("validate: %w", err)
		}
	}

	txSnap := statedb.Snapshot()
	dpProps, dpDirty := dynProps.Snapshot()
	revertTx := func() {
		statedb.RevertToSnapshot(txSnap)
		dynProps.Restore(dpProps, dpDirty)
	}

	resourceTime := ctx.ResourceTime()
	bwResult, err := consumeBandwidthWithResourceTime(statedb, dynProps, tx, prevBlockTime, resourceTime)
	if err != nil {
		revertTx()
		return nil, fmt.Errorf("bandwidth: %w", err)
	}

	multiSignFee, err := actuator.ConsumeMultiSignFee(ctx)
	if err != nil {
		revertTx()
		return nil, fmt.Errorf("multi-sign fee: %w", err)
	}
	memoFee, err := actuator.ConsumeMemoFee(ctx)
	if err != nil {
		revertTx()
		return nil, fmt.Errorf("memo fee: %w", err)
	}

	result, err := act.Execute(ctx)
	if err != nil {
		revertTx()
		return nil, fmt.Errorf("execute: %w", err)
	}

	// Settle the energy bill after Execute returns, mirroring java-tron's
	// VMActuator -> TransactionTrace.pay -> ReceiptCapsule.payEnergyBill
	// chain. For non-TVM actuators result.EnergyUsageTotal is zero and
	// PayEnergyBill is a no-op; for VMActuator it does the stake/balance
	// split, debits the caller, and routes the bill (transactionFeePool /
	// burn_trx_amount / blackhole). Failures here are unwound by
	// reverting to the pre-Execute snapshot — keeps state consistent if
	// the caller doesn't have enough TRX to cover the overage. Mirrors
	// java's BalanceInsufficientException re-throw at line 299 of
	// ReceiptCapsule.java.
	if err := actuator.PayEnergyBill(ctx, result); err != nil {
		revertTx()
		return nil, fmt.Errorf("pay energy bill: %w", err)
	}

	result.NetUsage = bwResult.NetUsage
	result.NetFee = bwResult.NetFee
	result.NetFeeForBandwidth = bwResult.NetFeeForBandwidth
	result.Fee += multiSignFee + memoFee

	return result, nil
}

// buildTransactionInfo constructs a TransactionInfo proto from the execution result.
func buildTransactionInfo(tx *types.Transaction, result *actuator.Result, blockNum uint64, blockTime int64, supportTransactionFeePool bool) *corepb.TransactionInfo {
	txID := tx.Hash()
	isVMContract := isVMContractType(tx.ContractType())

	// Receipt fields mirror java-tron `Protocol.ResourceReceipt`: EnergyUsage
	// is the stake-funded portion only (proto field 1), EnergyUsageTotal is
	// the full VM energy spent (proto field 4) and EnergyFee is the
	// balance-paid bill in SUN (proto field 2). The split between
	// EnergyUsed/EnergyFee is set by actuator.PayEnergyBill.
	info := &corepb.TransactionInfo{
		Id:             txID[:],
		Fee:            result.Fee + result.NetFee,
		BlockNumber:    int64(blockNum),
		BlockTimeStamp: blockTime,
		Receipt: &corepb.ResourceReceipt{
			EnergyUsage:       result.EnergyUsed,
			EnergyFee:         result.EnergyFee,
			OriginEnergyUsage: result.OriginEnergyUsage,
			EnergyUsageTotal:  result.EnergyUsageTotal,
			NetUsage:          result.NetUsage,
			NetFee:            result.NetFee,
		},
	}
	if isVMContract {
		info.Receipt.Result = corepb.Transaction_ResultContractResult(result.ContractRet)
	}
	if supportTransactionFeePool {
		if result.NetFeeForBandwidth {
			info.PackingFee += result.NetFee
		}
		if corepb.Transaction_ResultContractResult(result.ContractRet) != corepb.Transaction_Result_OUT_OF_TIME {
			info.PackingFee += result.EnergyFee
		}
	}

	if result.ContractResultPresent || len(result.ContractResult) > 0 {
		info.ContractResult = [][]byte{result.ContractResult}
	} else if !isVMContract && result.ContractRet == int32(corepb.Transaction_Result_SUCCESS) {
		info.ContractResult = [][]byte{{}}
	}

	if len(result.ContractAddress) > 0 {
		info.ContractAddress = result.ContractAddress
	}
	if result.AssetIssueID != "" {
		info.AssetIssueID = result.AssetIssueID
	}
	if result.WithdrawAmount != 0 {
		info.WithdrawAmount = result.WithdrawAmount
	}
	if result.UnfreezeAmount != 0 {
		info.UnfreezeAmount = result.UnfreezeAmount
	}
	if result.WithdrawExpireAmount != 0 {
		info.WithdrawExpireAmount = result.WithdrawExpireAmount
	}
	if len(result.CancelUnfreezeV2Amount) > 0 {
		info.CancelUnfreezeV2Amount = result.CancelUnfreezeV2Amount
	}
	if result.ExchangeReceivedAmount != 0 {
		info.ExchangeReceivedAmount = result.ExchangeReceivedAmount
	}
	if result.ExchangeInjectAnotherAmount != 0 {
		info.ExchangeInjectAnotherAmount = result.ExchangeInjectAnotherAmount
	}
	if result.ExchangeWithdrawAnotherAmount != 0 {
		info.ExchangeWithdrawAnotherAmount = result.ExchangeWithdrawAnotherAmount
	}
	if result.ShieldedTransactionFee != 0 {
		info.ShieldedTransactionFee = result.ShieldedTransactionFee
	}
	if result.ExchangeID != 0 {
		info.ExchangeId = result.ExchangeID
	}
	if len(result.OrderID) > 0 {
		info.OrderId = result.OrderID
	}
	if len(result.OrderDetails) > 0 {
		info.OrderDetails = result.OrderDetails
	}

	for _, l := range result.Logs {
		pbLog := &corepb.TransactionInfo_Log{
			Address: transactionInfoLogAddress(l.Address),
			Data:    l.Data,
		}
		for _, topic := range l.Topics {
			pbLog.Topics = append(pbLog.Topics, topic)
		}
		info.Log = append(info.Log, pbLog)
	}
	if len(result.InternalTransactions) > 0 {
		info.InternalTransactions = append(info.InternalTransactions, result.InternalTransactions...)
	}

	if result.ContractRet > 1 {
		info.Result = corepb.TransactionInfo_FAILED
		if len(result.ResMessage) > 0 {
			info.ResMessage = result.ResMessage
		}
	}

	return info
}

func buildTransactionResult(result *actuator.Result) *corepb.Transaction_Result {
	ret := &corepb.Transaction_Result{
		Ret:         corepb.Transaction_Result_SUCESS,
		ContractRet: corepb.Transaction_ResultContractResult(result.ContractRet),
	}
	if result.AssetIssueID != "" {
		ret.AssetIssueID = result.AssetIssueID
	}
	if result.WithdrawAmount != 0 {
		ret.WithdrawAmount = result.WithdrawAmount
	}
	if result.UnfreezeAmount != 0 {
		ret.UnfreezeAmount = result.UnfreezeAmount
	}
	if result.WithdrawExpireAmount != 0 {
		ret.WithdrawExpireAmount = result.WithdrawExpireAmount
	}
	if len(result.CancelUnfreezeV2Amount) > 0 {
		ret.CancelUnfreezeV2Amount = result.CancelUnfreezeV2Amount
	}
	if result.ExchangeReceivedAmount != 0 {
		ret.ExchangeReceivedAmount = result.ExchangeReceivedAmount
	}
	if result.ExchangeInjectAnotherAmount != 0 {
		ret.ExchangeInjectAnotherAmount = result.ExchangeInjectAnotherAmount
	}
	if result.ExchangeWithdrawAnotherAmount != 0 {
		ret.ExchangeWithdrawAnotherAmount = result.ExchangeWithdrawAnotherAmount
	}
	if result.ShieldedTransactionFee != 0 {
		ret.ShieldedTransactionFee = result.ShieldedTransactionFee
	}
	if result.ExchangeID != 0 {
		ret.ExchangeId = result.ExchangeID
	}
	if len(result.OrderID) > 0 {
		ret.OrderId = result.OrderID
	}
	if len(result.OrderDetails) > 0 {
		ret.OrderDetails = result.OrderDetails
	}
	return ret
}

func isVMContractType(contractType corepb.Transaction_Contract_ContractType) bool {
	return contractType == corepb.Transaction_Contract_CreateSmartContract ||
		contractType == corepb.Transaction_Contract_TriggerSmartContract
}

func transactionInfoLogAddress(addr tcommon.Address) []byte {
	if addr[0] == tcommon.AddressPrefixMainnet {
		return append([]byte(nil), addr[1:]...)
	}
	return addr.Bytes()
}

// ProcessBlock executes all transactions in a block and pays the block reward.
// It does NOT commit state — the caller (InsertBlock/BuildBlock) is responsible
// for committing after any post-processing (e.g., maintenance).
// Returns the TransactionInfos for all executed transactions.
//
// validateEnvelope toggles per-tx signature/permission verification inside
// the tx loop. Production callers (BlockChain.applyBlock when the engine
// is wired) pass true; test fixtures that bypass envelope checks pass false.
//
// The db parameter carries non-rooted chain/runtime data visible during
// execution, such as TAPOS references and genesis witness metadata. Mutable
// state writes go through StateDB typed stores.
func ProcessBlock(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, genesisTimestamp int64, validateEnvelope bool, genesisHashOpt ...tcommon.Hash) ([]*corepb.TransactionInfo, error) {
	txInfos, _, err := processBlock(statedb, dynProps, block, db, activeWitnesses, genesisTimestamp, params.DefaultBlockNumForEnergyLimit, validateEnvelope, optionalGenesisHash(genesisHashOpt), nil, nil, nil)
	return txInfos, err
}

func ProcessBlockWithJavaAccountStateRoot(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, genesisTimestamp int64, validateEnvelope bool, parentAccountStateRoot tcommon.Hash, genesisHashOpt ...tcommon.Hash) ([]*corepb.TransactionInfo, tcommon.Hash, error) {
	return processBlock(statedb, dynProps, block, db, activeWitnesses, genesisTimestamp, params.DefaultBlockNumForEnergyLimit, validateEnvelope, optionalGenesisHash(genesisHashOpt), &parentAccountStateRoot, nil, nil)
}

func ProcessBlockWithEnergyFork(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, genesisTimestamp int64, energyLimitForkBlockNum int64, validateEnvelope bool, genesisHashOpt ...tcommon.Hash) ([]*corepb.TransactionInfo, error) {
	txInfos, _, err := processBlock(statedb, dynProps, block, db, activeWitnesses, genesisTimestamp, energyLimitForkBlockNum, validateEnvelope, optionalGenesisHash(genesisHashOpt), nil, nil, nil)
	return txInfos, err
}

func ProcessBlockWithJavaAccountStateRootAndEnergyFork(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, genesisTimestamp int64, energyLimitForkBlockNum int64, validateEnvelope bool, parentAccountStateRoot tcommon.Hash, genesisHashOpt ...tcommon.Hash) ([]*corepb.TransactionInfo, tcommon.Hash, error) {
	return processBlock(statedb, dynProps, block, db, activeWitnesses, genesisTimestamp, energyLimitForkBlockNum, validateEnvelope, optionalGenesisHash(genesisHashOpt), &parentAccountStateRoot, nil, nil)
}

func optionalGenesisHash(values []tcommon.Hash) tcommon.Hash {
	if len(values) == 0 {
		return tcommon.Hash{}
	}
	return values[0]
}

func processBlock(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, genesisTimestamp int64, energyLimitForkBlockNum int64, validateEnvelope bool, genesisHash tcommon.Hash, parentAccountStateRoot *tcommon.Hash, standbyPaySet *standbyWitnessPaySet, domainChanges *state.DomainChangeStage) (txInfos []*corepb.TransactionInfo, javaAccountStateRoot tcommon.Hash, err error) {
	blockSnap := statedb.Snapshot()
	dpProps, dpDirty := dynProps.Snapshot()
	defer func() {
		if err != nil {
			statedb.RevertToSnapshot(blockSnap)
			dynProps.Restore(dpProps, dpDirty)
		}
	}()

	// Reset per-block energy accumulator (matches java-tron Manager.processBlock).
	dynProps.SetBlockEnergyUsage(0)

	// Snapshot the chain head's timestamp before the tx loop. java-tron's
	// Manager.applyBlock runs processTransaction *before*
	// updateDynamicProperties(block), so during tx Execute the DP value
	// LatestBlockHeaderTimestamp is still the *previous* block's timestamp.
	// blockchain.go advances `LatestBlockHeaderTimestamp` only after this
	// function returns, so reading the DP here yields the prev-block
	// timestamp for the entire block.
	prevBlockTime := dynProps.LatestBlockHeaderTimestamp()
	prevBlockHeadSlot := HeadSlot(prevBlockTime, genesisTimestamp)

	writeHistoryBlockHash(statedb, dynProps, block.Number(), block.ParentHash())
	accountStateMark := statedb.JournalMark()

	for i, tx := range block.Transactions() {
		domainChangeMark := statedb.DomainChangeJournalMark()
		if domainChanges != nil {
			domainChangeMark = domainChanges.JournalMark()
		}
		if dynProps.ConsensusLogicOptimization() {
			if err := ValidateTxRetCount(tx); err != nil {
				return nil, tcommon.Hash{}, fmt.Errorf("tx %d: %w", i, err)
			}
		}
		// validate=true: replay calls actuator.Validate (P0-2a). Every
		// actuator's Validate is read-only (audited 2026-05-15) so re-running
		// on replay matches java-tron Manager.processBlock parity.
		//
		// validateEnvelope is per-tx so a same-block tx2 sees tx1's effects
		// (e.g. an AccountPermissionUpdate followed by a Transfer signed with
		// the post-rotation key).
		result, err := applyTransaction(statedb, dynProps, tx, prevBlockTime, true, prevBlockHeadSlot, block.Timestamp(), block.Number(), db, activeWitnesses, energyLimitForkBlockNum, genesisHash, block.WitnessAddress(), true, validateEnvelope, true)
		if err != nil {
			return nil, tcommon.Hash{}, fmt.Errorf("tx %d: %w", i, err)
		}
		if err := ValidateTxVMContractRet(tx, corepb.Transaction_ResultContractResult(result.ContractRet)); err != nil {
			return nil, tcommon.Hash{}, fmt.Errorf("tx %d: %w", i, err)
		}
		info := buildTransactionInfo(tx, result, block.Number(), block.Timestamp(), dynProps.AllowTransactionFeePool())
		txInfos = append(txInfos, info)
		statedb.FinalizeTransaction()
		if domainChanges != nil {
			if err := domainChanges.FlushOrdinal(domainChangeMark, uint64(i)); err != nil {
				return nil, tcommon.Hash{}, fmt.Errorf("tx %d domain changes: %w", i, err)
			}
		} else {
			txNum, err := statedb.DomainChangeTxNumAtOrdinal(uint64(i))
			if err != nil {
				return nil, tcommon.Hash{}, fmt.Errorf("tx %d state txNum: %w", i, err)
			}
			if err := statedb.FlushDomainChangesSince(domainChangeMark, txNum); err != nil {
				return nil, tcommon.Hash{}, fmt.Errorf("tx %d domain changes: %w", i, err)
			}
		}

		accumulateBlockEnergyUsage(dynProps, statedb, prevBlockTime, result)
	}

	if parentAccountStateRoot != nil {
		javaAccountStateRoot, err = defaultStateRootAdapter.JavaAccountStateRoot(statedb, *parentAccountStateRoot, accountStateMark)
		if err != nil {
			return nil, tcommon.Hash{}, fmt.Errorf("account state root: %w", err)
		}
	}

	// Per-block adaptive energy limit adjustment.
	if dynProps.AllowAdaptiveEnergy() {
		UpdateTotalEnergyAverageUsage(dynProps, genesisTimestamp)
		UpdateAdaptiveTotalEnergyLimit(dynProps)
	}

	// Pay block reward to witness (and standby top-127 when change_delegation
	// is active — the new-algorithm reward path goes through payBlockReward
	// which splits by brokerage and accumulates the voter pool). java-tron
	// runs this after adaptive-energy updates, then pays the transaction-fee
	// pool reward from the same payReward path.
	witnessAddr := block.WitnessAddress()
	if witnessAddr != (tcommon.Address{}) {
		payBlockReward(db, statedb, dynProps, witnessAddr, dynProps.WitnessPayPerBlock())
		payStandbyWitnessWithSet(db, statedb, dynProps, standbyPaySet)
		payTransactionFeeReward(db, statedb, dynProps, witnessAddr)
	}

	return txInfos, javaAccountStateRoot, nil
}
