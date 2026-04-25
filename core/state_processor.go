package core

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// ApplyTransaction executes a single transaction against the given state.
// Returns the full actuator Result including fee, energy, net, and contract details.
func ApplyTransaction(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, blockTime int64, blockNum uint64, db ethdb.KeyValueStore, activeWitnesses []tcommon.Address) (*actuator.Result, error) {
	act, err := actuator.CreateActuator(tx)
	if err != nil {
		return nil, fmt.Errorf("create actuator: %w", err)
	}

	ctx := &actuator.Context{
		State:           statedb,
		DynProps:        dynProps,
		Tx:              tx,
		BlockTime:       blockTime,
		BlockNumber:     blockNum,
		DB:              db,
		ActiveWitnesses: activeWitnesses,
	}

	if err := act.Validate(ctx); err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}

	bwResult, err := consumeBandwidth(statedb, dynProps, tx, blockTime)
	if err != nil {
		return nil, fmt.Errorf("bandwidth: %w", err)
	}

	snap := statedb.Snapshot()
	result, err := act.Execute(ctx)
	if err != nil {
		statedb.RevertToSnapshot(snap)
		return nil, fmt.Errorf("execute: %w", err)
	}

	result.NetUsage = bwResult.NetUsage
	result.NetFee = bwResult.NetFee

	return result, nil
}

// buildTransactionInfo constructs a TransactionInfo proto from the execution result.
func buildTransactionInfo(tx *types.Transaction, result *actuator.Result, blockNum uint64, blockTime int64) *corepb.TransactionInfo {
	txID := tx.Hash()

	info := &corepb.TransactionInfo{
		Id:             txID[:],
		Fee:            result.Fee + result.NetFee,
		BlockNumber:    int64(blockNum),
		BlockTimeStamp: blockTime,
		Receipt: &corepb.ResourceReceipt{
			EnergyUsage:       result.EnergyUsed,
			EnergyFee:         result.EnergyFee,
			OriginEnergyUsage: result.OriginEnergyUsage,
			EnergyUsageTotal:  result.EnergyUsed + result.OriginEnergyUsage,
			NetUsage:          result.NetUsage,
			NetFee:            result.NetFee,
			Result:            corepb.Transaction_ResultContractResult(result.ContractRet),
		},
	}

	if len(result.ContractResult) > 0 {
		info.ContractResult = [][]byte{result.ContractResult}
	}

	if len(result.ContractAddress) > 0 {
		info.ContractAddress = result.ContractAddress
	}

	for _, l := range result.Logs {
		pbLog := &corepb.TransactionInfo_Log{
			Address: l.Address[:],
			Data:    l.Data,
		}
		for _, topic := range l.Topics {
			pbLog.Topics = append(pbLog.Topics, topic)
		}
		info.Log = append(info.Log, pbLog)
	}

	if result.ContractRet > 1 {
		info.Result = corepb.TransactionInfo_FAILED
		if result.ContractRet == 2 && len(result.ContractResult) > 0 {
			info.ResMessage = result.ContractResult
		}
	}

	return info
}

// ProcessBlock executes all transactions in a block and pays the block reward.
// It does NOT commit state — the caller (InsertBlock/BuildBlock) is responsible
// for committing after any post-processing (e.g., maintenance).
// Returns the TransactionInfos for all executed transactions.
func ProcessBlock(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block, db ethdb.KeyValueStore, activeWitnesses []tcommon.Address, genesisTimestamp int64) ([]*corepb.TransactionInfo, error) {
	// Reset per-block energy accumulator (matches java-tron Manager.processBlock).
	dynProps.SetBlockEnergyUsage(0)

	var txInfos []*corepb.TransactionInfo

	for i, tx := range block.Transactions() {
		result, err := ApplyTransaction(statedb, dynProps, tx, block.Timestamp(), block.Number(), db, activeWitnesses)
		if err != nil {
			return nil, fmt.Errorf("tx %d: %w", i, err)
		}
		info := buildTransactionInfo(tx, result, block.Number(), block.Timestamp())
		txInfos = append(txInfos, info)

		if dynProps.AllowAdaptiveEnergy() && result.EnergyUsed > 0 {
			dynProps.SetBlockEnergyUsage(dynProps.BlockEnergyUsage() + result.EnergyUsed)
		}
	}

	// Pay block reward to witness (and standby top-127 when change_delegation
	// is active — the new-algorithm reward path goes through payBlockReward
	// which splits by brokerage and accumulates the voter pool).
	witnessAddr := block.WitnessAddress()
	if witnessAddr != (tcommon.Address{}) {
		payBlockReward(db, statedb, dynProps, witnessAddr, dynProps.WitnessPayPerBlock())
	}
	payStandbyWitness(db, statedb, dynProps)

	// Per-block adaptive energy limit adjustment.
	if dynProps.AllowAdaptiveEnergy() {
		UpdateTotalEnergyAverageUsage(dynProps, genesisTimestamp)
		UpdateAdaptiveTotalEnergyLimit(dynProps)
	}

	return txInfos, nil
}
