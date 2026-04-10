package core

import (
	"fmt"

	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
)

// ApplyTransaction executes a single transaction against the given state.
// Returns the fee charged by the actuator.
func ApplyTransaction(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, blockTime int64, blockNum uint64) (int64, error) {
	act, err := actuator.CreateActuator(tx)
	if err != nil {
		return 0, fmt.Errorf("create actuator: %w", err)
	}

	ctx := &actuator.Context{
		State:       statedb,
		DynProps:    dynProps,
		Tx:          tx,
		BlockTime:   blockTime,
		BlockNumber: blockNum,
	}

	if err := act.Validate(ctx); err != nil {
		return 0, fmt.Errorf("validate: %w", err)
	}

	// Consume bandwidth
	if err := consumeBandwidth(statedb, dynProps, tx, blockTime); err != nil {
		return 0, fmt.Errorf("bandwidth: %w", err)
	}

	snap := statedb.Snapshot()
	result, err := act.Execute(ctx)
	if err != nil {
		statedb.RevertToSnapshot(snap)
		return 0, fmt.Errorf("execute: %w", err)
	}

	return result.Fee, nil
}

// ProcessBlock executes all transactions in a block and returns the new state root.
func ProcessBlock(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block) (tcommon.Hash, error) {
	for i, tx := range block.Transactions() {
		_, err := ApplyTransaction(statedb, dynProps, tx, block.Timestamp(), block.Number())
		if err != nil {
			return tcommon.Hash{}, fmt.Errorf("tx %d: %w", i, err)
		}
	}

	// Pay block reward to witness
	witnessAddr := block.WitnessAddress()
	if witnessAddr != (tcommon.Address{}) {
		reward := dynProps.WitnessPayPerBlock()
		if reward > 0 {
			statedb.AddAllowance(witnessAddr, reward)
		}
	}

	// Commit state to get new root
	root, err := statedb.Commit()
	if err != nil {
		return tcommon.Hash{}, fmt.Errorf("commit state: %w", err)
	}

	return root, nil
}
