package core

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// BuildResult holds the output of BuildBlock.
type BuildResult struct {
	Block       *types.Block
	FailedTxIDs []tcommon.Hash // transactions that failed validation and should be evicted
}

// BuildBlock assembles a new block from pending transactions.
// Failing transactions are skipped rather than aborting the block.
// The returned block is unsigned — call SignBlock separately.
func BuildBlock(bc *BlockChain, pool *txpool.TxPool, witnessAddr tcommon.Address, timestamp int64) (*BuildResult, error) {
	parent := bc.CurrentBlock()

	// Open StateDB from parent's state root
	parentRoot := parent.AccountStateRoot()
	statedb, err := state.New(parentRoot, bc.stateDB)
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}

	dynProps := state.LoadDynamicProperties(bc.db)

	// Load witnesses into statedb for maintenance access
	witnessAddrs := rawdb.ReadWitnessIndex(bc.db)
	for _, addr := range witnessAddrs {
		if statedb.GetWitness(addr) == nil {
			w := rawdb.ReadWitness(bc.db, addr)
			if w != nil {
				statedb.PutWitness(addr, w.URL())
				statedb.AddWitnessVoteCount(addr, w.VoteCount())
			}
		}
	}

	// Pull all pending transactions
	pendingTxs := pool.Pending()

	// Reset per-block energy accumulator.
	dynProps.SetBlockEnergyUsage(0)

	// Execute transactions, collecting successful ones
	var appliedTxProtos []*corepb.Transaction
	var failedTxIDs []tcommon.Hash
	blockNum := parent.Number() + 1

	for _, tx := range pendingTxs {
		result, err := ApplyTransaction(statedb, dynProps, tx, timestamp, blockNum, bc.db, bc.ActiveWitnesses())
		if err != nil {
			failedTxIDs = append(failedTxIDs, tx.Hash())
			continue // skip failing transactions
		}
		appliedTxProtos = append(appliedTxProtos, tx.Proto())
		if dynProps.AllowAdaptiveEnergy() && result.EnergyUsed > 0 {
			dynProps.SetBlockEnergyUsage(dynProps.BlockEnergyUsage() + result.EnergyUsed)
		}
	}

	// Pay block reward to witness (brokerage-aware once change_delegation is on).
	payBlockReward(bc.db, statedb, dynProps, witnessAddr, dynProps.WitnessPayPerBlock())
	payStandbyWitness(bc.db, statedb, dynProps)

	// Per-block adaptive energy limit adjustment.
	if dynProps.AllowAdaptiveEnergy() {
		UpdateTotalEnergyAverageUsage(dynProps, bc.GenesisTimestamp())
		UpdateAdaptiveTotalEnergyLimit(dynProps)
	}

	// Run maintenance if at boundary (before commit so allowances are included)
	if dynProps.NextMaintenanceTime() > 0 && timestamp >= dynProps.NextMaintenanceTime() {
		allWitnesses := bc.gatherWitnessVotes(statedb)
		dpos.DoMaintenance(&chainHeaderAdapter{statedb: statedb, dynProps: dynProps}, timestamp, allWitnesses)
		applyRewardMaintenance(bc.db, statedb, dynProps)
		newActive := dpos.SelectActiveWitnesses(allWitnesses)
		bc.SetActiveWitnesses(newActive)
		if err := ProcessProposals(bc.db, dynProps, newActive, timestamp, bc.fc); err != nil {
			return nil, fmt.Errorf("process proposals: %w", err)
		}
	}

	// Commit state to get the root
	root, err := statedb.Commit()
	if err != nil {
		return nil, fmt.Errorf("commit state: %w", err)
	}

	// Construct the block
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:           int64(blockNum),
				Timestamp:        timestamp,
				ParentHash:       parent.Hash().Bytes(),
				WitnessAddress:   witnessAddr.Bytes(),
				AccountStateRoot: root.Bytes(),
				Version:          params.BlockVersion,
			},
		},
		Transactions: appliedTxProtos,
	})

	return &BuildResult{Block: block, FailedTxIDs: failedTxIDs}, nil
}

// SignBlock signs the block with the witness private key.
// The signature is SHA256(marshaled BlockHeaderRaw) signed with ECDSA.
func SignBlock(block *types.Block, privKey *ecdsa.PrivateKey) error {
	headerRaw := block.Proto().BlockHeader.RawData
	data, err := proto.Marshal(headerRaw)
	if err != nil {
		return fmt.Errorf("marshal header: %w", err)
	}

	hash := sha256.Sum256(data)
	sig, err := crypto.Sign(hash[:], privKey)
	if err != nil {
		return fmt.Errorf("sign: %w", err)
	}

	block.SetWitnessSignature(sig)
	return nil
}
