package core

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
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

	// Open StateDB from parent's state root (side store keyed by block
	// hash; falls back to the genesis state root for block #1). Goes
	// through bc.chaindb so frozen blocks transparently resolve through
	// the freezer's `state_roots` table.
	parentRoot := rawdb.ReadBlockStateRoot(bc.chaindb, parent.Hash())
	if parentRoot == (tcommon.Hash{}) && parent.Number() == 0 {
		parentRoot = rawdb.ReadGenesisStateRoot(bc.db)
	}
	if parentRoot == (tcommon.Hash{}) {
		parentRoot = parent.AccountStateRoot()
	}
	statedb, err := state.New(parentRoot, bc.stateDB)
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}

	// statedb is already opened at parentRoot; reuse it as the system-KV reader
	// so rooted dynprops load from the parent state the built block extends.
	dynProps := state.LoadDynamicProperties(bc.buffer, statedb)

	// Load witnesses into statedb for maintenance access. Reads go through
	// bc.buffer to mirror applyBlock — the chain buffer holds VoteCount /
	// URL deltas from blocks that haven't yet been flushed to disk, and we
	// must see the same values applyBlock will see when it inserts the
	// block we're about to build.
	//
	// Uses LoadWitness (not PutWitness+AddWitnessVoteCount) so this hydration
	// pass does not mark witnesses dirty — see applyBlock for the rationale.
	witnessAddrs := rawdb.ReadWitnessIndex(bc.buffer)
	for _, addr := range witnessAddrs {
		if statedb.GetWitness(addr) == nil {
			statedb.LoadWitness(rawdb.ReadWitness(bc.buffer, addr))
		}
	}

	// Pull all pending transactions
	pendingTxs := pool.Pending()

	// Reset per-block energy accumulator.
	dynProps.SetBlockEnergyUsage(0)

	// Throwaway buffer: all rawdb-accumulator writes during block assembly
	// (cycle rewards, brokerage snapshots, VI accumulations) go here and are
	// never flushed to disk. Reads fall through to bc.buffer first so pending
	// but not-yet-solidified writes from prior blocks (including shielded
	// Merkle anchors) are visible during block assembly. The statedb still sees
	// the full reward (correct account_state_root), and InsertBlock → applyBlock
	// → ProcessBlock is the single canonical rawdb write path. Without this,
	// BuildBlock would write to bc.db directly, then applyBlock would read those
	// values and add again — doubling cycleReward[N][witness] and allowance.
	buildBuf := blockbuffer.New(bc.buffer)
	buildBuf.BeginBlock(tcommon.Hash{}) // sentinel hash; this layer is never committed

	// Execute transactions, collecting successful ones
	var appliedTxProtos []*corepb.Transaction
	var failedTxIDs []tcommon.Hash
	blockNum := parent.Number() + 1

	// The block being built has not yet committed, so the chain head
	// timestamp is the parent's — same value java-tron actuators see via
	// LatestBlockHeaderTimestamp during processTransaction.
	prevBlockTime := parent.Timestamp()
	prevBlockHeadSlot := HeadSlot(prevBlockTime, bc.GenesisTimestamp())
	writeHistoryBlockHash(statedb, dynProps, blockNum, parent.Hash())
	accountStateMark := statedb.JournalMark()

	for _, tx := range pendingTxs {
		// Producer pulls from txpool whose Add gate already validates the
		// envelope; re-validating here would re-recover signatures for every
		// pending tx on every slot. Trust the pool, run only actuator.Validate.
		result, err := ApplyTransactionWithResourceSlotAndEnergyFork(statedb, dynProps, tx, prevBlockTime, prevBlockHeadSlot, timestamp, blockNum, buildBuf, bc.ActiveWitnesses(), bc.config.EnergyLimitForkBlockNum(), true, false)
		if err != nil {
			h := tx.Hash()
			log.Debug("Skipping tx in build", "tx", fmt.Sprintf("%x", h[:8]), "err", err)
			failedTxIDs = append(failedTxIDs, h)
			continue // skip failing transactions
		}
		txPB := proto.Clone(tx.Proto()).(*corepb.Transaction)
		txPB.Ret = []*corepb.Transaction_Result{buildTransactionResult(result)}
		appliedTxProtos = append(appliedTxProtos, txPB)
		statedb.FinalizeTransaction()
		accumulateBlockEnergyUsage(dynProps, buildBuf, prevBlockTime, result)
	}

	var accountStateRoot tcommon.Hash
	if dynProps.AllowAccountStateRoot() {
		accountStateRoot, err = statedb.JavaAccountStateRoot(parent.AccountStateRoot(), accountStateMark)
		if err != nil {
			return nil, fmt.Errorf("account state root: %w", err)
		}
	}

	// Per-block adaptive energy limit adjustment.
	if dynProps.AllowAdaptiveEnergy() {
		UpdateTotalEnergyAverageUsage(dynProps, bc.GenesisTimestamp())
		UpdateAdaptiveTotalEnergyLimit(dynProps)
	}

	// Pay block reward to witness (brokerage-aware once change_delegation is on)
	// and drain the transaction-fee pool share. Writes go through buildBuf
	// (throwaway) so they don't reach disk here.
	payBlockReward(buildBuf, statedb, dynProps, witnessAddr, dynProps.WitnessPayPerBlock())
	payStandbyWitness(buildBuf, statedb, dynProps)
	payTransactionFeeReward(buildBuf, statedb, dynProps, witnessAddr)

	// Run maintenance if at boundary (before commit so allowances are included)
	if dynProps.NextMaintenanceTime() > 0 && timestamp >= dynProps.NextMaintenanceTime() {
		if err := ProcessProposals(buildBuf, dynProps, bc.ActiveWitnesses(), timestamp, bc.fc, statedb); err != nil {
			return nil, fmt.Errorf("process proposals: %w", err)
		}
		adapter := &chainHeaderAdapter{
			statedb:          statedb,
			dynProps:         dynProps,
			genesisWitnesses: bc.genesisWitnesses,
		}
		allWitnesses := bc.gatherWitnessVotes(statedb)
		dpos.TryRemoveThePowerOfTheGr(adapter, allWitnesses)
		applyRewardVI(buildBuf, statedb, dynProps)
		hasPendingVotes := applyPendingVotes(buildBuf, statedb)
		if hasPendingVotes {
			allWitnesses = bc.gatherWitnessVotes(statedb)
			sorted := dpos.SortWitnessesByVotesWithOptimization(allWitnesses, dynProps.ConsensusLogicOptimization())
			if !dynProps.ChangeDelegation() {
				dpos.DistributeLegacyStandby(adapter, sorted)
			}
		}
		// Writes go through buildBuf (throwaway); applyBlock's maintenance
		// path is the canonical writer.
		applyRewardCycleSnapshot(buildBuf, statedb, dynProps)
		nextMaint := dpos.CalcNextMaintenanceTime(timestamp, dynProps.NextMaintenanceTime(), dynProps.MaintenanceTimeInterval())
		dynProps.SetNextMaintenanceTime(nextMaint)
	}

	// Stage rooted dynprops into the system account's KV before Commit, exactly
	// as applyBlock does, so the built block's internal state root matches what
	// the applier recomputes on the same block (producer/replay consistency).
	if err := dynProps.FlushRooted(statedb); err != nil {
		return nil, fmt.Errorf("flush rooted dynamic properties: %w", err)
	}

	// Commit state so the throwaway StateDB observes the same post-processing
	// path as applyBlock. The full root is persisted only by InsertBlock;
	// java-tron's block header carries the lightweight account-state root.
	_, err = statedb.Commit()
	if err != nil {
		return nil, fmt.Errorf("commit state: %w", err)
	}

	// Construct the block
	raw := &corepb.BlockHeaderRaw{
		Number:         int64(blockNum),
		Timestamp:      timestamp,
		ParentHash:     parent.Hash().Bytes(),
		WitnessAddress: witnessAddr.Bytes(),
		Version:        params.BlockVersion,
	}
	if dynProps.AllowAccountStateRoot() {
		raw.AccountStateRoot = accountStateRoot.Bytes()
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: raw,
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
