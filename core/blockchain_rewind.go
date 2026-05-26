package core

import (
	"errors"
	"fmt"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
)

const restartSyncReplayBatchSize = 100

// RestartSyncProgress is emitted by RestartSyncFromHeight after each major
// phase and replayed block. Block is meaningful for replay/done phases.
type RestartSyncProgress struct {
	Phase  string
	Block  uint64
	Target uint64
}

// RestartSyncFromHeight rewinds the local materialized state to height and
// leaves the chain ready to request height+1 from peers.
//
// This is an offline startup operation. Call it before P2P, producer, PBFT, and
// API hooks are registered; it intentionally replays canonical blocks through
// the same staged range importer used by sync and therefore would otherwise
// re-fire apply hooks.
//
// The implementation is deliberately conservative: it clears every
// replay-derived flat namespace, reseeds genesis, replays blocks [1, height] in
// staged batches, then force-flushes all blockbuffer layers. That makes the
// result correct even while some consensus stores still live outside the rooted
// account/state KV.
func (bc *BlockChain) RestartSyncFromHeight(height uint64, genesis *params.Genesis, ancient rawdb.AncientWriter, progressFn func(RestartSyncProgress)) error {
	if bc == nil {
		return errors.New("restart sync: nil blockchain")
	}
	if genesis == nil || genesis.Config == nil {
		return errors.New("restart sync: genesis with chain config is required")
	}

	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

	current := bc.CurrentBlock()
	if current == nil {
		return errors.New("restart sync: current block is nil")
	}
	if height > current.Number() {
		return fmt.Errorf("restart sync: target height %d exceeds current head %d", height, current.Number())
	}
	target := rawdb.ReadBlock(bc.chaindb, height)
	if target == nil {
		return fmt.Errorf("restart sync: canonical block %d not found", height)
	}
	targetHash := target.Hash()

	emit := func(phase string, block uint64) {
		if progressFn != nil {
			progressFn(RestartSyncProgress{Phase: phase, Block: block, Target: height})
		}
	}

	emit("reset", 0)
	bc.WaitForFlushSettled()
	if errPtr := bc.flushErr.Load(); errPtr != nil {
		return fmt.Errorf("restart sync: pending async flush failed: %w", *errPtr)
	}
	bc.buffer.Discard()
	if ancient != nil {
		if _, err := ancient.TruncateHead(height + 1); err != nil {
			return fmt.Errorf("restart sync: truncate ancient head to %d: %w", height+1, err)
		}
		if err := ancient.Sync(); err != nil {
			return fmt.Errorf("restart sync: sync ancient truncate: %w", err)
		}
	}
	if err := rawdb.ResetMutableState(bc.db); err != nil {
		return fmt.Errorf("restart sync: reset mutable state: %w", err)
	}

	emit("genesis", 0)
	genesisBlock, genesisRoot, genesisDP, err := genesisBlockAndStateRoot(genesis, bc.stateDB)
	if err != nil {
		return fmt.Errorf("restart sync: rebuild genesis state: %w", err)
	}
	if bc.genesisBlock != nil && genesisBlock.Hash() != bc.genesisBlock.Hash() {
		return errors.New("restart sync: genesis hash mismatch after rebuild")
	}
	if err := writeGenesisMaterializedState(bc.db, genesis, genesisBlock, genesisRoot, genesisDP); err != nil {
		return fmt.Errorf("restart sync: write genesis state: %w", err)
	}
	bc.resetRuntimeStateLocked(genesisBlock, genesisRoot)

	for start := uint64(1); start <= height; {
		end := start + restartSyncReplayBatchSize - 1
		if end < start || end > height {
			end = height
		}
		blocks := make([]*types.Block, 0, end-start+1)
		for n := start; n <= end; n++ {
			block := rawdb.ReadBlock(bc.chaindb, n)
			if block == nil {
				return fmt.Errorf("restart sync: block %d not found during replay", n)
			}
			if len(blocks) == 0 {
				parent := bc.CurrentBlock()
				if block.Number() != parent.Number()+1 {
					return fmt.Errorf("restart sync: block %d has number %d, want %d", n, block.Number(), parent.Number()+1)
				}
				if block.ParentHash() != parent.Hash() {
					return fmt.Errorf("restart sync: block %d parent mismatch: have %x want %x", n, block.ParentHash(), parent.Hash())
				}
			} else {
				parent := blocks[len(blocks)-1]
				if block.Number() != parent.Number()+1 {
					return fmt.Errorf("restart sync: block %d has number %d, want %d", n, block.Number(), parent.Number()+1)
				}
				if block.ParentHash() != parent.Hash() {
					return fmt.Errorf("restart sync: block %d parent mismatch: have %x want %x", n, block.ParentHash(), parent.Hash())
				}
			}
			blocks = append(blocks, block)
		}
		if err := bc.insertBlocksLocked(blocks); err != nil {
			var rangeErr *InsertBlocksError
			if errors.As(err, &rangeErr) {
				for i := 0; i < rangeErr.Index && i < len(blocks); i++ {
					emit("replay", blocks[i].Number())
				}
				if rangeErr.BlockNumber != 0 {
					return fmt.Errorf("restart sync: replay block %d: %w", rangeErr.BlockNumber, err)
				}
			}
			return fmt.Errorf("restart sync: replay block range %d-%d: %w", start, end, err)
		}
		for _, block := range blocks {
			emit("replay", block.Number())
		}
		start = end + 1
	}

	emit("flush", height)
	bc.WaitForFlushSettled()
	if errPtr := bc.flushErr.Load(); errPtr != nil {
		return fmt.Errorf("restart sync: async flush failed during replay: %w", *errPtr)
	}
	if err := bc.buffer.Flush(bc.db); err != nil {
		return fmt.Errorf("restart sync: flush replay buffer: %w", err)
	}
	bc.buffer.Discard()

	final := bc.CurrentBlock()
	if final == nil || final.Number() != height || final.Hash() != targetHash {
		if final == nil {
			return errors.New("restart sync: final head is nil")
		}
		return fmt.Errorf("restart sync: final head mismatch: got #%d %x want #%d %x", final.Number(), final.Hash(), height, targetHash)
	}
	rawdb.WriteHeadBlockHash(bc.db, final.Hash())
	if err := rewindCanonicalStagePipeline(bc.db, height, final.Hash()); err != nil {
		return fmt.Errorf("restart sync: rewind canonical stage progress: %w", err)
	}
	bc.resetRuntimeStateLocked(final, bc.HeadStateRoot())
	emit("done", height)
	return nil
}

func (bc *BlockChain) resetRuntimeStateLocked(head *types.Block, root tcommon.Hash) {
	bc.genesisWitnesses = bc.genesisWitnesses[:0]
	for _, gw := range rawdb.ReadGenesisWitnesses(bc.db) {
		bc.genesisWitnesses = append(bc.genesisWitnesses, consensus.GenesisWitnessInfo{
			Address:   gw.Address,
			VoteCount: gw.VoteCount,
		})
	}
	bc.currentBlock.Store(head)
	bc.lastInsertNano.Store(time.Now().UnixNano())
	bc.khaosDB = NewKhaosDB()
	bc.khaosDB.Start(head)
	bc.activeWitnesses.Store([]tcommon.Address(nil))
	bc.reloadActiveWitnesses(root)
	bc.storeDynPropsCache(state.LoadDynamicProperties(bc.buffer, bc.sysKVAt(root)))
	bc.fc = forks.NewForkController(bc.buffer)
	bc.invalidateStandbyPayCache()
	bc.clearSystemAccountCache()
	bc.clearRewardAccountCache()
	bc.clearWitnessBlockCache()
	bc.clearForkStatsCache()
}
