package core

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	"google.golang.org/protobuf/proto"
)

type BalanceTraceReplayBackfillOptions struct {
	FromBlock         uint64
	ToBlock           uint64
	Overwrite         bool
	ETL               etl.Options
	TargetTraceReader ethdb.KeyValueReader
	Progress          func(BalanceTraceReplayBackfillProgress)
}

type BalanceTraceReplayBackfillProgress struct {
	Phase  string
	Block  uint64
	Target uint64
}

type BalanceTraceReplayBackfillResult struct {
	FromBlock             uint64
	ToBlock               uint64
	ReplayStartBlock      uint64
	ReplayHeadBlock       uint64
	BlocksReplayed        uint64
	BlocksBackfilled      uint64
	BlockTraceRows        uint64
	AccountTraceRows      uint64
	ExistingBlockTraces   uint64
	ExistingAccountTraces uint64
	ETL                   etl.Stats
}

// BackfillBalanceTracesByReplay rebuilds java-tron-compatible block/account
// balance trace rows by replaying canonical blocks into an isolated database.
// The source chain is read-only; only trace rows for [FromBlock, ToBlock] are
// copied back to target. The replay database may be empty or may already hold a
// replayed canonical prefix, letting interrupted backfills resume without
// replaying from genesis again.
func BackfillBalanceTracesByReplay(source *rawdb.ChainDB, target ethdb.KeyValueStore, replayDB ethdb.KeyValueStore, genesis *params.Genesis, opts BalanceTraceReplayBackfillOptions) (*BalanceTraceReplayBackfillResult, error) {
	if source == nil {
		return nil, errors.New("core: nil source chain db")
	}
	if target == nil {
		return nil, errors.New("core: nil balance trace target db")
	}
	if replayDB == nil {
		return nil, errors.New("core: nil balance trace replay db")
	}
	if genesis == nil || genesis.Config == nil {
		return nil, errors.New("core: nil genesis")
	}
	if opts.ToBlock < opts.FromBlock {
		return nil, fmt.Errorf("core: inverted balance trace backfill range [%d,%d]", opts.FromBlock, opts.ToBlock)
	}
	if opts.FromBlock == 0 {
		return nil, errors.New("core: balance trace replay backfill starts at block 1; genesis has no BlockBalanceTrace replay path")
	}
	result := &BalanceTraceReplayBackfillResult{
		FromBlock: opts.FromBlock,
		ToBlock:   opts.ToBlock,
	}
	targetTraceReader := opts.TargetTraceReader
	if targetTraceReader == nil {
		targetTraceReader = target
	}
	collector, err := rawdb.NewDerivedIndexCollector(opts.ETL)
	if err != nil {
		return nil, err
	}
	defer collector.Close()

	replayGenesis := cloneBalanceTraceBackfillGenesis(genesis)
	replayGenesis.Config.HistoryEnabled = true
	replayGenesis.Config.StateCommitmentCheckpoints = false

	_, replayGenesisHash, err := SetupGenesisBlockWithAncient(replayDB, rawdb.NoopAncient{}, replayGenesis)
	if err != nil {
		return nil, fmt.Errorf("setup replay genesis: %w", err)
	}
	sourceGenesis, err := readBalanceTraceBackfillSourceBlock(source, 0, "genesis check")
	if err != nil {
		return nil, err
	}
	if sourceGenesis.Hash() != replayGenesisHash {
		return nil, fmt.Errorf("core: replay genesis hash %x does not match source genesis %x", replayGenesisHash, sourceGenesis.Hash())
	}

	replayChain, err := NewBlockChain(replayDB, state.NewDatabase(rawdb.WrapKeyValueStore(replayDB)), replayGenesis.Config)
	if err != nil {
		return nil, fmt.Errorf("create replay blockchain: %w", err)
	}
	defer replayChain.Close()

	replayHead := replayChain.CurrentBlock()
	if replayHead == nil {
		return nil, errors.New("core: replay blockchain has no head")
	}
	if replayHead.Number() > 0 {
		sourceHead, err := readBalanceTraceBackfillSourceBlock(source, replayHead.Number(), "replay head check")
		if err != nil {
			return nil, err
		}
		if sourceHead.Hash() != replayHead.Hash() {
			return nil, fmt.Errorf("core: replay head %d hash %x does not match source canonical hash %x", replayHead.Number(), replayHead.Hash(), sourceHead.Hash())
		}
	}
	result.ReplayStartBlock = replayHead.Number() + 1
	result.ReplayHeadBlock = replayHead.Number()

	replayReader := rawdb.NewChainDB(replayDB, rawdb.NoopAncient{})

	copyEnd := min(opts.ToBlock, replayHead.Number())
	for blockNum := opts.FromBlock; blockNum <= copyEnd; blockNum++ {
		block, err := readBalanceTraceBackfillSourceBlock(source, blockNum, "balance trace replay backfill copy")
		if err != nil {
			return nil, err
		}
		if opts.Progress != nil {
			opts.Progress(BalanceTraceReplayBackfillProgress{Phase: "copy", Block: blockNum, Target: opts.ToBlock})
		}
		if err := collectReplayedBalanceTraceRows(target, targetTraceReader, replayReader, blockNum, block.Hash(), opts.Overwrite, collector, result); err != nil {
			return nil, err
		}
		result.BlocksBackfilled++
	}
	for blockNum := replayHead.Number() + 1; blockNum <= opts.ToBlock; blockNum++ {
		block, err := readBalanceTraceBackfillSourceBlock(source, blockNum, "balance trace replay backfill")
		if err != nil {
			return nil, err
		}
		if opts.Progress != nil {
			opts.Progress(BalanceTraceReplayBackfillProgress{Phase: "replay", Block: blockNum, Target: opts.ToBlock})
		}
		if err := replayChain.InsertBlock(block); err != nil {
			return nil, fmt.Errorf("replay block %d: %w", blockNum, err)
		}
		result.BlocksReplayed++
		result.ReplayHeadBlock = blockNum
		if blockNum < opts.FromBlock {
			continue
		}
		if err := collectReplayedBalanceTraceRows(target, targetTraceReader, replayReader, blockNum, block.Hash(), opts.Overwrite, collector, result); err != nil {
			return nil, err
		}
		result.BlocksBackfilled++
	}
	stats, err := collector.Load(target)
	if err != nil {
		return nil, err
	}
	result.ETL = stats
	return result, nil
}

func readBalanceTraceBackfillSourceBlock(source *rawdb.ChainDB, blockNum uint64, context string) (*types.Block, error) {
	block, ok, err := rawdb.ReadBlockStrict(source, blockNum)
	if err != nil {
		return nil, fmt.Errorf("core: source canonical block %d during %s is corrupt: %w", blockNum, context, err)
	}
	if !ok {
		return nil, fmt.Errorf("core: missing canonical block %d during %s", blockNum, context)
	}
	return block, nil
}

func cloneBalanceTraceBackfillGenesis(genesis *params.Genesis) *params.Genesis {
	out := *genesis
	cfg := *genesis.Config
	out.Config = &cfg
	return &out
}

func collectReplayedBalanceTraceRows(target ethdb.KeyValueStore, targetTraceReader ethdb.KeyValueReader, replay ethdb.KeyValueStore, blockNum uint64, blockHash tcommon.Hash, overwrite bool, collector *rawdb.DerivedIndexCollector, result *BalanceTraceReplayBackfillResult) error {
	trace, ok, err := rawdb.ReadBlockBalanceTraceStrict(rawdb.NewChainDB(replay, rawdb.NoopAncient{}), int64(blockNum))
	if err != nil {
		return fmt.Errorf("core: replay BlockBalanceTrace at block %d is corrupt: %w", blockNum, err)
	}
	if !ok {
		return fmt.Errorf("core: replay produced no BlockBalanceTrace for block %d", blockNum)
	}
	if id := trace.GetBlockIdentifier(); id == nil {
		return fmt.Errorf("core: replay BlockBalanceTrace at block %d has no block identifier", blockNum)
	} else if id.GetNumber() != int64(blockNum) {
		return fmt.Errorf("core: replay BlockBalanceTrace payload number %d does not match block %d", id.GetNumber(), blockNum)
	} else if !bytes.Equal(id.GetHash(), blockHash.Bytes()) {
		return fmt.Errorf("core: replay BlockBalanceTrace payload hash %x does not match canonical block %d hash %x", id.GetHash(), blockNum, blockHash)
	}

	existing, exists, err := rawdb.ReadBlockBalanceTraceStrict(targetTraceReader, int64(blockNum))
	if err != nil {
		return fmt.Errorf("core: target BlockBalanceTrace at block %d is corrupt: %w", blockNum, err)
	}
	if exists {
		if proto.Equal(existing, trace) {
			result.ExistingBlockTraces++
		} else if !overwrite {
			return fmt.Errorf("core: target BlockBalanceTrace at block %d differs from replay output", blockNum)
		}
	}
	if !exists || overwrite {
		if err := collector.PutBlockBalanceTrace(int64(blockNum), trace); err != nil {
			return fmt.Errorf("collect BlockBalanceTrace block %d: %w", blockNum, err)
		}
		result.BlockTraceRows++
	}

	if err := rawdb.IterateAccountTraceRows(replay, int64(blockNum), int64(blockNum), func(owner []byte, traceBlock int64, balance int64) (bool, error) {
		existingBalance, ok, err := rawdb.ReadAccountTraceStrict(targetTraceReader, owner, traceBlock)
		if err != nil {
			return false, fmt.Errorf("core: target AccountTrace at block %d owner %x is corrupt: %w", traceBlock, owner, err)
		}
		if ok {
			if existingBalance == balance {
				result.ExistingAccountTraces++
				return true, nil
			}
			if !overwrite {
				return false, fmt.Errorf("core: target AccountTrace at block %d owner %x differs from replay output", traceBlock, owner)
			}
		}
		if err := collector.PutAccountTrace(owner, traceBlock, balance); err != nil {
			return false, fmt.Errorf("collect AccountTrace block %d owner %x: %w", traceBlock, owner, err)
		}
		result.AccountTraceRows++
		return true, nil
	}); err != nil {
		return err
	}
	return nil
}
