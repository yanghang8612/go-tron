package core

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
)

// PrepareHistoryStagingResetStartup runs before NewBlockChain when a durable
// RESETTING intent survived a crash. NewBlockChain cannot load a partially
// erased mutable head, so this function rechecks the immutable replay source
// and restores exactly the genesis materialization. The caller must then
// install the same manager and call RestartSyncFromHeight(intent.TargetHeight)
// before registering any API, sync, producer, or maintenance lifecycle.
func PrepareHistoryStagingResetStartup(ctx context.Context, db ethdb.KeyValueStore, ancientReader rawdb.AncientReader, ancientWriter rawdb.AncientWriter, manager *rawdb.HistoryStagingManager, genesis *params.Genesis) (*rawdb.HistoryStagingResetIntent, error) {
	if ctx == nil || db == nil || manager == nil || genesis == nil || genesis.Config == nil {
		return nil, fmt.Errorf("history staging reset recovery: missing store, manager, or genesis")
	}
	intent, present, err := manager.ReadResetIntent()
	if err != nil || !present || intent.Complete {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	chainDB := rawdb.NewChainDB(db, ancientReader)
	block, err := readRestartSyncBlock(chainDB, intent.TargetHeight, fmt.Sprintf("reset target block %d not found", intent.TargetHeight))
	if err != nil {
		return nil, fmt.Errorf("history staging reset recovery: target block: %w", err)
	}
	if block.Hash() != intent.TargetHash {
		return nil, fmt.Errorf("history staging reset recovery: target block identity changed")
	}
	digest, err := preflightRestartSyncBlocks(chainDB, intent.TargetHeight, intent.TargetHash)
	if err != nil {
		return nil, fmt.Errorf("history staging reset recovery: replay input: %w", err)
	}
	if digest != intent.BlockDigest {
		return nil, fmt.Errorf("history staging reset recovery: replay input differs from durable intent")
	}
	if err := manager.PrepareResetReplay(ctx, intent); err != nil {
		return nil, err
	}
	if ancientWriter != nil {
		if _, err := ancientWriter.TruncateHead(intent.TargetHeight + 1); err != nil {
			return nil, fmt.Errorf("history staging reset recovery: truncate ancient head: %w", err)
		}
		if err := ancientWriter.Sync(); err != nil {
			return nil, fmt.Errorf("history staging reset recovery: sync ancient head: %w", err)
		}
	}
	if err := rawdb.ResetMutableState(db); err != nil {
		return nil, fmt.Errorf("history staging reset recovery: clear partial mutable state: %w", err)
	}
	genesisDB := state.NewDatabase(rawdb.WrapKeyValueStore(db))
	genesisBlock, root, dp, err := genesisBlockAndStateRoot(genesis, genesisDB)
	if err != nil {
		return nil, fmt.Errorf("history staging reset recovery: rebuild genesis: %w", err)
	}
	storedGenesis, err := readRestartSyncBlock(chainDB, 0, "genesis block not found")
	if err != nil {
		return nil, fmt.Errorf("history staging reset recovery: genesis block: %w", err)
	}
	if genesisBlock.Hash() != storedGenesis.Hash() {
		return nil, fmt.Errorf("history staging reset recovery: genesis identity changed")
	}
	if err := writeGenesisMaterializedState(db, genesis, genesisBlock, root, dp); err != nil {
		return nil, err
	}
	if err := syncKeyValueStore(db); err != nil {
		return nil, fmt.Errorf("history staging reset recovery: sync genesis: %w", err)
	}
	return &intent, nil
}
