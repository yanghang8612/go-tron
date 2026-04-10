package core

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
)

var (
	ErrKnownBlock    = errors.New("block already known")
	ErrInvalidParent = errors.New("parent block not found")
	ErrInvalidNumber = errors.New("invalid block number")
)

// BlockChain manages the canonical chain and provides block insertion.
type BlockChain struct {
	db      ethdb.KeyValueStore
	stateDB *state.Database
	config  *params.ChainConfig

	currentBlock atomic.Pointer[types.Block]
	chainmu      sync.Mutex // serializes block insertion

	genesisBlock    *types.Block
	activeWitnesses atomic.Value // []tcommon.Address
}

// NewBlockChain creates a new BlockChain, loading head from DB.
func NewBlockChain(db ethdb.KeyValueStore, stateDB *state.Database, config *params.ChainConfig) (*BlockChain, error) {
	bc := &BlockChain{
		db:      db,
		stateDB: stateDB,
		config:  config,
	}

	// Load genesis
	bc.genesisBlock = rawdb.ReadBlock(db, 0)
	if bc.genesisBlock == nil {
		return nil, errors.New("genesis block not found in database")
	}

	// Load head block
	headHash := rawdb.ReadHeadBlockHash(db)
	if headHash == (tcommon.Hash{}) {
		bc.currentBlock.Store(bc.genesisBlock)
	} else {
		num := rawdb.ReadBlockNumber(db, headHash)
		if num == nil {
			bc.currentBlock.Store(bc.genesisBlock)
		} else {
			block := rawdb.ReadBlock(db, *num)
			if block == nil {
				bc.currentBlock.Store(bc.genesisBlock)
			} else {
				bc.currentBlock.Store(block)
			}
		}
	}

	// Load active witnesses from DB; if empty, derive from genesis witnesses
	witnesses := rawdb.ReadActiveWitnesses(db)
	if len(witnesses) == 0 {
		var allWitnesses []dpos.WitnessVote
		witnessAddrs := rawdb.ReadWitnessIndex(db)
		for _, addr := range witnessAddrs {
			w := rawdb.ReadWitness(db, addr)
			if w != nil {
				allWitnesses = append(allWitnesses, dpos.WitnessVote{
					Address: w.Address(),
					Votes:   w.VoteCount(),
				})
			}
		}
		if len(allWitnesses) > 0 {
			witnesses = dpos.SelectActiveWitnesses(allWitnesses)
			rawdb.WriteActiveWitnesses(db, witnesses)
		}
	}
	if len(witnesses) > 0 {
		bc.activeWitnesses.Store(witnesses)
	}

	return bc, nil
}

// CurrentBlock returns the head of the canonical chain.
func (bc *BlockChain) CurrentBlock() *types.Block {
	return bc.currentBlock.Load()
}

// GetBlockByNumber retrieves a block by its number.
func (bc *BlockChain) GetBlockByNumber(number uint64) *types.Block {
	return rawdb.ReadBlock(bc.db, number)
}

// GetBlockByHash retrieves a block by its hash.
func (bc *BlockChain) GetBlockByHash(hash tcommon.Hash) *types.Block {
	num := rawdb.ReadBlockNumber(bc.db, hash)
	if num == nil {
		return nil
	}
	return rawdb.ReadBlock(bc.db, *num)
}

// GenesisTimestamp returns the genesis block timestamp.
func (bc *BlockChain) GenesisTimestamp() int64 {
	return bc.genesisBlock.Timestamp()
}

// Config returns the chain config.
func (bc *BlockChain) Config() *params.ChainConfig {
	return bc.config
}

// InsertBlockWithoutVerify inserts a block without consensus verification.
func (bc *BlockChain) InsertBlockWithoutVerify(block *types.Block) error {
	if block == nil {
		return errors.New("block is nil")
	}

	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

	current := bc.CurrentBlock()

	if block.Number() != current.Number()+1 {
		return ErrInvalidNumber
	}
	if block.ParentHash() != current.Hash() {
		return ErrInvalidParent
	}

	rawdb.WriteBlock(bc.db, block)
	rawdb.WriteHeadBlockHash(bc.db, block.Hash())

	bc.currentBlock.Store(block)

	return nil
}

// InsertBlock inserts a block with full state processing.
func (bc *BlockChain) InsertBlock(block *types.Block) error {
	if block == nil {
		return errors.New("block is nil")
	}

	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

	current := bc.CurrentBlock()
	if block.Number() != current.Number()+1 {
		return ErrInvalidNumber
	}
	if block.ParentHash() != current.Hash() {
		return ErrInvalidParent
	}

	// Open StateDB from parent's state root
	parentRoot := current.AccountStateRoot()
	statedb, err := state.New(parentRoot, bc.stateDB)
	if err != nil {
		return fmt.Errorf("open state: %w", err)
	}

	// Load dynamic properties
	dynProps := state.LoadDynamicProperties(bc.db)

	// Process block (execute transactions, pay reward)
	newRoot, err := ProcessBlock(statedb, dynProps, block)
	if err != nil {
		return fmt.Errorf("process block: %w", err)
	}

	// Verify state root if the block has one set
	blockRoot := block.AccountStateRoot()
	if blockRoot != (tcommon.Hash{}) && blockRoot != newRoot {
		return fmt.Errorf("state root mismatch: block=%x computed=%x", blockRoot, newRoot)
	}

	// Update dynamic properties
	dynProps.SetLatestBlockHeaderNumber(int64(block.Number()))
	dynProps.SetLatestBlockHeaderTimestamp(block.Timestamp())
	dynProps.SetLatestBlockHeaderHash(block.Hash())
	dynProps.Flush(bc.db)

	// Persist block
	rawdb.WriteBlock(bc.db, block)
	rawdb.WriteHeadBlockHash(bc.db, block.Hash())
	bc.currentBlock.Store(block)

	return nil
}

// StateDB returns the state database for reading state.
func (bc *BlockChain) StateDB() *state.Database {
	return bc.stateDB
}

// ActiveWitnesses returns the current active witness list.
func (bc *BlockChain) ActiveWitnesses() []tcommon.Address {
	v := bc.activeWitnesses.Load()
	if v == nil {
		return nil
	}
	return v.([]tcommon.Address)
}

// SetActiveWitnesses updates the active witness list in memory and persists it to the DB.
func (bc *BlockChain) SetActiveWitnesses(witnesses []tcommon.Address) {
	bc.activeWitnesses.Store(witnesses)
	rawdb.WriteActiveWitnesses(bc.db, witnesses)
}

// NextMaintenanceTime returns the next scheduled maintenance time from dynamic properties.
func (bc *BlockChain) NextMaintenanceTime() int64 {
	dynProps := state.LoadDynamicProperties(bc.db)
	return dynProps.NextMaintenanceTime()
}
