package core

import (
	"errors"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
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

	genesisBlock *types.Block
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

	return bc, nil
}

func (bc *BlockChain) CurrentBlock() *types.Block {
	return bc.currentBlock.Load()
}

func (bc *BlockChain) GetBlockByNumber(number uint64) *types.Block {
	return rawdb.ReadBlock(bc.db, number)
}

func (bc *BlockChain) GetBlockByHash(hash tcommon.Hash) *types.Block {
	num := rawdb.ReadBlockNumber(bc.db, hash)
	if num == nil {
		return nil
	}
	return rawdb.ReadBlock(bc.db, *num)
}

func (bc *BlockChain) GenesisTimestamp() int64 {
	return bc.genesisBlock.Timestamp()
}

func (bc *BlockChain) Config() *params.ChainConfig {
	return bc.config
}

// InsertBlockWithoutVerify inserts a block without consensus verification.
func (bc *BlockChain) InsertBlockWithoutVerify(block *types.Block) error {
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
