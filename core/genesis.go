package core

import (
	"errors"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var errGenesisNoConfig = errors.New("genesis has no chain configuration")

// SetupGenesisBlock writes the genesis block and chain config to the database
// if they don't exist. Returns the chain config and genesis hash.
func SetupGenesisBlock(db ethdb.KeyValueStore, genesis *params.Genesis) (*params.ChainConfig, tcommon.Hash, error) {
	if genesis == nil {
		return nil, tcommon.Hash{}, errors.New("genesis is nil")
	}
	if genesis.Config == nil {
		return nil, tcommon.Hash{}, errGenesisNoConfig
	}

	// Check if genesis already exists
	storedBlock := rawdb.ReadBlock(db, 0)
	if storedBlock != nil {
		storedHash := storedBlock.Hash()

		// Compute expected hash to validate
		sdb := state.NewDatabase(rawdb.WrapKeyValueStore(db))
		expectedBlock, err := GenesisToBlock(genesis, sdb)
		if err != nil {
			return genesis.Config, storedHash, nil // Can't verify, trust stored
		}
		if storedHash != expectedBlock.Hash() {
			return genesis.Config, storedHash, errors.New("genesis hash mismatch: database contains incompatible genesis")
		}
		return genesis.Config, storedHash, nil
	}

	// Write genesis
	sdb := state.NewDatabase(rawdb.WrapKeyValueStore(db))
	block, err := GenesisToBlock(genesis, sdb)
	if err != nil {
		return nil, tcommon.Hash{}, err
	}

	rawdb.WriteBlock(db, block)
	rawdb.WriteHeadBlockHash(db, block.Hash())

	// Write dynamic properties
	if genesis.DynamicProperties != nil {
		dp := state.NewDynamicProperties()
		for k, v := range genesis.DynamicProperties {
			dp.Set(k, v)
		}
		dp.SetLatestBlockHeaderNumber(0)
		dp.SetLatestBlockHeaderTimestamp(genesis.Timestamp)
		dp.SetLatestBlockHeaderHash(block.Hash())
		dp.Flush(db)
	}

	// Write witnesses
	for _, gw := range genesis.Witnesses {
		w := types.NewWitness(gw.Address, gw.URL)
		w.SetVoteCount(gw.VoteCount)
		rawdb.WriteWitness(db, gw.Address, w)
	}

	return genesis.Config, block.Hash(), nil
}

// GenesisToBlock creates the genesis block from the Genesis config.
func GenesisToBlock(g *params.Genesis, db *state.Database) (*types.Block, error) {
	statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		return nil, err
	}

	// Create accounts
	for _, ga := range g.Accounts {
		obj := statedb.GetOrCreateAccount(ga.Address)
		if ga.AccountName != "" {
			obj.Account().SetAccountName(ga.AccountName)
		}
		if ga.Balance != 0 {
			obj.Account().SetBalance(ga.Balance)
		}
	}

	// Commit state → accountStateRoot
	root, err := statedb.Commit()
	if err != nil {
		return nil, err
	}

	// Build genesis block
	header := &corepb.BlockHeaderRaw{
		Number:           0,
		Timestamp:        g.Timestamp,
		ParentHash:       g.ParentHash.Bytes(),
		AccountStateRoot: root[:],
	}

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: header,
		},
	})

	return block, nil
}
