package core

import (
	"crypto/sha256"
	"errors"
	"fmt"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var errGenesisNoConfig = errors.New("genesis has no chain configuration")

// genesisWitnessAddress is the literal byte string java-tron's
// `BlockUtil.newGenesisBlockCapsule` writes into `block_header.raw_data
// .witness_address` for the genesis block. It is the ASCII bytes of the
// famous Tim Berners-Lee quote, *not* a 21-byte TRON address.
//
// Source: chainbase/.../capsule/utils/BlockUtil.java#newGenesisBlockCapsule
// (`blockCapsule.setWitness("A new system must allow ...")`).
var genesisWitnessAddress = []byte("A new system must allow existing systems to be linked together without requiring any central control or coordination")

// genesisOwnerAddress is the literal byte string java-tron's
// `TransactionUtil.newGenesisTransaction` writes into
// `TransferContract.owner_address` for every genesis allocation tx.
//
// It is the ASCII bytes of the 21-character string
// "0x000000000000000000000", *not* a zeroed 21-byte address. Required
// for byte-for-byte parity of the genesis block hash.
//
// Source: chainbase/.../capsule/utils/TransactionUtil.java#newGenesisTransaction
var genesisOwnerAddress = []byte("0x000000000000000000000")

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
		expectedBlock, _, err := genesisBlockAndStateRoot(genesis, sdb)
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
	block, stateRoot, err := genesisBlockAndStateRoot(genesis, sdb)
	if err != nil {
		return nil, tcommon.Hash{}, err
	}

	if err := rawdb.WriteBlock(db, block); err != nil {
		return nil, tcommon.Hash{}, fmt.Errorf("write genesis block: %w", err)
	}
	rawdb.WriteHeadBlockHash(db, block.Hash())
	// Persist post-genesis state root separately. The genesis block header
	// itself omits account_state_root for java-tron parity, so block #1's
	// applyBlock falls back to this when current.Number()==0.
	rawdb.WriteGenesisStateRoot(db, stateRoot)

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
		rawdb.AppendWitnessIndex(db, gw.Address)
	}

	return genesis.Config, block.Hash(), nil
}

// GenesisToBlock builds the genesis block from the Genesis config.
//
// The block layout matches java-tron's `BlockUtil.newGenesisBlockCapsule`
// byte-for-byte so that gtron's genesis hash equals java-tron's for the
// same `genesis.block` config:
//
//   - One TransferContract transaction per `g.Accounts` entry, in slice
//     order. The TransferContract sets `owner_address` to the literal
//     bytes "0x000000000000000000000" (java-tron quirk; not a zeroed
//     address) and `to_address` to the account's TRON address.
//   - `tx_trie_root` is the binary Merkle root of `SHA256(tx.proto bytes)`
//     leaves (see `core/types.MerkleRoot`).
//   - `witness_address` is the famous-quote ASCII bytes, not an address.
//   - `account_state_root`, `witness_signature`, and `version` are left
//     unset (java-tron does not set them for genesis).
//   - In-memory account state is still committed so that block #1 onwards
//     has account balances available, but the resulting state root is
//     deliberately NOT placed on the genesis block header. Use
//     `genesisBlockAndStateRoot` (or `rawdb.ReadGenesisStateRoot` after
//     `SetupGenesisBlock`) when the post-genesis state root is needed.
func GenesisToBlock(g *params.Genesis, db *state.Database) (*types.Block, error) {
	block, _, err := genesisBlockAndStateRoot(g, db)
	return block, err
}

func genesisBlockAndStateRoot(g *params.Genesis, db *state.Database) (*types.Block, tcommon.Hash, error) {
	statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		return nil, tcommon.Hash{}, err
	}

	// Populate accounts (so block #1 onwards finds them) and build genesis txs.
	txs := make([]*corepb.Transaction, 0, len(g.Accounts))
	for _, ga := range g.Accounts {
		obj := statedb.GetOrCreateAccount(ga.Address)
		if ga.AccountName != "" {
			obj.Account().SetAccountName(ga.AccountName)
		}
		if ga.Balance != 0 {
			obj.Account().SetBalance(ga.Balance)
		}
		tx, err := buildGenesisTransferTx(ga.Address, ga.Balance)
		if err != nil {
			return nil, tcommon.Hash{}, fmt.Errorf("genesis tx for %s: %w", ga.Address.Hex(), err)
		}
		txs = append(txs, tx)
	}

	// Persist account state. The returned root does NOT go on the block
	// header (java-tron parity), but it is needed by block #1's applyBlock
	// to open the StateDB on the correct trie. Caller persists via
	// `rawdb.WriteGenesisStateRoot`.
	stateRoot, err := statedb.Commit()
	if err != nil {
		return nil, tcommon.Hash{}, err
	}

	// Compute tx_trie_root: SHA256 over each tx's full proto bytes, fed
	// into the java-tron Merkle algorithm.
	leaves := make([]tcommon.Hash, len(txs))
	for i, tx := range txs {
		data, err := proto.Marshal(tx)
		if err != nil {
			return nil, tcommon.Hash{}, fmt.Errorf("marshal genesis tx %d: %w", i, err)
		}
		leaves[i] = tcommon.Hash(sha256Sum(data))
	}
	txTrieRoot := types.MerkleRoot(leaves)

	header := &corepb.BlockHeaderRaw{
		Number:         0,
		Timestamp:      g.Timestamp,
		ParentHash:     g.ParentHash.Bytes(),
		TxTrieRoot:     txTrieRootBytes(txTrieRoot, len(txs)),
		WitnessAddress: genesisWitnessAddress,
	}

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: header},
		Transactions: txs,
	})

	return block, stateRoot, nil
}

func buildGenesisTransferTx(toAddr tcommon.Address, amount int64) (*corepb.Transaction, error) {
	tc := &contractpb.TransferContract{
		Amount:       amount,
		OwnerAddress: genesisOwnerAddress,
		ToAddress:    toAddr.Bytes(),
	}
	param, err := anypb.New(tc)
	if err != nil {
		return nil, fmt.Errorf("pack TransferContract: %w", err)
	}
	return &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_TransferContract,
					Parameter: param,
				},
			},
		},
	}, nil
}

// txTrieRootBytes returns the byte slice to write into BlockHeaderRaw.TxTrieRoot.
// java-tron writes empty bytes (not 32 zeros) when the genesis has no
// transactions; we mirror that so the proto encoding matches.
func txTrieRootBytes(root tcommon.Hash, txCount int) []byte {
	if txCount == 0 {
		return nil
	}
	return root.Bytes()
}

func sha256Sum(b []byte) [32]byte {
	return sha256.Sum256(b)
}
