package core

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// BuildBlock assembles a new block from pending transactions.
// Failing transactions are skipped rather than aborting the block.
// The returned block is unsigned — call SignBlock separately.
func BuildBlock(bc *BlockChain, pool *txpool.TxPool, witnessAddr tcommon.Address, timestamp int64) (*types.Block, error) {
	parent := bc.CurrentBlock()

	// Open StateDB from parent's state root
	parentRoot := parent.AccountStateRoot()
	statedb, err := state.New(parentRoot, bc.stateDB)
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}

	dynProps := state.LoadDynamicProperties(bc.db)

	// Pull all pending transactions
	pendingTxs := pool.Pending()

	// Execute transactions, collecting successful ones
	var appliedTxProtos []*corepb.Transaction
	blockNum := parent.Number() + 1

	for _, tx := range pendingTxs {
		_, err := ApplyTransaction(statedb, dynProps, tx, timestamp, blockNum)
		if err != nil {
			continue // skip failing transactions
		}
		appliedTxProtos = append(appliedTxProtos, tx.Proto())
	}

	// Pay block reward to witness
	reward := dynProps.WitnessPayPerBlock()
	if reward > 0 {
		statedb.AddAllowance(witnessAddr, reward)
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
			},
		},
		Transactions: appliedTxProtos,
	})

	return block, nil
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
