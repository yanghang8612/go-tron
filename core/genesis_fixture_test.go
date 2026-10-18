package core

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"testing"
)

// commitGenesisFixture edits an explicit test-only genesis allocation. Production
// genesis has no post-publication edits; this helper temporarily unbinds the
// fixture HEAD, executes the normal owned-delta commit, and reseals its identity.
// It must never be used to build a recipe-replayed durable fork fixture.
func commitGenesisFixture(tb testing.TB, store ethdb.KeyValueStore, db *state.StateDB) (common.Hash, error) {
	tb.Helper()
	head, present, err := rawdb.ReadStateCommitmentHead(store)
	if err != nil || !present {
		tb.Fatalf("fixture HEAD: %v", err)
	}
	if head.BlockNum != 0 || head.TxNum != 0 {
		tb.Fatal("genesis fixture edit after block execution")
	}
	identity := head.BlockHash
	head.BlockHash = common.Hash{}
	head.ParentHash = common.Hash{}
	if err = rawdb.WriteStateCommitmentHead(store, head); err != nil {
		return common.Hash{}, err
	}
	root, err := db.Commit()
	if err != nil {
		return common.Hash{}, err
	}
	head, present, err = rawdb.ReadStateCommitmentHead(store)
	if err != nil || !present {
		tb.Fatalf("fixture postcommit HEAD: %v", err)
	}
	head.BlockHash = identity
	batch := store.NewBatch()
	defer batch.Close()
	if err = rawdb.WriteStateCommitmentHead(batch, head); err != nil {
		return common.Hash{}, err
	}
	rawdb.WriteGenesisStateRoot(batch, root)
	if err = rawdb.WriteBlockStateRoot(batch, identity, root); err != nil {
		return common.Hash{}, err
	}
	if err = batch.Write(); err != nil {
		return common.Hash{}, err
	}
	return root, nil
}

// commitArchiveFixture is only for manual domain-history fixtures, which append
// synthetic history to a private active/sentinel layer after building bodies.
// A root label is not a historical source view. Preserve the actual current cut
// identity while resealing its edited physical set; never use this for canonical
// block execution, startup or fork/recipe replay fixtures.
func commitArchiveFixture(tb testing.TB, bc *BlockChain, db *state.StateDB) (common.Hash, error) {
	tb.Helper()
	saved, present, err := rawdb.ReadStateCommitmentHead(bc.buffer)
	if err != nil || !present {
		tb.Fatalf("archive fixture HEAD: %v", err)
	}
	if _, active := bc.buffer.NewestInflight(); !active {
		tb.Fatal("archive fixture requires its private active layer")
	}
	unbound := saved
	unbound.BlockHash = common.Hash{}
	unbound.ParentHash = common.Hash{}
	if err = rawdb.WriteStateCommitmentHead(bc.buffer, unbound); err != nil {
		return common.Hash{}, err
	}
	root, err := db.Commit()
	if err != nil {
		return common.Hash{}, err
	}
	head, present, err := rawdb.ReadStateCommitmentHead(bc.buffer)
	if err != nil || !present {
		tb.Fatalf("archive postcommit HEAD: %v", err)
	}
	head.BlockNum = saved.BlockNum
	head.BlockHash = saved.BlockHash
	head.ParentHash = saved.ParentHash
	head.TxNum = saved.TxNum
	if err = rawdb.WriteStateCommitmentHead(bc.buffer, head); err != nil {
		return common.Hash{}, err
	}
	return root, nil
}
