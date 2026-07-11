package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestBlockChainSyncInsertDefersTransactionLookupUntilStage(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{{
			Address: testInsertAddr(1),
			Balance: 99_000_000_000_000_000,
		}},
		DynamicProperties: map[string]int64{},
	}
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()

	transfer, err := anypb.New(&contractpb.TransferContract{
		OwnerAddress: testInsertAddr(1).Bytes(),
		ToAddress:    testInsertAddr(2).Bytes(),
		Amount:       5_000_000,
	})
	if err != nil {
		t.Fatalf("wrap transfer contract: %v", err)
	}
	txPB := &corepb.Transaction{RawData: &corepb.TransactionRaw{
		Expiration: 60_000,
		Contract: []*corepb.Transaction_Contract{{
			Type:      corepb.Transaction_Contract_TransferContract,
			Parameter: transfer,
		}},
	}}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number:     1,
			Timestamp:  3000,
			ParentHash: genesisHash[:],
		}},
		Transactions: []*corepb.Transaction{txPB},
	})
	txHash := block.Transactions()[0].Hash()

	if err := bc.InsertSyncBlocksWithStageHook([]*types.Block{block}, nil); err != nil {
		t.Fatalf("InsertSyncBlocksWithStageHook: %v", err)
	}
	if got := rawdb.ReadTransactionIndex(bc.ChainDB(), txHash[:]); got != nil {
		t.Fatalf("tx lookup before derived stage = %d, want nil", *got)
	}
	if infos := rawdb.ReadTransactionInfosByBlock(bc.ChainDB(), block.Number()); len(infos) != 1 {
		t.Fatalf("tx infos by block = %+v, want durable receipt", infos)
	}

	result, err := bc.AdvanceTransactionLookupStage(1)
	if err != nil {
		t.Fatalf("AdvanceTransactionLookupStage: %v", err)
	}
	if !result.Advanced || result.Rebuilt == nil || result.Rebuilt.TransactionsIndexed != 1 {
		t.Fatalf("stage result = %+v, want one advanced transaction index", result)
	}
	if got := rawdb.ReadTransactionIndex(bc.ChainDB(), txHash[:]); got == nil || *got != block.Number() {
		t.Fatalf("tx lookup after derived stage = %v, want block %d", got, block.Number())
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageTxLookup); err != nil || !ok || row.BlockNum != block.Number() || !row.HasBlockHash || row.BlockHash != block.Hash() {
		t.Fatalf("TxLookup progress = %+v ok=%v err=%v, want block 1 hash-bound", row, ok, err)
	}
}

func TestNewBlockChainClampsTransactionLookupStageAheadOfStoredHead(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	genesis := &params.Genesis{
		Config:            params.MainnetChainConfig,
		DynamicProperties: map[string]int64{},
	}
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStageProgressWithHash(diskdb, rawdb.StageTxLookup, 7, [32]byte{0x07}); err != nil {
		t.Fatalf("seed ahead TxLookup stage: %v", err)
	}

	bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
	if err != nil {
		t.Fatalf("NewBlockChain with ahead TxLookup stage: %v", err)
	}
	defer bc.Close()
	if row, ok, err := rawdb.ReadStageProgressRow(diskdb, rawdb.StageTxLookup); err != nil || !ok || row.BlockNum != 0 || !row.HasBlockHash || row.BlockHash != genesisHash {
		t.Fatalf("clamped TxLookup stage = %+v ok=%v err=%v, want genesis hash-bound", row, ok, err)
	}
}
