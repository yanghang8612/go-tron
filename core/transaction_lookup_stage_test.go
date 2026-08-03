package core

import (
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

type compactTransactionIndexAncient struct {
	rawdb.AncientReader
	coverage   uint64
	candidates map[[32]byte][]uint64
}

func (a *compactTransactionIndexAncient) TransactionIndexCandidates(hash [32]byte) ([]uint64, error) {
	return append([]uint64(nil), a.candidates[hash]...), nil
}

func (a *compactTransactionIndexAncient) TransactionIndexCoverage() uint64 {
	return a.coverage
}

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
	bc.SetTransactionLookupETLOptions(etl.Options{TempDir: t.TempDir(), BufferLimit: 1, BatchSize: 1})

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
	if got := rawdb.ReadTransactionInfo(bc.ChainDB(), txHash[:]); got != nil {
		t.Fatalf("tx info before derived stage = %+v, want nil without ti-/tx- materialization", got)
	}
	if infos := rawdb.ReadTransactionInfosByBlock(bc.ChainDB(), block.Number()); len(infos) != 1 {
		t.Fatalf("tx infos by block = %+v, want durable receipt", infos)
	}
	if result, err := bc.AdvanceTransactionLookupStageInterruptible(1, func() bool { return true }); !errors.Is(err, rawdb.ErrTransactionLookupRebuildInterrupted) || result.Advanced {
		t.Fatalf("interrupted TxLookup result=%+v err=%v", result, err)
	}
	if got := rawdb.ReadTransactionIndex(bc.ChainDB(), txHash[:]); got != nil {
		t.Fatalf("tx lookup after interrupted derived stage = %d, want nil", *got)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageTxLookup); err != nil || !ok || row.BlockNum != 0 || !row.HasBlockHash || row.BlockHash != genesisHash {
		t.Fatalf("TxLookup progress after interruption = %+v ok=%v err=%v, want genesis", row, ok, err)
	}

	result, err := bc.AdvanceTransactionLookupStage(1)
	if err != nil {
		t.Fatalf("AdvanceTransactionLookupStage: %v", err)
	}
	if !result.Advanced || result.Rebuilt == nil || result.Rebuilt.TransactionsIndexed != 1 {
		t.Fatalf("stage result = %+v, want one advanced transaction index", result)
	}
	if result.Rebuilt.ETL.SpilledRuns == 0 || result.Rebuilt.ETL.BatchWrites == 0 {
		t.Fatalf("TxLookup ETL stats = %+v, want configured spill and batch writes", result.Rebuilt.ETL)
	}
	if got := rawdb.ReadTransactionIndex(bc.ChainDB(), txHash[:]); got == nil || *got != block.Number() {
		t.Fatalf("tx lookup after derived stage = %v, want block %d", got, block.Number())
	}
	if got := rawdb.ReadTransactionInfo(bc.ChainDB(), txHash[:]); got == nil || got.Fee == 0 || got.BlockNumber != int64(block.Number()) {
		t.Fatalf("tx info after derived lookup fallback = %+v, want block receipt", got)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageTxLookup); err != nil || !ok || row.BlockNum != block.Number() || !row.HasBlockHash || row.BlockHash != block.Hash() {
		t.Fatalf("TxLookup progress = %+v ok=%v err=%v, want block 1 hash-bound", row, ok, err)
	}
}

func TestBlockMetadataWriterDoesNotRecreateCompactHistoricalIndex(t *testing.T) {
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
	ancient := &compactTransactionIndexAncient{
		AncientReader: rawdb.NoopAncient{},
		coverage:      2,
		candidates:    make(map[[32]byte][]uint64),
	}
	bc, err := NewBlockChainWithAncient(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig, ancient)
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
		t.Fatal(err)
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: 1, Timestamp: 3000, ParentHash: genesisHash[:],
		}},
		Transactions: []*corepb.Transaction{{RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			Contract: []*corepb.Transaction_Contract{{
				Type: corepb.Transaction_Contract_TransferContract, Parameter: transfer,
			}},
		}}},
	})
	txHash := block.Transactions()[0].Hash()
	ancient.candidates[txHash] = []uint64{block.Number()}
	if err := bc.InsertBlock(block); err != nil {
		t.Fatal(err)
	}
	hotKey := append([]byte("tx-"), txHash[:]...)
	if has, err := diskdb.Has(hotKey); err != nil || has {
		t.Fatalf("covered tx-* row present=%v err=%v, want absent", has, err)
	}
	if got := rawdb.ReadTransactionIndex(bc.ChainDB(), txHash[:]); got == nil || *got != block.Number() {
		t.Fatalf("cold transaction lookup=%v, want block %d", got, block.Number())
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
