package core

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// discardMetadataBatch exercises Pebble's real batch representation and
// lifecycle without making a benchmark of allocation sizing wait for fsync.
type discardMetadataBatch struct {
	ethdb.Batch
	store *discardMetadataStore
}

func (b *discardMetadataBatch) Write() error {
	b.store.lastValueSize = b.ValueSize()
	return nil
}

func (b *discardMetadataBatch) Close() {
	b.store.closeCalls++
	b.Batch.Close()
}

type discardMetadataStore struct {
	ethdb.KeyValueStore
	pebble             *pebbledb.Database
	newBatchCalls      int
	newSizedBatchCalls int
	lastSizeHint       int
	lastValueSize      int
	closeCalls         int
}

func (s *discardMetadataStore) NewBatch() ethdb.Batch {
	s.newBatchCalls++
	return &discardMetadataBatch{Batch: s.pebble.NewBatch(), store: s}
}

func (s *discardMetadataStore) NewBatchWithSize(size int) ethdb.Batch {
	s.newSizedBatchCalls++
	s.lastSizeHint = size
	return &discardMetadataBatch{Batch: s.pebble.NewBatchWithSize(size), store: s}
}

func TestWriteBlockMetadataBatchPreallocatesAndCloses(t *testing.T) {
	db, err := pebbledb.New(t.TempDir(), 16, 16, "", false, pebbledb.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := &discardMetadataStore{KeyValueStore: rawdb.NewMemoryDatabase(), pebble: db}
	chain := &BlockChain{db: store}
	block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{
		RawData: &corepb.BlockHeaderRaw{Number: 7, Timestamp: 1_700_000_000_000},
	}})
	infos := []*corepb.TransactionInfo{{BlockNumber: 7, BlockTimeStamp: block.Timestamp(), ContractResult: [][]byte{bytes.Repeat([]byte{1}, 128<<10)}}}
	trace := &blockBalanceTraceData{
		trace:           &contractpb.BlockBalanceTrace{Timestamp: block.Timestamp()},
		accountBalances: map[tcommon.Address]int64{{0x41}: 7},
	}
	if err := chain.writeBlockMetadataBatch(block, bytes.Repeat([]byte{2}, 64<<10), tcommon.Hash{1}, infos, trace, false); err != nil {
		t.Fatal(err)
	}
	if store.newBatchCalls != 0 || store.newSizedBatchCalls != 1 {
		t.Fatalf("batch constructors = unsized %d sized %d, want 0/1", store.newBatchCalls, store.newSizedBatchCalls)
	}
	if store.closeCalls != 1 {
		t.Fatalf("batch close calls = %d, want 1", store.closeCalls)
	}
	if store.lastSizeHint <= store.lastValueSize {
		t.Fatalf("batch size hint = %d, must cover %d key/value bytes plus Pebble framing", store.lastSizeHint, store.lastValueSize)
	}
}

func BenchmarkWriteBlockMetadataBatch(b *testing.B) {
	db, err := pebbledb.New(b.TempDir(), 16, 16, "", false, pebbledb.DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	store := &discardMetadataStore{KeyValueStore: rawdb.NewMemoryDatabase(), pebble: db}
	chain := &BlockChain{db: store}
	block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{
		RawData: &corepb.BlockHeaderRaw{Number: 7, Timestamp: 1_700_000_000_000},
	}})
	blockData := bytes.Repeat([]byte{0xab}, 300<<10)
	infos := make([]*corepb.TransactionInfo, 128)
	for i := range infos {
		infos[i] = &corepb.TransactionInfo{
			BlockNumber:    7,
			BlockTimeStamp: block.Timestamp(),
			ContractResult: [][]byte{bytes.Repeat([]byte{byte(i)}, 8<<10)},
		}
	}
	balances := make(map[tcommon.Address]int64, 128)
	for i := range 128 {
		var address tcommon.Address
		address[0] = 0x41
		address[len(address)-1] = byte(i)
		balances[address] = int64(i)
	}
	trace := &blockBalanceTraceData{
		trace:           &contractpb.BlockBalanceTrace{Timestamp: block.Timestamp()},
		accountBalances: balances,
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(blockData)))
	b.ResetTimer()
	for b.Loop() {
		if err := chain.writeBlockMetadataBatch(block, blockData, tcommon.Hash{1}, infos, trace, false); err != nil {
			b.Fatal(err)
		}
	}
}
