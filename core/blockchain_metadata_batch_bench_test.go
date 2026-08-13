package core

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
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

func (b *discardMetadataBatch) PutValueFunc(key []byte, valueLen int, fill func([]byte) error) error {
	return b.Batch.(interface {
		PutValueFunc([]byte, int, func([]byte) error) error
	}).PutValueFunc(key, valueLen, fill)
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

func TestWriteBlockMetadataBatchStoresCompactTransactionInfos(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	chain := &BlockChain{db: db}
	block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{
		RawData: &corepb.BlockHeaderRaw{Number: 7, Timestamp: 1_700_000_000_000},
	}})
	txID := bytes.Repeat([]byte{0x42}, tcommon.HashLength)
	infos := []*corepb.TransactionInfo{{
		Id:             txID,
		Fee:            99,
		BlockNumber:    7,
		BlockTimeStamp: block.Timestamp(),
	}}
	if err := chain.writeBlockMetadataBatch(block, nil, tcommon.Hash{1}, infos, nil, false); err != nil {
		t.Fatal(err)
	}
	key := make([]byte, len("tib-")+8)
	copy(key, "tib-")
	binary.BigEndian.PutUint64(key[len("tib-"):], block.Number())
	data, err := db.Get(key)
	if err != nil {
		t.Fatalf("read transaction infos: %v", err)
	}
	stored := new(corepb.TransactionRet)
	if err := proto.Unmarshal(data, stored); err != nil {
		t.Fatalf("decode transaction infos: %v", err)
	}
	if stored.BlockNumber != 7 || stored.BlockTimeStamp != block.Timestamp() || len(stored.Transactioninfo) != 1 {
		t.Fatalf("stored transaction ret = %+v", stored)
	}
	info := stored.Transactioninfo[0]
	if len(info.Id) != 0 || info.BlockNumber != 0 || info.BlockTimeStamp != 0 || info.Fee != 99 {
		t.Fatalf("stored transaction info = %+v, want compact identity fields", info)
	}
	if !bytes.Equal(infos[0].Id, txID) || infos[0].BlockNumber != 7 || infos[0].BlockTimeStamp != block.Timestamp() {
		t.Fatalf("input transaction info mutated: %+v", infos[0])
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
