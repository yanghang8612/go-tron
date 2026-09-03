package core

import (
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func persistTelemetryBlock(number uint64, txs int) *types.Block {
	transactions := make([]*corepb.Transaction, txs)
	for i := range transactions {
		transactions[i] = &corepb.Transaction{RawData: &corepb.TransactionRaw{
			Timestamp: int64(number)*1000 + int64(i),
		}}
	}
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number:    int64(number),
			Timestamp: int64(number) * 3000,
		}},
		Transactions: transactions,
	})
}

func persistTelemetryInfos(block *types.Block) []*corepb.TransactionInfo {
	infos := make([]*corepb.TransactionInfo, len(block.Transactions()))
	for i, tx := range block.Transactions() {
		hash := tx.Hash()
		infos[i] = &corepb.TransactionInfo{
			Id:             append([]byte(nil), hash[:]...),
			BlockNumber:    int64(block.Number()),
			BlockTimeStamp: block.Timestamp(),
		}
	}
	return infos
}

func TestWriteBlockMetadataBatchReportsStats(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block := persistTelemetryBlock(7, 2)
	chain := &BlockChain{
		db:      db,
		chaindb: rawdb.NewChainDB(db, rawdb.NoopAncient{}),
	}
	trace := &blockBalanceTraceData{
		trace: &contractpb.BlockBalanceTrace{Timestamp: block.Timestamp()},
		accountBalances: map[tcommon.Address]int64{
			{0x41, 0x01}: 11,
			{0x41, 0x02}: 22,
		},
	}

	stats, err := chain.writeBlockMetadataBatch(
		block, nil, tcommon.Hash{1}, persistTelemetryInfos(block), trace, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	// Six fixed records + two transaction lookups + one block trace + two
	// account traces.
	if stats.MetadataRecords != 11 {
		t.Fatalf("metadata records = %d, want 11", stats.MetadataRecords)
	}
	if stats.TransactionLookupRows != 2 {
		t.Fatalf("transaction lookup rows = %d, want 2", stats.TransactionLookupRows)
	}
	if stats.TraceAccounts != 2 {
		t.Fatalf("trace accounts = %d, want 2", stats.TraceAccounts)
	}

	var rows, logicalBytes uint64
	it := db.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		rows++
		logicalBytes += uint64(len(it.Key()) + len(it.Value()))
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if stats.MetadataRecords != rows {
		t.Fatalf("metadata records = %d, persisted rows = %d", stats.MetadataRecords, rows)
	}
	if stats.MetadataBytes != logicalBytes {
		t.Fatalf("metadata bytes = %d, persisted logical bytes = %d", stats.MetadataBytes, logicalBytes)
	}
}

var errPersistTelemetryWrite = errors.New("persist telemetry write failed")

type failingPersistTelemetryBatch struct{ ethdb.Batch }

func (b failingPersistTelemetryBatch) Write() error { return errPersistTelemetryWrite }

type failingPersistTelemetryStore struct{ ethdb.KeyValueStore }

func (s failingPersistTelemetryStore) NewBatch() ethdb.Batch {
	return failingPersistTelemetryBatch{Batch: s.KeyValueStore.NewBatch()}
}

func (s failingPersistTelemetryStore) NewBatchWithSize(size int) ethdb.Batch {
	return failingPersistTelemetryBatch{Batch: s.KeyValueStore.NewBatchWithSize(size)}
}

func TestWriteBlockMetadataBatchDoesNotReportStatsForFailedBatch(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	store := failingPersistTelemetryStore{KeyValueStore: base}
	block := persistTelemetryBlock(8, 1)
	chain := &BlockChain{
		db:      store,
		chaindb: rawdb.NewChainDB(store, rawdb.NoopAncient{}),
	}

	stats, err := chain.writeBlockMetadataBatch(
		block, nil, tcommon.Hash{2}, persistTelemetryInfos(block), nil, true,
	)
	if !errors.Is(err, errPersistTelemetryWrite) {
		t.Fatalf("error = %v, want %v", err, errPersistTelemetryWrite)
	}
	if stats != (PersistStats{}) {
		t.Fatalf("failed batch stats = %+v, want zero", stats)
	}
}

func TestPersistStatsAdd(t *testing.T) {
	stats := PersistStats{
		MetadataRecords:       2,
		MetadataBytes:         30,
		TransactionLookupRows: 1,
		TraceAccounts:         3,
	}
	stats.Add(PersistStats{
		MetadataRecords:       6,
		MetadataBytes:         70,
		TransactionLookupRows: 4,
		TraceAccounts:         5,
	})
	if stats != (PersistStats{
		MetadataRecords:       8,
		MetadataBytes:         100,
		TransactionLookupRows: 5,
		TraceAccounts:         8,
	}) {
		t.Fatalf("aggregate = %+v", stats)
	}

	// A nil receiver is allowed so optional collectors need no branch.
	var nilStats *PersistStats
	nilStats.Add(PersistStats{MetadataRecords: 1})
}

func TestCompleteAsyncApplyTelemetryAddsPersistDetail(t *testing.T) {
	block := persistTelemetryBlock(9, 0)
	chain := new(BlockChain)
	var published ApplyStats
	chain.AddApplyStatsHook(func(gotBlock *types.Block, stats ApplyStats) {
		if gotBlock != block {
			t.Fatalf("published block = %p, want %p", gotBlock, block)
		}
		published = stats
	})
	job := &commitJob{
		block: block,
		telemetry: &applyStats{ApplyStats: ApplyStats{PersistDetail: PersistStats{
			MetadataRecords: 2,
			MetadataBytes:   30,
		}}},
		deferredPersistDetail: PersistStats{
			MetadataRecords:       6,
			MetadataBytes:         70,
			TransactionLookupRows: 3,
			TraceAccounts:         4,
		},
	}

	chain.completeAsyncApplyTelemetry(job, false, true)
	if published != (ApplyStats{}) {
		t.Fatalf("published before worker completion: %+v", published)
	}
	chain.completeAsyncApplyTelemetry(job, true, true)
	if published.PersistDetail != (PersistStats{
		MetadataRecords:       8,
		MetadataBytes:         100,
		TransactionLookupRows: 3,
		TraceAccounts:         4,
	}) {
		t.Fatalf("published persist detail = %+v", published.PersistDetail)
	}
}

func TestPersistFlushMetricsRemainProcessCumulative(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	buffer := blockbuffer.New(db)
	buffer.BeginBlock(tcommon.Hash{1}, 1)
	key, value := []byte("persist-key"), []byte("persist-value")
	if err := buffer.Put(key, value); err != nil {
		t.Fatal(err)
	}
	buffer.CommitBlock()

	layers := metrics.GetOrRegisterCounter("blockbuffer/flush/layers", nil)
	bytes := metrics.GetOrRegisterCounter("blockbuffer/flush/output/bytes", nil)
	layersBefore, bytesBefore := layers.Snapshot().Count(), bytes.Snapshot().Count()
	if err := buffer.FlushUpTo(1, db); err != nil {
		t.Fatal(err)
	}
	if got := layers.Snapshot().Count() - layersBefore; got != 1 {
		t.Fatalf("flush layer delta = %d, want 1", got)
	}
	if got, want := bytes.Snapshot().Count()-bytesBefore, int64(len(key)+len(value)); got != want {
		t.Fatalf("flush byte delta = %d, want %d", got, want)
	}
}
