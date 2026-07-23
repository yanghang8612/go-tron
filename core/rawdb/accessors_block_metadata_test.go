package rawdb

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type metadataBatchProbe struct {
	ethdb.KeyValueStore
	newCalls   int
	sizedCalls int
	hint       int
	actual     int
}

func (p *metadataBatchProbe) NewBatch() ethdb.Batch {
	p.newCalls++
	return p.KeyValueStore.NewBatch()
}

func (p *metadataBatchProbe) NewBatchWithSize(size int) ethdb.Batch {
	p.sizedCalls++
	p.hint = size
	return &metadataProbeBatch{
		Batch:  p.KeyValueStore.NewBatchWithSize(size),
		parent: p,
		actual: metadataBatchHeaderSize,
	}
}

type metadataProbeBatch struct {
	ethdb.Batch
	parent *metadataBatchProbe
	actual int
}

func (b *metadataProbeBatch) Put(key, value []byte) error {
	b.actual += metadataBatchSetRecordSize(key, value)
	return b.Batch.Put(key, value)
}

func (b *metadataProbeBatch) Write() error {
	b.parent.actual = b.actual
	return b.Batch.Write()
}

func TestWriteBlockMetadataBatchReservesPebbleScratchAndPreservesRows(t *testing.T) {
	pb := newBlockProto(21, 63_000)
	pb.Transactions = []*corepb.Transaction{
		{RawData: &corepb.TransactionRaw{Timestamp: 1}},
		{RawData: &corepb.TransactionRaw{Timestamp: 2}},
	}
	block := types.NewBlockFromPB(pb)
	txs := block.Transactions()
	infos := make([]*corepb.TransactionInfo, len(txs))
	for i, tx := range txs {
		hash := tx.Hash()
		infos[i] = &corepb.TransactionInfo{
			Id:             append([]byte(nil), hash[:]...),
			Fee:            int64(100 + i),
			BlockNumber:    21,
			BlockTimeStamp: 63_000,
		}
	}
	root := common.Hash{0xaa, 0xbb}
	disk := NewMemoryDatabase()
	probe := &metadataBatchProbe{KeyValueStore: disk}
	if err := WriteBlockMetadataBatch(probe, block, root, infos); err != nil {
		t.Fatal(err)
	}
	if probe.newCalls != 0 || probe.sizedCalls != 1 {
		t.Fatalf("batch constructors: NewBatch=%d NewBatchWithSize=%d, want 0/1", probe.newCalls, probe.sizedCalls)
	}
	if probe.hint != probe.actual+metadataBatchRecordSlack {
		t.Fatalf("batch size hint = %d, want encoded %d + scratch %d",
			probe.hint, probe.actual, metadataBatchRecordSlack)
	}

	chainDB := NewChainDB(disk, NoopAncient{})
	gotBlock := ReadBlock(chainDB, block.Number())
	if gotBlock == nil || gotBlock.Hash() != block.Hash() {
		t.Fatalf("block metadata round trip failed: got %v", gotBlock)
	}
	if got := ReadBlockStateRoot(chainDB, block.Hash()); got != root {
		t.Fatalf("state root = %x, want %x", got, root)
	}
	blockHash := block.Hash()
	ref := taposRefBytes(block.Number())
	if got := ReadTaposRef(chainDB, ref[:]); !bytes.Equal(got, blockHash[8:16]) {
		t.Fatalf("tapos ref = %x, want %x", got, blockHash[8:16])
	}
	gotInfos := ReadTransactionInfosByBlock(chainDB, block.Number())
	if len(gotInfos) != len(infos) {
		t.Fatalf("block tx infos = %d, want %d", len(gotInfos), len(infos))
	}
	for i, tx := range txs {
		hash := tx.Hash()
		if got := ReadTransactionInfo(chainDB, hash[:]); got == nil || got.Fee != infos[i].Fee {
			t.Fatalf("tx info %d = %+v, want fee %d", i, got, infos[i].Fee)
		}
		if got := ReadTransactionIndex(chainDB, hash[:]); got == nil || *got != block.Number() {
			t.Fatalf("tx index %d = %v, want %d", i, got, block.Number())
		}
	}
}

func BenchmarkWriteBlockMetadataBatch(b *testing.B) {
	pb := newBlockProto(21, 63_000)
	pb.Transactions = make([]*corepb.Transaction, 100)
	for i := range pb.Transactions {
		pb.Transactions[i] = &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: int64(i + 1)}}
	}
	block := types.NewBlockFromPB(pb)
	txs := block.Transactions()
	payload := bytes.Repeat([]byte{0xab}, 512)
	infos := make([]*corepb.TransactionInfo, len(txs))
	for i, tx := range txs {
		hash := tx.Hash()
		infos[i] = &corepb.TransactionInfo{
			Id:             append([]byte(nil), hash[:]...),
			Fee:            int64(100 + i),
			BlockNumber:    21,
			BlockTimeStamp: 63_000,
			ContractResult: [][]byte{payload},
		}
	}
	root := common.Hash{0xaa, 0xbb}

	b.Run("legacy-unsized", func(b *testing.B) {
		db, err := NewPebbleDB(b.TempDir(), 16, 16)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { db.Close() })
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := writeBlockMetadataBatchLegacy(db, block, root, infos); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("exact-sized", func(b *testing.B) {
		db, err := NewPebbleDB(b.TempDir(), 16, 16)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { db.Close() })
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := WriteBlockMetadataBatch(db, block, root, infos); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func writeBlockMetadataBatchLegacy(db ethdb.Batcher, block *types.Block, stateRoot common.Hash, infos []*corepb.TransactionInfo) error {
	batch := db.NewBatch()
	defer closeMetadataBatch(batch)
	if err := WriteBlockStateRoot(batch, block.Hash(), stateRoot); err != nil {
		return err
	}
	if err := WriteBlock(batch, block); err != nil {
		return err
	}
	if err := WriteTaposRef(batch, block.Number(), block.Hash()); err != nil {
		return err
	}
	for _, info := range infos {
		if err := WriteTransactionInfo(batch, info.Id, info); err != nil {
			return err
		}
	}
	if err := WriteTransactionInfosByBlock(batch, block.Number(), infos); err != nil {
		return err
	}
	for _, tx := range block.Transactions() {
		hash := tx.Hash()
		if err := WriteTransactionIndex(batch, hash[:], block.Number()); err != nil {
			return err
		}
	}
	return batch.Write()
}
