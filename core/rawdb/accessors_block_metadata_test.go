package rawdb

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

var metadataRetBenchmarkSink []byte
var metadataInfoRowsBenchmarkSink [][]byte
var metadataInfoArenaSizeBenchmarkSink int

type metadataBatchProbe struct {
	ethdb.KeyValueStore
	newCalls   int
	sizedCalls int
	valueCalls int
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

func (b *metadataProbeBatch) PutValueFunc(key []byte, valueLen int, fill func([]byte) error) error {
	value := make([]byte, valueLen)
	if err := fill(value); err != nil {
		return err
	}
	b.parent.valueCalls++
	return b.Put(key, value)
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
	blockData, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteBlockMetadataBatchEncoded(probe, block, blockData, root, infos); err != nil {
		t.Fatal(err)
	}
	if probe.newCalls != 0 || probe.sizedCalls != 1 {
		t.Fatalf("batch constructors: NewBatch=%d NewBatchWithSize=%d, want 0/1", probe.newCalls, probe.sizedCalls)
	}
	if probe.valueCalls != 1 {
		t.Fatalf("deferred value calls = %d, want 1", probe.valueCalls)
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
	if gotHash, ok := ReadBlockHash(chainDB, block.Number()); !ok || gotHash != block.Hash() {
		t.Fatalf("block hash index = %x,%v want %x,true", gotHash, ok, block.Hash())
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
	wantRet, err := proto.Marshal(&corepb.TransactionRet{
		BlockNumber:     int64(block.Number()),
		BlockTimeStamp:  infos[0].BlockTimeStamp,
		Transactioninfo: infos,
	})
	if err != nil {
		t.Fatal(err)
	}
	gotRet, err := disk.Get(txInfoBlockKey(block.Number()))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotRet, wantRet) {
		t.Fatalf("transaction ret wire mismatch:\n got %x\nwant %x", gotRet, wantRet)
	}
}

func TestMarshalTransactionRetRowsMatchesProto(t *testing.T) {
	unknown := protowire.AppendTag(nil, 100, protowire.BytesType)
	unknown = protowire.AppendString(unknown, "unknown-info-field")
	infoWithUnknown := &corepb.TransactionInfo{}
	known, err := proto.Marshal(&corepb.TransactionInfo{
		Id:                     []byte{0xaa, 0xbb},
		Fee:                    17,
		BlockNumber:            21,
		BlockTimeStamp:         63_000,
		ContractResult:         [][]byte{{1, 2, 3}},
		CancelUnfreezeV2Amount: map[string]int64{"z": 9, "a": 1},
		WithdrawExpireAmount:   5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(append(known, unknown...), infoWithUnknown); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		number    int64
		timestamp int64
		infos     []*corepb.TransactionInfo
	}{
		{name: "all-zero"},
		{
			name:      "ordinary",
			number:    21,
			timestamp: 63_000,
			infos: []*corepb.TransactionInfo{
				{Id: []byte{1}, Fee: 100, BlockNumber: 21, BlockTimeStamp: 63_000},
				{Id: []byte{2}, Result: corepb.TransactionInfo_FAILED, ResMessage: []byte("revert")},
			},
		},
		{name: "empty-info-message", number: 1, infos: []*corepb.TransactionInfo{{}}},
		{name: "negative-int64-wire-values", number: -1, timestamp: -2},
		{name: "nested-map-and-unknown-fields", number: 21, timestamp: 63_000, infos: []*corepb.TransactionInfo{infoWithUnknown}},
	}
	marshal := proto.MarshalOptions{Deterministic: true}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := make([]blockMetadataRow, 0, len(test.infos))
			for _, info := range test.infos {
				data, err := marshal.Marshal(info)
				if err != nil {
					t.Fatal(err)
				}
				rows = append(rows, blockMetadataRow{value: data})
			}
			got := marshalTransactionRetRows(test.number, test.timestamp, rows)
			want, err := marshal.Marshal(&corepb.TransactionRet{
				BlockNumber:     test.number,
				BlockTimeStamp:  test.timestamp,
				Transactioninfo: test.infos,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("wire mismatch:\n got %x\nwant %x", got, want)
			}
			if len(got) != cap(got) {
				t.Fatalf("result len/cap = %d/%d, want exact allocation", len(got), cap(got))
			}
			decoded := &corepb.TransactionRet{}
			if err := proto.Unmarshal(got, decoded); err != nil {
				t.Fatal(err)
			}
			wantMessage := &corepb.TransactionRet{
				BlockNumber:     test.number,
				BlockTimeStamp:  test.timestamp,
				Transactioninfo: test.infos,
			}
			if !proto.Equal(decoded, wantMessage) {
				t.Fatalf("decoded = %v, want %v", decoded, wantMessage)
			}
		})
	}
}

func BenchmarkMarshalTransactionRetRows(b *testing.B) {
	payload := bytes.Repeat([]byte{0xab}, 512)
	infos := make([]*corepb.TransactionInfo, 100)
	rows := make([]blockMetadataRow, len(infos))
	for i := range infos {
		infos[i] = &corepb.TransactionInfo{
			Id:                   bytes.Repeat([]byte{byte(i)}, 32),
			Fee:                  int64(100 + i),
			BlockNumber:          21,
			BlockTimeStamp:       63_000,
			ContractResult:       [][]byte{payload},
			WithdrawExpireAmount: int64(i),
		}
		data, err := proto.Marshal(infos[i])
		if err != nil {
			b.Fatal(err)
		}
		rows[i].value = data
	}
	ret := &corepb.TransactionRet{
		BlockNumber:     21,
		BlockTimeStamp:  63_000,
		Transactioninfo: infos,
	}
	wireSize := len(marshalTransactionRetRows(ret.BlockNumber, ret.BlockTimeStamp, rows))

	b.Run("proto-remarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(wireSize))
		for i := 0; i < b.N; i++ {
			data, err := proto.Marshal(ret)
			if err != nil {
				b.Fatal(err)
			}
			metadataRetBenchmarkSink = data
		}
	})
	b.Run("reuse-info-payloads", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(wireSize))
		for i := 0; i < b.N; i++ {
			metadataRetBenchmarkSink = marshalTransactionRetRows(ret.BlockNumber, ret.BlockTimeStamp, rows)
		}
	})
	b.Run("direct-final-buffer", func(b *testing.B) {
		data := make([]byte, wireSize)
		b.ReportAllocs()
		b.SetBytes(int64(wireSize))
		for i := 0; i < b.N; i++ {
			encoded := appendTransactionRetRows(data[:0], ret.BlockNumber, ret.BlockTimeStamp, rows)
			if len(encoded) != wireSize {
				b.Fatalf("encoded size = %d, want %d", len(encoded), wireSize)
			}
		}
	})
}

func BenchmarkWriteBlockMetadataBatch(b *testing.B) {
	pb := newBlockProto(21, 63_000)
	pb.Transactions = make([]*corepb.Transaction, 100)
	blockPayload := bytes.Repeat([]byte{0xcd}, 512)
	signature := bytes.Repeat([]byte{0xee}, 65)
	for i := range pb.Transactions {
		pb.Transactions[i] = &corepb.Transaction{
			RawData:   &corepb.TransactionRaw{Timestamp: int64(i + 1), Data: blockPayload},
			Signature: [][]byte{signature},
		}
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
	blockData, err := block.Marshal()
	if err != nil {
		b.Fatal(err)
	}

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

	b.Run("encoded-exact-sized", func(b *testing.B) {
		db, err := NewPebbleDB(b.TempDir(), 16, 16)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { db.Close() })
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := WriteBlockMetadataBatchEncoded(db, block, blockData, root, infos); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("memory-remarshal", func(b *testing.B) {
		db := NewMemoryDatabase()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := WriteBlockMetadataBatch(db, block, root, infos); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("memory-encoded", func(b *testing.B) {
		db := NewMemoryDatabase()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := WriteBlockMetadataBatchEncoded(db, block, blockData, root, infos); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMarshalTransactionInfoRows(b *testing.B) {
	payload := bytes.Repeat([]byte{0xab}, 512)
	infos := make([]*corepb.TransactionInfo, 100)
	totalSize := 0
	for i := range infos {
		infos[i] = &corepb.TransactionInfo{
			Id:                   bytes.Repeat([]byte{byte(i)}, 32),
			Fee:                  int64(100 + i),
			BlockNumber:          21,
			BlockTimeStamp:       63_000,
			ContractResult:       [][]byte{payload},
			WithdrawExpireAmount: int64(i),
		}
		totalSize += proto.Size(infos[i])
	}
	b.SetBytes(int64(totalSize))

	b.Run("individual", func(b *testing.B) {
		rows := make([][]byte, len(infos))
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			for i, info := range infos {
				data, err := proto.Marshal(info)
				if err != nil {
					b.Fatal(err)
				}
				rows[i] = data
			}
			metadataInfoRowsBenchmarkSink = rows
		}
	})

	b.Run("pooled-arena", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			size := 0
			for _, info := range infos {
				size += proto.Size(info)
			}
			arenaPtr := borrowMetadataInfoArena(size)
			arena := *arenaPtr
			for _, info := range infos {
				var err error
				arena, err = proto.MarshalOptions{}.MarshalAppend(arena, info)
				if err != nil {
					b.Fatal(err)
				}
			}
			metadataInfoArenaSizeBenchmarkSink = len(arena) + size
			returnMetadataInfoArena(arenaPtr)
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
