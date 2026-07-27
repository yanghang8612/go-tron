package rawdb

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestCompactTransactionInfoIDsPreservesOtherWireFields(t *testing.T) {
	unknownBefore := protowire.AppendTag(nil, 90, protowire.VarintType)
	unknownBefore = protowire.AppendVarint(unknownBefore, 123)
	idField := protowire.AppendTag(nil, 1, protowire.BytesType)
	idField = protowire.AppendBytes(idField, bytes.Repeat([]byte{0xab}, 32))
	feeField := protowire.AppendTag(nil, 2, protowire.VarintType)
	feeField = protowire.AppendVarint(feeField, 99)
	unknownAfter := protowire.AppendTag(nil, 91, protowire.BytesType)
	unknownAfter = protowire.AppendString(unknownAfter, "keep-me")
	info := append(append(append(append([]byte(nil), unknownBefore...), idField...), feeField...), unknownAfter...)

	outerBefore := protowire.AppendTag(nil, 1, protowire.VarintType)
	outerBefore = protowire.AppendVarint(outerBefore, 7)
	ret := append([]byte(nil), outerBefore...)
	ret = protowire.AppendTag(ret, 3, protowire.BytesType)
	ret = protowire.AppendBytes(ret, info)
	outerAfter := protowire.AppendTag(nil, 100, protowire.Fixed32Type)
	outerAfter = protowire.AppendFixed32(outerAfter, 0x11223344)
	ret = append(ret, outerAfter...)

	got, infos, removed, err := CompactTransactionInfoIDs(ret, 1)
	if err != nil {
		t.Fatal(err)
	}
	if infos != 1 || removed != len(idField) {
		t.Fatalf("infos=%d removed=%d, want 1/%d", infos, removed, len(idField))
	}
	wantInfo := append(append(append([]byte(nil), unknownBefore...), feeField...), unknownAfter...)
	want := append([]byte(nil), outerBefore...)
	want = protowire.AppendTag(want, 3, protowire.BytesType)
	want = protowire.AppendBytes(want, wantInfo)
	want = append(want, outerAfter...)
	if !bytes.Equal(got, want) {
		t.Fatalf("compacted wire:\n got %x\nwant %x", got, want)
	}

	unchanged, _, removed, err := CompactTransactionInfoIDs(ret, 2)
	if err != nil || removed != 0 || !bytes.Equal(unchanged, ret) {
		t.Fatalf("count mismatch changed row: removed=%d err=%v", removed, err)
	}
}

func TestCompactTransactionInfoIDsRejectsMalformedWire(t *testing.T) {
	if _, _, _, err := CompactTransactionInfoIDs([]byte{0x1a, 0xff}, 1); err == nil {
		t.Fatal("malformed TransactionRet unexpectedly succeeded")
	}
}

func TestCompactTransactionInfoIDsForBlockValidatesHashes(t *testing.T) {
	pb := &corepb.Block{
		Transactions: []*corepb.Transaction{{RawData: &corepb.TransactionRaw{Timestamp: 123}}},
	}
	blockData, err := proto.Marshal(pb)
	if err != nil {
		t.Fatal(err)
	}
	hash := types.NewBlockFromPB(pb).Transactions()[0].Hash()
	valid, err := proto.Marshal(&corepb.TransactionRet{Transactioninfo: []*corepb.TransactionInfo{{Id: hash[:], Fee: 9}}})
	if err != nil {
		t.Fatal(err)
	}
	compact, _, removed, err := CompactTransactionInfoIDsForBlock(valid, blockData)
	if err != nil || removed == 0 || len(compact) >= len(valid) {
		t.Fatalf("valid compact len=%d/%d removed=%d err=%v", len(compact), len(valid), removed, err)
	}
	mismatch, err := proto.Marshal(&corepb.TransactionRet{Transactioninfo: []*corepb.TransactionInfo{{Id: bytes.Repeat([]byte{0xff}, 32), Fee: 9}}})
	if err != nil {
		t.Fatal(err)
	}
	unchanged, _, removed, err := CompactTransactionInfoIDsForBlock(mismatch, blockData)
	if err != nil || removed != 0 || !bytes.Equal(unchanged, mismatch) {
		t.Fatalf("mismatched ID changed: removed=%d err=%v", removed, err)
	}
}

func TestTransactionInfoIDsMatchBlockAllowsGenesisStyleMismatch(t *testing.T) {
	blockData, err := proto.Marshal(&corepb.Block{Transactions: []*corepb.Transaction{
		{RawData: &corepb.TransactionRaw{Timestamp: 1}},
		{RawData: &corepb.TransactionRaw{Timestamp: 2}},
		{RawData: &corepb.TransactionRaw{Timestamp: 3}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	retData, err := proto.Marshal(&corepb.TransactionRet{})
	if err != nil {
		t.Fatal(err)
	}
	matched, err := TransactionInfoIDsMatchBlock(retData, blockData)
	if err != nil {
		t.Fatal(err)
	}
	if matched {
		t.Fatal("genesis-style 3/0 transaction-info mismatch reported as compactable")
	}
	compact, infos, removed, err := CompactTransactionInfoIDsForBlock(retData, blockData)
	if err != nil || infos != 0 || removed != 0 || !bytes.Equal(compact, retData) {
		t.Fatalf("genesis-style row changed: infos=%d removed=%d err=%v", infos, removed, err)
	}
}

func TestIDLessTransactionInfosRoundTripWithNewAndLegacyIndexes(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 27, Timestamp: 81_000}},
		Transactions: []*corepb.Transaction{
			{RawData: &corepb.TransactionRaw{Timestamp: 1}},
			{RawData: &corepb.TransactionRaw{Timestamp: 2}},
		},
	}
	block := types.NewBlockFromPB(pb)
	txs := block.Transactions()
	infos := make([]*corepb.TransactionInfo, len(txs))
	for i, tx := range txs {
		hash := tx.Hash()
		infos[i] = &corepb.TransactionInfo{
			Id:             append([]byte(nil), hash[:]...),
			Fee:            int64(100 + i),
			BlockNumber:    int64(block.Number()),
			BlockTimeStamp: block.Timestamp(),
		}
	}
	retData, err := proto.Marshal(&corepb.TransactionRet{
		BlockNumber:     int64(block.Number()),
		BlockTimeStamp:  block.Timestamp(),
		Transactioninfo: infos,
	})
	if err != nil {
		t.Fatal(err)
	}
	compact, count, removed, err := CompactTransactionInfoIDs(retData, len(txs))
	if err != nil || count != len(txs) || removed == 0 {
		t.Fatalf("compact count=%d removed=%d err=%v", count, removed, err)
	}

	db := NewMemoryChainDB()
	if err := WriteBlock(db, block); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(txInfoBlockKey(block.Number()), compact); err != nil {
		t.Fatal(err)
	}
	secondHash := txs[1].Hash()
	if err := WriteTransactionLocation(db, secondHash[:], block.Number(), 1); err != nil {
		t.Fatal(err)
	}
	firstHash := txs[0].Hash()
	if err := WriteTransactionIndex(db, firstHash[:], block.Number()); err != nil {
		t.Fatal(err)
	}

	rawLocation, err := db.Get(txKey(secondHash[:]))
	if err != nil {
		t.Fatal(err)
	}
	if packed := binary.BigEndian.Uint64(rawLocation); packed&transactionLocationMarker == 0 {
		t.Fatalf("new transaction location lacks marker: %x", packed)
	}
	if got := ReadTransactionIndex(db, secondHash[:]); got == nil || *got != block.Number() {
		t.Fatalf("packed index block = %v, want %d", got, block.Number())
	}

	// The packed index resolves the compact row directly by ordinal.
	if got := ReadTransactionInfo(db, secondHash[:]); !proto.Equal(got, infos[1]) {
		t.Fatalf("packed lookup changed info:\n got %v\nwant %v", got, infos[1])
	}
	// A database whose tx-* row predates ordinal packing remains readable by
	// deriving the ordinal from the corresponding block.
	if got := ReadTransactionInfo(db, firstHash[:]); !proto.Equal(got, infos[0]) {
		t.Fatalf("legacy lookup changed info:\n got %v\nwant %v", got, infos[0])
	}
	gotAll := ReadTransactionInfosByBlock(db, block.Number())
	if len(gotAll) != len(infos) {
		t.Fatalf("block lookup returned %d infos, want %d", len(gotAll), len(infos))
	}
	for i := range infos {
		if !proto.Equal(gotAll[i], infos[i]) {
			t.Fatalf("block lookup info %d changed", i)
		}
	}
}

func TestCountBlockTransactions(t *testing.T) {
	blockData, err := proto.Marshal(&corepb.Block{Transactions: []*corepb.Transaction{{}, {}, {}}})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := CountBlockTransactions(blockData); err != nil || got != 3 {
		t.Fatalf("count=%d err=%v, want 3,nil", got, err)
	}
}
