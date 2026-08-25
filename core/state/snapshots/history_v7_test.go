package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

var stateDomainChangeV7FrameBenchmarkSink []stateDomainChangeBinaryAccessorV6Posting

func BenchmarkStateDomainChangeV7SingleFrameRead(b *testing.B) {
	const count = uint16(stateDomainChangeBinaryAccessorV7FramePostings)
	postings := make([]stateDomainChangeBinaryAccessorV6Posting, count)
	for i := range postings {
		postings[i] = stateDomainChangeBinaryAccessorV6Posting{
			txNum:       10_000 + uint64(i),
			offset:      1_000 + uint64(i)*97,
			recordIndex: uint32(i),
		}
	}
	encoded, err := encodeStateDomainChangeBinaryAccessorV7PostingList(10_000, postings)
	if err != nil {
		b.Fatal(err)
	}
	h := stateDomainChangeBinaryAccessorV6Header{fromTxNum: 10_000, postingLen: uint64(len(encoded))}
	record := stateDomainChangeBinaryAccessorV6Record{postings: uint32(count)}
	frame := stateDomainChangeBinaryAccessorV7Frame{count: count, dataOff: 1}
	reader := bytes.NewReader(encoded)

	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for b.Loop() {
		got, err := stateDomainChangeBinaryAccessorV7ReadFrame(reader, h, record, frame)
		if err != nil {
			b.Fatal(err)
		}
		stateDomainChangeV7FrameBenchmarkSink = got
	}
}

func buildStateDomainChangeV7AccessorBytes(t testing.TB, keys int) []byte {
	t.Helper()
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, make([]byte, common.AccountIDLength)...))
	for i := 1; i <= keys; i++ {
		txNum := uint64(i)
		var hash common.Hash
		binary.BigEndian.PutUint64(hash[24:], txNum)
		if err := rawdb.WriteStateTxRange(db, txNum, hash, txNum, txNum); err != nil {
			t.Fatal(err)
		}
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, txNum)
		if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
			BlockNum: txNum, BlockHash: hash, TxNum: txNum, Seq: 1,
			FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: 5,
			Domain: kvdomains.ContractStorage, Key: key, PrevExists: true, Prev: []byte{byte(i)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, uint64(keys), "history/state-domain-change-validation.seg")
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		if ref.Kind != SegmentAccessor {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, ref.Path))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	t.Fatal("V7 build did not publish an accessor")
	return nil
}

func TestStateDomainChangeV7ValidationUsesBoundedSourceReads(t *testing.T) {
	const keys = 2_048 // One single-frame posting list per distinct key.
	data := buildStateDomainChangeV7AccessorBytes(t, keys)

	direct := &countingStateDomainReaderAt{reader: bytes.NewReader(data)}
	if err := checkStateDomainChangeBinaryAccessorV7(direct, uint64(len(data))); err != nil {
		t.Fatalf("direct validation: %v", err)
	}

	source := &countingStateDomainReaderAt{reader: bytes.NewReader(data)}
	buffered := acquireStateDomainChangeAccessorValidationReader(source, uint64(len(data)))
	if err := checkStateDomainChangeBinaryAccessorV7(buffered, uint64(len(data))); err != nil {
		releaseStateDomainChangeAccessorValidationReader(&buffered)
		t.Fatalf("windowed validation: %v", err)
	}
	releaseStateDomainChangeAccessorValidationReader(&buffered)

	fileWindows := (len(data) + stateDomainChangeAccessorValidationWindowSize - 1) / stateDomainChangeAccessorValidationWindowSize
	maxSourceReads := 4*fileWindows + 2 // Allow alternating dictionary/posting windows.
	if source.reads > maxSourceReads {
		t.Fatalf("windowed V7 validation used %d source reads for %d bytes/%d windows, want <= %d", source.reads, len(data), fileWindows, maxSourceReads)
	}
	if direct.reads < source.reads*100 {
		t.Fatalf("V7 validation source-read reduction = %d/%d, want at least 100x", direct.reads, source.reads)
	}
	t.Logf("V7 validation ReaderAt calls direct=%d windowed=%d bytes=%d", direct.reads, source.reads, len(data))
}

func TestStateDomainChangeV7SingleFrameBulkReadRejectsCorruption(t *testing.T) {
	postings := []stateDomainChangeBinaryAccessorV6Posting{
		{txNum: 100, offset: 50, recordIndex: 0},
		{txNum: 102, offset: 75, recordIndex: 1},
	}
	encoded, err := encodeStateDomainChangeBinaryAccessorV7PostingList(100, postings)
	if err != nil {
		t.Fatal(err)
	}
	h := stateDomainChangeBinaryAccessorV6Header{fromTxNum: 100, postingLen: uint64(len(encoded))}
	record := stateDomainChangeBinaryAccessorV6Record{postings: uint32(len(postings))}
	frame := stateDomainChangeBinaryAccessorV7Frame{count: uint16(len(postings)), dataOff: 1}
	if _, err := stateDomainChangeBinaryAccessorV7ReadFrame(bytes.NewReader(encoded), h, record, frame); err != nil {
		t.Fatalf("valid single frame: %v", err)
	}

	corrupt := append([]byte(nil), encoded...)
	corrupt[1] ^= 0x01
	if _, err := stateDomainChangeBinaryAccessorV7ReadFrame(bytes.NewReader(corrupt), h, record, frame); err == nil {
		t.Fatal("bulk single-frame read accepted corrupt payload")
	}

	truncated := encoded[:len(encoded)-1]
	h.postingLen = uint64(len(truncated))
	if _, err := stateDomainChangeBinaryAccessorV7ReadFrame(bytes.NewReader(truncated), h, record, frame); err == nil {
		t.Fatal("bulk single-frame read accepted truncated checksum")
	}
}

func TestStateDomainChangeIndexV7RoundTripAndCorruption(t *testing.T) {
	dir := t.TempDir()
	file, name, err := createStateDomainChangeBinaryTempFileInDir(dir, "history.idx")
	if err != nil {
		t.Fatal(err)
	}
	if err := writeStateDomainChangeBinaryHeaderToVersion(file, stateDomainChangeBinaryIndexMagic, 1_000, 2_000, 0, stateDomainChangeBinaryIndexVersion); err != nil {
		t.Fatal(err)
	}
	const count = 700
	offset := uint64(12345)
	recordIndex := uint64(0)
	for i := uint64(0); i < count; i++ {
		entry := stateDomainChangeBinaryTxOffset{txNum: 1_000 + i, offset: offset, recordIndex: recordIndex, count: 1 + i%5}
		if err := writeStateDomainChangeBinaryIndexEntryTo(file, entry); err != nil {
			t.Fatal(err)
		}
		offset += 17 + i%11
		recordIndex += entry.count
	}
	if err := writeStateDomainChangeBinaryHeaderCount(file, count); err != nil {
		t.Fatal(err)
	}
	file, name, err = rewriteStateDomainChangeBinaryIndexV7(file, name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close(); _ = os.Remove(name) }()
	stat, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if uint64(stat.Size()) >= uint64(stateDomainChangeBinaryHeaderSize)+count*stateDomainChangeBinaryIndexEntrySize {
		t.Fatalf("V7 index size %d did not shrink fixed layout", stat.Size())
	}
	header, err := readStateDomainChangeBinaryHeaderAt(file, stateDomainChangeBinaryIndexMagic)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := openStateDomainChangeBinaryIndexV7Reader(file, uint64(stat.Size()), header)
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []uint64{0, 1, 255, 256, 511, 699} {
		entry, err := readStateDomainChangeBinaryIndexEntryAt(reader, index)
		if err != nil {
			t.Fatal(err)
		}
		if entry.txNum != 1_000+index || entry.count != 1+index%5 {
			t.Fatalf("entry %d = %+v", index, entry)
		}
	}
	frame := reader.frames[1]
	var one [1]byte
	if _, err := file.ReadAt(one[:], int64(frame.dataOff)); err != nil {
		t.Fatal(err)
	}
	one[0] ^= 0x80
	if _, err := file.WriteAt(one[:], int64(frame.dataOff)); err != nil {
		t.Fatal(err)
	}
	reader.cacheValid = false
	if _, err := readStateDomainChangeBinaryIndexEntryAt(reader, 256); err == nil {
		t.Fatal("corrupt V7 index frame was accepted")
	}
}

func TestStateDomainChangeV7FramedAccessorQuery(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, make([]byte, common.AccountIDLength)...))
	changes := make([]*rawdb.StateDomainChange, 0, 300)
	for txNum := uint64(1); txNum <= 300; txNum++ {
		var hash common.Hash
		binary.BigEndian.PutUint64(hash[24:], txNum)
		if err := rawdb.WriteStateTxRange(db, txNum, hash, txNum, txNum); err != nil {
			t.Fatal(err)
		}
		change := &rawdb.StateDomainChange{
			BlockNum: txNum, BlockHash: hash, TxNum: txNum, Seq: 1,
			FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: 9,
			Domain: kvdomains.ContractStorage, Key: []byte("hot-slot"), PrevExists: true, Prev: []byte{byte(txNum)},
		}
		changes = append(changes, change)
		if err := rawdb.WriteStateDomainChange(db, change); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 300, "history/state-domain-change-1-300.seg")
	if err != nil {
		t.Fatal(err)
	}
	var historyRef, accessorRef, indexRef SegmentRef
	for _, ref := range refs {
		switch ref.Kind {
		case SegmentHistory:
			historyRef = ref
		case SegmentAccessor:
			accessorRef = ref
		case SegmentInverted:
			indexRef = ref
		}
	}
	accessor, header, size, err := openStateDomainChangeBinaryAccessorReader(dir, accessorRef)
	if err != nil {
		t.Fatal(err)
	}
	if header.version != stateDomainChangeBinaryVersionV7 {
		t.Fatalf("accessor version = %d", header.version)
	}
	if err := checkStateDomainChangeBinaryAccessorV7(accessor, size); err != nil {
		_ = accessor.Close()
		t.Fatal(err)
	}
	_ = accessor.Close()
	index, indexHeader, err := openStateDomainChangeBinaryIndexReader(dir, indexRef)
	if err != nil {
		t.Fatal(err)
	}
	if indexHeader.version != stateDomainChangeBinaryVersionV7 {
		t.Fatalf("index version = %d", indexHeader.version)
	}
	_ = index.Close()
	_, _, legacyAccessor, err := encodeStateDomainChangeBinarySegmentV6(1, 300, changes)
	if err != nil {
		t.Fatal(err)
	}
	accessorInfo, err := os.Stat(filepath.Join(dir, accessorRef.Path))
	if err != nil {
		t.Fatal(err)
	}
	indexInfo, err := os.Stat(filepath.Join(dir, indexRef.Path))
	if err != nil {
		t.Fatal(err)
	}
	if uint64(accessorInfo.Size())*2 >= uint64(len(legacyAccessor)) {
		t.Fatalf("V7 accessor size %d did not halve V6 size %d", accessorInfo.Size(), len(legacyAccessor))
	}
	legacyIndexSize := uint64(stateDomainChangeBinaryHeaderSize) + 300*stateDomainChangeBinaryIndexEntrySize
	t.Logf("V7/V6 synthetic hot-key bytes: accessor=%d/%d index=%d/%d", accessorInfo.Size(), len(legacyAccessor), indexInfo.Size(), legacyIndexSize)
	if uint64(indexInfo.Size())*2 >= legacyIndexSize {
		t.Fatalf("V7 index size %d did not halve V2 size %d", indexInfo.Size(), legacyIndexSize)
	}
	lookup := stateDomainChangeBinaryAccessorLookupKey(rawdb.StateFlatDomainKVLatest, owner, 9, kvdomains.ContractStorage, []byte("hot-slot"))
	var got []uint64
	err = iterateStateDomainChangeBinarySegmentByAccessorFile(dir, historyRef, accessorRef, lookup, 130, 260, func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, change.TxNum)
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 131 || got[0] != 130 || got[len(got)-1] != 260 {
		t.Fatalf("framed lookup = %d rows [%d,%d]", len(got), got[0], got[len(got)-1])
	}
}

func TestManagerBatchesFirstStateDomainChangesAcrossV7Segment(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, make([]byte, common.AccountIDLength)...))
	for txNum := uint64(1); txNum <= 100; txNum++ {
		var hash common.Hash
		binary.BigEndian.PutUint64(hash[24:], txNum)
		if err := rawdb.WriteStateTxRange(db, txNum, hash, txNum, txNum); err != nil {
			t.Fatal(err)
		}
		var key []byte
		switch txNum {
		case 20, 80:
			key = []byte("alpha")
		case 30:
			key = []byte("beta")
		case 70:
			key = []byte("gamma")
		default:
			continue
		}
		if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
			BlockNum: txNum, BlockHash: hash, TxNum: txNum, Seq: 1,
			FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: 4,
			Domain: kvdomains.SystemDynamicProperty, Key: key, PrevExists: true, Prev: []byte{byte(txNum)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 50, "history/state-domain-change-1-50.seg")
	if err != nil {
		t.Fatal(err)
	}
	later, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 51, 100, "history/state-domain-change-51-100.seg")
	if err != nil {
		t.Fatal(err)
	}
	refs = append(refs, later...)
	if err := PublishManifest(dir, NewManifest(1, 100, refs)); err != nil {
		t.Fatal(err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := mgr.FirstStateDomainChangesByKeysContext(context.Background(), 10, 90, rawdb.StateFlatDomainKVLatest, owner, 4, kvdomains.SystemDynamicProperty, [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("missing")})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got["alpha"] == nil || got["alpha"].TxNum != 20 || got["beta"] == nil || got["beta"].TxNum != 30 || got["gamma"] == nil || got["gamma"].TxNum != 70 || got["missing"] != nil {
		t.Fatalf("batched first changes = %+v, want alpha@20 beta@30 gamma@70", got)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := mgr.FirstStateDomainChangesByKeysContext(canceled, 10, 90, rawdb.StateFlatDomainKVLatest, owner, 4, kvdomains.SystemDynamicProperty, [][]byte{[]byte("alpha")}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled batch error = %v, want context.Canceled", err)
	}
}

func TestStateDomainChangeV7SequentialVerificationExternalSort(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, make([]byte, common.AccountIDLength)...))
	for txNum := uint64(1); txNum <= 300; txNum++ {
		var hash common.Hash
		binary.BigEndian.PutUint64(hash[24:], txNum)
		if err := rawdb.WriteStateTxRange(db, txNum, hash, txNum, txNum); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
			BlockNum: txNum, BlockHash: hash, TxNum: txNum, Seq: 1,
			FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: txNum % 7,
			Domain: kvdomains.ContractStorage, Key: []byte{byte(txNum % 19)}, PrevExists: true, Prev: []byte{byte(txNum)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 300, "history/state-domain-change-1-300.seg")
	if err != nil {
		t.Fatal(err)
	}
	var historyRef, indexRef, accessorRef SegmentRef
	for _, ref := range refs {
		switch ref.Kind {
		case SegmentHistory:
			historyRef = ref
		case SegmentInverted:
			indexRef = ref
		case SegmentAccessor:
			accessorRef = ref
		}
	}
	history, historyHeader, historySize, err := openStateDomainChangeBinarySegmentReader(dir, historyRef)
	if err != nil {
		t.Fatal(err)
	}
	defer history.Close()
	historyReader := contextReaderAt{ctx: context.Background(), r: history}
	recordOffset, err := validateStateDomainChangeBinaryTxRangeTableAt(historyReader, historySize, historyRef, historyHeader)
	if err != nil {
		t.Fatal(err)
	}
	index, indexHeader, err := openStateDomainChangeBinaryIndexReader(dir, indexRef)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	accessor, accessorHeader, accessorSize, err := openStateDomainChangeBinaryAccessorReader(dir, accessorRef)
	if err != nil {
		t.Fatal(err)
	}
	defer accessor.Close()
	if accessorHeader.version != stateDomainChangeBinaryVersionV7 {
		t.Fatalf("accessor version = %d, want V7", accessorHeader.version)
	}
	if err := verifyStateDomainChangeBinaryV7CoverageSequentialWithBuffer(context.Background(), dir, historyRef, indexRef, historyReader, historySize, recordOffset, historyHeader, index, indexHeader.count, accessor, accessorSize, 1); err != nil {
		t.Fatalf("verify forced external-sort path: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := verifyStateDomainChangeBinaryV7CoverageSequentialWithBuffer(canceled, dir, historyRef, indexRef, historyReader, historySize, recordOffset, historyHeader, index, indexHeader.count, accessor, accessorSize, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled verification error = %v, want context.Canceled", err)
	}
}
