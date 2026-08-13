package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestStateDomainChangeBinaryRecordRoundTrip(t *testing.T) {
	want := binaryStateDomainChange(7, 42, 3, "account/key")
	want.FlatDomain = rawdb.StateFlatDomainAccountLatest
	want.Generation = 9
	want.PrevExists = true
	want.Prev = []byte{0x01, 0x02, 0x03}
	want.NextExists = true
	want.Next = []byte{0x04, 0x05, 0x06}

	encoded, err := encodeStateDomainChangeRecord(want)
	if err != nil {
		t.Fatalf("encode record: %v", err)
	}
	got, err := decodeStateDomainChangeRecord(encoded)
	if err != nil {
		t.Fatalf("decode record: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded record mismatch:\ngot  %+v\nwant %+v", got, want)
	}

	encoded = append(encoded, 0xff)
	if _, err := decodeStateDomainChangeRecord(encoded); err == nil {
		t.Fatal("record with trailing bytes decoded successfully")
	}
}

func TestStateDomainChangeBinaryAccessorV4GroupWriterAssemblesCounts(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "groups-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	offsets := make([]uint64, 0, 2)
	writer := stateDomainChangeBinaryAccessorV4GroupETLWriter{
		payload: acquireStateDomainChangeHistoryWriter(file),
		offsets: &offsets,
	}
	put := func(group byte, recordOffset uint64) {
		t.Helper()
		value := make([]byte, stateDomainChangeBinaryAccessorV3GroupKeySize+stateDomainChangeBinaryAccessorV4GroupEntrySize)
		value[stateDomainChangeBinaryAccessorV3GroupKeySize-1] = group
		binary.BigEndian.PutUint64(value[stateDomainChangeBinaryAccessorV3GroupKeySize+4:], recordOffset)
		if err := writer.Put(nil, value); err != nil {
			t.Fatalf("put group %d: %v", group, err)
		}
	}
	put(1, stateDomainChangeBinaryHeaderSize)
	put(1, stateDomainChangeBinaryHeaderSize+1)
	put(2, stateDomainChangeBinaryHeaderSize+2)
	put(2, stateDomainChangeBinaryHeaderSize+3)
	put(2, stateDomainChangeBinaryHeaderSize+4)
	if err := writer.Finish(); err != nil {
		t.Fatalf("finish group writer: %v", err)
	}
	wantOffsets := []uint64{0, stateDomainChangeBinaryAccessorV3GroupKeySize + 8 + 2*stateDomainChangeBinaryAccessorV4GroupEntrySize}
	if !reflect.DeepEqual(offsets, wantOffsets) {
		t.Fatalf("group offsets = %v, want %v", offsets, wantOffsets)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	var assembled bytes.Buffer
	if _, err := copyStateDomainChangeBinaryAccessorV4GroupPayload(&assembled, file, offsets, writer.payloadOffset); err != nil {
		t.Fatalf("assemble group payload: %v", err)
	}
	data := assembled.Bytes()
	for i, want := range []uint64{2, 3} {
		countOffset := offsets[i] + stateDomainChangeBinaryAccessorV3GroupKeySize
		if got := binary.BigEndian.Uint64(data[countOffset : countOffset+8]); got != want {
			t.Fatalf("group %d count = %d, want %d", i, got, want)
		}
	}
}

func TestStateDomainChangeBinaryAccessorV4GroupCopyPatchesSplitCount(t *testing.T) {
	headerSize := uint64(stateDomainChangeBinaryAccessorV3GroupKeySize + 8)
	entrySize := uint64(stateDomainChangeBinaryAccessorV4GroupEntrySize)
	bufferBoundary := uint64(stateDomainChangeHistoryWriteBufferSize)
	offsets := []uint64{0, headerSize + entrySize}
	var middleCount uint64
	for middleCount = 1; ; middleCount++ {
		thirdStart := offsets[1] + headerSize + middleCount*entrySize
		countStart := thirdStart + stateDomainChangeBinaryAccessorV3GroupKeySize
		if countStart < bufferBoundary && countStart+8 > bufferBoundary {
			offsets = append(offsets, thirdStart)
			break
		}
		if countStart >= bufferBoundary {
			t.Fatal("could not place a group count across the copy buffer boundary")
		}
	}
	payloadSize := offsets[2] + headerSize + entrySize
	payload := bytes.Repeat([]byte{0xa5}, int(payloadSize))
	var assembled bytes.Buffer
	written, err := copyStateDomainChangeBinaryAccessorV4GroupPayload(&assembled, bytes.NewReader(payload), offsets, payloadSize)
	if err != nil {
		t.Fatalf("assemble split-count group payload: %v", err)
	}
	if uint64(written) != payloadSize {
		t.Fatalf("assembled payload size = %d, want %d", written, payloadSize)
	}
	for i, want := range []uint64{1, middleCount, 1} {
		countOffset := offsets[i] + stateDomainChangeBinaryAccessorV3GroupKeySize
		if got := binary.BigEndian.Uint64(assembled.Bytes()[countOffset : countOffset+8]); got != want {
			t.Fatalf("group %d count = %d, want %d", i, got, want)
		}
	}
}

func TestStateDomainChangeHistoryRecordWriterRejectsBorrowedV5OrderRegression(t *testing.T) {
	index, err := os.CreateTemp(t.TempDir(), "history-index-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	var segment bytes.Buffer
	writer := newStateDomainChangeHistoryRecordWriter(&segment, index, nil, SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 1,
		ToTxNum:   1,
	}, 2, stateDomainChangeBinaryHeaderSize)
	defer writer.Release()
	first := binaryStateDomainChange(1, 1, 2, "b")
	second := binaryStateDomainChange(1, 1, 1, "a")
	if err := writer.WriteBorrowedV5Change(first); err != nil {
		t.Fatalf("write first borrowed v5 change: %v", err)
	}
	if err := writer.WriteBorrowedV5Change(second); !errors.Is(err, errStateDomainChangeHistoryRecordsNotOrdered) {
		t.Fatalf("write regressing borrowed v5 change error = %v", err)
	}
}

func TestStateDomainChangeTxRangeCursorSeeksThenAdvances(t *testing.T) {
	rows := []*rawdb.StateTxRange{
		{BlockNum: 1, BlockHash: common.Hash{1}, BeginTxNum: 10, EndTxNum: 19},
		{BlockNum: 2, BlockHash: common.Hash{2}, BeginTxNum: 20, EndTxNum: 29},
		{BlockNum: 3, BlockHash: common.Hash{3}, BeginTxNum: 30, EndTxNum: 39},
	}
	blob := make([]byte, stateDomainChangeBinaryHeaderSize+8+len(rows)*stateDomainChangeBinaryTxRangeSize)
	binary.BigEndian.PutUint64(blob[stateDomainChangeBinaryHeaderSize:], uint64(len(rows)))
	offset := stateDomainChangeBinaryHeaderSize + 8
	for i, row := range rows {
		var raw [stateDomainChangeBinaryTxRangeSize]byte
		if err := putStateDomainChangeBinaryTxRangeEntry(&raw, row); err != nil {
			t.Fatal(err)
		}
		copy(blob[offset+i*stateDomainChangeBinaryTxRangeSize:], raw[:])
	}
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 10, ToTxNum: 39, Path: "cursor.seg"}
	cursor, err := newStateDomainChangeTxRangeCursor(bytes.NewReader(blob), uint64(len(blob)), ref, stateDomainChangeBinaryHeader{
		version: stateDomainChangeBinaryVersionV5, fromTxNum: 10, toTxNum: 39,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		txNum uint64
		block uint64
	}{{25, 2}, {26, 2}, {35, 3}} {
		row, err := cursor.txRangeForTxNum(tc.txNum)
		if err != nil {
			t.Fatalf("tx %d: %v", tc.txNum, err)
		}
		if row.BlockNum != tc.block {
			t.Fatalf("tx %d block = %d, want %d", tc.txNum, row.BlockNum, tc.block)
		}
	}
	if _, err := cursor.txRangeForTxNum(5); err == nil {
		t.Fatal("cursor accepted tx before its first range")
	}
}

func TestStateDomainChangeBinaryV5OmitsDuplicatedContextAndNext(t *testing.T) {
	changes := []*rawdb.StateDomainChange{
		binaryStateDomainChange(7, 40, 1, "slot/a"),
		binaryStateDomainChange(7, 41, 2, "slot/b"),
		binaryStateDomainChange(8, 42, 1, "slot/c"),
	}
	txRanges := []*rawdb.StateTxRange{
		{BlockNum: 7, BlockHash: changes[0].BlockHash, BeginTxNum: 40, EndTxNum: 41},
		{BlockNum: 8, BlockHash: changes[2].BlockHash, BeginTxNum: 42, EndTxNum: 42},
	}
	v5, _, _, err := encodeStateDomainChangeBinarySegment(40, 42, changes, txRanges)
	if err != nil {
		t.Fatalf("encode v5 segment: %v", err)
	}
	v2 := encodeStateDomainChangeBinarySegmentV2ForTest(t, 40, 42, changes, txRanges)
	var omitted int
	for _, change := range changes {
		// BlockNum(8) + BlockHash(32) + Seq(8) + NextExists(1) +
		// Next length(4), plus the transient next image itself.
		omitted += 53 + len(change.Next)
	}
	if got := len(v2) - len(v5); got != omitted {
		t.Fatalf("v5 saved %d bytes, want %d", got, omitted)
	}
	if version := binary.BigEndian.Uint32(v5[8:12]); version != stateDomainChangeBinaryVersionV5 {
		t.Fatalf("segment version = %d, want v5", version)
	}

	dir := t.TempDir()
	legacyRef := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 40, ToTxNum: 42, Path: "history/v2-context.seg"}
	setStateDomainChangeBinaryRefMetadata(&legacyRef, v2)
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, legacyRef.Path), v2); err != nil {
		t.Fatal(err)
	}
	legacy, err := readStateDomainChangeBinarySegment(dir, legacyRef)
	if err != nil {
		t.Fatalf("read legacy v2 segment: %v", err)
	}
	if len(legacy) != len(changes) || legacy[1].Seq != 2 || !legacy[1].NextExists || !bytes.Equal(legacy[1].Next, changes[1].Next) {
		t.Fatalf("legacy v2 context was not preserved: %+v", legacy)
	}

	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 40, ToTxNum: 42, Path: "history/v5-context.seg"}
	segRef, _, err := writeStateDomainChangeBinaryFiles(dir, ref, changes, txRanges)
	if err != nil {
		t.Fatalf("write v5 segment: %v", err)
	}
	got, err := readStateDomainChangeBinarySegment(dir, segRef)
	if err != nil {
		t.Fatalf("read v5 segment: %v", err)
	}
	for i, change := range got {
		want := changes[i]
		row := txRanges[0]
		if want.BlockNum == 8 {
			row = txRanges[1]
		}
		wantSeq, err := stateDomainChangeBinaryV5Sequence(row, want.TxNum, uint64(i))
		if err != nil {
			t.Fatal(err)
		}
		if change.BlockNum != want.BlockNum || change.BlockHash != want.BlockHash || change.TxNum != want.TxNum || change.Seq != wantSeq {
			t.Fatalf("change %d context = block %d/%x tx %d seq %d", i, change.BlockNum, change.BlockHash, change.TxNum, change.Seq)
		}
		if change.NextExists || len(change.Next) != 0 {
			t.Fatalf("change %d retained transient next image", i)
		}
	}
}

func TestStateDomainChangeBinaryV5RecordViewRoundTripAndBounds(t *testing.T) {
	want := binaryStateDomainChange(7, 42, 3, "account/key")
	want.FlatDomain = rawdb.StateFlatDomainKVLatest
	want.Generation = 9
	want.Domain = kvdomains.ContractStorage
	want.PrevExists = true
	want.Prev = []byte{0x01, 0x02, 0x03}

	payload, err := encodeStateDomainChangeRecordV5(want)
	if err != nil {
		t.Fatalf("encode v5 record: %v", err)
	}
	got, err := decodeStateDomainChangeRecordV5(payload)
	if err != nil {
		t.Fatalf("decode v5 record: %v", err)
	}
	if got.TxNum != want.TxNum || got.FlatDomain != want.FlatDomain || got.Owner != want.Owner ||
		got.Generation != want.Generation || got.Domain != want.Domain || !bytes.Equal(got.Key, want.Key) ||
		got.PrevExists != want.PrevExists || !bytes.Equal(got.Prev, want.Prev) {
		t.Fatalf("decoded v5 record mismatch:\ngot  %+v\nwant %+v", got, want)
	}
	for end := 0; end < len(payload); end++ {
		if _, err := decodeStateDomainChangeRecordV5(payload[:end]); err == nil {
			t.Fatalf("decoded v5 record truncated at %d/%d", end, len(payload))
		}
	}
	withTrailing := append(append([]byte(nil), payload...), 0xff)
	if _, err := decodeStateDomainChangeRecordV5(withTrailing); err == nil {
		t.Fatal("decoded v5 record with trailing byte")
	}
	badBool := append([]byte(nil), payload...)
	boolOffset := 8 + 1 + len(want.Owner) + 8 + 2 + 4 + len(want.Key)
	badBool[boolOffset] = 2
	if _, err := decodeStateDomainChangeRecordV5(badBool); err == nil {
		t.Fatal("decoded v5 record with invalid boolean")
	}
}

func TestStateDomainChangeBinaryV5FrameScratchReuse(t *testing.T) {
	change := binaryStateDomainChange(7, 42, 3, "account/key")
	change.PrevExists = true
	change.Prev = []byte{0x01, 0x02, 0x03}
	scratch := make([]byte, 0, 512)
	base := &scratch[:cap(scratch)][0]

	frame, err := appendStateDomainChangeBinaryRecordFrame(scratch, change)
	if err != nil {
		t.Fatal(err)
	}
	if &frame[0] != base {
		t.Fatal("record frame did not use supplied scratch storage")
	}
	if got, want := int(binary.BigEndian.Uint32(frame[:4])), len(frame)-4; got != want {
		t.Fatalf("record frame payload size = %d, want %d", got, want)
	}
	decoded, err := decodeStateDomainChangeRecordV5(frame[4:])
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.PrevExists || !bytes.Equal(decoded.Prev, change.Prev) {
		t.Fatalf("first decoded previous value = exists %t value %x", decoded.PrevExists, decoded.Prev)
	}

	change.PrevExists = false
	change.Prev = nil
	frame, err = appendStateDomainChangeBinaryRecordFrame(frame[:0], change)
	if err != nil {
		t.Fatal(err)
	}
	if &frame[0] != base {
		t.Fatal("record frame replaced reusable scratch storage")
	}
	decoded, err = decodeStateDomainChangeRecordV5(frame[4:])
	if err != nil {
		t.Fatal(err)
	}
	if decoded.PrevExists || len(decoded.Prev) != 0 {
		t.Fatalf("reused frame retained previous value = exists %t value %x", decoded.PrevExists, decoded.Prev)
	}
}

func TestStateDomainChangeBinaryV5DecodeIntoResetsBorrowedViews(t *testing.T) {
	first := binaryStateDomainChange(7, 42, 3, "account/first")
	first.PrevExists = true
	first.Prev = []byte{0x01, 0x02, 0x03}
	firstPayload, err := encodeStateDomainChangeRecordV5(first)
	if err != nil {
		t.Fatal(err)
	}

	var decoded rawdb.StateDomainChange
	if err := decodeStateDomainChangeRecordV5Into(&decoded, firstPayload); err != nil {
		t.Fatal(err)
	}
	if !decoded.PrevExists || !bytes.Equal(decoded.Key, first.Key) || !bytes.Equal(decoded.Prev, first.Prev) {
		t.Fatalf("first decode = %+v", decoded)
	}
	firstPayload[len(firstPayload)-1] = 0x7f
	if decoded.Prev[len(decoded.Prev)-1] != 0x7f {
		t.Fatal("decoded previous value does not borrow the immutable payload")
	}

	second := binaryStateDomainChange(8, 43, 1, "b")
	second.PrevExists = false
	second.Prev = nil
	secondPayload, err := encodeStateDomainChangeRecordV5(second)
	if err != nil {
		t.Fatal(err)
	}
	if err := decodeStateDomainChangeRecordV5Into(&decoded, secondPayload); err != nil {
		t.Fatal(err)
	}
	if decoded.BlockNum != 0 || decoded.BlockHash != (common.Hash{}) || decoded.Seq != 0 || decoded.NextExists || len(decoded.Next) != 0 {
		t.Fatalf("reused decode retained derived/legacy fields: %+v", decoded)
	}
	if decoded.PrevExists || len(decoded.Prev) != 0 || !bytes.Equal(decoded.Key, second.Key) {
		t.Fatalf("second decode retained first payload fields: %+v", decoded)
	}
}

func TestStateDomainChangeBinaryV5SequenceIsUniqueAcrossSplitBlock(t *testing.T) {
	dir := t.TempDir()
	blockHash := common.Hash{0x77}
	rangeRow := &rawdb.StateTxRange{BlockNum: 7, BlockHash: blockHash, BeginTxNum: 40, EndTxNum: 41}
	first := binaryStateDomainChange(7, 40, 1, "slot/a")
	first.BlockHash = blockHash
	second := binaryStateDomainChange(7, 41, 2, "slot/b")
	second.BlockHash = blockHash

	firstRef := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 40, ToTxNum: 40, Path: "history/split-40.seg"}
	secondRef := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 41, ToTxNum: 41, Path: "history/split-41.seg"}
	firstRef, _, err := writeStateDomainChangeBinaryFiles(dir, firstRef, []*rawdb.StateDomainChange{first}, []*rawdb.StateTxRange{rangeRow})
	if err != nil {
		t.Fatal(err)
	}
	secondRef, _, err = writeStateDomainChangeBinaryFiles(dir, secondRef, []*rawdb.StateDomainChange{second}, []*rawdb.StateTxRange{rangeRow})
	if err != nil {
		t.Fatal(err)
	}
	firstChanges, err := readStateDomainChangeBinarySegment(dir, firstRef)
	if err != nil {
		t.Fatal(err)
	}
	secondChanges, err := readStateDomainChangeBinarySegment(dir, secondRef)
	if err != nil {
		t.Fatal(err)
	}
	if firstChanges[0].Seq == secondChanges[0].Seq || firstChanges[0].Seq >= secondChanges[0].Seq {
		t.Fatalf("split-block sequences = %d, %d; want unique increasing values", firstChanges[0].Seq, secondChanges[0].Seq)
	}
}

func encodeStateDomainChangeBinarySegmentV2ForTest(t *testing.T, fromTxNum, toTxNum uint64, changes []*rawdb.StateDomainChange, txRanges []*rawdb.StateTxRange) []byte {
	t.Helper()
	data, _, _ := encodeStateDomainChangeBinarySegmentV2IndexesForTest(t, fromTxNum, toTxNum, changes, txRanges)
	return data
}

func encodeStateDomainChangeBinarySegmentV2IndexesForTest(t *testing.T, fromTxNum, toTxNum uint64, changes []*rawdb.StateDomainChange, txRanges []*rawdb.StateTxRange) ([]byte, []stateDomainChangeBinaryTxOffset, []stateDomainChangeBinaryAccessorEntry) {
	t.Helper()
	var out bytes.Buffer
	writeStateDomainChangeBinaryHeaderVersion(&out, stateDomainChangeBinarySegmentMagic, fromTxNum, toTxNum, uint64(len(changes)), stateDomainChangeBinaryVersionV2)
	if err := writeStateDomainChangeBinaryTxRangeTable(&out, txRanges); err != nil {
		t.Fatal(err)
	}
	var index []stateDomainChangeBinaryTxOffset
	accessor := make([]stateDomainChangeBinaryAccessorEntry, 0, len(changes))
	for i, change := range changes {
		payload, err := encodeStateDomainChangeRecord(change)
		if err != nil {
			t.Fatal(err)
		}
		offset := uint64(out.Len())
		writeUint32(&out, uint32(len(payload)))
		out.Write(payload)
		accessor = append(accessor, stateDomainChangeBinaryAccessorEntry{key: stateDomainChangeBinaryAccessorKey(change), txNum: change.TxNum, seq: change.Seq, offset: offset, recordIndex: uint64(i)})
		if len(index) == 0 || index[len(index)-1].txNum != change.TxNum {
			index = append(index, stateDomainChangeBinaryTxOffset{txNum: change.TxNum, offset: offset, recordIndex: uint64(i), count: 1})
		} else {
			index[len(index)-1].count++
		}
	}
	return out.Bytes(), index, accessor
}

func TestStateDomainChangeBinaryV2AccessorCompatibility(t *testing.T) {
	dir := t.TempDir()
	ref := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 10,
		ToTxNum:   11,
		Path:      "history/state-domain-change-v2.seg",
	}
	owner := binaryAddress(0xa4)
	changes := []*rawdb.StateDomainChange{
		binaryStateDomainChange(10, 10, 1, "slot/a"),
		binaryStateDomainChange(11, 11, 1, "slot/b"),
	}
	for _, change := range changes {
		change.Owner = owner
		change.Generation = 3
		change.Domain = kvdomains.ContractStorage
	}
	normalized := normalizeStateDomainChangesForBinary(changes)
	segmentData, index, accessor, err := encodeStateDomainChangeBinarySegment(ref.FromTxNum, ref.ToTxNum, normalized)
	if err != nil {
		t.Fatal(err)
	}
	indexData, err := encodeStateDomainChangeBinaryIndex(ref.FromTxNum, ref.ToTxNum, index)
	if err != nil {
		t.Fatal(err)
	}
	accessorData, err := encodeStateDomainChangeBinaryAccessorV2ForTest(ref.FromTxNum, ref.ToTxNum, accessor)
	if err != nil {
		t.Fatal(err)
	}
	segRef := ref
	setStateDomainChangeBinaryRefMetadata(&segRef, segmentData)
	idxRef := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentInverted, FromTxNum: ref.FromTxNum, ToTxNum: ref.ToTxNum, Path: stateDomainChangeBinaryIndexPath(ref.Path)}
	setStateDomainChangeBinaryRefMetadata(&idxRef, indexData)
	accessorRef := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor, FromTxNum: ref.FromTxNum, ToTxNum: ref.ToTxNum, Path: stateDomainChangeBinaryAccessorPath(ref.Path)}
	setStateDomainChangeBinaryRefMetadata(&accessorRef, accessorData)
	for _, file := range []struct {
		ref  SegmentRef
		data []byte
	}{{segRef, segmentData}, {idxRef, indexData}, {accessorRef, accessorData}} {
		if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, file.ref.Path), file.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := verifyStateDomainChangeBinaryCompanionsAgainstSegment(dir, segRef, idxRef, accessorRef); err != nil {
		t.Fatalf("verify v2 companions: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(10, 11, []SegmentRef{segRef, accessorRef, idxRef})); err != nil {
		t.Fatal(err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	var keyed []*rawdb.StateDomainChange
	if err := mgr.IterateStateDomainChangesByKey(10, 11, rawdb.StateFlatDomainKVLatest, owner, 3, kvdomains.ContractStorage, []byte("slot/a"), func(change *rawdb.StateDomainChange) (bool, error) {
		keyed = append(keyed, change)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	assertBinaryChangeOrder(t, keyed, []binaryChangeOrder{{txNum: 10, seq: 1, key: "slot/a"}})
	var prefixed []*rawdb.StateDomainChange
	if err := mgr.IterateStateDomainChangesByPrefix(10, 11, owner, 3, kvdomains.ContractStorage, []byte("slot/"), func(change *rawdb.StateDomainChange) (bool, error) {
		prefixed = append(prefixed, change)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	assertBinaryChangeOrder(t, prefixed, []binaryChangeOrder{{txNum: 10, seq: 1, key: "slot/a"}, {txNum: 11, seq: 2, key: "slot/b"}})
}

func TestStateDomainChangeBinaryV3AccessorRejectsCountBeyondRecordIndex(t *testing.T) {
	var data bytes.Buffer
	writeStateDomainChangeBinaryHeaderVersion(&data, stateDomainChangeBinaryAccessorMagic, 1, 1, uint64(math.MaxUint32)+1, stateDomainChangeBinaryVersionV3)
	writeUint64(&data, 0)
	header, err := readStateDomainChangeBinaryHeaderAt(bytes.NewReader(data.Bytes()), stateDomainChangeBinaryAccessorMagic)
	if err != nil {
		t.Fatalf("read v3 header: %v", err)
	}
	if _, err := stateDomainChangeBinaryAccessorV3LayoutAt(bytes.NewReader(data.Bytes()), uint64(data.Len()), header); err == nil || !strings.Contains(err.Error(), "exceeds uint32") {
		t.Fatalf("v3 oversized count error = %v, want uint32 limit", err)
	}
}

func TestStateDomainChangeBinaryV3AccessorCompatibility(t *testing.T) {
	dir := t.TempDir()
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 10, ToTxNum: 11, Path: "history/state-domain-change-v3.seg"}
	owner := binaryAddress(0xa5)
	changes := []*rawdb.StateDomainChange{binaryStateDomainChange(10, 10, 1, "slot/a"), binaryStateDomainChange(11, 11, 1, "slot/b")}
	for _, change := range changes {
		change.Owner = owner
		change.Generation = 3
		change.Domain = kvdomains.ContractStorage
	}
	segmentData, index, entries, err := encodeStateDomainChangeBinarySegment(ref.FromTxNum, ref.ToTxNum, normalizeStateDomainChangesForBinary(changes))
	if err != nil {
		t.Fatal(err)
	}
	indexData, err := encodeStateDomainChangeBinaryIndex(ref.FromTxNum, ref.ToTxNum, index)
	if err != nil {
		t.Fatal(err)
	}
	accessorData, err := encodeStateDomainChangeBinaryAccessorV3(ref.FromTxNum, ref.ToTxNum, entries)
	if err != nil {
		t.Fatal(err)
	}
	segRef := ref
	setStateDomainChangeBinaryRefMetadata(&segRef, segmentData)
	idxRef := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentInverted, FromTxNum: ref.FromTxNum, ToTxNum: ref.ToTxNum, Path: stateDomainChangeBinaryIndexPath(ref.Path)}
	setStateDomainChangeBinaryRefMetadata(&idxRef, indexData)
	accessorRef := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor, FromTxNum: ref.FromTxNum, ToTxNum: ref.ToTxNum, Path: stateDomainChangeBinaryAccessorPath(ref.Path)}
	setStateDomainChangeBinaryRefMetadata(&accessorRef, accessorData)
	for _, file := range []struct {
		ref  SegmentRef
		data []byte
	}{{segRef, segmentData}, {idxRef, indexData}, {accessorRef, accessorData}} {
		if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, file.ref.Path), file.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := verifyStateDomainChangeBinaryCompanionsAgainstSegment(dir, segRef, idxRef, accessorRef); err != nil {
		t.Fatalf("verify v3 companions: %v", err)
	}
	var got []*rawdb.StateDomainChange
	if err := iterateStateDomainChangeBinarySegmentByAccessorPrefixFile(dir, segRef, accessorRef, stateDomainChangeBinaryAccessorLookupPrefix(owner, 3, kvdomains.ContractStorage, []byte("slot/")), 10, 11, func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	assertBinaryChangeOrder(t, got, []binaryChangeOrder{{txNum: 10, seq: 1, key: "slot/a"}, {txNum: 11, seq: 2, key: "slot/b"}})
}

func encodeStateDomainChangeBinaryAccessorV2ForTest(fromTxNum, toTxNum uint64, entries []stateDomainChangeBinaryAccessorEntry) ([]byte, error) {
	sorted := append([]stateDomainChangeBinaryAccessorEntry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool {
		return compareStateDomainChangeBinaryAccessorEntry(sorted[i], sorted[j]) < 0
	})
	var out bytes.Buffer
	writeStateDomainChangeBinaryHeaderVersion(&out, stateDomainChangeBinaryAccessorMagic, fromTxNum, toTxNum, uint64(len(sorted)), stateDomainChangeBinaryVersionV2)
	payloadStart := uint64(stateDomainChangeBinaryHeaderSize + len(sorted)*8)
	var offsets bytes.Buffer
	var payload bytes.Buffer
	for i, entry := range sorted {
		if err := validateStateDomainChangeBinaryAccessorEntry(SegmentRef{Path: "v2 accessor", FromTxNum: fromTxNum, ToTxNum: toTxNum}, entry, uint64(i)); err != nil {
			return nil, err
		}
		if len(entry.key) > math.MaxUint32 {
			return nil, fmt.Errorf("accessor key too large: %d", len(entry.key))
		}
		writeUint64(&offsets, payloadStart+uint64(payload.Len()))
		writeUint32(&payload, uint32(len(entry.key)))
		payload.Write(entry.key)
		writeUint64(&payload, entry.txNum)
		writeUint64(&payload, entry.seq)
		writeUint64(&payload, entry.offset)
		writeUint64(&payload, entry.recordIndex)
	}
	out.Write(offsets.Bytes())
	out.Write(payload.Bytes())
	return out.Bytes(), nil
}

func TestStateDomainChangeBinaryFilesRoundTripChecksumSizeAndIndex(t *testing.T) {
	dir := t.TempDir()
	ref := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 10,
		ToTxNum:   12,
		Path:      "history/state-domain-change-10-12.seg",
	}
	changes := []*rawdb.StateDomainChange{
		binaryStateDomainChange(12, 12, 2, "d"),
		binaryStateDomainChange(10, 10, 2, "b"),
		binaryStateDomainChange(11, 11, 1, "c"),
		binaryStateDomainChange(10, 10, 1, "a"),
		binaryStateDomainChange(12, 12, 1, "e"),
	}

	owner := binaryAddress(0xaa)
	for _, change := range changes {
		change.Owner = owner
		change.Generation = 1
		change.Domain = kvdomains.ContractStorage
	}

	segRef, idxRef, accessorRef, err := writeStateDomainChangeBinaryFilesWithAccessor(dir, ref, changes)
	if err != nil {
		t.Fatalf("write binary files: %v", err)
	}
	if segRef.Kind != SegmentHistory || segRef.Dataset != SegmentDatasetStateDomainChange || segRef.Size == 0 || segRef.Checksum == "" {
		t.Fatalf("unexpected segment ref: %+v", segRef)
	}
	assertContentAddressedPath(t, segRef.Path, ref.Path, segRef.Checksum)
	if idxRef.Kind != SegmentInverted || idxRef.Dataset != SegmentDatasetStateDomainChange || idxRef.Path != stateDomainChangeBinaryIndexPath(segRef.Path) || idxRef.Size == 0 || idxRef.Checksum == "" {
		t.Fatalf("unexpected index ref: %+v", idxRef)
	}
	if accessorRef.Kind != SegmentAccessor || accessorRef.Dataset != SegmentDatasetStateDomainChange || accessorRef.Path != stateDomainChangeBinaryAccessorPath(segRef.Path) || accessorRef.Size == 0 || accessorRef.Checksum == "" {
		t.Fatalf("unexpected accessor ref: %+v", accessorRef)
	}
	assertFileSize(t, filepath.Join(dir, segRef.Path), segRef.Size)
	assertFileSize(t, filepath.Join(dir, idxRef.Path), idxRef.Size)
	assertFileSize(t, filepath.Join(dir, accessorRef.Path), accessorRef.Size)

	got, err := readStateDomainChangeBinarySegment(dir, segRef)
	if err != nil {
		t.Fatalf("read binary segment: %v", err)
	}
	assertBinaryChangeOrder(t, got, []binaryChangeOrder{
		{txNum: 10, seq: 1, key: "a"},
		{txNum: 10, seq: 2, key: "b"},
		{txNum: 11, seq: 3, key: "c"},
		{txNum: 12, seq: 4, key: "e"},
		{txNum: 12, seq: 5, key: "d"},
	})

	index, err := readStateDomainChangeBinaryIndex(dir, idxRef)
	if err != nil {
		t.Fatalf("read binary index: %v", err)
	}
	if err := CheckStateDomainChangeIndexSegment(dir, idxRef); err != nil {
		t.Fatalf("check binary index: %v", err)
	}
	if len(index) != 3 {
		t.Fatalf("index entries = %d, want 3", len(index))
	}
	if index[0].txNum != 10 || index[0].count != 2 || index[1].txNum != 11 || index[1].count != 1 || index[2].txNum != 12 || index[2].count != 2 {
		t.Fatalf("unexpected index entries: %+v", index)
	}
	recordOffset := binarySegmentRecordOffsetForTest(t, dir, segRef)
	if index[0].offset != recordOffset {
		t.Fatalf("first index offset = %d, want %d", index[0].offset, recordOffset)
	}
	accessor, err := readStateDomainChangeBinaryAccessor(dir, accessorRef)
	if err != nil {
		t.Fatalf("read binary accessor: %v", err)
	}
	if err := CheckStateDomainChangeAccessorSegment(dir, accessorRef); err != nil {
		t.Fatalf("check binary accessor: %v", err)
	}
	assertBinaryAccessorOrder(t, accessor, []binaryChangeOrder{
		{txNum: 10, seq: 1, key: "a"},
		{txNum: 10, seq: 2, key: "b"},
		{txNum: 11, seq: 3, key: "c"},
		{txNum: 12, seq: 5, key: "d"},
		{txNum: 12, seq: 4, key: "e"},
	})
	if accessor[0].offset != recordOffset {
		t.Fatalf("first accessor offset = %d, want %d", accessor[0].offset, recordOffset)
	}

	badSize := segRef
	badSize.Size++
	if _, err := readStateDomainChangeBinarySegment(dir, badSize); err == nil {
		t.Fatal("segment with bad size read successfully")
	}
	badChecksum := segRef
	badChecksum.Checksum = "sha256:bad"
	if _, err := readStateDomainChangeBinarySegment(dir, badChecksum); err == nil {
		t.Fatal("segment with bad checksum read successfully")
	}
	badIndexSize := idxRef
	badIndexSize.Size++
	if _, err := readStateDomainChangeBinaryIndex(dir, badIndexSize); err == nil {
		t.Fatal("index with bad size read successfully")
	}
	if err := CheckStateDomainChangeIndexSegment(dir, badIndexSize); err == nil {
		t.Fatal("index with bad size checked successfully")
	}
	badIndexChecksum := idxRef
	badIndexChecksum.Checksum = "sha256:bad"
	if _, err := readStateDomainChangeBinaryIndex(dir, badIndexChecksum); err == nil {
		t.Fatal("index with bad checksum read successfully")
	}
	if err := CheckStateDomainChangeIndexSegment(dir, badIndexChecksum); err == nil {
		t.Fatal("index with bad checksum checked successfully")
	}
	badAccessorSize := accessorRef
	badAccessorSize.Size++
	if _, err := readStateDomainChangeBinaryAccessor(dir, badAccessorSize); err == nil {
		t.Fatal("accessor with bad size read successfully")
	}
	if err := CheckStateDomainChangeAccessorSegment(dir, badAccessorSize); err == nil {
		t.Fatal("accessor with bad size checked successfully")
	}
	badAccessorChecksum := accessorRef
	badAccessorChecksum.Checksum = "sha256:bad"
	if _, err := readStateDomainChangeBinaryAccessor(dir, badAccessorChecksum); err == nil {
		t.Fatal("accessor with bad checksum read successfully")
	}
	if err := CheckStateDomainChangeAccessorSegment(dir, badAccessorChecksum); err == nil {
		t.Fatal("accessor with bad checksum checked successfully")
	}
}

func TestStateDomainChangeBinarySegmentCheckStreamsAndValidates(t *testing.T) {
	dir := t.TempDir()
	ref := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 70,
		ToTxNum:   71,
		Path:      "history/state-domain-change-70-71.seg",
	}
	changes := []*rawdb.StateDomainChange{
		binaryStateDomainChange(70, 70, 1, "a"),
		binaryStateDomainChange(71, 71, 1, "b"),
	}
	segRef, idxRef, accessorRef, err := writeStateDomainChangeBinaryFilesWithAccessor(dir, ref, changes)
	if err != nil {
		t.Fatalf("write binary files: %v", err)
	}
	if err := CheckStateDomainChangeSegment(dir, segRef); err != nil {
		t.Fatalf("check binary segment: %v", err)
	}
	checked, err := CheckRegisteredSegment(dir, segRef)
	if err != nil || !checked {
		t.Fatalf("registered binary segment check checked=%v err=%v", checked, err)
	}
	badSize := segRef
	badSize.Size++
	if err := CheckStateDomainChangeSegment(dir, badSize); err == nil {
		t.Fatal("segment with bad size checked successfully")
	}
	badChecksum := segRef
	badChecksum.Checksum = "sha256:bad"
	if err := CheckStateDomainChangeSegment(dir, badChecksum); err == nil {
		t.Fatal("segment with bad checksum checked successfully")
	}

	data := mustReadFile(t, filepath.Join(dir, segRef.Path))
	badTrailing := segRef
	badTrailing.Path = "history/state-domain-change-70-71-trailing.seg"
	trailingData := append(append([]byte(nil), data...), 0xff)
	setStateDomainChangeBinaryRefMetadata(&badTrailing, trailingData)
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, badTrailing.Path), trailingData); err != nil {
		t.Fatalf("write trailing segment: %v", err)
	}
	if err := CheckStateDomainChangeSegment(dir, badTrailing); err == nil {
		t.Fatal("segment with trailing bytes checked successfully")
	}

	hugeRecord := segRef
	hugeRecord.Path = "history/state-domain-change-70-71-huge-record.seg"
	hugeRecordData := append([]byte(nil), data...)
	recordOffset := binarySegmentRecordOffsetForTest(t, dir, segRef)
	binary.BigEndian.PutUint32(hugeRecordData[recordOffset:recordOffset+4], ^uint32(0))
	setStateDomainChangeBinaryRefMetadata(&hugeRecord, hugeRecordData)
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, hugeRecord.Path), hugeRecordData); err != nil {
		t.Fatalf("write huge-record segment: %v", err)
	}
	if err := CheckStateDomainChangeSegment(dir, hugeRecord); err == nil {
		t.Fatal("segment with oversized record frame checked successfully")
	}
	if _, err := readStateDomainChangeBinarySegmentTxRangeByIndexFile(dir, hugeRecord, idxRef, 70, 70); err == nil {
		t.Fatal("range read accepted oversized record frame")
	}
	if _, err := readStateDomainChangeBinarySegmentByAccessorFile(dir, hugeRecord, accessorRef, stateDomainChangeBinaryAccessorKey(changes[0]), 70, 70); err == nil {
		t.Fatal("key read accepted oversized record frame")
	}

	accessorData := mustReadFile(t, filepath.Join(dir, accessorRef.Path))
	if len(accessorData) < stateDomainChangeBinaryHeaderSize+8 {
		t.Fatalf("accessor too small for corruption: %d", len(accessorData))
	}
	firstEntryOffset := binary.BigEndian.Uint64(accessorData[stateDomainChangeBinaryHeaderSize : stateDomainChangeBinaryHeaderSize+8])
	if firstEntryOffset+4 > uint64(len(accessorData)) {
		t.Fatalf("first accessor entry offset %d outside file size %d", firstEntryOffset, len(accessorData))
	}
	hugeAccessor := accessorRef
	hugeAccessor.Path = "history/state-domain-change-70-71-huge-accessor.kv"
	hugeAccessorData := append([]byte(nil), accessorData...)
	binary.BigEndian.PutUint32(hugeAccessorData[firstEntryOffset:firstEntryOffset+4], ^uint32(0))
	setStateDomainChangeBinaryRefMetadata(&hugeAccessor, hugeAccessorData)
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, hugeAccessor.Path), hugeAccessorData); err != nil {
		t.Fatalf("write huge-accessor file: %v", err)
	}
	if err := CheckStateDomainChangeAccessorSegment(dir, hugeAccessor); err == nil {
		t.Fatal("accessor with oversized key frame checked successfully")
	}
	if _, err := readStateDomainChangeBinarySegmentByAccessorFile(dir, segRef, hugeAccessor, stateDomainChangeBinaryAccessorKey(changes[0]), 70, 70); err == nil {
		t.Fatal("key read accepted oversized accessor frame")
	}

	unsorted := ref
	unsorted.Path = "history/state-domain-change-70-71-unsorted.seg"
	unsortedData, _, _, err := encodeStateDomainChangeBinarySegment(unsorted.FromTxNum, unsorted.ToTxNum, []*rawdb.StateDomainChange{
		binaryStateDomainChange(71, 71, 1, "b"),
		binaryStateDomainChange(70, 70, 1, "a"),
	}, nil)
	if err != nil {
		t.Fatalf("encode unsorted segment: %v", err)
	}
	setStateDomainChangeBinaryRefMetadata(&unsorted, unsortedData)
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, unsorted.Path), unsortedData); err != nil {
		t.Fatalf("write unsorted segment: %v", err)
	}
	if err := CheckStateDomainChangeSegment(dir, unsorted); err == nil {
		t.Fatal("unsorted segment checked successfully")
	}
}

func TestStateDomainChangeBinaryStableSortAndBytes(t *testing.T) {
	ref := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 20,
		ToTxNum:   21,
		Path:      "history/state-domain-change-20-21.seg",
	}
	changes := []*rawdb.StateDomainChange{
		binaryStateDomainChange(21, 21, 1, "c"),
		binaryStateDomainChange(20, 20, 2, "b"),
		binaryStateDomainChange(20, 20, 2, "a"),
		binaryStateDomainChange(20, 20, 1, "d"),
	}
	reversed := []*rawdb.StateDomainChange{changes[3], changes[2], changes[1], changes[0]}

	dirA := t.TempDir()
	segA, idxA, accessorA, err := writeStateDomainChangeBinaryFilesWithAccessor(dirA, ref, changes)
	if err != nil {
		t.Fatalf("write A: %v", err)
	}
	dirB := t.TempDir()
	segB, idxB, accessorB, err := writeStateDomainChangeBinaryFilesWithAccessor(dirB, ref, reversed)
	if err != nil {
		t.Fatalf("write B: %v", err)
	}
	if segA.Checksum != segB.Checksum || idxA.Checksum != idxB.Checksum || accessorA.Checksum != accessorB.Checksum {
		t.Fatalf("checksums differ for reordered input: seg %q/%q idx %q/%q accessor %q/%q", segA.Checksum, segB.Checksum, idxA.Checksum, idxB.Checksum, accessorA.Checksum, accessorB.Checksum)
	}
	if segA.Path != segB.Path || idxA.Path != idxB.Path || accessorA.Path != accessorB.Path {
		t.Fatalf("paths differ for reordered input: seg %q/%q idx %q/%q accessor %q/%q", segA.Path, segB.Path, idxA.Path, idxB.Path, accessorA.Path, accessorB.Path)
	}
	segBytesA := mustReadFile(t, filepath.Join(dirA, segA.Path))
	segBytesB := mustReadFile(t, filepath.Join(dirB, segB.Path))
	if !bytes.Equal(segBytesA, segBytesB) {
		t.Fatal("segment bytes differ for reordered input")
	}
	idxBytesA := mustReadFile(t, filepath.Join(dirA, idxA.Path))
	idxBytesB := mustReadFile(t, filepath.Join(dirB, idxB.Path))
	if !bytes.Equal(idxBytesA, idxBytesB) {
		t.Fatal("index bytes differ for reordered input")
	}
	accessorBytesA := mustReadFile(t, filepath.Join(dirA, accessorA.Path))
	accessorBytesB := mustReadFile(t, filepath.Join(dirB, accessorB.Path))
	if !bytes.Equal(accessorBytesA, accessorBytesB) {
		t.Fatal("accessor bytes differ for reordered input")
	}

	got, err := readStateDomainChangeBinarySegment(dirA, segA)
	if err != nil {
		t.Fatalf("read sorted segment: %v", err)
	}
	assertBinaryChangeOrder(t, got, []binaryChangeOrder{
		{txNum: 20, seq: 1, key: "d"},
		{txNum: 20, seq: 2, key: "a"},
		{txNum: 20, seq: 3, key: "b"},
		{txNum: 21, seq: 4, key: "c"},
	})
}

func TestStateDomainChangeBinaryIndexReadsTxRange(t *testing.T) {
	dir := t.TempDir()
	ref := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 30,
		ToTxNum:   33,
		Path:      "history/state-domain-change-30-33.seg",
	}
	changes := []*rawdb.StateDomainChange{
		binaryStateDomainChange(30, 30, 1, "a"),
		binaryStateDomainChange(31, 31, 1, "b"),
		binaryStateDomainChange(31, 31, 2, "c"),
		binaryStateDomainChange(32, 32, 1, "d"),
		binaryStateDomainChange(33, 33, 1, "e"),
	}
	segRef, idxRef, err := writeStateDomainChangeBinaryFiles(dir, ref, changes)
	if err != nil {
		t.Fatalf("write binary files: %v", err)
	}
	index, err := readStateDomainChangeBinaryIndex(dir, idxRef)
	if err != nil {
		t.Fatalf("read binary index: %v", err)
	}

	got, err := readStateDomainChangeBinarySegmentTxRange(dir, segRef, index, 31, 32)
	if err != nil {
		t.Fatalf("read tx range through index: %v", err)
	}
	assertBinaryChangeOrder(t, got, []binaryChangeOrder{
		{txNum: 31, seq: 2, key: "b"},
		{txNum: 31, seq: 3, key: "c"},
		{txNum: 32, seq: 4, key: "d"},
	})
	fileGot, err := readStateDomainChangeBinarySegmentTxRangeByIndexFile(dir, segRef, idxRef, 31, 32)
	if err != nil {
		t.Fatalf("read tx range through index file: %v", err)
	}
	assertBinaryChangeOrder(t, fileGot, []binaryChangeOrder{
		{txNum: 31, seq: 2, key: "b"},
		{txNum: 31, seq: 3, key: "c"},
		{txNum: 32, seq: 4, key: "d"},
	})
}

func TestStateDomainChangeBinaryIndexReadsBlockTxRange(t *testing.T) {
	dir := t.TempDir()
	ref := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 40,
		ToTxNum:   44,
		Path:      "history/state-domain-change-40-44.seg",
	}
	changes := []*rawdb.StateDomainChange{
		binaryStateDomainChange(10, 40, 1, "a"),
		binaryStateDomainChange(11, 41, 1, "b"),
		binaryStateDomainChange(11, 42, 1, "c"),
		binaryStateDomainChange(11, 42, 2, "d"),
		binaryStateDomainChange(12, 44, 1, "e"),
	}
	block11Hash := common.Hash{0x11}
	for _, change := range changes {
		if change.BlockNum == 11 {
			change.BlockHash = block11Hash
		}
	}
	segRef, idxRef, err := writeStateDomainChangeBinaryFiles(dir, ref, changes)
	if err != nil {
		t.Fatalf("write binary files: %v", err)
	}

	got, ok, err := readStateDomainChangeBinaryTxRangeForBlockByIndexFile(dir, segRef, idxRef, 11)
	if err != nil || !ok {
		t.Fatalf("read block tx range through index file: ok=%v err=%v", ok, err)
	}
	if got.BlockNum != 11 || got.BlockHash != block11Hash || got.BeginTxNum != 41 || got.EndTxNum != 42 {
		t.Fatalf("block tx range = %+v, want block 11 tx [41,42]", got)
	}
	if _, ok, err := readStateDomainChangeBinaryTxRangeForBlockByIndexFile(dir, segRef, idxRef, 13); err != nil || ok {
		t.Fatalf("missing block tx range: ok=%v err=%v", ok, err)
	}
}

func TestStateDomainChangeBinaryStoresNoChangeBlockTxRange(t *testing.T) {
	dir := t.TempDir()
	ref := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 40,
		ToTxNum:   46,
		Path:      "history/state-domain-change-40-46.seg",
	}
	changes := []*rawdb.StateDomainChange{
		binaryStateDomainChange(10, 40, 1, "a"),
		binaryStateDomainChange(11, 41, 1, "b"),
		binaryStateDomainChange(11, 42, 1, "c"),
		binaryStateDomainChange(12, 44, 1, "e"),
	}
	noChangeHash := common.Hash{0x13}
	txRanges := []*rawdb.StateTxRange{
		{BlockNum: 13, BlockHash: noChangeHash, BeginTxNum: 45, EndTxNum: 46},
	}
	segRef, idxRef, err := writeStateDomainChangeBinaryFiles(dir, ref, changes, txRanges)
	if err != nil {
		t.Fatalf("write binary files: %v", err)
	}
	rows, err := readStateDomainChangeBinaryTxRanges(dir, segRef)
	if err != nil {
		t.Fatalf("read binary tx ranges: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("tx ranges = %d, want 4: %+v", len(rows), rows)
	}
	var streamed []*rawdb.StateTxRange
	if err := iterateStateDomainChangeBinaryTxRanges(dir, segRef, func(row *rawdb.StateTxRange) (bool, error) {
		streamed = append(streamed, cloneStateTxRangeForSegment(row))
		return true, nil
	}); err != nil {
		t.Fatalf("iterate binary tx ranges: %v", err)
	}
	if !reflect.DeepEqual(streamed, rows) {
		t.Fatalf("streamed tx ranges = %+v, want %+v", streamed, rows)
	}
	got, ok, err := readStateDomainChangeBinaryTxRangeForBlockByIndexFile(dir, segRef, idxRef, 13)
	if err != nil || !ok {
		t.Fatalf("read no-change block tx range: ok=%v err=%v", ok, err)
	}
	if got.BlockNum != 13 || got.BlockHash != noChangeHash || got.BeginTxNum != 45 || got.EndTxNum != 46 {
		t.Fatalf("no-change block tx range = %+v, want block 13 tx [45,46]", got)
	}
}

func TestStateDomainChangeBinaryTxRangePointLookupUsesLogarithmicReads(t *testing.T) {
	dir := t.TempDir()
	const rangeCount = 4096
	ref := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 1,
		ToTxNum:   rangeCount,
		Path:      "history/state-domain-change-point-lookup.seg",
	}
	ranges := make([]*rawdb.StateTxRange, rangeCount)
	for i := range ranges {
		num := uint64(i + 1)
		ranges[i] = &rawdb.StateTxRange{
			BlockNum:   num,
			BlockHash:  common.Hash{byte(num)},
			BeginTxNum: num,
			EndTxNum:   num,
		}
	}
	segRef, _, err := writeStateDomainChangeBinaryFiles(dir, ref, nil, ranges)
	if err != nil {
		t.Fatalf("write binary files: %v", err)
	}
	reader, size, header, err := openHistorySegmentForRead(dir, segRef)
	if err != nil {
		t.Fatalf("open binary segment: %v", err)
	}
	defer reader.Close()
	counted := &countingStateDomainReaderAt{reader: reader}

	got, hasTableRows, found, err := findStateDomainChangeBinaryTxRangeForBlock(counted, size, segRef, header, 3073)
	if err != nil || !hasTableRows || !found {
		t.Fatalf("point lookup: hasTableRows=%v found=%v err=%v", hasTableRows, found, err)
	}
	if got.BlockNum != 3073 || got.BeginTxNum != 3073 || got.EndTxNum != 3073 {
		t.Fatalf("point lookup range = %+v, want block 3073 tx [3073,3073]", got)
	}
	if counted.reads > 20 {
		t.Fatalf("point lookup used %d reads for %d ranges, want logarithmic reads", counted.reads, rangeCount)
	}
}

func TestStateDomainChangeBinaryV4KeyRangeSeeksTxLowerBound(t *testing.T) {
	const changeCount = 4096
	owner := binaryAddress(0xb4)
	changes := make([]*rawdb.StateDomainChange, changeCount)
	for i := range changes {
		txNum := uint64(i + 1)
		change := binaryStateDomainChange(txNum, txNum, 1, "frequent-key")
		change.Owner = owner
		change.Generation = 7
		changes[i] = change
	}
	segmentData, _, accessorEntries, err := encodeStateDomainChangeBinarySegment(1, changeCount, normalizeStateDomainChangesForBinary(changes))
	if err != nil {
		t.Fatalf("encode segment: %v", err)
	}
	accessorData, err := encodeStateDomainChangeBinaryAccessorV4(1, changeCount, accessorEntries)
	if err != nil {
		t.Fatalf("encode accessor: %v", err)
	}
	header, err := readStateDomainChangeBinaryHeaderAt(bytes.NewReader(accessorData), stateDomainChangeBinaryAccessorMagic)
	if err != nil {
		t.Fatalf("read accessor header: %v", err)
	}
	countedSegment := &countingStateDomainReaderAt{reader: bytes.NewReader(segmentData)}
	var got []*rawdb.StateDomainChange
	err = iterateStateDomainChangeBinarySegmentByAccessorV4Key(
		countedSegment, uint64(len(segmentData)), bytes.NewReader(accessorData), uint64(len(accessorData)), header,
		stateDomainChangeBinaryAccessorKey(changes[0]), 4000, changeCount,
		func(change *rawdb.StateDomainChange) (bool, error) {
			got = append(got, change)
			return false, nil
		},
	)
	if err != nil {
		t.Fatalf("seek keyed history: %v", err)
	}
	if len(got) != 1 || got[0].TxNum != 4000 {
		t.Fatalf("changes = %+v, want first tx 4000", got)
	}
	t.Logf("key tx lower-bound seek used %d segment reads for %d changes", countedSegment.reads, changeCount)
	// v4 also resolves the returned record's BlockNum/BlockHash through the
	// logarithmic StateTxRange table; 48 bounds both binary searches.
	if countedSegment.reads > 48 {
		t.Fatalf("key tx lower-bound seek used %d segment reads for %d changes, want logarithmic reads", countedSegment.reads, changeCount)
	}
}

func TestStateDomainChangeBinaryV5KeyRangeSeeksTxLowerBound(t *testing.T) {
	const changeCount = 4096
	owner := binaryAddress(0xc7)
	changes := make([]*rawdb.StateDomainChange, changeCount)
	for i := range changes {
		txNum := uint64(i + 1)
		change := binaryStateDomainChange(txNum, txNum, 1, "frequent-key")
		change.Owner = owner
		change.Generation = 7
		changes[i] = change
	}
	segmentData, _, accessorEntries, err := encodeStateDomainChangeBinarySegment(1, changeCount, normalizeStateDomainChangesForBinary(changes))
	if err != nil {
		t.Fatalf("encode segment: %v", err)
	}
	accessorData, err := encodeStateDomainChangeBinaryAccessorV5(1, changeCount, accessorEntries)
	if err != nil {
		t.Fatalf("encode accessor: %v", err)
	}
	header, err := readStateDomainChangeBinaryHeaderAt(bytes.NewReader(accessorData), stateDomainChangeBinaryAccessorMagic)
	if err != nil {
		t.Fatalf("read accessor header: %v", err)
	}
	countedSegment := &countingStateDomainReaderAt{reader: bytes.NewReader(segmentData)}
	var got []*rawdb.StateDomainChange
	err = iterateStateDomainChangeBinarySegmentByAccessorV5Key(
		countedSegment, uint64(len(segmentData)), bytes.NewReader(accessorData), uint64(len(accessorData)), header,
		stateDomainChangeBinaryAccessorKey(changes[0]), 4000, changeCount,
		func(change *rawdb.StateDomainChange) (bool, error) {
			got = append(got, change)
			return false, nil
		},
	)
	if err != nil {
		t.Fatalf("seek keyed history: %v", err)
	}
	if len(got) != 1 || got[0].TxNum != 4000 {
		t.Fatalf("changes = %+v, want first tx 4000", got)
	}
	t.Logf("v5 key tx lower-bound seek used %d segment reads for %d changes", countedSegment.reads, changeCount)
	if countedSegment.reads > 48 {
		t.Fatalf("v5 key tx lower-bound seek used %d segment reads for %d changes, want logarithmic reads", countedSegment.reads, changeCount)
	}
}

func TestStateDomainChangeBinaryV4CoverageBuffersAccessorReads(t *testing.T) {
	const changeCount = 8192
	owner := binaryAddress(0xb5)
	changes := make([]*rawdb.StateDomainChange, changeCount)
	for i := range changes {
		txNum := uint64(i + 1)
		change := binaryStateDomainChange(txNum, txNum, 1, "frequent-key")
		change.Owner = owner
		change.Generation = 7
		changes[i] = change
	}
	segmentData, _, accessorEntries, err := encodeStateDomainChangeBinarySegment(1, changeCount, normalizeStateDomainChangesForBinary(changes))
	if err != nil {
		t.Fatalf("encode segment: %v", err)
	}
	accessorData, err := encodeStateDomainChangeBinaryAccessorV4(1, changeCount, accessorEntries)
	if err != nil {
		t.Fatalf("encode accessor: %v", err)
	}
	header, err := readStateDomainChangeBinaryHeaderAt(bytes.NewReader(accessorData), stateDomainChangeBinaryAccessorMagic)
	if err != nil {
		t.Fatalf("read accessor header: %v", err)
	}
	countedAccessor := &countingStateDomainReaderAt{reader: bytes.NewReader(accessorData)}
	if err := verifyStateDomainChangeBinaryAccessorV4Coverage(
		SegmentRef{Path: "history/state-domain-change-buffered.kv"},
		bytes.NewReader(segmentData), uint64(len(segmentData)), changeCount,
		countedAccessor, uint64(len(accessorData)), header,
	); err != nil {
		t.Fatalf("verify v4 coverage: %v", err)
	}
	t.Logf("v4 coverage used %d accessor reads for %d changes", countedAccessor.reads, changeCount)
	if countedAccessor.reads > 16 {
		t.Fatalf("v4 coverage used %d accessor reads for %d changes, want bounded window reads", countedAccessor.reads, changeCount)
	}
}

func TestStateDomainChangeBinaryV4ExpectedMetadataSpillsAndCancels(t *testing.T) {
	const changeCount = 2048
	owner := binaryAddress(0xb6)
	changes := make([]*rawdb.StateDomainChange, changeCount)
	for i := range changes {
		change := binaryStateDomainChange(uint64(i+1), uint64(i+1), 1, fmt.Sprintf("slot/%08d/shared-prefix", i))
		change.Owner = owner
		change.Generation = 7
		change.Domain = kvdomains.ContractStorage
		changes[i] = change
	}
	_, _, entries, err := encodeStateDomainChangeBinarySegment(1, changeCount, normalizeStateDomainChangesForBinary(changes))
	if err != nil {
		t.Fatalf("encode segment: %v", err)
	}
	accessorData, err := encodeStateDomainChangeBinaryAccessorV4(1, changeCount, entries)
	if err != nil {
		t.Fatalf("encode accessor: %v", err)
	}

	newCollectors := func() *stateDomainChangeBinaryAccessorV4Collectors {
		collectors, err := newStateDomainChangeBinaryAccessorV4Collectors(etl.Options{TempDir: t.TempDir(), BufferLimit: 1})
		if err != nil {
			t.Fatal(err)
		}
		for i, change := range changes {
			if err := collectors.Collect(change, entries[i].offset, uint64(i)); err != nil {
				collectors.Close()
				t.Fatal(err)
			}
		}
		return collectors
	}

	collectors := newCollectors()
	metadata, stats, err := collectors.ExpectedMetadataContext(context.Background(), t.TempDir(), SegmentRef{FromTxNum: 1, ToTxNum: changeCount, Path: "expected.kv"}, changeCount)
	collectors.Close()
	if err != nil {
		t.Fatalf("build expected metadata: %v", err)
	}
	if stats.SpilledRuns == 0 {
		t.Fatalf("expected metadata ETL stats = %+v, want forced spill", stats)
	}
	if got := "sha256:" + fmt.Sprintf("%x", metadata.checksum[:]); metadata.size != uint64(len(accessorData)) || got != checksumBytes(accessorData) {
		t.Fatalf("expected metadata = %d/%s, want %d/%s", metadata.size, got, len(accessorData), checksumBytes(accessorData))
	}

	collectors = newCollectors()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = collectors.ExpectedMetadataContext(ctx, t.TempDir(), SegmentRef{FromTxNum: 1, ToTxNum: changeCount, Path: "expected.kv"}, changeCount)
	collectors.Close()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled expected metadata error = %v, want context.Canceled", err)
	}
}

type countingStateDomainReaderAt struct {
	reader interface {
		ReadAt([]byte, int64) (int, error)
	}
	reads int
}

func (r *countingStateDomainReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.reads++
	return r.reader.ReadAt(p, off)
}

func TestStateDomainChangeBinaryContentAddressedPathsDifferForSameRange(t *testing.T) {
	dir := t.TempDir()
	ref := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 40,
		ToTxNum:   41,
		Path:      "history/state-domain-change-40-41.seg",
	}

	segA, idxA, err := writeStateDomainChangeBinaryFiles(dir, ref, []*rawdb.StateDomainChange{
		binaryStateDomainChange(40, 40, 1, "a"),
	})
	if err != nil {
		t.Fatalf("write A: %v", err)
	}
	segB, idxB, err := writeStateDomainChangeBinaryFiles(dir, ref, []*rawdb.StateDomainChange{
		binaryStateDomainChange(40, 40, 1, "b"),
	})
	if err != nil {
		t.Fatalf("write B: %v", err)
	}

	if segA.Path == segB.Path {
		t.Fatalf("same-range segments used same content-addressed path %q", segA.Path)
	}
	if idxA.Path == idxB.Path {
		t.Fatalf("same-range indexes used same content-addressed path %q", idxA.Path)
	}
	assertContentAddressedPath(t, segA.Path, ref.Path, segA.Checksum)
	assertContentAddressedPath(t, segB.Path, ref.Path, segB.Checksum)
	if idxA.Path != stateDomainChangeBinaryIndexPath(segA.Path) || idxB.Path != stateDomainChangeBinaryIndexPath(segB.Path) {
		t.Fatalf("index paths do not share segment stems: %q/%q %q/%q", segA.Path, idxA.Path, segB.Path, idxB.Path)
	}
	assertFileSize(t, filepath.Join(dir, segA.Path), segA.Size)
	assertFileSize(t, filepath.Join(dir, idxA.Path), idxA.Size)
	assertFileSize(t, filepath.Join(dir, segB.Path), segB.Size)
	assertFileSize(t, filepath.Join(dir, idxB.Path), idxB.Size)
}

func TestStateDomainChangeBinaryContentAddressedPathNotDuplicated(t *testing.T) {
	dir := t.TempDir()
	ref := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 50,
		ToTxNum:   51,
		Path:      "history/state-domain-change-50-51.seg",
	}
	changes := []*rawdb.StateDomainChange{
		binaryStateDomainChange(50, 50, 1, "a"),
		binaryStateDomainChange(51, 51, 1, "b"),
	}
	segA, _, err := writeStateDomainChangeBinaryFiles(dir, ref, changes)
	if err != nil {
		t.Fatalf("write A: %v", err)
	}

	ref.Path = segA.Path
	segB, idxB, err := writeStateDomainChangeBinaryFiles(dir, ref, changes)
	if err != nil {
		t.Fatalf("write B: %v", err)
	}
	if segB.Path != segA.Path {
		t.Fatalf("content-addressed path was appended again: got %q want %q", segB.Path, segA.Path)
	}
	if idxB.Path != stateDomainChangeBinaryIndexPath(segB.Path) {
		t.Fatalf("index path = %q, want %q", idxB.Path, stateDomainChangeBinaryIndexPath(segB.Path))
	}
}

func TestStateDomainChangeBinaryIndexPathFromLegacySegmentPath(t *testing.T) {
	if got, want := stateDomainChangeBinaryIndexPath("history/state-domain-change-10-12.seg"), "history/state-domain-change-10-12.idx"; got != want {
		t.Fatalf("legacy index path = %q, want %q", got, want)
	}
	if got, want := stateDomainChangeBinaryIndexPath("history/state-domain-change-10-12-0123456789abcdef.seg"), "history/state-domain-change-10-12-0123456789abcdef.idx"; got != want {
		t.Fatalf("content-addressed index path = %q, want %q", got, want)
	}
	if got, want := stateDomainChangeBinaryAccessorPath("history/state-domain-change-10-12.seg"), "history/state-domain-change-10-12.kv"; got != want {
		t.Fatalf("legacy accessor path = %q, want %q", got, want)
	}
	if got, want := stateDomainChangeBinaryAccessorPath("history/state-domain-change-10-12-0123456789abcdef.seg"), "history/state-domain-change-10-12-0123456789abcdef.kv"; got != want {
		t.Fatalf("content-addressed accessor path = %q, want %q", got, want)
	}
}

func TestStateDomainChangeBinaryAccessorLogicalKeyDomains(t *testing.T) {
	owner := binaryAddress(0xbb)
	id := owner.AccountID()
	change := binaryStateDomainChange(60, 60, 1, "slot/a")
	change.Owner = owner
	change.Generation = 9
	change.Domain = kvdomains.ContractStorage

	key := stateDomainChangeBinaryAccessorKey(change)
	if len(key) != 1+common.AccountIDLength+8+2+len(change.Key) {
		t.Fatalf("kv latest accessor key length = %d", len(key))
	}
	if key[0] != byte(rawdb.StateFlatDomainKVLatest) || !bytes.Equal(key[1:1+common.AccountIDLength], id[:]) {
		t.Fatalf("kv latest accessor key prefix = %x", key[:1+common.AccountIDLength])
	}
	if got := binary.BigEndian.Uint64(key[1+common.AccountIDLength : 1+common.AccountIDLength+8]); got != change.Generation {
		t.Fatalf("kv latest generation = %d, want %d", got, change.Generation)
	}
	if got := kvdomains.KVDomain(binary.BigEndian.Uint16(key[1+common.AccountIDLength+8 : 1+common.AccountIDLength+8+2])); got != change.Domain {
		t.Fatalf("kv latest domain = %#04x, want %#04x", uint16(got), uint16(change.Domain))
	}
	if got := string(key[1+common.AccountIDLength+8+2:]); got != string(change.Key) {
		t.Fatalf("kv latest logical key = %q, want %q", got, change.Key)
	}

	change.FlatDomain = rawdb.StateFlatDomainKVGeneration
	generationKey := stateDomainChangeBinaryAccessorKey(change)
	if len(generationKey) != 1+common.AccountIDLength || generationKey[0] != byte(rawdb.StateFlatDomainKVGeneration) || !bytes.Equal(generationKey[1:], id[:]) {
		t.Fatalf("kv generation accessor key = %x", generationKey)
	}

	change.FlatDomain = rawdb.StateFlatDomainAccountLatest
	accountKey := stateDomainChangeBinaryAccessorKey(change)
	if len(accountKey) != 1+common.AccountIDLength || accountKey[0] != byte(rawdb.StateFlatDomainAccountLatest) || !bytes.Equal(accountKey[1:], id[:]) {
		t.Fatalf("account latest accessor key = %x", accountKey)
	}
}

type binaryChangeOrder struct {
	txNum uint64
	seq   uint64
	key   string
}

func assertBinaryChangeOrder(t *testing.T, got []*rawdb.StateDomainChange, want []binaryChangeOrder) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("changes = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].TxNum != want[i].txNum || got[i].Seq != want[i].seq || string(got[i].Key) != want[i].key {
			t.Fatalf("change %d = tx %d seq %d key %q, want tx %d seq %d key %q",
				i, got[i].TxNum, got[i].Seq, got[i].Key, want[i].txNum, want[i].seq, want[i].key)
		}
	}
}

func assertBinaryAccessorOrder(t *testing.T, got []stateDomainChangeBinaryAccessorEntry, want []binaryChangeOrder) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("accessor entries = %d, want %d: %+v", len(got), len(want), got)
	}
	const kvLatestLogicalKeyOffset = 1 + common.AccountIDLength + 8 + 2
	for i := range want {
		if len(got[i].key) < kvLatestLogicalKeyOffset {
			t.Fatalf("accessor entry %d key length = %d, want at least %d", i, len(got[i].key), kvLatestLogicalKeyOffset)
		}
		key := string(got[i].key[kvLatestLogicalKeyOffset:])
		if got[i].txNum != want[i].txNum || got[i].seq != want[i].seq || key != want[i].key {
			t.Fatalf("accessor entry %d = tx %d seq %d key %q, want tx %d seq %d key %q",
				i, got[i].txNum, got[i].seq, key, want[i].txNum, want[i].seq, want[i].key)
		}
	}
}

func assertFileSize(t *testing.T, path string, want uint64) {
	t.Helper()
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if uint64(stat.Size()) != want {
		t.Fatalf("%s size = %d, want %d", path, stat.Size(), want)
	}
}

func assertContentAddressedPath(t *testing.T, got, basePath, checksum string) {
	t.Helper()
	digest := strings.TrimPrefix(checksum, "sha256:")
	if len(digest) < snapshotPathChecksumPrefixLen {
		t.Fatalf("checksum %q shorter than path prefix length %d", checksum, snapshotPathChecksumPrefixLen)
	}
	ext := filepath.Ext(basePath)
	want := strings.TrimSuffix(basePath, ext) + "-" + digest[:snapshotPathChecksumPrefixLen] + ext
	if got != want {
		t.Fatalf("content-addressed path = %q, want %q", got, want)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func binarySegmentRecordOffsetForTest(t *testing.T, dir string, ref SegmentRef) uint64 {
	t.Helper()
	file, header, size, err := openStateDomainChangeBinarySegmentReader(dir, ref)
	if err != nil {
		t.Fatalf("open binary segment: %v", err)
	}
	defer file.Close()
	_, offset, err := readStateDomainChangeBinaryTxRangeTableAt(file, size, ref, header)
	if err != nil {
		t.Fatalf("read binary tx range table: %v", err)
	}
	return offset
}

func binaryStateDomainChange(blockNum, txNum, seq uint64, key string) *rawdb.StateDomainChange {
	return &rawdb.StateDomainChange{
		BlockNum:   blockNum,
		BlockHash:  common.Hash{byte(blockNum)},
		TxNum:      txNum,
		Seq:        seq,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      binaryAddress(byte(blockNum + txNum + seq)),
		Generation: txNum % 7,
		Domain:     kvdomains.SystemReward,
		Key:        []byte(key),
		PrevExists: true,
		Prev:       []byte("prev:" + key),
		NextExists: true,
		Next:       []byte("next:" + key),
	}
}

func binaryAddress(fill byte) common.Address {
	return common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{fill}, common.AccountIDLength)...))
}
