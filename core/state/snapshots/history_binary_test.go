package snapshots

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
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
	assertBinaryChangeOrder(t, prefixed, []binaryChangeOrder{{txNum: 10, seq: 1, key: "slot/a"}, {txNum: 11, seq: 1, key: "slot/b"}})
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
	assertBinaryChangeOrder(t, got, []binaryChangeOrder{{txNum: 10, seq: 1, key: "slot/a"}, {txNum: 11, seq: 1, key: "slot/b"}})
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
		{txNum: 11, seq: 1, key: "c"},
		{txNum: 12, seq: 1, key: "e"},
		{txNum: 12, seq: 2, key: "d"},
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
		{txNum: 11, seq: 1, key: "c"},
		{txNum: 12, seq: 2, key: "d"},
		{txNum: 12, seq: 1, key: "e"},
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
		binaryStateDomainChange(70, 70, 2, "b"),
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
		{txNum: 20, seq: 2, key: "b"},
		{txNum: 21, seq: 1, key: "c"},
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
		{txNum: 31, seq: 1, key: "b"},
		{txNum: 31, seq: 2, key: "c"},
		{txNum: 32, seq: 1, key: "d"},
	})
	fileGot, err := readStateDomainChangeBinarySegmentTxRangeByIndexFile(dir, segRef, idxRef, 31, 32)
	if err != nil {
		t.Fatalf("read tx range through index file: %v", err)
	}
	assertBinaryChangeOrder(t, fileGot, []binaryChangeOrder{
		{txNum: 31, seq: 1, key: "b"},
		{txNum: 31, seq: 2, key: "c"},
		{txNum: 32, seq: 1, key: "d"},
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
	if countedSegment.reads > 40 {
		t.Fatalf("key tx lower-bound seek used %d segment reads for %d changes, want logarithmic reads", countedSegment.reads, changeCount)
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
