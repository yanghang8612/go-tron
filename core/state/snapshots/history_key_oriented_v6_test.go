package snapshots

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestStateDomainChangeKeyOrientedV6RoundTripAndLookups(t *testing.T) {
	changes := normalizeStateDomainChangesForBinary(buildHistoryStructs(80, 12))
	for i, change := range changes {
		source := changes[i%31]
		change.Owner, change.Generation, change.Domain = source.Owner, source.Generation, source.Domain
		change.Key = append(change.Key[:0], source.Key...)
	}
	from, to := uint64(9_000_000), uint64(9_000_079)
	segment, index, accessor, err := encodeStateDomainChangeBinarySegmentV6(from, to, changes)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkStateDomainChangeBinaryAccessorV6(bytes.NewReader(accessor), uint64(len(accessor))); err != nil {
		t.Fatal(err)
	}
	reader := &stateDomainChangeHistoryReader{
		historySegmentReader: nopHistoryReader{ioReaderAt: bytes.NewReader(segment)},
		header:               stateDomainChangeBinaryHeader{version: stateDomainChangeBinaryVersionV6, fromTxNum: from, toTxNum: to, count: uint64(len(changes))},
		logicalSize:          uint64(len(segment)), ref: SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: from, ToTxNum: to},
		v6Accessor: nopHistoryReader{ioReaderAt: bytes.NewReader(accessor)}, v6Size: uint64(len(accessor)),
	}
	for _, entry := range index {
		off := entry.offset
		for i := uint64(0); i < entry.count; i++ {
			got, next, err := readStateDomainChangeBinaryRecordAtBoundedIndex(reader, off, uint64(len(segment)), entry.recordIndex+i)
			if err != nil {
				t.Fatal(err)
			}
			want := changes[entry.recordIndex+i]
			if got.TxNum != want.TxNum || got.FlatDomain != want.FlatDomain || got.Owner != want.Owner || got.Generation != want.Generation || got.Domain != want.Domain || !bytes.Equal(got.Key, want.Key) || got.PrevExists != want.PrevExists || !bytes.Equal(got.Prev, want.Prev) {
				t.Fatalf("record %d mismatch\ngot  %#v\nwant %#v", entry.recordIndex+i, got, want)
			}
			off = next
		}
	}
	lookup := stateDomainChangeBinaryAccessorKey(changes[7])
	var got []*rawdb.StateDomainChange
	err = iterateStateDomainChangeBinarySegmentByAccessorV6Key(reader, uint64(len(segment)), bytes.NewReader(accessor), uint64(len(accessor)), lookup, from, to, func(change *rawdb.StateDomainChange) (bool, error) { got = append(got, change); return true, nil })
	if err != nil {
		t.Fatal(err)
	}
	var want int
	for _, change := range changes {
		if bytes.Equal(stateDomainChangeBinaryAccessorKey(change), lookup) {
			want++
		}
	}
	if len(got) != want {
		t.Fatalf("key lookup got %d changes, want %d", len(got), want)
	}
	if changes[7].FlatDomain == rawdb.StateFlatDomainKVLatest {
		prefix := stateDomainChangeBinaryAccessorLookupPrefix(changes[7].Owner, changes[7].Generation, changes[7].Domain, changes[7].Key[:min(2, len(changes[7].Key))])
		got = nil
		err = iterateStateDomainChangeBinarySegmentByAccessorV6Prefix(reader, uint64(len(segment)), bytes.NewReader(accessor), uint64(len(accessor)), prefix, from, to, func(change *rawdb.StateDomainChange) (bool, error) { got = append(got, change); return true, nil })
		if err != nil {
			t.Fatal(err)
		}
		for _, change := range got {
			if !bytes.HasPrefix(stateDomainChangeBinaryAccessorKey(change), prefix) {
				t.Fatal("prefix lookup returned nonmatching key")
			}
		}
	}
}

func TestStateDomainChangeKeyOrientedV6Corruption(t *testing.T) {
	changes := normalizeStateDomainChangesForBinary(buildHistoryStructs(8, 4))
	_, _, accessor, err := encodeStateDomainChangeBinarySegmentV6(9_000_000, 9_000_007, changes)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), accessor...)
	dirOffset := int(binary.BigEndian.Uint64(corrupt[stateDomainChangeBinaryAccessorV6HeaderSize : stateDomainChangeBinaryAccessorV6HeaderSize+8]))
	keyLen := int(binary.BigEndian.Uint16(corrupt[dirOffset : dirOffset+2]))
	corrupt[dirOffset+2+keyLen] ^= 1
	if err := checkStateDomainChangeBinaryAccessorV6(bytes.NewReader(corrupt), uint64(len(corrupt))); err == nil {
		t.Fatal("directory corruption accepted")
	}
	corrupt = append([]byte(nil), accessor...)
	corrupt[72] ^= 1
	if err := checkStateDomainChangeBinaryAccessorV6(bytes.NewReader(corrupt), uint64(len(corrupt))); err == nil {
		t.Fatal("header corruption accepted")
	}
}

func TestStateDomainChangeKeyOrientedV6RejectsMismatchedDictionary(t *testing.T) {
	changes := normalizeStateDomainChangesForBinary(buildHistoryStructs(8, 4))
	segment, index, accessor, err := encodeStateDomainChangeBinarySegmentV6(9_000_000, 9_000_007, changes)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the accessor structurally valid while changing its dictionary
	// identity. Without the segment/accessor commitment this would reinterpret
	// every keyID against a different, internally consistent dictionary.
	stale := append([]byte(nil), accessor...)
	stale[72] ^= 1
	binary.BigEndian.PutUint32(stale[104:108], crc32.ChecksumIEEE(stale[:104]))
	if err := checkStateDomainChangeBinaryAccessorV6(bytes.NewReader(stale), uint64(len(stale))); err != nil {
		t.Fatalf("structurally valid stale accessor: %v", err)
	}
	reader := &stateDomainChangeHistoryReader{
		historySegmentReader: nopHistoryReader{ioReaderAt: bytes.NewReader(segment)},
		header:               stateDomainChangeBinaryHeader{version: stateDomainChangeBinaryVersionV6, fromTxNum: 9_000_000, toTxNum: 9_000_007, count: uint64(len(changes))},
		logicalSize:          uint64(len(segment)),
		ref:                  SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 9_000_000, ToTxNum: 9_000_007},
		v6Accessor:           nopHistoryReader{ioReaderAt: bytes.NewReader(stale)},
		v6Size:               uint64(len(stale)),
	}
	err = iterateStateDomainChangeBinarySegmentByAccessorV6Key(reader, uint64(len(segment)), bytes.NewReader(stale), uint64(len(stale)), []byte("definitely-absent"), 9_000_000, 9_000_007, func(*rawdb.StateDomainChange) (bool, error) {
		return true, nil
	})
	if err == nil || !strings.Contains(err.Error(), "dictionary commitment mismatch") {
		t.Fatalf("negative lookup with mismatched V6 dictionary error = %v", err)
	}
	_, _, err = readStateDomainChangeBinaryRecordAtBoundedIndex(reader, index[0].offset, uint64(len(segment)), index[0].recordIndex)
	if err == nil || !strings.Contains(err.Error(), "dictionary commitment mismatch") {
		t.Fatalf("mismatched V6 dictionary error = %v", err)
	}
}

func TestStateDomainChangeKeyOrientedV6LookupReadsLogarithmicDirectory(t *testing.T) {
	const count = 32_768
	entries := make([]stateDomainChangeBinaryAccessorEntry, count)
	for i := range entries {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(i))
		entries[i] = stateDomainChangeBinaryAccessorEntry{
			key: key, txNum: uint64(i), offset: uint64(i + 1), recordIndex: uint64(i),
		}
	}
	accessor, _, err := encodeStateDomainChangeBinaryAccessorV6(0, count-1, entries)
	if err != nil {
		t.Fatal(err)
	}
	reader := &countingStateDomainReaderAt{reader: bytes.NewReader(accessor)}
	lookup := make([]byte, 4)
	binary.BigEndian.PutUint32(lookup, count-17)
	_, record, ok, err := stateDomainChangeBinaryAccessorV6Lookup(reader, uint64(len(accessor)), lookup)
	if err != nil || !ok {
		t.Fatalf("lookup ok=%t err=%v", ok, err)
	}
	if record.keyID != count-17 {
		t.Fatalf("key id = %d, want %d", record.keyID, count-17)
	}
	if reader.reads > 48 {
		t.Fatalf("V6 lookup used %d accessor reads for %d keys, want logarithmic directory reads", reader.reads, count)
	}
}

type nopHistoryReader struct{ ioReaderAt }
type ioReaderAt interface {
	ReadAt([]byte, int64) (int, error)
}

func (nopHistoryReader) Close() error { return nil }
