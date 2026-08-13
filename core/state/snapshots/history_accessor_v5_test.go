package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestStateDomainChangeBinaryAccessorV5OffsetRoundTripAndLimit(t *testing.T) {
	for _, want := range []uint64{0, 1, 255, 256, 1<<32 - 1, stateDomainChangeBinaryAccessorV5MaxOffset} {
		var raw [stateDomainChangeBinaryAccessorV5OffsetSize]byte
		if err := putStateDomainChangeBinaryAccessorV5Offset(raw[:], want); err != nil {
			t.Fatalf("put offset %d: %v", want, err)
		}
		got, err := stateDomainChangeBinaryAccessorV5Offset(raw[:])
		if err != nil || got != want {
			t.Fatalf("offset round trip %d = %d/%v", want, got, err)
		}
	}
	var raw [stateDomainChangeBinaryAccessorV5OffsetSize]byte
	if err := putStateDomainChangeBinaryAccessorV5Offset(raw[:], stateDomainChangeBinaryAccessorV5MaxOffset+1); err == nil {
		t.Fatal("48-bit offset overflow accepted")
	}
	_, err := encodeStateDomainChangeBinaryAccessorV5(1, 1, []stateDomainChangeBinaryAccessorEntry{{
		key: []byte{1}, txNum: 1, seq: 1, offset: stateDomainChangeBinaryAccessorV5MaxOffset + 1,
	}})
	if err == nil || !strings.Contains(err.Error(), "exceeds 48 bits") {
		t.Fatalf("oversized accessor entry error = %v", err)
	}
}

func TestStateDomainChangeBinaryAccessorV5StreamingMatchesCanonicalAndSpills(t *testing.T) {
	const changeCount = 2048
	owner := binaryAddress(0xc5)
	changes := make([]*rawdb.StateDomainChange, changeCount)
	for i := range changes {
		change := binaryStateDomainChange(uint64(i+1), uint64(i+1), 1, string([]byte{byte(i >> 8), byte(i), 'x'}))
		change.Owner = owner
		change.Generation = 9
		change.Domain = kvdomains.ContractStorage
		changes[i] = change
	}
	normalized := normalizeStateDomainChangesForBinary(changes)
	_, _, entries, err := encodeStateDomainChangeBinarySegment(1, changeCount, normalized)
	if err != nil {
		t.Fatal(err)
	}
	want, err := encodeStateDomainChangeBinaryAccessorV5(1, changeCount, entries)
	if err != nil {
		t.Fatal(err)
	}
	collectors, err := newStateDomainChangeBinaryAccessorV5Collectors(etl.Options{TempDir: t.TempDir(), BufferLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer collectors.Close()
	for i, change := range normalized {
		if err := collectors.Collect(change, entries[i].offset, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	metadata, stats, err := collectors.ExpectedMetadataContext(context.Background(), t.TempDir(), SegmentRef{FromTxNum: 1, ToTxNum: changeCount, Path: "expected-v5.kv"}, changeCount)
	if err != nil {
		t.Fatal(err)
	}
	if stats.SpilledRuns == 0 {
		t.Fatalf("v5 accessor ETL stats = %+v, want forced spill", stats)
	}
	if got := "sha256:" + hex.EncodeToString(metadata.checksum[:]); metadata.size != uint64(len(want)) || got != checksumBytes(want) {
		t.Fatalf("streamed v5 metadata = %d/%s, want %d/%s", metadata.size, got, len(want), checksumBytes(want))
	}
}

func TestStateDomainChangeBinaryAccessorV5FingerprintCollisionVerifiesFullKey(t *testing.T) {
	dir := t.TempDir()
	owner := binaryAddress(0xc6)
	first := binaryStateDomainChange(1, 1, 1, "slot/a")
	second := binaryStateDomainChange(2, 2, 1, "slot/b")
	for _, change := range []*rawdb.StateDomainChange{first, second} {
		change.Owner = owner
		change.Generation = 3
		change.Domain = kvdomains.ContractStorage
	}
	segRef, _, accessorRef, err := writeStateDomainChangeBinaryFilesWithAccessor(dir, SegmentRef{
		Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
		FromTxNum: 1, ToTxNum: 2, Path: "history/state-domain-change-v5-collision.seg",
	}, []*rawdb.StateDomainChange{first, second})
	if err != nil {
		t.Fatal(err)
	}
	data := mustReadFile(t, filepath.Join(dir, accessorRef.Path))
	header, tail, err := decodeStateDomainChangeBinaryHeader(data, stateDomainChangeBinaryAccessorMagic)
	if err != nil || header.version != stateDomainChangeBinaryVersionV5 || len(tail) == 0 {
		t.Fatalf("decode v5 accessor = %+v tail=%d err=%v", header, len(tail), err)
	}
	layout, err := stateDomainChangeBinaryAccessorV5LayoutAt(bytes.NewReader(data), uint64(len(data)), header)
	if err != nil {
		t.Fatal(err)
	}
	target := stateDomainChangeBinaryAccessorV5Fingerprint(stateDomainChangeBinaryAccessorKey(second))
	entries := make([]stateDomainChangeBinaryAccessorV5ExactEntry, header.count)
	for i := range entries {
		entries[i], err = readStateDomainChangeBinaryAccessorV5ExactEntryAt(bytes.NewReader(data), layout, uint64(i))
		if err != nil {
			t.Fatal(err)
		}
		entries[i].fingerprint = target
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].offset < entries[j].offset })
	for i, entry := range entries {
		start := int(layout.exactOffset) + i*stateDomainChangeBinaryAccessorV5ExactEntrySize
		copy(data[start:start+stateDomainChangeBinaryAccessorV5FingerprintSize], entry.fingerprint[:])
		if err := putStateDomainChangeBinaryAccessorV5Offset(data[start+stateDomainChangeBinaryAccessorV5FingerprintSize:], entry.offset); err != nil {
			t.Fatal(err)
		}
		binary.BigEndian.PutUint32(data[start+stateDomainChangeBinaryAccessorV5FingerprintSize+stateDomainChangeBinaryAccessorV5OffsetSize:], entry.recordIndex)
	}
	setStateDomainChangeBinaryRefMetadata(&accessorRef, data)
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, accessorRef.Path), data); err != nil {
		t.Fatal(err)
	}
	if err := CheckStateDomainChangeAccessorSegment(dir, accessorRef); err != nil {
		t.Fatalf("collision accessor structural check: %v", err)
	}
	var got []*rawdb.StateDomainChange
	err = iterateStateDomainChangeBinarySegmentByAccessorFile(dir, segRef, accessorRef, stateDomainChangeBinaryAccessorKey(second), 1, 2, func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !bytes.Equal(got[0].Key, second.Key) {
		t.Fatalf("collision lookup returned %+v, want only %q", got, second.Key)
	}
}
