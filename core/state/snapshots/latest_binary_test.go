package snapshots

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestLatestBinarySegmentRoundTripGetAndIteratePrefix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "latest", "system-dp-1-5.seg")
	owner1 := latestBinaryAddress(0x11)
	owner2 := latestBinaryAddress(0x22)
	key1 := AccountKVSnapshotKey(owner1, 7, []byte("alpha"))
	key2 := AccountKVSnapshotKey(owner1, 7, []byte("beta"))
	key3 := AccountKVSnapshotKey(owner2, 9, []byte("gamma"))
	seg := &LatestSegment{
		Version:   LatestSegmentVersion,
		Dataset:   SegmentDatasetKVLatest,
		Domain:    kvdomains.SystemDynamicProperty,
		FromTxNum: 1,
		ToTxNum:   5,
		Entries: []LatestEntry{
			{Key: key3, Value: []byte("v3")},
			{Key: key2, Value: []byte("v2")},
			{Key: key1, Value: []byte("v1")},
		},
	}

	finalPath, size, checksum, err := writeLatestBinarySegment(path, seg)
	if err != nil {
		t.Fatalf("write latest binary segment: %v", err)
	}
	if size == 0 || checksum == "" {
		t.Fatalf("metadata not filled: size=%d checksum=%q", size, checksum)
	}
	if !isLatestBinarySegmentPath(path) {
		t.Fatalf("%s was not detected as latest binary segment path", path)
	}
	if finalPath == path {
		t.Fatalf("latest binary segment path was not content addressed: %q", finalPath)
	}
	if filepath.Ext(finalPath) != ".seg" {
		t.Fatalf("content-addressed path = %q, want .seg extension", finalPath)
	}
	if isLatestBinarySegmentPath(filepath.Join(dir, "latest", "system-dp-1-5.json")) {
		t.Fatal("json path detected as latest binary segment path")
	}

	got, err := readLatestBinarySegment(finalPath, SegmentRef{
		Dataset:   SegmentDatasetKVLatest,
		Domain:    kvdomains.SystemDynamicProperty,
		Kind:      SegmentLatest,
		FromTxNum: 1,
		ToTxNum:   5,
		Size:      size,
		Checksum:  checksum,
	})
	if err != nil {
		t.Fatalf("read latest binary segment: %v", err)
	}
	value, ok, err := got.Get(key2)
	if err != nil || !ok || string(value) != "v2" {
		t.Fatalf("Get = %q ok=%v err=%v", value, ok, err)
	}
	var keys [][]byte
	var values []string
	if err := got.IteratePrefix(AccountKVSnapshotKey(owner1, 7, nil), func(key, value []byte) (bool, error) {
		keys = append(keys, key)
		values = append(values, string(value))
		return true, nil
	}); err != nil {
		t.Fatalf("IteratePrefix: %v", err)
	}
	if len(keys) != 2 || !bytes.Equal(keys[0], key1) || !bytes.Equal(keys[1], key2) || values[0] != "v1" || values[1] != "v2" {
		t.Fatalf("prefix iteration keys=%x values=%q", keys, values)
	}
}

func TestLatestBinarySegmentStableSortAndBytes(t *testing.T) {
	owner1 := latestBinaryAddress(0x01)
	owner2 := latestBinaryAddress(0x02)
	owner3 := latestBinaryAddress(0x03)
	entries := []LatestEntry{
		{Key: AccountSnapshotKey(owner3), Value: []byte("c")},
		{Key: AccountSnapshotKey(owner1), Value: []byte("a")},
		{Key: AccountSnapshotKey(owner2), Value: []byte("b")},
	}
	reversed := []LatestEntry{entries[2], entries[1], entries[0]}
	segA := latestBinaryAccountSegment(entries)
	segB := latestBinaryAccountSegment(reversed)
	dirA := t.TempDir()
	dirB := t.TempDir()
	pathA := filepath.Join(dirA, "latest", "accounts.seg")
	pathB := filepath.Join(dirB, "latest", "accounts.seg")

	finalPathA, sizeA, checksumA, err := writeLatestBinarySegment(pathA, segA)
	if err != nil {
		t.Fatalf("write A: %v", err)
	}
	finalPathB, sizeB, checksumB, err := writeLatestBinarySegment(pathB, segB)
	if err != nil {
		t.Fatalf("write B: %v", err)
	}
	if sizeA != sizeB || checksumA != checksumB {
		t.Fatalf("metadata differs for reordered input: size %d/%d checksum %q/%q", sizeA, sizeB, checksumA, checksumB)
	}
	if filepath.Base(finalPathA) != filepath.Base(finalPathB) {
		t.Fatalf("content-addressed basenames differ for reordered input: %q vs %q", finalPathA, finalPathB)
	}
	bytesA := latestBinaryMustReadFile(t, finalPathA)
	bytesB := latestBinaryMustReadFile(t, finalPathB)
	if !bytes.Equal(bytesA, bytesB) {
		t.Fatal("latest binary segment bytes differ for reordered input")
	}
}

func TestLatestBinarySegmentChecksumAndSizeMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "latest", "accounts.seg")
	finalPath, size, checksum, err := writeLatestBinarySegment(path, latestBinaryAccountSegment([]LatestEntry{
		{Key: AccountSnapshotKey(latestBinaryAddress(0x33)), Value: []byte("account")},
	}))
	if err != nil {
		t.Fatalf("write latest binary segment: %v", err)
	}
	ref := SegmentRef{
		Dataset:   SegmentDatasetAccountLatest,
		Kind:      SegmentLatest,
		FromTxNum: 10,
		ToTxNum:   12,
		Size:      size,
		Checksum:  checksum,
	}
	badSize := ref
	badSize.Size++
	if _, err := readLatestBinarySegment(finalPath, badSize); err == nil {
		t.Fatal("segment with bad size read successfully")
	}
	badChecksum := ref
	badChecksum.Checksum = "sha256:bad"
	if _, err := readLatestBinarySegment(finalPath, badChecksum); err == nil {
		t.Fatal("segment with bad checksum read successfully")
	}
}

func TestLatestBinarySegmentCheckStreamsAndValidates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "latest", "accounts.seg")
	finalPath, size, checksum, err := writeLatestBinarySegment(path, latestBinaryAccountSegment([]LatestEntry{
		{Key: AccountSnapshotKey(latestBinaryAddress(0x34)), Value: []byte("account-a")},
		{Key: AccountSnapshotKey(latestBinaryAddress(0x35)), Value: []byte("account-b")},
	}))
	if err != nil {
		t.Fatalf("write latest binary segment: %v", err)
	}
	relPath, err := filepath.Rel(dir, finalPath)
	if err != nil {
		t.Fatalf("rel path: %v", err)
	}
	ref := SegmentRef{
		Dataset:   SegmentDatasetAccountLatest,
		Kind:      SegmentLatest,
		FromTxNum: 10,
		ToTxNum:   12,
		Path:      relPath,
		Size:      size,
		Checksum:  checksum,
	}
	if err := CheckLatestSegment(dir, ref); err != nil {
		t.Fatalf("check latest binary segment: %v", err)
	}
	checked, err := CheckRegisteredSegment(dir, ref)
	if err != nil || !checked {
		t.Fatalf("registered latest binary check checked=%v err=%v", checked, err)
	}
	badChecksum := ref
	badChecksum.Checksum = "sha256:bad"
	if err := CheckLatestSegment(dir, badChecksum); err == nil {
		t.Fatal("latest binary segment with bad checksum checked successfully")
	}
}

func TestLatestBinaryAccessorCheckStreamsAndValidates(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := latestBinaryAddress(0x36)
	if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.SystemDynamicProperty, []byte("a"), []byte("value-a")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.SystemDynamicProperty, []byte("b"), []byte("value-b")); err != nil {
		t.Fatal(err)
	}
	_, accessorRef, _, err := BuildLatestDomainSegmentFilesFromDB(db, dir, kvdomains.SystemDynamicProperty, 1, 10, "latest/system-dp.seg")
	if err != nil {
		t.Fatalf("build latest binary segment files: %v", err)
	}
	if err := CheckLatestAccessorSegment(dir, accessorRef); err != nil {
		t.Fatalf("check latest binary accessor: %v", err)
	}
	checked, err := CheckRegisteredSegment(dir, accessorRef)
	if err != nil || !checked {
		t.Fatalf("registered latest binary accessor check checked=%v err=%v", checked, err)
	}
	badChecksum := accessorRef
	badChecksum.Checksum = "sha256:bad"
	if err := CheckLatestAccessorSegment(dir, badChecksum); err == nil {
		t.Fatal("latest binary accessor with bad checksum checked successfully")
	}
	badSize := accessorRef
	badSize.Size++
	if err := CheckLatestAccessorSegment(dir, badSize); err == nil {
		t.Fatal("latest binary accessor with bad size checked successfully")
	}

	data := latestBinaryMustReadFile(t, filepath.Join(dir, accessorRef.Path))
	if len(data) < latestBinaryAccessorHeaderSize+16 {
		t.Fatalf("latest binary accessor too small for offset corruption: %d", len(data))
	}
	firstOffset := binary.BigEndian.Uint64(data[latestBinaryAccessorHeaderSize : latestBinaryAccessorHeaderSize+8])
	binary.BigEndian.PutUint64(data[latestBinaryAccessorHeaderSize+8:latestBinaryAccessorHeaderSize+16], firstOffset)
	badOffsets := accessorRef
	badOffsets.Path = "latest/bad-offsets.lidx"
	badOffsets.Size = uint64(len(data))
	badOffsets.Checksum = ""
	if err := os.WriteFile(filepath.Join(dir, badOffsets.Path), data, 0o644); err != nil {
		t.Fatalf("write bad latest binary accessor: %v", err)
	}
	if err := CheckLatestAccessorSegment(dir, badOffsets); err == nil {
		t.Fatal("latest binary accessor with duplicate offsets checked successfully")
	}
}

func TestLatestBinarySegmentCommitmentRootSingleEntryValidatedOnRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commitment", "root.seg")
	rootKey := rawdb.LatestDomainCommitmentRootLogicalKey()
	root := bytes.Repeat([]byte{0xab}, common.HashLength)
	data, err := encodeLatestBinarySegment(&LatestSegment{
		Version:   LatestSegmentVersion,
		Dataset:   SegmentDatasetCommitmentRoot,
		FromTxNum: 100,
		ToTxNum:   120,
		Entries: []LatestEntry{
			{Key: rootKey, Value: root},
			{Key: rootKey, Value: root},
		},
	})
	if err != nil {
		t.Fatalf("encode invalid commitment root segment: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write invalid commitment root segment: %v", err)
	}
	size, checksum := latestBinaryMetadata(data)
	if _, err := readLatestBinarySegment(path, SegmentRef{
		Dataset:   SegmentDatasetCommitmentRoot,
		Kind:      SegmentLatest,
		FromTxNum: 100,
		ToTxNum:   120,
		Size:      size,
		Checksum:  checksum,
	}); err == nil {
		t.Fatal("invalid commitment root segment read successfully")
	}
	if err := checkLatestBinarySegment(dir, SegmentRef{
		Dataset:   SegmentDatasetCommitmentRoot,
		Kind:      SegmentLatest,
		FromTxNum: 100,
		ToTxNum:   120,
		Path:      "commitment/root.seg",
		Size:      size,
		Checksum:  checksum,
	}); err == nil {
		t.Fatal("invalid commitment root segment checked successfully")
	}
}

func latestBinaryAccountSegment(entries []LatestEntry) *LatestSegment {
	return &LatestSegment{
		Version:   LatestSegmentVersion,
		Dataset:   SegmentDatasetAccountLatest,
		FromTxNum: 10,
		ToTxNum:   12,
		Entries:   entries,
	}
}

func latestBinaryAddress(fill byte) common.Address {
	return common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{fill}, common.AccountIDLength)...))
}

func latestBinaryMustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}
