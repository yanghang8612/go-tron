package snapshots

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
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

func TestLatestBinarySegmentCompressesValuesWithAccessorReads(t *testing.T) {
	dir := t.TempDir()
	owner := latestBinaryAddress(0x45)
	key := AccountKVSnapshotKey(owner, 7, []byte("compressed"))
	value := bytes.Repeat([]byte("repeated latest-state value "), 512)
	ref := SegmentRef{
		Dataset:   SegmentDatasetKVLatest,
		Domain:    kvdomains.SystemDynamicProperty,
		Kind:      SegmentLatest,
		FromTxNum: 1,
		ToTxNum:   1,
		Path:      "latest/compressed.seg",
	}
	segRef, accessorRef, btreeRef, err := writeLatestBinarySegmentAndAccessor(dir, ref, func(yield func(LatestEntry) error) error {
		return yield(LatestEntry{Key: key, Value: value})
	})
	if err != nil {
		t.Fatalf("write latest segment: %v", err)
	}
	path := filepath.Join(dir, segRef.Path)
	data := latestBinaryMustReadFile(t, path)
	flags := binary.BigEndian.Uint16(data[22:24])
	if flags != latestBinaryCompressedValues {
		t.Fatalf("compressed latest segment flags = %#04x, want %#04x", flags, latestBinaryCompressedValues)
	}
	valueLenOffset := latestBinaryHeaderSize + 4
	storedLen := binary.BigEndian.Uint32(data[valueLenOffset : valueLenOffset+4])
	if storedLen&latestBinaryValueCompressedFlag == 0 {
		t.Fatal("compressible latest value was not marked compressed")
	}
	rawSize := uint64(latestBinaryHeaderSize + 8 + len(key) + len(value))
	if segRef.Size >= rawSize {
		t.Fatalf("compressed latest size = %d, want below raw size %d", segRef.Size, rawSize)
	}
	if err := checkLatestBinarySegment(dir, segRef); err != nil {
		t.Fatalf("check compressed latest segment: %v", err)
	}
	if err := checkLatestBinaryAccessor(dir, accessorRef); err != nil {
		t.Fatalf("check compressed latest accessor: %v", err)
	}
	got, ok, err := readLatestBinaryValueByAccessorFile(dir, path, segRef, accessorRef, key)
	if err != nil || !ok || !bytes.Equal(got, value) {
		t.Fatalf("read compressed latest value = %d bytes ok=%v err=%v, want %d bytes", len(got), ok, err, len(value))
	}
	got, ok, err = readLatestBinaryValueByBTreeFile(dir, path, segRef, btreeRef, key)
	if err != nil || !ok || !bytes.Equal(got, value) {
		t.Fatalf("btree read compressed latest value = %d bytes ok=%v err=%v, want %d bytes", len(got), ok, err, len(value))
	}
}

func TestLatestBinarySegmentWriterWithoutAccessor(t *testing.T) {
	dir := t.TempDir()
	owner := latestBinaryAddress(0x47)
	key := AccountKVSnapshotKey(owner, 7, []byte("btree-only"))
	ref := SegmentRef{
		Dataset:   SegmentDatasetKVLatest,
		Domain:    kvdomains.SystemDynamicProperty,
		Kind:      SegmentLatest,
		FromTxNum: 1,
		ToTxNum:   1,
		Path:      "latest/btree-only.seg",
	}
	segRef, accessorRef, btreeRef, err := writeLatestBinarySegmentWithCompanions(dir, ref, func(yield func(LatestEntry) error) error {
		return yield(LatestEntry{Key: key, Value: []byte("value")})
	}, false)
	if err != nil {
		t.Fatalf("write btree-only latest segment: %v", err)
	}
	if accessorRef != (SegmentRef{}) {
		t.Fatalf("btree-only latest accessor = %+v, want empty", accessorRef)
	}
	if _, err := os.Stat(filepath.Join(dir, latestBinaryAccessorPath(segRef.Path))); !os.IsNotExist(err) {
		t.Fatalf("btree-only accessor file stat error = %v, want not exist", err)
	}
	if err := checkLatestBinarySegment(dir, segRef); err != nil {
		t.Fatalf("check btree-only latest segment: %v", err)
	}
	if err := CheckLatestBTreeSegment(dir, btreeRef); err != nil {
		t.Fatalf("check btree-only latest btree: %v", err)
	}
	got, ok, err := readLatestBinaryValueByBTreeFile(dir, filepath.Join(dir, segRef.Path), segRef, btreeRef, key)
	if err != nil || !ok || string(got) != "value" {
		t.Fatalf("read btree-only latest value = %q ok=%v err=%v", got, ok, err)
	}
}

func TestLatestBinaryEntryRejectsMalformedCompressedValue(t *testing.T) {
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data[4:], latestBinaryValueCompressedFlag)
	if _, _, err := decodeLatestBinaryEntry(data, true); err == nil || !strings.Contains(err.Error(), "zero encoded length") {
		t.Fatalf("decode malformed compressed latest entry error = %v, want zero-length rejection", err)
	}
}

func TestLatestBinaryValueFrameRejectsOversizeEncodedValue(t *testing.T) {
	frame := latestBinaryValueFrame{encodedLen: latestBinaryMaxDecodedValueSize + 1}
	if err := validateLatestBinaryValueFrame(frame); err == nil || !strings.Contains(err.Error(), "encoded length") {
		t.Fatalf("validate oversized latest binary value frame error = %v, want encoded-length rejection", err)
	}
}

func TestLatestBinaryEntryReadsLegacyRawValue(t *testing.T) {
	key := []byte("legacy-key")
	value := bytes.Repeat([]byte("legacy uncompressed latest value "), 32)
	data := make([]byte, 8+len(key)+len(value))
	binary.BigEndian.PutUint32(data[:4], uint32(len(key)))
	binary.BigEndian.PutUint32(data[4:8], uint32(len(value)))
	copy(data[8:], key)
	copy(data[8+len(key):], value)

	entry, rest, err := decodeLatestBinaryEntry(data, false)
	if err != nil {
		t.Fatalf("decode legacy raw latest entry: %v", err)
	}
	if len(rest) != 0 || !bytes.Equal(entry.Key, key) || !bytes.Equal(entry.Value, value) {
		t.Fatalf("legacy raw latest entry = key %q value %d bytes rest %d", entry.Key, len(entry.Value), len(rest))
	}
}

func TestCompressLatestSegmentsGate(t *testing.T) {
	prev := CompressLatestSegments
	t.Cleanup(func() { CompressLatestSegments = prev })
	owner := latestBinaryAddress(0x46)
	key := AccountKVSnapshotKey(owner, 7, []byte("compression-gate"))
	value := bytes.Repeat([]byte("compressible latest-state value "), 32)
	segment := &LatestSegment{
		Version:   LatestSegmentVersion,
		Dataset:   SegmentDatasetKVLatest,
		Domain:    kvdomains.SystemDynamicProperty,
		FromTxNum: 1,
		ToTxNum:   1,
		Entries:   []LatestEntry{{Key: key, Value: value}},
	}

	CompressLatestSegments = true
	compressed, err := encodeLatestBinarySegment(segment)
	if err != nil {
		t.Fatalf("encode compressed latest segment: %v", err)
	}
	compressedHeader, compressedPayload, err := decodeLatestBinaryHeader(compressed)
	if err != nil {
		t.Fatalf("decode compressed latest header: %v", err)
	}
	if !compressedHeader.compressedValues {
		t.Fatal("compressed latest segment did not set the header capability bit")
	}
	compressedEntry, rest, err := decodeLatestBinaryEntry(compressedPayload, compressedHeader.compressedValues)
	if err != nil || len(rest) != 0 || !bytes.Equal(compressedEntry.Value, value) {
		t.Fatalf("decode compressed latest segment = %d bytes rest=%d err=%v", len(compressedEntry.Value), len(rest), err)
	}

	CompressLatestSegments = false
	raw, err := encodeLatestBinarySegment(segment)
	if err != nil {
		t.Fatalf("encode raw latest segment: %v", err)
	}
	rawHeader, rawPayload, err := decodeLatestBinaryHeader(raw)
	if err != nil {
		t.Fatalf("decode raw latest header: %v", err)
	}
	if rawHeader.compressedValues {
		t.Fatal("raw latest segment set the compressed-value capability bit")
	}
	rawEntry, rest, err := decodeLatestBinaryEntry(rawPayload, rawHeader.compressedValues)
	if err != nil || len(rest) != 0 || !bytes.Equal(rawEntry.Value, value) {
		t.Fatalf("decode raw latest segment = %d bytes rest=%d err=%v", len(rawEntry.Value), len(rest), err)
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
	if err := rawdb.WriteStateKVGeneration(db, owner, 7); err != nil {
		t.Fatal(err)
	}
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

func TestLatestBinaryHeaderReadersRejectOutOfBoundsBeforeAlloc(t *testing.T) {
	tests := []struct {
		name      string
		fixedSize int
		magic     [8]byte
		version   uint32
		read      func([]byte) error
	}{
		{
			name:      "segment",
			fixedSize: latestBinaryHeaderSize,
			magic:     latestBinarySegmentMagic,
			version:   latestBinarySegmentVersion,
			read: func(fixed []byte) error {
				_, err := readLatestBinaryHeader(bytes.NewReader(fixed), uint64(len(fixed)))
				return err
			},
		},
		{
			name:      "accessor",
			fixedSize: latestBinaryAccessorHeaderSize,
			magic:     latestBinaryAccessorMagic,
			version:   latestBinaryAccessorVersion,
			read: func(fixed []byte) error {
				_, err := readLatestBinaryAccessorHeader(bytes.NewReader(fixed), uint64(len(fixed)))
				return err
			},
		},
		{
			name:      "btree",
			fixedSize: latestBinaryBTreeHeaderSize,
			magic:     latestBinaryBTreeMagic,
			version:   latestBinaryBTreeVersion,
			read: func(fixed []byte) error {
				_, err := readLatestBinaryBTreeHeader(bytes.NewReader(fixed), uint64(len(fixed)))
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixed := make([]byte, tt.fixedSize)
			copy(fixed[:8], tt.magic[:])
			binary.BigEndian.PutUint32(fixed[8:12], tt.version)
			binary.BigEndian.PutUint32(fixed[12:16], ^uint32(0))

			err := tt.read(fixed)
			if err == nil {
				t.Fatal("oversized latest binary header read successfully")
			}
			if !strings.Contains(err.Error(), "exceeds file size") {
				t.Fatalf("error = %v, want file-size bound", err)
			}
		})
	}
}

func TestLatestBinaryEntryReadersRejectOutOfBoundsBeforeAlloc(t *testing.T) {
	keyOutOfBounds := make([]byte, 8)
	binary.BigEndian.PutUint32(keyOutOfBounds[:4], ^uint32(0))
	if _, _, err := readLatestBinaryEntryKeyAt(bytes.NewReader(keyOutOfBounds), 0, uint64(len(keyOutOfBounds)), false); err == nil || !strings.Contains(err.Error(), "entry key") {
		t.Fatalf("key reader err = %v, want key bound", err)
	}
	keyPath := filepath.Join(t.TempDir(), "latest-key.seg")
	if err := os.WriteFile(keyPath, keyOutOfBounds, 0o644); err != nil {
		t.Fatal(err)
	}
	keyFile, err := os.Open(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer keyFile.Close()
	if _, _, err := readLatestBinaryEntryKey(keyFile, uint64(len(keyOutOfBounds)), false); err == nil || !strings.Contains(err.Error(), "entry key") {
		t.Fatalf("streaming key reader err = %v, want key bound", err)
	}

	valueOutOfBounds := make([]byte, 8)
	binary.BigEndian.PutUint32(valueOutOfBounds[4:8], ^uint32(0))
	if _, _, _, err := readLatestBinaryEntryAtWithNext(bytes.NewReader(valueOutOfBounds), 0, uint64(len(valueOutOfBounds)), false); err == nil || !strings.Contains(err.Error(), "entry value") {
		t.Fatalf("value reader err = %v, want value bound", err)
	}

	path := filepath.Join(t.TempDir(), "latest-record.seg")
	if err := os.WriteFile(path, valueOutOfBounds, 0o644); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	_, valueLen, err := readLatestBinaryEntryKey(file, uint64(len(valueOutOfBounds)), false)
	if err != nil {
		t.Fatalf("read zero-key entry header: %v", err)
	}
	if _, err := readLatestBinaryValueBytes(file, valueLen, uint64(len(valueOutOfBounds))); err == nil || !strings.Contains(err.Error(), "entry value") {
		t.Fatalf("streaming value reader err = %v, want value bound", err)
	}

	btreeOutOfBounds := make([]byte, 20)
	binary.BigEndian.PutUint32(btreeOutOfBounds[:4], ^uint32(0))
	if _, err := readLatestBinaryBTreeEntryAtOffset(bytes.NewReader(btreeOutOfBounds), 0, uint64(len(btreeOutOfBounds))); err == nil || !strings.Contains(err.Error(), "btree entry key") {
		t.Fatalf("btree reader err = %v, want key bound", err)
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
