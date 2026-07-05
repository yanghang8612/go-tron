package snapshots

import (
	"bytes"
	"compress/zlib"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

type sectionBloomMalformedReader struct {
	raw []byte
}

func (r sectionBloomMalformedReader) SectionBloom(section, bitIndex uint64) ([]byte, bool, error) {
	if section == 0 && bitIndex == 42 {
		return r.raw, true, nil
	}
	return nil, false, nil
}

func TestSectionBloomSegmentBuildVerifyLookup(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()

	rowA := sectionBloomTestEncodedBit(t, 5)
	rowB := sectionBloomTestEncodedBit(t, 3)
	rowOutside := sectionBloomTestEncodedBit(t, 11)
	if err := rawdb.WriteSectionBloom(source, 0, 42, rowA); err != nil {
		t.Fatalf("WriteSectionBloom 0/42: %v", err)
	}
	if err := rawdb.WriteSectionBloom(source, 1, 99, rowB); err != nil {
		t.Fatalf("WriteSectionBloom 1/99: %v", err)
	}
	if err := rawdb.WriteSectionBloom(source, 2, 100, rowOutside); err != nil {
		t.Fatalf("WriteSectionBloom outside: %v", err)
	}

	result, err := NewAggregator(snapshotDir).BuildSectionBlooms(source, 0, rawdb.SectionBloomBlockPerSection*2-1)
	if err != nil {
		t.Fatalf("BuildSectionBlooms: %v", err)
	}
	if len(result.Segments) != 1 {
		t.Fatalf("BuildSectionBlooms segments = %d, want 1", len(result.Segments))
	}
	ref := result.Segments[0]
	if ref.Dataset != SegmentDatasetSectionBloom || ref.Kind != SegmentSectionBloom {
		t.Fatalf("ref family = %s/%s, want %s/%s", ref.Dataset, ref.Kind, SegmentDatasetSectionBloom, SegmentSectionBloom)
	}
	if ref.Size == 0 || ref.Checksum == "" {
		t.Fatalf("ref metadata missing: size=%d checksum=%q", ref.Size, ref.Checksum)
	}
	if err := CheckSectionBloomSegment(snapshotDir, ref); err != nil {
		t.Fatalf("CheckSectionBloomSegment: %v", err)
	}
	if _, err := VerifyManifestFiles(snapshotDir, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatalf("VerifyManifestFiles: %v", err)
	}

	seg, err := OpenSectionBloomSegment(snapshotDir, ref)
	if err != nil {
		t.Fatalf("OpenSectionBloomSegment: %v", err)
	}
	defer seg.Close()
	raw, ok, err := seg.SectionBloom(0, 42)
	if err != nil || !ok || !bytes.Equal(raw, rowA) {
		t.Fatalf("SectionBloom 0/42 = %x/%v/%v, want rowA", raw, ok, err)
	}
	if raw, ok, err := seg.SectionBloom(2, 100); err != nil || ok || raw != nil {
		t.Fatalf("SectionBloom outside = %x/%v/%v, want nil/false/nil", raw, ok, err)
	}

	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	raw, ok, err = mgr.SectionBloom(1, 99)
	if err != nil || !ok || !bytes.Equal(raw, rowB) {
		t.Fatalf("manager SectionBloom 1/99 = %x/%v/%v, want rowB", raw, ok, err)
	}
	coldOnly := rawdb.NewMemoryChainDB()
	coldOnly.SetSectionBloomReader(mgr)
	bitset, ok, err := rawdb.ReadSectionBloomBitSet(coldOnly, 1, 99)
	if err != nil || !ok || !rawdb.SectionBloomBitSetHas(bitset, 3) {
		t.Fatalf("rawdb cold ReadSectionBloomBitSet = %x/%v/%v, want bit 3", bitset, ok, err)
	}
}

func TestBuildSectionBloomSegmentFromReaderMaterializesColdRows(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	targetDir := filepath.Join(root, "target")
	source := rawdb.NewMemoryChainDB()

	rowA := sectionBloomTestEncodedBit(t, 5)
	rowB := sectionBloomTestEncodedBit(t, 3)
	rowOutside := sectionBloomTestEncodedBit(t, 11)
	if err := rawdb.WriteSectionBloom(source, 0, 42, rowA); err != nil {
		t.Fatalf("WriteSectionBloom 0/42: %v", err)
	}
	if err := rawdb.WriteSectionBloom(source, 1, 99, rowB); err != nil {
		t.Fatalf("WriteSectionBloom 1/99: %v", err)
	}
	if err := rawdb.WriteSectionBloom(source, 2, 100, rowOutside); err != nil {
		t.Fatalf("WriteSectionBloom outside: %v", err)
	}
	if _, err := NewAggregator(sourceDir).BuildSectionBlooms(source, 0, rawdb.SectionBloomBlockPerSection*3-1); err != nil {
		t.Fatalf("BuildSectionBlooms source: %v", err)
	}
	sourceManager, err := OpenManager(sourceDir)
	if err != nil {
		t.Fatalf("OpenManager source: %v", err)
	}

	result, err := NewAggregator(targetDir).BuildSectionBloomsFromReaderWithOptions(sourceManager, 0, rawdb.SectionBloomBlockPerSection*2-1, RestoreETLOptions{
		TempDir:     filepath.Join(root, "etl-scratch"),
		BufferLimit: 1,
	})
	if err != nil {
		t.Fatalf("BuildSectionBloomsFromReaderWithOptions: %v", err)
	}
	if len(result.Segments) != 1 {
		t.Fatalf("BuildSectionBloomsFromReaderWithOptions segments = %d, want 1", len(result.Segments))
	}
	ref := result.Segments[0]
	if err := CheckSectionBloomSegment(targetDir, ref); err != nil {
		t.Fatalf("CheckSectionBloomSegment: %v", err)
	}
	if _, err := VerifyManifestFiles(targetDir, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatalf("VerifyManifestFiles: %v", err)
	}

	seg, err := OpenSectionBloomSegment(targetDir, ref)
	if err != nil {
		t.Fatalf("OpenSectionBloomSegment target: %v", err)
	}
	defer seg.Close()
	raw, ok, err := seg.SectionBloom(1, 99)
	if err != nil || !ok || !bytes.Equal(raw, rowB) {
		t.Fatalf("SectionBloom 1/99 = %x/%v/%v, want rowB", raw, ok, err)
	}
	if raw, ok, err := seg.SectionBloom(2, 100); err != nil || ok || raw != nil {
		t.Fatalf("SectionBloom outside = %x/%v/%v, want nil/false/nil", raw, ok, err)
	}

	targetManager, err := OpenManager(targetDir)
	if err != nil {
		t.Fatalf("OpenManager target: %v", err)
	}
	coldOnly := rawdb.NewMemoryChainDB()
	coldOnly.SetSectionBloomReader(targetManager)
	bitset, ok, err := rawdb.ReadSectionBloomBitSet(coldOnly, 0, 42)
	if err != nil || !ok || !rawdb.SectionBloomBitSetHas(bitset, 5) {
		t.Fatalf("rawdb cold ReadSectionBloomBitSet = %x/%v/%v, want bit 5", bitset, ok, err)
	}
}

func TestBuildSectionBloomSegmentFromReaderRejectsMalformedPayload(t *testing.T) {
	_, err := BuildSectionBloomSegmentFromReader(sectionBloomMalformedReader{raw: []byte{0x01}}, t.TempDir(), "", 0, rawdb.SectionBloomBlockPerSection-1)
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("BuildSectionBloomSegmentFromReader error = %v, want decode error", err)
	}
}

func TestSectionBloomPayloadReadRejectsOutOfBoundsBeforeAlloc(t *testing.T) {
	path := filepath.Join(t.TempDir(), "section-bloom-payload.bin")
	if err := os.WriteFile(path, []byte{0x01, 0x02, 0x03, 0x04}, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer file.Close()
	if _, err := readSectionBloomPayloadAt(file, 0, ^uint64(0), 4); err == nil || !strings.Contains(err.Error(), "exceeds segment size") {
		t.Fatalf("readSectionBloomPayloadAt error = %v, want bounded rejection", err)
	}
}

func TestBuildSectionBloomSegmentWithOptionsUsesETLScratch(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()
	rowA := sectionBloomTestEncodedBit(t, 7)
	rowB := sectionBloomTestEncodedBit(t, 8)
	if err := rawdb.WriteSectionBloom(source, 0, 41, rowA); err != nil {
		t.Fatalf("WriteSectionBloom 0/41: %v", err)
	}
	if err := rawdb.WriteSectionBloom(source, 0, 42, rowB); err != nil {
		t.Fatalf("WriteSectionBloom 0/42: %v", err)
	}

	etlTemp := filepath.Join(root, "etl-scratch")
	ref, err := BuildSectionBloomSegmentFromDBWithOptions(source, snapshotDir, "log/section-bloom-0-0.seg", 0, rawdb.SectionBloomBlockPerSection-1, RestoreETLOptions{
		TempDir:     etlTemp,
		BufferLimit: 1,
	})
	if err != nil {
		t.Fatalf("BuildSectionBloomSegmentFromDBWithOptions: %v", err)
	}
	if _, err := os.Stat(etlTemp); err != nil {
		t.Fatalf("ETL temp parent stat: %v", err)
	}
	if err := CheckSectionBloomSegment(snapshotDir, ref); err != nil {
		t.Fatalf("CheckSectionBloomSegment: %v", err)
	}
	seg, err := OpenSectionBloomSegment(snapshotDir, ref)
	if err != nil {
		t.Fatalf("OpenSectionBloomSegment: %v", err)
	}
	defer seg.Close()
	raw, ok, err := seg.SectionBloom(0, 42)
	if err != nil || !ok || !bytes.Equal(raw, rowB) {
		t.Fatalf("SectionBloom 0/42 = %x/%v/%v, want rowB", raw, ok, err)
	}
}

func TestBuildSectionBloomSegmentRejectsPartialSectionRange(t *testing.T) {
	source := rawdb.NewMemoryChainDB()
	if _, err := BuildSectionBloomSegmentFromDB(source, t.TempDir(), "", 1, rawdb.SectionBloomBlockPerSection-1); err == nil || !strings.Contains(err.Error(), "complete") {
		t.Fatalf("BuildSectionBloomSegmentFromDB partial start error = %v, want complete-section error", err)
	}
	if _, err := BuildSectionBloomSegmentFromDB(source, t.TempDir(), "", 0, rawdb.SectionBloomBlockPerSection-2); err == nil || !strings.Contains(err.Error(), "complete") {
		t.Fatalf("BuildSectionBloomSegmentFromDB partial end error = %v, want complete-section error", err)
	}
}

func TestCheckSectionBloomSegmentRejectsPartialSectionRef(t *testing.T) {
	ref := SegmentRef{
		Dataset:   SegmentDatasetSectionBloom,
		Kind:      SegmentSectionBloom,
		FromTxNum: 0,
		ToTxNum:   rawdb.SectionBloomBlockPerSection - 2,
		Path:      "log/section-bloom-0-partial.seg",
	}
	if err := CheckSectionBloomSegment(t.TempDir(), ref); err == nil || !strings.Contains(err.Error(), "complete") {
		t.Fatalf("CheckSectionBloomSegment partial ref error = %v, want complete-section error", err)
	}
}

func TestBuildSectionBloomSegmentRejectsOversizedDecodedRow(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()
	row := sectionBloomTestEncodedRawPayload(t, sectionBloomTestSetBit(nil, rawdb.SectionBloomBitSize))
	if err := rawdb.WriteSectionBloom(source, 0, 42, row); err != nil {
		t.Fatalf("WriteSectionBloom oversized row: %v", err)
	}

	_, err := BuildSectionBloomSegmentFromDB(source, snapshotDir, "log/section-bloom-0-0.seg", 0, rawdb.SectionBloomBlockPerSection-1)
	if err == nil || !strings.Contains(err.Error(), "decoded bitset has") {
		t.Fatalf("BuildSectionBloomSegmentFromDB error = %v, want decoded-size error", err)
	}
}

func TestSectionBloomSegmentBitSetRejectsMalformedPayload(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()
	row := sectionBloomTestEncodedBit(t, 5)
	if err := rawdb.WriteSectionBloom(source, 0, 42, row); err != nil {
		t.Fatalf("WriteSectionBloom: %v", err)
	}
	ref, err := BuildSectionBloomSegmentFromDB(source, snapshotDir, "", 0, rawdb.SectionBloomBlockPerSection-1)
	if err != nil {
		t.Fatalf("BuildSectionBloomSegmentFromDB: %v", err)
	}
	rewriteSectionBloomEntryLength(t, snapshotDir, ref, 0, 42, 1)
	if err := CheckSectionBloomSegment(snapshotDir, ref); err == nil {
		t.Fatal("CheckSectionBloomSegment accepted malformed section bloom payload")
	}

	seg, err := OpenSectionBloomSegment(snapshotDir, ref)
	if err != nil {
		t.Fatalf("OpenSectionBloomSegment: %v", err)
	}
	raw, ok, lookupErr := seg.SectionBloom(0, 42)
	if lookupErr != nil || !ok || len(raw) != 1 {
		t.Fatalf("SectionBloom malformed raw payload = %x/%v/%v, want raw hit", raw, ok, lookupErr)
	}
	bitset, ok, lookupErr := seg.SectionBloomBitSet(0, 42)
	closeErr := seg.Close()
	if lookupErr == nil || ok || bitset != nil || !strings.Contains(lookupErr.Error(), "decode 0/42") {
		t.Fatalf("SectionBloomBitSet malformed payload = %x/%v/%v, want decode error", bitset, ok, lookupErr)
	}
	if closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}

	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{ref})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	raw, ok, lookupErr = mgr.SectionBloom(0, 42)
	if lookupErr != nil || !ok || len(raw) != 1 {
		t.Fatalf("manager SectionBloom malformed raw payload = %x/%v/%v, want raw hit", raw, ok, lookupErr)
	}
	bitset, ok, lookupErr = mgr.SectionBloomBitSet(0, 42)
	if lookupErr == nil || ok || bitset != nil || !strings.Contains(lookupErr.Error(), "decode 0/42") {
		t.Fatalf("manager SectionBloomBitSet malformed payload = %x/%v/%v, want decode error", bitset, ok, lookupErr)
	}

	coldOnly := rawdb.NewMemoryChainDB()
	coldOnly.SetSectionBloomReader(mgr)
	bitset, ok, lookupErr = rawdb.ReadSectionBloomBitSetStrict(coldOnly, 0, 42)
	if lookupErr == nil || ok || bitset != nil || !strings.Contains(lookupErr.Error(), "decode 0/42") {
		t.Fatalf("rawdb strict malformed cold section bloom = %x/%v/%v, want decode error", bitset, ok, lookupErr)
	}
}

func TestSectionBloomSegmentLookupRejectsInvalidEntryBounds(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()
	row := sectionBloomTestEncodedBit(t, 5)
	if err := rawdb.WriteSectionBloom(source, 0, 42, row); err != nil {
		t.Fatalf("WriteSectionBloom: %v", err)
	}
	ref, err := BuildSectionBloomSegmentFromDB(source, snapshotDir, "", 0, rawdb.SectionBloomBlockPerSection-1)
	if err != nil {
		t.Fatalf("BuildSectionBloomSegmentFromDB: %v", err)
	}
	rewriteSectionBloomEntryLength(t, snapshotDir, ref, 0, 42, 0)
	if err := CheckSectionBloomSegment(snapshotDir, ref); err == nil {
		t.Fatal("CheckSectionBloomSegment accepted empty section bloom payload")
	}

	seg, err := OpenSectionBloomSegment(snapshotDir, ref)
	if err != nil {
		t.Fatalf("OpenSectionBloomSegment: %v", err)
	}
	raw, ok, lookupErr := seg.SectionBloom(0, 42)
	closeErr := seg.Close()
	if lookupErr == nil || ok || raw != nil || !strings.Contains(lookupErr.Error(), "empty payload") {
		t.Fatalf("SectionBloom empty payload = %x/%v/%v, want bounds error", raw, ok, lookupErr)
	}
	if closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}

	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{ref})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	raw, ok, lookupErr = mgr.SectionBloom(0, 42)
	if lookupErr == nil || ok || raw != nil || !strings.Contains(lookupErr.Error(), "empty payload") {
		t.Fatalf("manager SectionBloom empty payload = %x/%v/%v, want bounds error", raw, ok, lookupErr)
	}
}

func sectionBloomTestEncodedBit(t *testing.T, bit uint64) []byte {
	t.Helper()
	encoded, err := rawdb.EncodeSectionBloomBitSet(sectionBloomTestSetBit(nil, bit))
	if err != nil {
		t.Fatalf("EncodeSectionBloomBitSet: %v", err)
	}
	return encoded
}

func sectionBloomTestEncodedRawPayload(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	if _, err := zw.Write(payload); err != nil {
		t.Fatalf("zlib write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zlib close: %v", err)
	}
	return buf.Bytes()
}

func sectionBloomTestSetBit(bitset []byte, bit uint64) []byte {
	byteIndex := bit / 8
	if byteIndex >= uint64(len(bitset)) {
		grown := make([]byte, byteIndex+1)
		copy(grown, bitset)
		bitset = grown
	}
	bitset[byteIndex] |= 1 << (bit % 8)
	return bitset
}

func rewriteSectionBloomEntryLength(t *testing.T, dir string, ref SegmentRef, section, bitIndex, length uint64) {
	t.Helper()
	file, err := os.OpenFile(filepath.Join(dir, ref.Path), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer file.Close()
	header, err := readSectionBloomHeader(file)
	if err != nil {
		t.Fatalf("readSectionBloomHeader: %v", err)
	}
	for i := uint64(0); i < header.rowCount; i++ {
		entry, err := readSectionBloomIndexEntryAt(file, sectionBloomIndexEntryOffset(header, i))
		if err != nil {
			t.Fatalf("readSectionBloomIndexEntryAt: %v", err)
		}
		if entry.section != section || entry.bitIndex != bitIndex {
			continue
		}
		entry.length = length
		if err := writeSectionBloomIndexEntryAt(file, sectionBloomIndexEntryOffset(header, i), entry); err != nil {
			t.Fatalf("writeSectionBloomIndexEntryAt: %v", err)
		}
		return
	}
	t.Fatalf("section bloom row %d/%d not found", section, bitIndex)
}
