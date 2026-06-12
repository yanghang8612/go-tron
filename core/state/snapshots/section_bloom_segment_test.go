package snapshots

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

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

func sectionBloomTestEncodedBit(t *testing.T, bit uint64) []byte {
	t.Helper()
	encoded, err := rawdb.EncodeSectionBloomBitSet(sectionBloomTestSetBit(nil, bit))
	if err != nil {
		t.Fatalf("EncodeSectionBloomBitSet: %v", err)
	}
	return encoded
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
