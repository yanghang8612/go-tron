package snapshots

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestChainFreezerAccessorBuildCheckVerify(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: canonicalBoundaryTestBlock(t, 0)},
		{block: canonicalBoundaryTestBlock(t, 1)},
		{block: canonicalBoundaryTestBlock(t, 2)},
	})

	snapshotDir := filepath.Join(root, "snapshot")
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 2)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	accessorRef, err := BuildChainFreezerAccessorSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainFreezerAccessorSegmentFromChainFreezerSegment: %v", err)
	}
	if accessorRef.Dataset != SegmentDatasetChainFreezer || accessorRef.Kind != SegmentChainFreezerAccessor {
		t.Fatalf("accessor ref family = %s/%s, want %s/%s", accessorRef.Dataset, accessorRef.Kind, SegmentDatasetChainFreezer, SegmentChainFreezerAccessor)
	}
	if accessorRef.Size == 0 || accessorRef.Checksum == "" {
		t.Fatalf("accessor ref metadata missing: size=%d checksum=%q", accessorRef.Size, accessorRef.Checksum)
	}
	if err := CheckChainFreezerAccessorSegment(snapshotDir, accessorRef); err != nil {
		t.Fatalf("CheckChainFreezerAccessorSegment: %v", err)
	}
	if err := VerifyChainFreezerAccessorSegmentAgainstChainFreezer(snapshotDir, accessorRef, freezerRef); err != nil {
		t.Fatalf("VerifyChainFreezerAccessorSegmentAgainstChainFreezer: %v", err)
	}
	seg, err := OpenChainFreezerAccessorSegment(snapshotDir, accessorRef)
	if err != nil {
		t.Fatalf("OpenChainFreezerAccessorSegment: %v", err)
	}
	defer seg.Close()
	offset, ok, err := seg.RowOffset(2)
	if err != nil || !ok {
		t.Fatalf("RowOffset(2) = %d/%v/%v, want ok", offset, ok, err)
	}
	row, ok, err := readChainFreezerSegmentRowWithAccessor(snapshotDir, freezerRef, accessorRef, 2)
	if err != nil || !ok {
		t.Fatalf("readChainFreezerSegmentRowWithAccessor = ok %v err %v", ok, err)
	}
	if row.blockNum != 2 || len(row.blockRaw) == 0 {
		t.Fatalf("row = %+v, want block 2 with body", row)
	}
}

func TestManagerAncientUsesChainFreezerAccessorWhenPresent(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	block0 := canonicalBoundaryTestBlock(t, 0)
	block1 := canonicalBoundaryTestBlock(t, 1)
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1},
	})

	snapshotDir := filepath.Join(root, "snapshot")
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	accessorRef, err := BuildChainFreezerAccessorSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainFreezerAccessorSegmentFromChainFreezerSegment: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{freezerRef, accessorRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	// Prove the manager path is not using the full segment scanner: appending
	// trailing bytes makes CheckChainFreezerSegment/iterate fail, while the
	// accessor point-read remains valid for existing rows.
	abs := filepath.Join(snapshotDir, freezerRef.Path)
	data, err := os.ReadFile(abs)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data = append(data, 0xff)
	if err := os.WriteFile(abs, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := CheckChainFreezerSegment(snapshotDir, freezerRef); err == nil || (!strings.Contains(err.Error(), "trailing bytes") && !strings.Contains(err.Error(), "size")) {
		t.Fatalf("CheckChainFreezerSegment error = %v, want size or trailing-byte rejection", err)
	}

	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	gotRaw, err := mgr.Ancient(rawdb.AncientBlocksTable, 1)
	if err != nil {
		t.Fatalf("manager Ancient via accessor: %v", err)
	}
	wantRaw, err := block1.Marshal()
	if err != nil {
		t.Fatalf("Marshal block1: %v", err)
	}
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("manager Ancient via accessor returned %x, want %x", gotRaw, wantRaw)
	}
}

func TestManifestRejectsChainFreezerAccessorWithoutFreezer(t *testing.T) {
	ref := SegmentRef{
		Dataset:   SegmentDatasetChainFreezer,
		Kind:      SegmentChainFreezerAccessor,
		FromTxNum: 0,
		ToTxNum:   1,
		Path:      "chain/freezer-accessor-0-1.idx",
		Size:      chainFreezerAccessorHeaderSize + 2*chainFreezerAccessorEntrySize,
	}
	if err := NewManifest(0, 0, []SegmentRef{ref}).Validate(); err == nil || !strings.Contains(err.Error(), "no matching chain-freezer") {
		t.Fatalf("manifest.Validate error = %v, want missing chain-freezer companion", err)
	}
}

func TestCheckChainFreezerAccessorSegmentRejectsBadOffsetOrder(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: canonicalBoundaryTestBlock(t, 0)},
		{block: canonicalBoundaryTestBlock(t, 1)},
	})
	snapshotDir := filepath.Join(root, "snapshot")
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	accessorRef, err := BuildChainFreezerAccessorSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainFreezerAccessorSegmentFromChainFreezerSegment: %v", err)
	}
	abs := filepath.Join(snapshotDir, accessorRef.Path)
	data, err := os.ReadFile(abs)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	copy(data[chainFreezerAccessorHeaderSize+chainFreezerAccessorEntrySize:], data[chainFreezerAccessorHeaderSize:chainFreezerAccessorHeaderSize+chainFreezerAccessorEntrySize])
	if err := os.WriteFile(abs, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	accessorRef.Size = uint64(len(data))
	sum := sha256.Sum256(data)
	accessorRef.Checksum = "sha256:" + hex.EncodeToString(sum[:])
	if err := CheckChainFreezerAccessorSegment(snapshotDir, accessorRef); err == nil || !strings.Contains(err.Error(), "not increasing") {
		t.Fatalf("CheckChainFreezerAccessorSegment error = %v, want offset order rejection", err)
	}
}

func TestChainFreezerRangeCoveredRejectsStaleAccessorOffsets(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: canonicalBoundaryTestBlock(t, 0)},
		{block: canonicalBoundaryTestBlock(t, 1)},
		{block: canonicalBoundaryTestBlock(t, 2)},
	})
	snapshotDir := filepath.Join(root, "snapshot")
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 2)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	offsets, err := chainFreezerRowOffsets(snapshotDir, freezerRef)
	if err != nil {
		t.Fatalf("chainFreezerRowOffsets: %v", err)
	}
	if len(offsets) != 3 {
		t.Fatalf("offsets = %v, want three rows", offsets)
	}
	staleOffsets := append([]uint64(nil), offsets...)
	staleOffsets[1] = offsets[2]
	staleOffsets[2] = offsets[2] + 1
	accessorRef, err := writeChainFreezerAccessorSegment(snapshotDir, SegmentRef{
		Dataset:   SegmentDatasetChainFreezer,
		Kind:      SegmentChainFreezerAccessor,
		FromTxNum: 0,
		ToTxNum:   2,
		Path:      "chain/freezer-accessor-stale-0-2.idx",
	}, staleOffsets)
	if err != nil {
		t.Fatalf("writeChainFreezerAccessorSegment stale: %v", err)
	}
	if err := CheckChainFreezerAccessorSegment(snapshotDir, accessorRef); err != nil {
		t.Fatalf("CheckChainFreezerAccessorSegment stale shape: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{freezerRef, accessorRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if _, err := VerifyManifestFiles(snapshotDir, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err == nil || !strings.Contains(err.Error(), "offset for block 1 points at different row") {
		t.Fatalf("VerifyManifestFiles stale accessor = %v, want accessor/freezer mismatch", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	covered, err := mgr.ChainFreezerRangeCovered(0, 2)
	if err == nil || covered {
		t.Fatalf("ChainFreezerRangeCovered stale accessor = %v/%v, want false/error", covered, err)
	}
}
