package snapshots

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestManagerDerivedRangeCoverageRequiresReadableSegments(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	if err := rawdb.WriteSectionBloom(db, 0, 42, sectionBloomTestEncodedBit(t, 5)); err != nil {
		t.Fatalf("WriteSectionBloom: %v", err)
	}
	sectionEnd := uint64(rawdb.SectionBloomBlockPerSection) - 1
	sectionRef, err := BuildSectionBloomSegmentFromDB(db, dir, "", 0, sectionEnd)
	if err != nil {
		t.Fatalf("BuildSectionBloomSegmentFromDB: %v", err)
	}
	if err := rawdb.WriteBlockBalanceTrace(db, 3, &contractpb.BlockBalanceTrace{Timestamp: 123}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
	balanceRef, err := BuildBalanceTraceSegmentFromDB(db, dir, "", 3, 3)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{sectionRef, balanceRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	if covered, err := mgr.SectionBloomRangeCovered(0, sectionEnd); err != nil || !covered {
		t.Fatalf("SectionBloomRangeCovered = %v/%v, want true/nil", covered, err)
	}
	if covered, err := mgr.BalanceTraceRangeCovered(3, 3); err != nil || !covered {
		t.Fatalf("BalanceTraceRangeCovered = %v/%v, want true/nil", covered, err)
	}
	if covered, err := mgr.BalanceTraceRangeCovered(2, 3); err != nil || covered {
		t.Fatalf("BalanceTraceRangeCovered gapped = %v/%v, want false/nil", covered, err)
	}
	if err := os.Remove(filepath.Join(dir, balanceRef.Path)); err != nil {
		t.Fatalf("remove balance trace segment: %v", err)
	}
	if covered, err := mgr.BalanceTraceRangeCovered(3, 3); err == nil || covered {
		t.Fatalf("BalanceTraceRangeCovered missing file = %v/%v, want false/error", covered, err)
	}
}

func TestBalanceTraceRangeCoveredRejectsSparseBlockTraceSegment(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	if err := rawdb.WriteBlockBalanceTrace(db, 3, &contractpb.BlockBalanceTrace{Timestamp: 123}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
	ref, err := BuildBalanceTraceSegmentFromDB(db, dir, "", 3, 5)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{ref})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	if covered, err := mgr.BalanceTraceRangeCovered(3, 3); err != nil || !covered {
		t.Fatalf("BalanceTraceRangeCovered exact row = %v/%v, want true/nil", covered, err)
	}
	if covered, err := mgr.BalanceTraceRangeCovered(3, 5); err != nil || covered {
		t.Fatalf("BalanceTraceRangeCovered sparse segment = %v/%v, want false/nil", covered, err)
	}
}

func TestManagerChainIndexRangeCoverageRequiresMatchingIndex(t *testing.T) {
	root := t.TempDir()
	freezer := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer freezer.Close()
	block0, _, txInfos0 := chainFreezerBlockWithTx(t, 0)
	block1, _, txInfos1 := chainFreezerBlockWithTx(t, 1)
	appendChainFreezerRawRows(t, freezer, []chainFreezerRawTestRow{
		{block: block0, txInfosRaw: txInfos0, stateRoot: common.Hash{0x01}.Bytes()},
		{block: block1, txInfosRaw: txInfos1, stateRoot: common.Hash{0x02}.Bytes()},
	})
	dir := filepath.Join(root, "snapshot")
	freezerRef, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(freezer), dir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(dir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{freezerRef, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	if covered, err := mgr.ChainIndexRangeCovered(0, 1); err != nil || !covered {
		t.Fatalf("ChainIndexRangeCovered = %v/%v, want true/nil", covered, err)
	}
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{freezerRef})); err != nil {
		t.Fatalf("PublishManifest without index: %v", err)
	}
	if covered, err := mgr.ChainIndexRangeCovered(0, 1); err != nil || covered {
		t.Fatalf("ChainIndexRangeCovered without index = %v/%v, want false/nil", covered, err)
	}
}
