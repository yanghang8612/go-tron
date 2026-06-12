package snapshots

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestChainIndexSegmentBuildVerifyLookup(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	block0 := canonicalBoundaryTestBlock(t, 0)
	block1, txHash, txInfoRaw := chainFreezerBlockWithTx(t, 1)
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1, txInfosRaw: txInfoRaw},
	})

	snapshotDir := filepath.Join(root, "snapshot")
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	if indexRef.Dataset != SegmentDatasetChainFreezer || indexRef.Kind != SegmentChainIndex {
		t.Fatalf("index ref family = %s/%s, want %s/%s", indexRef.Dataset, indexRef.Kind, SegmentDatasetChainFreezer, SegmentChainIndex)
	}
	if indexRef.Size == 0 || indexRef.Checksum == "" {
		t.Fatalf("index ref metadata missing: size=%d checksum=%q", indexRef.Size, indexRef.Checksum)
	}
	if err := CheckChainIndexSegment(snapshotDir, indexRef); err != nil {
		t.Fatalf("CheckChainIndexSegment: %v", err)
	}
	if err := VerifyChainIndexSegmentAgainstChainFreezer(snapshotDir, indexRef, freezerRef); err != nil {
		t.Fatalf("VerifyChainIndexSegmentAgainstChainFreezer: %v", err)
	}

	identity := chainFreezerTestIdentity()
	if err := PublishManifest(snapshotDir, NewManifestForChain(0, 0, []SegmentRef{freezerRef, indexRef}, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if _, err := VerifyRemoteManifestFiles(snapshotDir, identity); err != nil {
		t.Fatalf("VerifyRemoteManifestFiles: %v", err)
	}

	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	blockNum, ok, err := mgr.BlockNumberByHash(block1.Hash())
	if err != nil || !ok || blockNum != 1 {
		t.Fatalf("BlockNumberByHash = %d/%v/%v, want 1/true/nil", blockNum, ok, err)
	}
	txLookup, ok, err := mgr.TransactionIndexByHash(common.Hash(txHash))
	if err != nil || !ok || txLookup.BlockNum != 1 || txLookup.TxIndex != 0 {
		t.Fatalf("TransactionIndexByHash = %+v/%v/%v, want block 1 tx index 0", txLookup, ok, err)
	}
	if _, ok, err := mgr.BlockNumberByHash(canonicalBoundaryTestBlock(t, 9).Hash()); err != nil || ok {
		t.Fatalf("missing BlockNumberByHash ok=%v err=%v, want false/nil", ok, err)
	}
}

func TestManifestRejectsChainIndexWithoutFreezer(t *testing.T) {
	ref := SegmentRef{
		Dataset:   SegmentDatasetChainFreezer,
		Kind:      SegmentChainIndex,
		FromTxNum: 0,
		ToTxNum:   1,
		Path:      "chain/index-0-1.idx",
	}
	manifest := NewManifestForChain(0, 0, []SegmentRef{ref}, chainFreezerTestIdentity())
	if err := manifest.Validate(); err == nil || !strings.Contains(err.Error(), "no matching chain-freezer") {
		t.Fatalf("manifest.Validate error = %v, want missing chain-freezer companion", err)
	}
}

func TestVerifyRemoteManifestFilesRejectsChainIndexFreezerMismatch(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")

	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{{block: canonicalBoundaryTestBlock(t, 0)}})
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "chain/freezer-0-0.seg", 0, 0)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient src: %v", err)
	}

	alt := openChainFreezerTestStore(t, filepath.Join(root, "alt"))
	defer alt.Close()
	altBlock := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    0,
				Timestamp: 9_999,
			},
		},
	})
	if altBlock.Hash() == canonicalBoundaryTestBlock(t, 0).Hash() {
		t.Fatalf("alternate block hash unexpectedly matched source block hash")
	}
	appendChainFreezerRawRows(t, alt, []chainFreezerRawTestRow{{block: altBlock}})
	altFreezerRef, err := BuildChainFreezerSegmentFromAncient(alt, snapshotDir, "chain/freezer-alt-0-0.seg", 0, 0)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient alt: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, altFreezerRef, "chain/index-0-0.idx")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment alt: %v", err)
	}
	if err := CheckChainIndexSegment(snapshotDir, indexRef); err != nil {
		t.Fatalf("CheckChainIndexSegment: %v", err)
	}

	identity := chainFreezerTestIdentity()
	if err := PublishManifest(snapshotDir, NewManifestForChain(0, 0, []SegmentRef{freezerRef, indexRef}, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if _, err := VerifyRemoteManifestFiles(snapshotDir, identity); err == nil || !strings.Contains(err.Error(), "missing block hash") {
		t.Fatalf("VerifyRemoteManifestFiles error = %v, want chain-index/freezer mismatch", err)
	}
}

func TestBuildChainIndexSegmentWithOptionsUsesETLScratch(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	block0 := canonicalBoundaryTestBlock(t, 0)
	block1, _, txInfoRaw := chainFreezerBlockWithTx(t, 1)
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1, txInfosRaw: txInfoRaw},
	})

	snapshotDir := filepath.Join(root, "snapshot")
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	etlTemp := filepath.Join(root, "etl-scratch")
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegmentWithOptions(snapshotDir, freezerRef, "", RestoreETLOptions{
		TempDir:     etlTemp,
		BufferLimit: 1,
	})
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegmentWithOptions: %v", err)
	}
	if _, err := os.Stat(etlTemp); err != nil {
		t.Fatalf("ETL temp parent stat: %v", err)
	}
	if err := CheckChainIndexSegment(snapshotDir, indexRef); err != nil {
		t.Fatalf("CheckChainIndexSegment: %v", err)
	}
	if err := VerifyChainIndexSegmentAgainstChainFreezer(snapshotDir, indexRef, freezerRef); err != nil {
		t.Fatalf("VerifyChainIndexSegmentAgainstChainFreezer: %v", err)
	}
}

func TestWriteChainIndexSegmentFromETLRejectsDuplicateBlockHashes(t *testing.T) {
	collector, err := etl.NewCollector(etl.Options{TempDir: t.TempDir(), BufferLimit: 1})
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	defer collector.Close()
	hash := common.Hash{0xaa}
	if err := collector.Put(chainIndexBlockETLKey(hash, 1), nil); err != nil {
		t.Fatalf("collector.Put block 1: %v", err)
	}
	if err := collector.Put(chainIndexBlockETLKey(hash, 2), nil); err != nil {
		t.Fatalf("collector.Put block 2: %v", err)
	}
	_, err = writeChainIndexSegmentFromETL(t.TempDir(), SegmentRef{
		Dataset:   SegmentDatasetChainFreezer,
		Kind:      SegmentChainIndex,
		FromTxNum: 1,
		ToTxNum:   2,
		Path:      "chain/index-1-2.idx",
	}, collector, 2)
	if err == nil || !strings.Contains(err.Error(), "duplicate chain-index block hash") {
		t.Fatalf("writeChainIndexSegmentFromETL error = %v, want duplicate hash rejection", err)
	}
}

func TestCheckChainIndexSegmentRejectsTrailingBytes(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	block0 := canonicalBoundaryTestBlock(t, 0)
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{{block: block0}})

	snapshotDir := filepath.Join(root, "snapshot")
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 0)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	abs := filepath.Join(snapshotDir, indexRef.Path)
	data, err := os.ReadFile(abs)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data = append(data, 0xff)
	if err := os.WriteFile(abs, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	indexRef.Size = uint64(len(data))
	sum := sha256.Sum256(data)
	indexRef.Checksum = "sha256:" + hex.EncodeToString(sum[:])
	if err := CheckChainIndexSegment(snapshotDir, indexRef); err == nil || !strings.Contains(err.Error(), "size") {
		t.Fatalf("CheckChainIndexSegment error = %v, want size/trailing-byte rejection", err)
	}
}
