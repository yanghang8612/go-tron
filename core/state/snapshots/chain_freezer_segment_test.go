package snapshots

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestChainFreezerSegmentBuildVerifyInstall(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	appendChainFreezerTestRows(t, src, 0, 2)

	snapshotDir := filepath.Join(root, "snapshot")
	ref, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "chain/freezer-0-2.seg", 0, 2)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	if ref.Dataset != SegmentDatasetChainFreezer || ref.Kind != SegmentChainFreezer {
		t.Fatalf("ref family = %s/%s, want %s/%s", ref.Dataset, ref.Kind, SegmentDatasetChainFreezer, SegmentChainFreezer)
	}
	if ref.Size == 0 || ref.Checksum == "" {
		t.Fatalf("ref metadata missing: size=%d checksum=%q", ref.Size, ref.Checksum)
	}
	if ref.Path == "chain/freezer-0-2.seg" {
		t.Fatalf("ref path was not content-addressed")
	}
	if err := CheckChainFreezerSegment(snapshotDir, ref); err != nil {
		t.Fatalf("CheckChainFreezerSegment: %v", err)
	}

	identity := chainFreezerTestIdentity()
	if err := PublishManifest(snapshotDir, NewManifestForChain(0, 0, []SegmentRef{ref}, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if _, err := VerifyRemoteManifestFiles(snapshotDir, identity); err != nil {
		t.Fatalf("VerifyRemoteManifestFiles: %v", err)
	}

	dst := openChainFreezerTestStore(t, filepath.Join(root, "dst-manifest"))
	defer dst.Close()
	result, err := RestoreChainFreezerFromVerifiedManifest(dst, snapshotDir, identity)
	if err != nil {
		t.Fatalf("RestoreChainFreezerFromVerifiedManifest: %v", err)
	}
	if !result.HasRange || result.FromBlock != 0 || result.ToBlock != 2 || result.BlocksRestored != 3 {
		t.Fatalf("restore result = %+v, want range 0..2 with 3 blocks", result)
	}
	assertChainFreezerRowsEqual(t, src, dst, 0, 2)

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := PublishSignedSnapshotCatalog(snapshotDir, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	dstCatalog := openChainFreezerTestStore(t, filepath.Join(root, "dst-catalog"))
	defer dstCatalog.Close()
	catalogResult, err := RestoreChainFreezerFromVerifiedCatalog(dstCatalog, snapshotDir, identity, []ed25519.PublicKey{pub})
	if err != nil {
		t.Fatalf("RestoreChainFreezerFromVerifiedCatalog: %v", err)
	}
	if !catalogResult.HasRange || catalogResult.FromBlock != 0 || catalogResult.ToBlock != 2 || catalogResult.BlocksRestored != 3 {
		t.Fatalf("catalog restore result = %+v, want range 0..2 with 3 blocks", catalogResult)
	}
	assertChainFreezerRowsEqual(t, src, dstCatalog, 0, 2)
}

func TestRestoreChainFreezerSegmentRejectsNonContiguousHead(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: canonicalBoundaryTestBlock(t, 0)},
		{block: canonicalBoundaryTestBlock(t, 1)},
	})
	snapshotDir := filepath.Join(root, "snapshot")
	ref, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}

	dst := openChainFreezerTestStore(t, filepath.Join(root, "dst"))
	defer dst.Close()
	appendChainFreezerTestRows(t, dst, 0, 0)
	if _, err := RestoreChainFreezerSegmentToAncient(dst, snapshotDir, ref); err == nil || !strings.Contains(err.Error(), "requires ancient heads all 0 or all 2") {
		t.Fatalf("RestoreChainFreezerSegmentToAncient error = %v, want non-contiguous head rejection", err)
	}
	if got, err := dst.AncientCount(rawdb.AncientBlocksTable); err != nil || got != 1 {
		t.Fatalf("destination ancient count = %d/%v, want 1", got, err)
	}
}

func TestRestoreChainFreezerSegmentRebuildsHotLookupIndexes(t *testing.T) {
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
	ref, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	dst := openChainFreezerTestStore(t, filepath.Join(root, "dst"))
	defer dst.Close()
	indexDB := rawdb.NewMemoryDatabase()
	result, err := RestoreChainFreezerSegmentToAncientWithOptions(dst, snapshotDir, ref, RestoreChainFreezerOptions{
		IndexWriter:    indexDB,
		ProgressWriter: indexDB,
	})
	if err != nil {
		t.Fatalf("RestoreChainFreezerSegmentToAncientWithOptions: %v", err)
	}
	if result.BlocksRestored != 2 || result.BlockIndexesRestored != 2 || result.TxIndexesRestored != 1 || result.TxInfosRestored != 1 {
		t.Fatalf("restore result = %+v, want 2 blocks, 2 block indexes, 1 tx index, 1 tx info", result)
	}
	if got, ok, err := rawdb.ReadStageProgress(indexDB, rawdb.StageChainFreezer); err != nil || !ok || got != 1 {
		t.Fatalf("StageChainFreezer after restore = %d ok=%v err=%v, want 1", got, ok, err)
	}
	chainDB := rawdb.NewChainDB(indexDB, rawdb.NewFreezerReader(dst))
	if num := rawdb.ReadBlockNumber(chainDB, block1.Hash()); num == nil || *num != 1 {
		t.Fatalf("ReadBlockNumber = %v, want 1", num)
	}
	if idx := rawdb.ReadTransactionIndex(chainDB, txHash[:]); idx == nil || *idx != 1 {
		t.Fatalf("ReadTransactionIndex = %v, want 1", idx)
	}
	if info := rawdb.ReadTransactionInfo(chainDB, txHash[:]); info == nil || info.Fee != 777 {
		t.Fatalf("ReadTransactionInfo = %+v, want fee 777", info)
	}
	if infos := rawdb.ReadTransactionInfosByBlock(chainDB, 1); len(infos) != 1 || infos[0].Fee != 777 {
		t.Fatalf("ReadTransactionInfosByBlock = %+v, want one fee 777", infos)
	}

	second, err := RestoreChainFreezerSegmentToAncientWithOptions(dst, snapshotDir, ref, RestoreChainFreezerOptions{
		IndexWriter:    indexDB,
		ProgressWriter: indexDB,
	})
	if err != nil {
		t.Fatalf("idempotent RestoreChainFreezerSegmentToAncientWithOptions: %v", err)
	}
	if !second.AlreadyInstalled || second.BlocksRestored != 0 || second.BlockIndexesRestored != 2 || second.TxIndexesRestored != 1 || second.TxInfosRestored != 1 {
		t.Fatalf("second restore result = %+v, want already installed plus rebuilt indexes", second)
	}
}

func TestRestoreChainFreezerIndexesLoadsThroughSortedETL(t *testing.T) {
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
	ref, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}

	direct := newChainFreezerIndexOrderWriter()
	if err := iterateChainFreezerSegmentRows(snapshotDir, ref, func(row chainFreezerRow) error {
		_, err := restoreChainFreezerIndexesForRow(direct, row)
		return err
	}); err != nil {
		t.Fatalf("capture direct chain-freezer index order: %v", err)
	}
	if sort.SliceIsSorted(direct.putKeys, func(i, j int) bool {
		return bytes.Compare(direct.putKeys[i], direct.putKeys[j]) < 0
	}) {
		t.Fatal("test setup produced already-sorted direct chain-freezer index order")
	}
	expectedKeys := append([][]byte(nil), direct.putKeys...)
	sort.Slice(expectedKeys, func(i, j int) bool {
		return bytes.Compare(expectedKeys[i], expectedKeys[j]) < 0
	})

	writer := newChainFreezerIndexOrderWriter()
	result, err := RestoreChainFreezerIndexes(writer, snapshotDir, ref)
	if err != nil {
		t.Fatalf("RestoreChainFreezerIndexes: %v", err)
	}
	if result.BlockIndexesRestored != 2 || result.TxIndexesRestored != 1 || result.TxInfosRestored != 1 {
		t.Fatalf("restore result = %+v, want 2 block indexes, 1 tx index, 1 tx info", result)
	}
	if !byteSlicesEqual(writer.putKeys, expectedKeys) {
		t.Fatalf("chain-freezer index restore put keys are not sorted by physical key\n got: %x\nwant: %x", writer.putKeys, expectedKeys)
	}
	if len(writer.deleteKeys) != 0 {
		t.Fatalf("chain-freezer index restore deletes = %d, want 0", len(writer.deleteKeys))
	}

	etlTemp := filepath.Join(root, "etl-scratch")
	if _, err := RestoreChainFreezerIndexesWithOptions(newChainFreezerIndexOrderWriter(), snapshotDir, ref, RestoreETLOptions{
		TempDir:     etlTemp,
		BufferLimit: 1,
	}); err != nil {
		t.Fatalf("RestoreChainFreezerIndexesWithOptions: %v", err)
	}
	if _, err := os.Stat(etlTemp); err != nil {
		t.Fatalf("custom chain-freezer index restore ETL temp dir stat: %v", err)
	}
}

func TestRestoreChainFreezerIndexesRejectsMismatchedTransactionInfo(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	block0 := canonicalBoundaryTestBlock(t, 0)
	block1, _, txInfoRaw := chainFreezerBlockWithTx(t, 1)
	var ret corepb.TransactionRet
	if err := proto.Unmarshal(txInfoRaw, &ret); err != nil {
		t.Fatalf("unmarshal tx info: %v", err)
	}
	ret.Transactioninfo[0].Id = bytes.Repeat([]byte{0xed}, common.HashLength)
	badTxInfoRaw, err := proto.Marshal(&ret)
	if err != nil {
		t.Fatalf("marshal bad tx info: %v", err)
	}
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1, txInfosRaw: badTxInfoRaw},
	})

	snapshotDir := filepath.Join(root, "snapshot")
	ref, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	writer := newChainFreezerIndexOrderWriter()
	if _, err := RestoreChainFreezerIndexes(writer, snapshotDir, ref); err == nil || !strings.Contains(err.Error(), "does not match canonical tx") {
		t.Fatalf("RestoreChainFreezerIndexes error = %v, want canonical tx mismatch", err)
	}
	if len(writer.putKeys) != 0 {
		t.Fatalf("RestoreChainFreezerIndexes loaded %d keys before rejecting bad tx info", len(writer.putKeys))
	}
}

func TestRestoreChainFreezerManifestPrefersColdLookupIndexes(t *testing.T) {
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
	identity := chainFreezerTestIdentity()
	if err := PublishManifest(snapshotDir, NewManifestForChain(0, 0, []SegmentRef{freezerRef, indexRef}, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	dst := openChainFreezerTestStore(t, filepath.Join(root, "dst"))
	defer dst.Close()
	indexDB := rawdb.NewMemoryDatabase()
	result, err := RestoreChainFreezerFromVerifiedManifestWithOptions(dst, snapshotDir, identity, RestoreChainFreezerOptions{
		IndexWriter:       indexDB,
		ProgressWriter:    indexDB,
		PreferColdIndexes: true,
	})
	if err != nil {
		t.Fatalf("RestoreChainFreezerFromVerifiedManifestWithOptions: %v", err)
	}
	if result.BlocksRestored != 2 || result.ColdIndexSegments != 1 || result.BlockIndexesRestored != 0 || result.TxIndexesRestored != 0 || result.TxInfosRestored != 0 {
		t.Fatalf("restore result = %+v, want freezer rows plus one cold index and no hot lookup rows", result)
	}
	if got, ok, err := rawdb.ReadStageProgress(indexDB, rawdb.StageChainFreezer); err != nil || !ok || got != 1 {
		t.Fatalf("StageChainFreezer after cold-index restore = %d ok=%v err=%v, want 1", got, ok, err)
	}

	chainDB := rawdb.NewChainDB(indexDB, rawdb.NewFreezerReader(dst))
	if num := rawdb.ReadBlockNumber(chainDB, block1.Hash()); num != nil {
		t.Fatalf("ReadBlockNumber without sidecar = %v, want nil hot lookup", num)
	}
	if idx := rawdb.ReadTransactionIndex(chainDB, txHash[:]); idx != nil {
		t.Fatalf("ReadTransactionIndex without sidecar = %v, want nil hot lookup", idx)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	chainDB.SetChainIndexReader(mgr)
	if num := rawdb.ReadBlockNumber(chainDB, block1.Hash()); num == nil || *num != 1 {
		t.Fatalf("ReadBlockNumber with sidecar = %v, want 1", num)
	}
	if idx := rawdb.ReadTransactionIndex(chainDB, txHash[:]); idx == nil || *idx != 1 {
		t.Fatalf("ReadTransactionIndex with sidecar = %v, want 1", idx)
	}
	if info := rawdb.ReadTransactionInfo(chainDB, txHash[:]); info == nil || info.Fee != 777 {
		t.Fatalf("ReadTransactionInfo with sidecar = %+v, want fee 777", info)
	}
}

func TestAggregatorBuildChainFreezerPreservesManifestChainIdentity(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: canonicalBoundaryTestBlock(t, 0)},
		{block: canonicalBoundaryTestBlock(t, 1)},
	})

	snapshotDir := filepath.Join(root, "snapshot")
	identity := chainFreezerTestIdentity()
	if err := PublishManifest(snapshotDir, NewManifestForChain(10, 20, nil, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	result, err := NewAggregator(snapshotDir).BuildChainFreezer(src, 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezer: %v", err)
	}
	if result.Manifest == nil || result.Manifest.Chain == nil {
		t.Fatalf("manifest chain identity missing after BuildChainFreezer")
	}
	if got := *result.Manifest.Chain; got != identity {
		t.Fatalf("manifest chain identity = %+v, want %+v", got, identity)
	}
	if result.Manifest.VisibleTxStart != 10 || result.Manifest.VisibleTxEnd != 20 {
		t.Fatalf("manifest visible range = [%d,%d], want [10,20]", result.Manifest.VisibleTxStart, result.Manifest.VisibleTxEnd)
	}
	if len(result.Segments) != 3 ||
		result.Segments[0].Kind != SegmentChainFreezer ||
		result.Segments[1].Kind != SegmentChainFreezerAccessor ||
		result.Segments[2].Kind != SegmentChainIndex {
		t.Fatalf("result segments = %+v, want chain-freezer, accessor, and chain-index segments", result.Segments)
	}
	if _, err := VerifyRemoteManifestFiles(snapshotDir, identity); err != nil {
		t.Fatalf("VerifyRemoteManifestFiles: %v", err)
	}
}

func TestManagerAncientReadsChainFreezerSegmentAfterLocalTailHidden(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	block0 := canonicalBoundaryTestBlock(t, 0)
	block1, txHash, txInfoRaw := chainFreezerBlockWithTx(t, 1)
	stateRoot := common.HexToHash("5656565656565656565656565656565656565656565656565656565656565656")
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1, txInfosRaw: txInfoRaw, stateRoot: stateRoot.Bytes()},
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
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{freezerRef, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	if count, err := mgr.AncientCount(rawdb.AncientBlocksTable); err != nil || count != 2 {
		t.Fatalf("manager AncientCount = %d/%v, want 2/nil", count, err)
	}
	if ok, err := mgr.HasAncient(rawdb.AncientBlocksTable, 1); err != nil || !ok {
		t.Fatalf("manager HasAncient = %v/%v, want true/nil", ok, err)
	}
	rows, err := mgr.AncientRange(rawdb.AncientBlocksTable, 0, 2, 0)
	if err != nil {
		t.Fatalf("manager AncientRange: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("manager AncientRange rows = %d, want 2", len(rows))
	}

	if _, err := src.TruncateTail(2); err != nil {
		t.Fatalf("TruncateTail: %v", err)
	}
	chainDB := rawdb.NewChainDB(
		rawdb.NewMemoryDatabase(),
		rawdb.NewFallbackAncientReader(rawdb.NewFreezerReader(src), mgr),
	)
	chainDB.SetChainIndexReader(mgr)
	if block := rawdb.ReadBlock(chainDB, 1); block == nil || block.Hash() != block1.Hash() {
		t.Fatalf("ReadBlock after tail hidden = %+v, want block1", block)
	}
	if num := rawdb.ReadBlockNumber(chainDB, block1.Hash()); num == nil || *num != 1 {
		t.Fatalf("ReadBlockNumber after tail hidden = %v, want 1", num)
	}
	if infos := rawdb.ReadTransactionInfosByBlock(chainDB, 1); len(infos) != 1 || infos[0].Fee != 777 {
		t.Fatalf("ReadTransactionInfosByBlock after tail hidden = %+v, want fee 777", infos)
	}
	if info := rawdb.ReadTransactionInfo(chainDB, txHash[:]); info == nil || info.Fee != 777 {
		t.Fatalf("ReadTransactionInfo after tail hidden = %+v, want fee 777", info)
	}
	if got := rawdb.ReadBlockStateRoot(chainDB, block1.Hash()); got != stateRoot {
		t.Fatalf("ReadBlockStateRoot after tail hidden = %x, want %x", got, stateRoot)
	}
}

func TestManagerAncientRangeStreamsAcrossChainFreezerSegments(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	appendChainFreezerTestRows(t, src, 0, 3)

	snapshotDir := filepath.Join(root, "snapshot")
	refA, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient 0..1: %v", err)
	}
	accessorA, err := BuildChainFreezerAccessorSegmentFromChainFreezerSegment(snapshotDir, refA, "")
	if err != nil {
		t.Fatalf("BuildChainFreezerAccessorSegmentFromChainFreezerSegment 0..1: %v", err)
	}
	refB, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 2, 3)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient 2..3: %v", err)
	}
	accessorB, err := BuildChainFreezerAccessorSegmentFromChainFreezerSegment(snapshotDir, refB, "")
	if err != nil {
		t.Fatalf("BuildChainFreezerAccessorSegmentFromChainFreezerSegment 2..3: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{refA, accessorA, refB, accessorB})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	rows, err := mgr.AncientRange(rawdb.AncientTxInfosTable, 1, 3, 0)
	if err != nil {
		t.Fatalf("AncientRange txinfos 1..3: %v", err)
	}
	if got, want := stringifyAncientRows(rows), []string{"txinfos-1", "txinfos-2", "txinfos-3"}; !stringSlicesEqual(got, want) {
		t.Fatalf("AncientRange txinfos = %q, want %q", got, want)
	}

	rows, err = mgr.AncientRange(rawdb.AncientBlocksTable, 0, 4, uint64(len("block-0")+1))
	if err != nil {
		t.Fatalf("AncientRange maxBytes: %v", err)
	}
	if got, want := stringifyAncientRows(rows), []string{"block-0"}; !stringSlicesEqual(got, want) {
		t.Fatalf("AncientRange maxBytes rows = %q, want %q", got, want)
	}

	if _, err := mgr.AncientRange(rawdb.AncientBlocksTable, 4, 1, 0); !errors.Is(err, rawdb.ErrNotInAncient) {
		t.Fatalf("AncientRange missing first row error = %v, want ErrNotInAncient", err)
	}
}

func TestManagerAncientRangeStopsAtChainFreezerSegmentGap(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	appendChainFreezerTestRows(t, src, 0, 3)

	snapshotDir := filepath.Join(root, "snapshot")
	refA, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient 0..1: %v", err)
	}
	accessorA, err := BuildChainFreezerAccessorSegmentFromChainFreezerSegment(snapshotDir, refA, "")
	if err != nil {
		t.Fatalf("BuildChainFreezerAccessorSegmentFromChainFreezerSegment 0..1: %v", err)
	}
	refC, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 3, 3)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient 3..3: %v", err)
	}
	accessorC, err := BuildChainFreezerAccessorSegmentFromChainFreezerSegment(snapshotDir, refC, "")
	if err != nil {
		t.Fatalf("BuildChainFreezerAccessorSegmentFromChainFreezerSegment 3..3: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{refA, accessorA, refC, accessorC})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	rows, err := mgr.AncientRange(rawdb.AncientBlocksTable, 0, 4, 0)
	if err != nil {
		t.Fatalf("AncientRange with later gap: %v", err)
	}
	if got, want := stringifyAncientRows(rows), []string{"block-0", "block-1"}; !stringSlicesEqual(got, want) {
		t.Fatalf("AncientRange with later gap rows = %q, want %q", got, want)
	}
	if _, err := mgr.AncientRange(rawdb.AncientBlocksTable, 2, 1, 0); !errors.Is(err, rawdb.ErrNotInAncient) {
		t.Fatalf("AncientRange gap first row error = %v, want ErrNotInAncient", err)
	}
}

func TestCheckChainFreezerSegmentRejectsTrailingBytes(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	appendChainFreezerTestRows(t, src, 0, 0)
	snapshotDir := filepath.Join(root, "snapshot")
	ref, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 0)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	abs := filepath.Join(snapshotDir, ref.Path)
	data, err := os.ReadFile(abs)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data = append(data, 0xff)
	if err := os.WriteFile(abs, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	ref.Size = uint64(len(data))
	sum := sha256.Sum256(data)
	ref.Checksum = "sha256:" + hex.EncodeToString(sum[:])
	if err := CheckChainFreezerSegment(snapshotDir, ref); err == nil || !strings.Contains(err.Error(), "trailing bytes") {
		t.Fatalf("CheckChainFreezerSegment error = %v, want trailing-byte rejection", err)
	}
}

func openChainFreezerTestStore(t *testing.T, dir string) *rawdbfreezer.Freezer {
	t.Helper()
	f, err := rawdbfreezer.NewFreezer(dir, "", false, 2049, map[string]rawdbfreezer.TableConfig{
		rawdb.AncientBlocksTable:     {NoSnappy: false, Prunable: true},
		rawdb.AncientTxInfosTable:    {NoSnappy: false, Prunable: true},
		rawdb.AncientStateRootsTable: {NoSnappy: true, Prunable: true},
	})
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	return f
}

func appendChainFreezerTestRows(t *testing.T, f *rawdbfreezer.Freezer, from, to uint64) {
	t.Helper()
	if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
		for n := from; n <= to; n++ {
			if err := op.AppendRaw(rawdb.AncientBlocksTable, n, []byte(fmt.Sprintf("block-%d", n))); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientTxInfosTable, n, []byte(fmt.Sprintf("txinfos-%d", n))); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientStateRootsTable, n, []byte(fmt.Sprintf("state-root-%d", n))); err != nil {
				return err
			}
			if n == to {
				break
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("ModifyAncients: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

type chainFreezerRawTestRow struct {
	block      *types.Block
	txInfosRaw []byte
	stateRoot  []byte
}

func appendChainFreezerRawRows(t *testing.T, f *rawdbfreezer.Freezer, rows []chainFreezerRawTestRow) {
	t.Helper()
	if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
		for i, row := range rows {
			if row.block == nil {
				return fmt.Errorf("nil block at row %d", i)
			}
			blockRaw, err := row.block.Marshal()
			if err != nil {
				return err
			}
			n := row.block.Number()
			if n != uint64(i) {
				return fmt.Errorf("test row %d has block number %d", i, n)
			}
			if err := op.AppendRaw(rawdb.AncientBlocksTable, n, blockRaw); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientTxInfosTable, n, row.txInfosRaw); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientStateRootsTable, n, row.stateRoot); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("ModifyAncients: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

func chainFreezerBlockWithTx(t *testing.T, number uint64) (*types.Block, [32]byte, []byte) {
	t.Helper()
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp:  int64(10_000 + number),
			Expiration: int64(20_000 + number),
		},
	}
	tx := types.NewTransactionFromPB(txPB)
	txHash := tx.Hash()
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(30_000 + number),
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})
	ret := &corepb.TransactionRet{
		BlockNumber: int64(number),
		Transactioninfo: []*corepb.TransactionInfo{
			{
				Id:          txHash[:],
				Fee:         777,
				BlockNumber: int64(number),
			},
		},
	}
	txInfoRaw, err := proto.Marshal(ret)
	if err != nil {
		t.Fatalf("marshal tx info: %v", err)
	}
	return block, txHash, txInfoRaw
}

func assertChainFreezerRowsEqual(t *testing.T, wantStore, gotStore *rawdbfreezer.Freezer, from, to uint64) {
	t.Helper()
	for _, table := range []string{rawdb.AncientBlocksTable, rawdb.AncientTxInfosTable, rawdb.AncientStateRootsTable} {
		for n := from; n <= to; n++ {
			want, err := wantStore.Ancient(table, n)
			if err != nil {
				t.Fatalf("want Ancient(%s,%d): %v", table, n, err)
			}
			got, err := gotStore.Ancient(table, n)
			if err != nil {
				t.Fatalf("got Ancient(%s,%d): %v", table, n, err)
			}
			if string(got) != string(want) {
				t.Fatalf("Ancient(%s,%d) = %q, want %q", table, n, got, want)
			}
			if n == to {
				break
			}
		}
	}
}

func stringifyAncientRows(rows [][]byte) []string {
	out := make([]string, len(rows))
	for i, row := range rows {
		out[i] = string(row)
	}
	return out
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type chainFreezerIndexOrderWriter struct {
	putKeys    [][]byte
	deleteKeys [][]byte
}

func newChainFreezerIndexOrderWriter() *chainFreezerIndexOrderWriter {
	return &chainFreezerIndexOrderWriter{}
}

func (w *chainFreezerIndexOrderWriter) Put(key, value []byte) error {
	w.putKeys = append(w.putKeys, append([]byte(nil), key...))
	return nil
}

func (w *chainFreezerIndexOrderWriter) Delete(key []byte) error {
	w.deleteKeys = append(w.deleteKeys, append([]byte(nil), key...))
	return nil
}

func chainFreezerTestIdentity() ChainIdentity {
	return ChainIdentity{
		ChainID:        1,
		NetworkID:      2,
		GenesisHash:    strings.Repeat("0", 64),
		ForkConfigHash: "sha256:" + strings.Repeat("a", 64),
	}
}
