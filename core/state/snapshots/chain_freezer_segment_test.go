package snapshots

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
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

func TestChainFreezerBlobCompressionRoundTripAndLegacyRead(t *testing.T) {
	compressedPayload := bytes.Repeat([]byte("repeated chain-freezer payload "), 256)
	var compressed bytes.Buffer
	if err := writeChainFreezerBytes(&compressed, compressedPayload); err != nil {
		t.Fatalf("write compressed chain-freezer blob: %v", err)
	}
	if len(compressed.Bytes()) < 4 || binary.BigEndian.Uint32(compressed.Bytes()[:4])&chainFreezerBlobCompressedFlag == 0 {
		t.Fatal("compressible chain-freezer blob was not marked compressed")
	}
	got, next, err := readChainFreezerBytesAt(bytes.NewReader(compressed.Bytes()), 0, uint64(compressed.Len()))
	if err != nil {
		t.Fatalf("read compressed chain-freezer blob: %v", err)
	}
	if next != uint64(compressed.Len()) || !bytes.Equal(got, compressedPayload) {
		t.Fatalf("compressed round trip = next:%d bytes:%d, want next:%d bytes:%d", next, len(got), compressed.Len(), len(compressedPayload))
	}

	legacyPayload := []byte("legacy raw chain-freezer blob")
	legacy := make([]byte, 4+len(legacyPayload))
	binary.BigEndian.PutUint32(legacy[:4], uint32(len(legacyPayload)))
	copy(legacy[4:], legacyPayload)
	got, next, err = readChainFreezerBytesAt(bytes.NewReader(legacy), 0, uint64(len(legacy)))
	if err != nil {
		t.Fatalf("read legacy chain-freezer blob: %v", err)
	}
	if next != uint64(len(legacy)) || !bytes.Equal(got, legacyPayload) {
		t.Fatalf("legacy round trip = next:%d payload:%q, want next:%d payload:%q", next, got, len(legacy), legacyPayload)
	}
}

func TestBuildChainFreezerSegmentCompressesLargeCanonicalRows(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()

	block0 := canonicalBoundaryTestBlock(t, 0)
	txPB := &corepb.Transaction{RawData: &corepb.TransactionRaw{
		Timestamp:  10_001,
		Expiration: 20_001,
		Data:       bytes.Repeat([]byte("compressible transaction input "), 512),
	}}
	tx := types.NewTransactionFromPB(txPB)
	block1 := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 30_001}},
		Transactions: []*corepb.Transaction{txPB},
	})
	txInfosRaw, err := proto.Marshal(&corepb.TransactionRet{
		BlockNumber:    1,
		BlockTimeStamp: 30_001,
		Transactioninfo: []*corepb.TransactionInfo{{
			Id:             tx.Hash().Bytes(),
			BlockNumber:    1,
			BlockTimeStamp: 30_001,
		}},
	})
	if err != nil {
		t.Fatalf("marshal transaction infos: %v", err)
	}
	appendChainFreezerRawRows(t, store, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1, txInfosRaw: txInfosRaw},
	})

	snapshotDir := filepath.Join(root, "snapshot")
	ref, err := BuildChainFreezerSegmentFromAncient(store, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	block0Raw, err := block0.Marshal()
	if err != nil {
		t.Fatalf("marshal block 0: %v", err)
	}
	block1Raw, err := block1.Marshal()
	if err != nil {
		t.Fatalf("marshal block 1: %v", err)
	}
	rawSize := uint64(chainFreezerHeaderSize + (4 + len(block0Raw)) + 4 + 4 + (4 + len(block1Raw)) + (4 + len(txInfosRaw)) + 4)
	if ref.Size >= rawSize {
		t.Fatalf("compressed segment size = %d, want below raw framing size %d", ref.Size, rawSize)
	}
	if err := CheckChainFreezerSegment(snapshotDir, ref); err != nil {
		t.Fatalf("CheckChainFreezerSegment compressed segment: %v", err)
	}
	accessorRef, err := BuildChainFreezerAccessorSegmentFromChainFreezerSegment(snapshotDir, ref, "")
	if err != nil {
		t.Fatalf("BuildChainFreezerAccessorSegmentFromChainFreezerSegment: %v", err)
	}
	if _, _, err := readChainFreezerSegmentRowWithAccessor(snapshotDir, ref, accessorRef, 1); err != nil {
		t.Fatalf("read compressed chain-freezer row through accessor: %v", err)
	}
}

func TestReadChainFreezerBlobRejectsEmptyCompressedPayload(t *testing.T) {
	raw := make([]byte, 4)
	binary.BigEndian.PutUint32(raw, chainFreezerBlobCompressedFlag)
	if _, _, err := readChainFreezerBytesAt(bytes.NewReader(raw), 0, uint64(len(raw))); err == nil || !strings.Contains(err.Error(), "zero encoded length") {
		t.Fatalf("read empty compressed chain-freezer blob error = %v, want zero encoded length rejection", err)
	}
}

func TestReadChainFreezerBlobRejectsOversizedCompressedPayload(t *testing.T) {
	encoded := make([]byte, binary.MaxVarintLen64)
	encodedLen := binary.PutUvarint(encoded, uint64(chainFreezerMaxDecodedBlobSize+1))
	encoded = encoded[:encodedLen]
	raw := make([]byte, 4+len(encoded))
	binary.BigEndian.PutUint32(raw[:4], uint32(len(encoded))|chainFreezerBlobCompressedFlag)
	copy(raw[4:], encoded)
	if _, _, err := readChainFreezerBytesAt(bytes.NewReader(raw), 0, uint64(len(raw))); err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("read oversized compressed chain-freezer blob error = %v, want decoded-size rejection", err)
	}
}

func TestReadChainFreezerBlobRejectsOversizedRawPayload(t *testing.T) {
	raw := make([]byte, 4)
	binary.BigEndian.PutUint32(raw, uint32(chainFreezerMaxDecodedBlobSize+1))
	fileSize := uint64(chainFreezerMaxDecodedBlobSize + 5)
	if _, _, err := readChainFreezerBytesAt(bytes.NewReader(raw), 0, fileSize); err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("read oversized raw chain-freezer blob error = %v, want decoded-size rejection", err)
	}
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

func TestVerifyChainFreezerRestoreTargetRejectsNonContiguousManifest(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: canonicalBoundaryTestBlock(t, 0)},
		{block: canonicalBoundaryTestBlock(t, 1)},
	})
	snapshotDir := filepath.Join(root, "snapshot")
	ref, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 1, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	manifest := NewManifestForChain(1, 1, []SegmentRef{ref}, chainFreezerTestIdentity())
	dst := openChainFreezerTestStore(t, filepath.Join(root, "dst"))
	defer dst.Close()

	err = VerifyChainFreezerRestoreTarget(dst, snapshotDir, manifest)
	if err == nil || !strings.Contains(err.Error(), "requires ancient heads all 1 or all 2") {
		t.Fatalf("VerifyChainFreezerRestoreTarget error = %v, want non-contiguous head rejection", err)
	}
	if got, err := dst.AncientCount(rawdb.AncientBlocksTable); err != nil || got != 0 {
		t.Fatalf("destination ancient count = %d/%v, want unchanged empty store", got, err)
	}
}

func TestVerifyChainFreezerRestoreTargetAcceptsContiguousManifest(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: canonicalBoundaryTestBlock(t, 0)},
		{block: canonicalBoundaryTestBlock(t, 1)},
	})
	snapshotDir := filepath.Join(root, "snapshot")
	ref0, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 0)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient ref0: %v", err)
	}
	ref1, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 1, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient ref1: %v", err)
	}
	manifest := NewManifestForChain(1, 1, []SegmentRef{ref1, ref0}, chainFreezerTestIdentity())
	dst := openChainFreezerTestStore(t, filepath.Join(root, "dst"))
	defer dst.Close()

	if err := VerifyChainFreezerRestoreTarget(dst, snapshotDir, manifest); err != nil {
		t.Fatalf("VerifyChainFreezerRestoreTarget: %v", err)
	}
	if got, err := dst.AncientCount(rawdb.AncientBlocksTable); err != nil || got != 0 {
		t.Fatalf("destination ancient count = %d/%v, want dry-run to leave store empty", got, err)
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
	if row, ok, err := rawdb.ReadStageProgressRow(indexDB, rawdb.StageChainFreezer); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block1.Hash() {
		t.Fatalf("StageChainFreezer row after restore = %+v ok=%v err=%v, want block1 hash", row, ok, err)
	}
	chainDB := rawdb.NewChainDB(indexDB, rawdb.NewFreezerReader(dst))
	if num := rawdb.ReadBlockNumber(chainDB, block1.Hash()); num == nil || *num != 1 {
		t.Fatalf("ReadBlockNumber = %v, want 1", num)
	}
	if idx := rawdb.ReadTransactionIndex(chainDB, txHash[:]); idx == nil || *idx != 1 {
		t.Fatalf("ReadTransactionIndex = %v, want 1", idx)
	}
	assertChainFreezerTxInfo(t, "ReadTransactionInfo", rawdb.ReadTransactionInfo(chainDB, txHash[:]), 1)
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

func TestBuildChainFreezerSegmentRejectsMismatchedTransactionInfo(t *testing.T) {
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
	if _, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1); err == nil || !strings.Contains(err.Error(), "does not match canonical tx") {
		t.Fatalf("BuildChainFreezerSegmentFromAncient error = %v, want canonical tx mismatch", err)
	}
}

func TestBuildChainFreezerSegmentRejectsMalformedAncientBlock(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	if _, err := src.ModifyAncients(func(op rawdb.AncientWriteOp) error {
		if err := op.AppendRaw(rawdb.AncientBlocksTable, 0, []byte{0xde, 0xad}); err != nil {
			return err
		}
		if err := op.AppendRaw(rawdb.AncientTxInfosTable, 0, nil); err != nil {
			return err
		}
		return op.AppendRaw(rawdb.AncientStateRootsTable, 0, nil)
	}); err != nil {
		t.Fatalf("ModifyAncients: %v", err)
	}
	if err := src.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := BuildChainFreezerSegmentFromAncient(src, filepath.Join(root, "snapshot"), "", 0, 0); err == nil || !strings.Contains(err.Error(), "decode chain-freezer segment build block 0") {
		t.Fatalf("BuildChainFreezerSegmentFromAncient error = %v, want malformed block rejection", err)
	}
}

func TestCheckChainFreezerSegmentRejectsMalformedPayloads(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	block0 := canonicalBoundaryTestBlock(t, 0)
	block0Raw, err := block0.Marshal()
	if err != nil {
		t.Fatalf("marshal block0: %v", err)
	}
	txBlock, _, _ := chainFreezerBlockWithTx(t, 0)
	txBlockRaw, err := txBlock.Marshal()
	if err != nil {
		t.Fatalf("marshal tx block: %v", err)
	}

	for _, tc := range []struct {
		name string
		row  chainFreezerRow
		want string
	}{
		{
			name: "bad-state-root",
			row: chainFreezerRow{
				blockNum:     0,
				blockRaw:     block0Raw,
				stateRootRaw: []byte{0x01},
			},
			want: "state root length",
		},
		{
			name: "missing-tx-info",
			row: chainFreezerRow{
				blockNum: 0,
				blockRaw: txBlockRaw,
			},
			want: "missing transaction info coverage",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref := writeChainFreezerSegmentRowsForTest(t, snapshotDir, tc.name+".seg", 0, 0, []chainFreezerRow{tc.row})
			if err := CheckChainFreezerSegment(snapshotDir, ref); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CheckChainFreezerSegment error = %v, want %q", err, tc.want)
			}
		})
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
	if row, ok, err := rawdb.ReadStageProgressRow(indexDB, rawdb.StageChainFreezer); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block1.Hash() {
		t.Fatalf("StageChainFreezer row after cold-index restore = %+v ok=%v err=%v, want block1 hash", row, ok, err)
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
	assertChainFreezerTxInfo(t, "ReadTransactionInfo after tail hidden", rawdb.ReadTransactionInfo(chainDB, txHash[:]), 1)
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
	if want := chainFreezerAncientRows(t, src, rawdb.AncientTxInfosTable, 1, 3); !byteRowsEqual(rows, want) {
		t.Fatalf("AncientRange txinfos = %x, want %x", rows, want)
	}

	block0Raw := chainFreezerAncientRows(t, src, rawdb.AncientBlocksTable, 0, 0)[0]
	rows, err = mgr.AncientRange(rawdb.AncientBlocksTable, 0, 4, uint64(len(block0Raw)+1))
	if err != nil {
		t.Fatalf("AncientRange maxBytes: %v", err)
	}
	if want := chainFreezerAncientRows(t, src, rawdb.AncientBlocksTable, 0, 0); !byteRowsEqual(rows, want) {
		t.Fatalf("AncientRange maxBytes rows = %x, want %x", rows, want)
	}

	if _, err := mgr.AncientRange(rawdb.AncientBlocksTable, 4, 1, 0); !errors.Is(err, rawdb.ErrNotInAncient) {
		t.Fatalf("AncientRange missing first row error = %v, want ErrNotInAncient", err)
	}
}

func TestManagerHasAncientRequiresReadableChainFreezerSegment(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	appendChainFreezerTestRows(t, src, 0, 0)

	snapshotDir := filepath.Join(root, "snapshot")
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 0)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{freezerRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	ok, err := mgr.HasAncient(rawdb.AncientBlocksTable, 0)
	if err != nil || !ok {
		t.Fatalf("HasAncient readable segment = %v/%v, want true/nil", ok, err)
	}
	if err := os.Remove(filepath.Join(snapshotDir, freezerRef.Path)); err != nil {
		t.Fatalf("remove chain-freezer segment: %v", err)
	}
	ok, err = mgr.HasAncient(rawdb.AncientBlocksTable, 0)
	if err == nil || ok {
		t.Fatalf("HasAncient missing advertised segment = %v/%v, want false/error", ok, err)
	}
}

func TestManagerAncientCountRequiresReadableHighestChainFreezerSegment(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, filepath.Join(root, "src"))
	defer src.Close()
	appendChainFreezerTestRows(t, src, 0, 1)

	snapshotDir := filepath.Join(root, "snapshot")
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{freezerRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	count, err := mgr.AncientCount(rawdb.AncientBlocksTable)
	if err != nil || count != 2 {
		t.Fatalf("AncientCount readable segment = %d/%v, want 2/nil", count, err)
	}
	if err := os.Remove(filepath.Join(snapshotDir, freezerRef.Path)); err != nil {
		t.Fatalf("remove chain-freezer segment: %v", err)
	}
	count, err = mgr.AncientCount(rawdb.AncientBlocksTable)
	if err == nil || count != 0 {
		t.Fatalf("AncientCount missing advertised segment = %d/%v, want 0/error", count, err)
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
	if want := chainFreezerAncientRows(t, src, rawdb.AncientBlocksTable, 0, 1); !byteRowsEqual(rows, want) {
		t.Fatalf("AncientRange with later gap rows = %x, want %x", rows, want)
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

func TestCheckChainFreezerSegmentRejectsOversizedLengthPrefixBeforeAlloc(t *testing.T) {
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
	if len(data) < chainFreezerHeaderSize+4 {
		t.Fatalf("chain-freezer fixture too small: %d", len(data))
	}
	binary.BigEndian.PutUint32(data[chainFreezerHeaderSize:chainFreezerHeaderSize+4], ^uint32(0))
	if err := os.WriteFile(abs, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	ref.Size = uint64(len(data))
	sum := sha256.Sum256(data)
	ref.Checksum = "sha256:" + hex.EncodeToString(sum[:])
	if err := CheckChainFreezerSegment(snapshotDir, ref); err == nil || !strings.Contains(err.Error(), "exceeds remaining") {
		t.Fatalf("CheckChainFreezerSegment error = %v, want bounded length rejection", err)
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
			blockRaw, err := canonicalBoundaryTestBlock(t, n).Marshal()
			if err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientBlocksTable, n, blockRaw); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientTxInfosTable, n, nil); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientStateRootsTable, n, nil); err != nil {
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

func writeChainFreezerSegmentRowsForTest(t *testing.T, dir, relPath string, from, to uint64, rows []chainFreezerRow) SegmentRef {
	t.Helper()
	if relPath == "" {
		relPath = ChainFreezerSegmentPath(from, to)
	}
	abs := filepath.Join(dir, relPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	file, err := os.Create(abs)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	count, err := chainFreezerRowCount(from, to)
	if err != nil {
		t.Fatalf("chainFreezerRowCount: %v", err)
	}
	if uint64(len(rows)) != count {
		t.Fatalf("test rows = %d, want %d", len(rows), count)
	}
	if err := writeChainFreezerHeader(file, from, to, count); err != nil {
		_ = file.Close()
		t.Fatalf("writeChainFreezerHeader: %v", err)
	}
	for _, row := range rows {
		if err := writeChainFreezerBytes(file, row.blockRaw); err != nil {
			_ = file.Close()
			t.Fatalf("write block row %d: %v", row.blockNum, err)
		}
		if err := writeChainFreezerBytes(file, row.txInfosRaw); err != nil {
			_ = file.Close()
			t.Fatalf("write tx info row %d: %v", row.blockNum, err)
		}
		if err := writeChainFreezerBytes(file, row.stateRootRaw); err != nil {
			_ = file.Close()
			t.Fatalf("write state root row %d: %v", row.blockNum, err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	sum := sha256.Sum256(data)
	return SegmentRef{
		Dataset:   SegmentDatasetChainFreezer,
		Kind:      SegmentChainFreezer,
		FromTxNum: from,
		ToTxNum:   to,
		Path:      relPath,
		Size:      uint64(len(data)),
		Checksum:  "sha256:" + hex.EncodeToString(sum[:]),
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
		BlockNumber:    int64(number),
		BlockTimeStamp: int64(30_000 + number),
		Transactioninfo: []*corepb.TransactionInfo{
			{
				Id:             txHash[:],
				Fee:            777,
				BlockNumber:    int64(number),
				BlockTimeStamp: int64(30_000 + number),
				Receipt: &corepb.ResourceReceipt{
					EnergyUsage:        45,
					EnergyFee:          678,
					OriginEnergyUsage:  9,
					EnergyUsageTotal:   90,
					NetUsage:           12,
					NetFee:             34,
					Result:             corepb.Transaction_Result_SUCCESS,
					EnergyPenaltyTotal: 56,
				},
			},
		},
	}
	txInfoRaw, err := proto.Marshal(ret)
	if err != nil {
		t.Fatalf("marshal tx info: %v", err)
	}
	return block, txHash, txInfoRaw
}

func chainFreezerTestStateRoot(n uint64) []byte {
	return common.HexToHash(fmt.Sprintf("%064x", n)).Bytes()
}

func assertChainFreezerTxInfo(t *testing.T, phase string, info *corepb.TransactionInfo, number uint64) {
	t.Helper()
	if info == nil {
		t.Fatalf("%s transaction info = nil, want fee 777 with receipt", phase)
	}
	if info.Fee != 777 || info.BlockNumber != int64(number) || info.BlockTimeStamp != int64(30_000+number) {
		t.Fatalf("%s transaction info = %+v, want fee 777 at block %d timestamp %d", phase, info, number, 30_000+number)
	}
	receipt := info.Receipt
	if receipt == nil {
		t.Fatalf("%s receipt = nil, want archived resource receipt", phase)
	}
	if receipt.EnergyUsage != 45 ||
		receipt.EnergyFee != 678 ||
		receipt.OriginEnergyUsage != 9 ||
		receipt.EnergyUsageTotal != 90 ||
		receipt.NetUsage != 12 ||
		receipt.NetFee != 34 ||
		receipt.Result != corepb.Transaction_Result_SUCCESS ||
		receipt.EnergyPenaltyTotal != 56 {
		t.Fatalf("%s receipt = %+v, want resource receipt fields preserved", phase, receipt)
	}
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

func chainFreezerAncientRows(t *testing.T, f *rawdbfreezer.Freezer, table string, from, to uint64) [][]byte {
	t.Helper()
	out := make([][]byte, 0, to-from+1)
	for n := from; n <= to; n++ {
		row, err := f.Ancient(table, n)
		if err != nil {
			t.Fatalf("Ancient(%s,%d): %v", table, n, err)
		}
		out = append(out, row)
		if n == to {
			break
		}
	}
	return out
}

func byteRowsEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
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
