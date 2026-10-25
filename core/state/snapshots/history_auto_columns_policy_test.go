package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

func autoColumnPhysicalVersion(t testing.TB, dir string, refs []SegmentRef) uint32 {
	t.Helper()
	for _, ref := range refs {
		if ref.Kind != SegmentHistory {
			continue
		}
		f, err := os.Open(filepath.Join(dir, ref.Path))
		if err != nil {
			t.Fatal(err)
		}
		var header [16]byte
		_, err = io.ReadFull(f, header[:])
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			t.Fatal(err, closeErr)
		}
		return binary.BigEndian.Uint32(header[8:12])
	}
	t.Fatal("missing history")
	return 0
}

func TestAutoColumnsOpaqueCDCAndOrdinaryBuild(t *testing.T) {
	// A repeat of opaque large bytes exposes forced V5's lost CDC benefit.
	// The policy must observe keys/lengths without interpreting or owning Prev.
	for _, large := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "opaque-large"}[large], func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			defer db.Close()
			prev := make([]byte, 64)
			if large {
				prev = make([]byte, 512<<10)
			}
			rand.New(rand.NewSource(20261026)).Read(prev)
			for block := uint64(1); block <= 16; block++ {
				c := compressionPolicyChange(prev)
				c.BlockNum, c.TxNum, c.Seq = block, block, 1
				c.BlockHash = common.Hash{byte(block)}
				if err := rawdb.WriteStateTxRange(db, block, c.BlockHash, block, block); err != nil {
					t.Fatal(err)
				}
				if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{&c}); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			want, err := DigestHotStateHistoryCompatibilityContext(ctx, db, t.TempDir(), 1, 16, 1, 16, 16<<20)
			if err != nil {
				t.Fatal(err)
			}
			cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
			var ordinaryBytes, columnsBytes uint64
			for _, format := range []string{"auto", historyCompressionAutoColumns} {
				dir := t.TempDir()
				refs, err := BuildDiagnosticStateHistoryTrioContext(ctx, db, dir, 1, 16, 1, 16, cfg.HistoryPath(1, 16), format)
				if err != nil {
					t.Fatal(err)
				}
				got, err := DigestColdStateHistoryContext(ctx, dir, refs)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatal("independent hot/cold oracle", err, got, want)
				}
				version := compressedBlockFooterVersion
				if format == historyCompressionAutoColumns {
					version = compressedBlockRecordColumnsVersion
				}
				if large {
					version = compressedBlockCDCVersion
				}
				if got := autoColumnPhysicalVersion(t, dir, refs); got != version {
					t.Fatalf("format=%s physical=%d want=%d", format, got, version)
				}
				var size uint64
				for _, ref := range refs {
					size += ref.Size
				}
				if format == "auto" {
					ordinaryBytes = size
				} else {
					columnsBytes = size
				}
			}
			if large && columnsBytes != ordinaryBytes {
				t.Fatalf("same CDC writer changed size: %d != %d", columnsBytes, ordinaryBytes)
			}
		})
	}
}

func TestAutoColumnsUnorderedFallbackKeepsResolvedPolicy(t *testing.T) {
	for _, large := range []bool{false, true} {
		t.Run(map[bool]string{false: "columns", true: "cdc"}[large], func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			defer db.Close()
			dir := t.TempDir()
			prev := make([]byte, 64)
			if large {
				prev = make([]byte, 512<<10)
			}
			rand.New(rand.NewSource(57)).Read(prev)
			var changes []*rawdb.StateDomainChange
			for i, tx := range []uint64{11, 10} {
				c := compressionPolicyChange(prev)
				c.BlockNum, c.BlockHash, c.TxNum, c.Seq = 1, common.Hash{1}, tx, uint64(i+1)
				changes = append(changes, &c)
				if err := rawdb.WriteStateDomainChange(db, &c); err != nil {
					t.Fatal(err)
				}
			}
			if err := rawdb.WriteStateTxRange(db, 1, common.Hash{1}, 10, 11); err != nil {
				t.Fatal(err)
			}
			cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
			cfg.IterateHotHistoryBlockTxBorrowed = nil
			built, err := buildStateDomainChangeHistoryBinarySegmentsFromDBRangeContextFormat(context.Background(), db, dir,
				SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 10, ToTxNum: 11, Path: cfg.HistoryPath(10, 11)}, cfg,
				etl.Options{TempDir: filepath.Join(dir, "etl"), BufferLimit: 1}, &stateDomainChangeHistoryBlockRange{from: 1, to: 1}, historyCompressionAutoColumns)
			if err != nil || built.recordETL.Collected != 2 || built.recordETL.SpilledRuns == 0 {
				t.Fatal("real ETL fallback not exercised", built.recordETL, err)
			}
			version := compressedBlockRecordColumnsVersion
			if large {
				version = compressedBlockCDCVersion
			}
			if got := autoColumnPhysicalVersion(t, dir, built.refs); got != version {
				t.Fatalf("fallback physical %d want %d", got, version)
			}
			want, _, _, err := encodeStateDomainChangeBinarySegmentV6(10, 11, normalizeStateDomainChangesForBinary(changes))
			if err != nil {
				t.Fatal(err)
			}
			r, n, _, err := openHistorySegmentForRead(dir, built.refs[0])
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(io.NewSectionReader(r, 0, int64(n)))
			r.Close()
			if err != nil || !bytes.Equal(got, want) {
				t.Fatal("fallback changed canonical bytes", err)
			}
			if _, err := VerifyLoadedManifestFiles(dir, NewManifest(10, 11, built.refs), VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAutoColumnsMixedMergeAndRestore(t *testing.T) {
	for _, formats := range [][2]string{{"2", "5"}, {"5", "3"}, {"3", "5"}} {
		t.Run(formats[0]+"+"+formats[1], func(t *testing.T) {
			t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", historyCompressionAutoColumns)
			db := rawdb.NewMemoryDatabase()
			defer db.Close()
			rows := v6StreamChanges(1, 512, 1, 80)
			for _, c := range rows {
				if err := rawdb.WriteStateTxRange(db, c.BlockNum, c.BlockHash, c.TxNum, c.TxNum); err != nil {
					t.Fatal(err)
				}
				if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{c}); err != nil {
					t.Fatal(err)
				}
			}
			ctx, dir := context.Background(), t.TempDir()
			cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
			var refs []SegmentRef
			for i, from := range []uint64{1, 257} {
				part, err := BuildDiagnosticStateHistoryTrioContext(ctx, db, dir, from, from+255, from, from+255, cfg.HistoryPath(from, from+255), formats[i])
				if err != nil {
					t.Fatal(err)
				}
				for _, ref := range part {
					if ref.Kind == SegmentHistory {
						current, err := historyUsesCurrentCompression(filepath.Join(dir, ref.Path))
						if err != nil || !current {
							t.Fatal("auto policy requested an unnecessary existing-file rewrite", err)
						}
					}
				}
				refs = append(refs, part...)
			}
			if err := PublishManifest(dir, NewManifest(1, 512, refs)); err != nil {
				t.Fatal(err)
			}
			merged, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
			if err != nil || !merged.Merged {
				t.Fatal("actual mixed merge", err)
			}
			version := compressedBlockRecordColumnsVersion
			if formats[0] == "3" || formats[1] == "3" {
				version = compressedBlockCDCVersion
			}
			if got := autoColumnPhysicalVersion(t, dir, merged.Segments); got != version {
				t.Fatalf("merged physical %d want %d", got, version)
			}
			want, err := DigestHotStateHistoryCompatibilityContext(ctx, db, t.TempDir(), 1, 512, 1, 512, 16<<20)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DigestColdStateHistoryContext(ctx, dir, merged.Segments)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatal("merged oracle mismatch", err)
			}
			mgr, err := OpenManager(dir)
			if err != nil {
				t.Fatal(err)
			}
			restored := rawdb.NewMemoryDatabase()
			defer restored.Close()
			stats, err := mgr.RestoreStateDomainHistory(restored, 1, 512)
			if err != nil || stats.ChangesRestored != 512 || stats.TxRangesRestored != 512 {
				t.Fatal("actual mixed history restore", stats, err)
			}
			actual, err := DigestHotStateHistoryCompatibilityContext(ctx, restored, t.TempDir(), 1, 512, 1, 512, 16<<20)
			if err != nil || !reflect.DeepEqual(actual, want) {
				t.Fatal("restored rows differ", err)
			}
		})
	}
}

func TestAutoColumnsLegacyFactoryAndMigrationCompatibility(t *testing.T) {
	raw := bytes.Repeat([]byte("opaque unannotated legacy bytes"), 10000)
	for _, format := range []string{"auto", historyCompressionAutoColumns, "2", "3", "4", "5"} {
		t.Run(format, func(t *testing.T) {
			dir := t.TempDir()
			w, err := newHistoryCompressedStreamFormat(context.Background(), dir, historyCompressChunkSize, 1, format)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			if _, err := w.Write(raw); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "legacy.seg")
			if err := w.Finish(path); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if (format == "auto" || format == historyCompressionAutoColumns) && binary.BigEndian.Uint32(data[8:12]) != compressedBlockFooterVersion {
				t.Fatal("unresolved legacy stream emitted record columns without row evidence")
			}
			got, err := decompressBlockBlob(data)
			if err != nil || !bytes.Equal(got, raw) {
				t.Fatal("legacy canonical bytes changed", err)
			}
			t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "auto")
			before, err := historyUsesCurrentCompression(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", historyCompressionAutoColumns)
			after, err := historyUsesCurrentCompression(path)
			if err != nil || after != before {
				t.Fatal("emission option changed existing-file migration eligibility", before, after, err)
			}
		})
	}
}

func TestAutoColumnsGenericCompressionFooterAdmission(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", historyCompressionAutoColumns)
	data := bytes.Repeat([]byte("legacy blob with no V6 record annotations"), 4000)
	dir := t.TempDir()
	path := filepath.Join(dir, "generic.seg")
	if err := compressBlobToFile(dir, path, data, historyCompressChunkSize); err != nil {
		t.Fatal("generic footer gate rejected runtime policy", err)
	}
	blob, err := os.ReadFile(path)
	if err != nil || binary.BigEndian.Uint32(blob[8:12]) != compressedBlockFooterVersion {
		t.Fatal("generic fallback changed", err)
	}
	got, err := decompressBlockBlob(blob)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("generic compression canonical bytes", err)
	}
}
