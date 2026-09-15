package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func historyExportOptions(from, to uint64) StateHistoryRangeExportOptions {
	return StateHistoryRangeExportOptions{FromBlock: from, ToBlock: to, MaxBytes: 1 << 30, MaxRows: 262144, MaxDecodedBytes: 4 << 30}
}

func historyExportFixture(t *testing.T, from uint64, count int, shared bool) (*historyViewTestFactory, []*types.Block) {
	t.Helper()
	f := &historyViewTestFactory{KeyValueStore: NewMemoryDatabase()}
	t.Cleanup(func() { _ = f.KeyValueStore.Close() })
	blocks := make([]*types.Block, count)
	rows := chunkHistoryRows(1<<18, count)
	var parent common.Hash
	for i := range blocks {
		height := from + uint64(i)
		block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(height), Timestamp: int64(height), ParentHash: parent[:]}}})
		if err := WriteBlock(f.KeyValueStore, block); err != nil {
			t.Fatal(err)
		}
		if err := WriteStateTxRange(f.KeyValueStore, height, block.Hash(), 100+uint64(i), 100+uint64(i)); err != nil {
			t.Fatal(err)
		}
		row := rows[i]
		row.BlockNum, row.Seq, row.TxNum = height, 1, 100+uint64(i)
		if shared {
			batch := newSharedHistoryTestBatch(f.KeyValueStore)
			if err := writeStateDomainChangeBlockRows(batch, []*StateDomainChange{row}, true, true); err != nil {
				t.Fatal(err)
			}
			if err := batch.batch.Write(); err != nil {
				t.Fatal(err)
			}
			pack, _ := f.KeyValueStore.Get(stateChangeSetKey(height, 0))
			if !isStateHistorySharedPack(pack) {
				t.Fatal("fixture did not select shared encoding")
			}
		} else {
			row.Prev = []byte("old")
			if err := writeStateDomainChangeBlockRows(f.KeyValueStore, []*StateDomainChange{row}, false, false); err != nil {
				t.Fatal(err)
			}
		}
		blocks[i], parent = block, block.Hash()
	}
	return f, blocks
}

func historyExportView(t *testing.T, source any) StateHistoryReadView {
	t.Helper()
	view, release, err := AcquireStateHistoryReadView(source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := release(); err != nil {
			t.Error(err)
		}
	})
	return view
}

func historyExportKVs(t *testing.T, db ethdb.Iteratee) map[string][]byte {
	t.Helper()
	rows := make(map[string][]byte)
	it := db.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		rows[string(it.Key())] = bytes.Clone(it.Value())
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return rows
}

func historyExportLogicalRows(t *testing.T, source ethdb.Iteratee, from, to uint64) []*StateDomainChange {
	t.Helper()
	var rows []*StateDomainChange
	if err := IterateStateDomainChangesByBlockRange(source, from, to, func(row *StateDomainChange) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestStateHistoryRangeExportPhysicalCopyReopensWithRepairsAndEmptyBlock(t *testing.T) {
	f, _ := historyExportFixture(t, 10, 3, true)
	// A block with no mutations legitimately has no sequence-zero pack.
	if err := f.KeyValueStore.Delete(stateChangeSetKey(12, 0)); err != nil {
		t.Fatal(err)
	}
	repair, exists, err := ReadStateDomainChange(f, 10, 1)
	if err != nil || !exists {
		t.Fatalf("read fixture: %v, %v", exists, err)
	}
	repair.Seq, repair.Key, repair.Prev = 9, []byte("repair-key"), []byte("repair-old")
	if err := WriteStateDomainChangeRow(f.KeyValueStore, repair); err != nil {
		t.Fatal(err)
	}
	before := historyExportKVs(t, f.KeyValueStore)
	view := historyExportView(t, f)
	wantLogical := historyExportLogicalRows(t, view, 10, 12)
	path := filepath.Join(t.TempDir(), "copy")
	dst, err := NewPebbleDB(path, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	report, err := ExportStateHistoryRange(context.Background(), view, dst, historyExportOptions(10, 12))
	if closeErr := dst.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || !report.Complete || report.ContentVerified || report.Blocks != 3 || report.FromTxNum != 100 || report.ToTxNum != 102 {
		t.Fatalf("invalid complete report: %+v, %v", report, err)
	}
	if report.UniqueChunkCount == 0 || report.UniqueChunkCount >= report.SharedReferenceCount {
		t.Fatalf("fixture shared dependencies not deduplicated: %d / %d", report.UniqueChunkCount, report.SharedReferenceCount)
	}
	if report.BlockDetails[0].RepairRows != 1 || report.BlockDetails[2].PackPresent {
		t.Fatalf("repairs or empty block lost: %+v", report.BlockDetails)
	}
	reopened, err := NewPebbleDBReadOnly(path, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	copyView := historyExportView(t, reopened)
	gotLogical := historyExportLogicalRows(t, copyView, 10, 12)
	if !reflect.DeepEqual(gotLogical, wantLogical) || len(gotLogical) != 3 {
		t.Fatalf("decoded copy lost real repair rows: got %d, want %d", len(gotLogical), len(wantLogical))
	}
	gotPhysical := historyExportKVs(t, copyView)
	if uint64(len(gotPhysical)) != report.PhysicalRows || len(gotPhysical) != len(report.Entries) {
		t.Fatal("report does not describe actual physical copy")
	}
	var total uint64
	for _, entry := range report.Entries {
		key, _ := hex.DecodeString(entry.KeyHex)
		value, found := gotPhysical[string(key)]
		if !found || !bytes.Equal(value, before[string(key)]) || uint64(len(value)) != entry.ValueBytes {
			t.Fatalf("physical row changed: %s", entry.KeyHex)
		}
		digest := sha256.Sum256(value)
		if hex.EncodeToString(digest[:]) != entry.ValueSHA256 {
			t.Fatal("wrong stored-value fingerprint")
		}
		total += uint64(len(key) + len(value))
	}
	if total != report.PhysicalBytes || !reflect.DeepEqual(before, historyExportKVs(t, f.KeyValueStore)) {
		t.Fatal("incorrect byte accounting or source mutation")
	}
	// Identical physical input produces the same deterministic manifest.
	dst2 := NewMemoryDatabase()
	t.Cleanup(func() { _ = dst2.Close() })
	report2, err := ExportStateHistoryRange(nil, view, dst2, historyExportOptions(10, 12))
	if err != nil || report2.ManifestSHA256 != report.ManifestSHA256 {
		t.Fatal("non-deterministic physical manifest", err)
	}
}

func TestStateHistoryRangeExportRejectsCanonicalAndRangeCorruption(t *testing.T) {
	for _, mode := range []string{"hash", "height", "gap", "parent", "missing-block", "missing-range", "inverted", "empty-header", "malformed-key"} {
		t.Run(mode, func(t *testing.T) {
			f, blocks := historyExportFixture(t, 10, 2, false)
			height := uint64(10)
			switch mode {
			case "hash":
				_ = WriteStateTxRange(f.KeyValueStore, 10, common.Hash{9}, 100, 100)
			case "height":
				raw, _ := rlp.EncodeToBytes(StateTxRange{BlockNum: 9, BlockHash: blocks[0].Hash(), BeginTxNum: 100, EndTxNum: 100})
				_ = f.KeyValueStore.Put(stateTxRangeKey(10), raw)
			case "gap":
				height = 11
				_ = WriteStateTxRange(f.KeyValueStore, 11, blocks[1].Hash(), 102, 102)
			case "parent":
				height = 11
				bad := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 11, ParentHash: make([]byte, 32)}}})
				_ = WriteBlock(f.KeyValueStore, bad)
				_ = WriteStateTxRange(f.KeyValueStore, 11, bad.Hash(), 101, 101)
			case "missing-block":
				_ = f.KeyValueStore.Delete(blockKey(10))
			case "missing-range":
				_ = f.KeyValueStore.Delete(stateTxRangeKey(10))
			case "inverted":
				raw, _ := rlp.EncodeToBytes(StateTxRange{BlockNum: 10, BlockHash: blocks[0].Hash(), BeginTxNum: 100, EndTxNum: 99})
				_ = f.KeyValueStore.Put(stateTxRangeKey(10), raw)
			case "empty-header":
				_ = f.KeyValueStore.Put(blockKey(10), nil)
			case "malformed-key":
				_ = f.KeyValueStore.Put(append(stateChangeSetBlockPrefix(10), 0), []byte{1})
			}
			dst := NewMemoryDatabase()
			t.Cleanup(func() { _ = dst.Close() })
			report, err := ExportStateHistoryRange(nil, historyExportView(t, f), dst, historyExportOptions(10, 11))
			if err == nil || report.Complete || report.Blocks != height-10 || report.StopReason != "error" {
				t.Fatalf("corruption accepted or wrong prefix: blocks %d, complete %v, %v", report.Blocks, report.Complete, err)
			}
		})
	}
}

func TestStateHistoryRangeExportBudgetsAndPin(t *testing.T) {
	f, _ := historyExportFixture(t, 10, 2, false)
	view := historyExportView(t, f)
	for _, mode := range []string{"rows", "bytes", "decoded", "unpinned", "too-many-blocks", "too-many-rows", "too-many-bytes", "too-many-decoded", "zero", "inverted"} {
		t.Run(mode, func(t *testing.T) {
			opts := historyExportOptions(10, 11)
			useView := view
			switch mode {
			case "rows":
				opts.MaxRows = 3 // exactly one raw block, then stop
			case "bytes":
				raw, _ := f.KeyValueStore.Get(blockKey(10))
				opts.MaxBytes = uint64(len(blockKey(10)) + len(raw))
			case "decoded":
				opts.MaxDecodedBytes = 1
			case "unpinned":
				useView = &stateHistoryReadView{reader: f.KeyValueStore, iteratee: f.KeyValueStore}
			case "too-many-blocks":
				opts.ToBlock = 266
			case "too-many-rows":
				opts.MaxRows++
			case "too-many-bytes":
				opts.MaxBytes++
			case "too-many-decoded":
				opts.MaxDecodedBytes++
			case "zero":
				opts.MaxBytes = 0
			case "inverted":
				opts.FromBlock = 12
			}
			dst := NewMemoryDatabase()
			t.Cleanup(func() { _ = dst.Close() })
			report, err := ExportStateHistoryRange(nil, useView, dst, opts)
			if err == nil || report.Complete || report.PhysicalRows > opts.MaxRows || report.PhysicalBytes > opts.MaxBytes || report.DeclaredDecodedBytes > opts.MaxDecodedBytes {
				t.Fatalf("limit failed: %+v, %v", report, err)
			}
			if mode == "rows" || mode == "bytes" || mode == "decoded" {
				if !errors.Is(err, ErrStateHistoryRangeExportBudget) || report.StopReason != "budget" || report.PhysicalRows == 0 {
					t.Fatalf("missing explicit partial budget: %+v, %v", report, err)
				}
			} else if report.PhysicalRows != 0 {
				t.Fatal("invalid options wrote data")
			}
			if mode == "rows" && (report.Blocks != 1 || report.ToTxNum != 100) {
				t.Fatal("completed prefix inaccurate")
			}
			if mode == "unpinned" && !errors.Is(err, ErrStateHistoryReadViewUnpinned) {
				t.Fatal("wrong unpinned error", err)
			}
			if uint64(len(historyExportKVs(t, dst))) != report.PhysicalRows {
				t.Fatal("partial report hides acknowledged writes")
			}
		})
	}
}

type historyExportFaultView struct {
	StateHistoryReadView
	getKey  []byte
	getErr  error
	iterErr error
	gets    int
}

func (v *historyExportFaultView) Get(key []byte) ([]byte, error) {
	v.gets++
	if bytes.Equal(key, v.getKey) {
		return nil, v.getErr
	}
	return v.StateHistoryReadView.Get(key)
}

func (v *historyExportFaultView) NewIterator(prefix, start []byte) ethdb.Iterator {
	if v.iterErr != nil {
		return &stateHistoryErrorIterator{err: v.iterErr}
	}
	return v.StateHistoryReadView.NewIterator(prefix, start)
}

type historyExportFaultWriter struct {
	ethdb.KeyValueWriter
	failAt, puts int
	err          error
	afterPut     func()
}

func (w *historyExportFaultWriter) Put(key, value []byte) error {
	w.puts++
	if w.puts == w.failAt {
		return w.err
	}
	err := w.KeyValueWriter.Put(key, value)
	if w.afterPut != nil {
		w.afterPut()
	}
	return err
}

func TestStateHistoryRangeExportPropagatesErrorsAndCancellation(t *testing.T) {
	f, _ := historyExportFixture(t, 10, 1, false)
	view := historyExportView(t, f)
	injected := errors.New("injected export failure")
	for _, mode := range []string{"get", "iterator", "write", "cancel", "already-canceled"} {
		t.Run(mode, func(t *testing.T) {
			dst := NewMemoryDatabase()
			t.Cleanup(func() { _ = dst.Close() })
			faultView := &historyExportFaultView{StateHistoryReadView: view}
			writer := &historyExportFaultWriter{KeyValueWriter: dst}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wantErr := injected
			switch mode {
			case "get":
				faultView.getKey, faultView.getErr = stateTxRangeKey(10), injected
			case "iterator":
				faultView.iterErr = injected
			case "write":
				writer.failAt, writer.err = 2, injected
			case "cancel":
				writer.afterPut, wantErr = cancel, context.Canceled
			case "already-canceled":
				cancel()
				wantErr = context.Canceled
			}
			report, err := ExportStateHistoryRange(ctx, faultView, writer, historyExportOptions(10, 10))
			if !errors.Is(err, wantErr) || report.Complete || report.Blocks != 0 {
				t.Fatalf("error lost: %+v, %v", report, err)
			}
			if uint64(len(historyExportKVs(t, dst))) != report.PhysicalRows {
				t.Fatal("failed operation counted as copied")
			}
			if mode == "already-canceled" && faultView.gets != 0 {
				t.Fatal("canceled export read source")
			}
		})
	}
}

func TestStateHistoryRangeExportSharedPreflightAndAuthenticationBoundary(t *testing.T) {
	for _, mode := range []string{"missing-chunk", "bad-size", "trailing-ref", "wrong-block", "bad-meta", "missing-meta", "corrupt-snappy-body"} {
		t.Run(mode, func(t *testing.T) {
			f, _ := historyExportFixture(t, 10, 1, true)
			pack, _ := f.KeyValueStore.Get(stateChangeSetKey(10, 0))
			refs, _, _, _, err := sharedStateHistoryPackHeader(pack, 10)
			if err != nil {
				t.Fatal(err)
			}
			size, n := binary.Uvarint(refs)
			var digest [32]byte
			copy(digest[:], refs[n:n+32])
			chunkKey := stateHistoryChunkKey(0, digest)
			switch mode {
			case "missing-chunk":
				_ = f.KeyValueStore.Delete(chunkKey)
			case "bad-size":
				_ = f.KeyValueStore.Put(chunkKey, []byte{1, 0, 1, 9})
			case "trailing-ref":
				_ = f.KeyValueStore.Put(stateChangeSetKey(10, 0), append(pack, 0))
			case "wrong-block":
				pack[len(stateDomainChangeBlockEnvelopeMagic)+1] = 11
				_ = f.KeyValueStore.Put(stateChangeSetKey(10, 0), pack)
			case "bad-meta":
				_ = f.KeyValueStore.Put(stateHistoryChunkBucketKey(0), []byte{9})
			case "missing-meta":
				_ = f.KeyValueStore.Delete(stateHistoryChunkBucketKey(0))
			case "corrupt-snappy-body":
				// Both envelope and Snappy declared lengths pass preflight, but
				// the Snappy body is missing. Capture must not claim full auth.
				value := binary.AppendUvarint([]byte{1, 1}, size)
				value = binary.AppendUvarint(value, size)
				_ = f.KeyValueStore.Put(chunkKey, value)
			}
			dst := NewMemoryDatabase()
			t.Cleanup(func() { _ = dst.Close() })
			report, err := ExportStateHistoryRange(nil, historyExportView(t, f), dst, historyExportOptions(10, 10))
			if mode == "missing-meta" || mode == "corrupt-snappy-body" {
				if err != nil || !report.Complete || report.ContentVerified {
					t.Fatalf("incorrect content verification boundary: %+v, %v", report, err)
				}
				if mode == "missing-meta" && !reflect.DeepEqual(report.MissingBucketMetadata, []uint64{0}) {
					t.Fatal("missing metadata was not disclosed")
				}
				if mode == "corrupt-snappy-body" {
					if _, decodeErr := decodeStateHistorySharedPack(dst, pack, 10); decodeErr == nil {
						t.Fatal("offline full decoder must reject corrupted payload")
					}
				}
			} else if err == nil || report.Complete {
				t.Fatalf("corrupt dependency accepted: %+v, %v", report, err)
			}
			if (mode == "trailing-ref" || mode == "wrong-block") && report.UniqueChunkCount != 0 {
				t.Fatal("malformed table caused dependency reads/copies")
			}
		})
	}
}

func TestStateHistoryRangeExportPreservesBucketBoundariesAndPinnedSequence(t *testing.T) {
	f, _ := historyExportFixture(t, 1023, 2, true)
	before := historyExportKVs(t, f.KeyValueStore)
	f.afterCapture = func() { _ = f.KeyValueStore.Delete(blockKey(1024)) }
	view := historyExportView(t, f)
	dst := NewMemoryDatabase()
	t.Cleanup(func() { _ = dst.Close() })
	report, err := ExportStateHistoryRange(nil, view, dst, historyExportOptions(1023, 1024))
	if err != nil || !report.Complete || f.opened != 1 {
		t.Fatalf("pinned source sequence lost: %+v, %v", report, err)
	}
	for _, bucket := range []uint64{0, 1} {
		if exists, _ := dst.Has(stateHistoryChunkBucketKey(bucket)); !exists {
			t.Fatalf("bucket %d metadata absent", bucket)
		}
	}
	for key, value := range historyExportKVs(t, dst) {
		if !bytes.Equal(value, before[key]) {
			t.Fatal("export used a newer source sequence")
		}
	}
}

func TestStateHistoryRangeExportCompleteDoesNotInventEmptyPack(t *testing.T) {
	f, _ := historyExportFixture(t, 10, 1, false)
	_ = f.KeyValueStore.Delete(stateChangeSetKey(10, 0))
	dst := NewMemoryDatabase()
	t.Cleanup(func() { _ = dst.Close() })
	report, err := ExportStateHistoryRange(nil, historyExportView(t, f), dst, historyExportOptions(10, 10))
	if err != nil || !report.Complete || report.PhysicalRows != 2 || report.BlockDetails[0].PackPresent || report.DeclaredDecodedBytes != 0 {
		t.Fatalf("legitimate no-change block rejected: %+v, %v", report, err)
	}
	for _, entry := range report.Entries {
		if strings.Contains(entry.Family, "changeset") {
			t.Fatal("fabricated empty pack")
		}
	}
}
