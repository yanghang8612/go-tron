package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestBudgetedHistoryCompactionLimitsLogicalBytes(t *testing.T) {
	candidates, read := budgetTestLeaves(
		historyCompactionInputCost{bytes: 10, records: 1, logicalBytes: 3 << 30},
		historyCompactionInputCost{bytes: 10, records: 1, logicalBytes: 768 << 20},
		historyCompactionInputCost{bytes: 10, records: 1, logicalBytes: 768 << 20},
		historyCompactionInputCost{bytes: 10, records: 1, logicalBytes: 768 << 20},
	)
	cfg := CompactionConfig{MaxInputBytes: 512 << 20, MaxInputRecords: 8_000_000, MaxInputLogicalBytes: 2 << 30, MaxSources: 16}
	selection, ok, err := selectBudgetedHistoryCompactionLeaves(context.Background(), candidates, cfg, read)
	if err != nil || !ok || selection.fromTxNum != 2 || selection.toTxNum != 3 || selection.inputLogicalBytes != 1536<<20 || selection.inputBytes != 20 || selection.inputRecords != 2 {
		t.Fatalf("high-ratio selection=%+v ok=%v err=%v", selection, ok, err)
	}
}

type compactionMetadataReadCounter struct {
	*bytes.Reader
	readBytes int
}

func (r *compactionMetadataReadCounter) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.Reader.ReadAt(p, off)
	r.readBytes += n
	return n, err
}

func TestHistoryCompactionLogicalMetadataAcrossContainers(t *testing.T) {
	const payloadSize = 2 << 20
	payload := bytes.Repeat([]byte{7}, payloadSize)
	for _, format := range []string{"1", "2", "3"} {
		t.Run(format, func(t *testing.T) {
			dir := t.TempDir()
			writer, err := newHistoryCompressedStreamFormat(context.Background(), dir, historyCompressChunkSize, 1, format)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Abort()
			if _, err := writer.Write(payload); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "test.seg")
			if _, err := writer.FinishWithMetadataContext(context.Background(), path); err != nil {
				t.Fatal(err)
			}
			encoded, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			reader := &compactionMetadataReadCounter{Reader: bytes.NewReader(encoded)}
			logical, err := readHistoryCompactionContainerLogicalBytes(reader, uint64(len(encoded)), encoded[:compressedBlockHeaderSize])
			if err != nil || logical != payloadSize {
				t.Fatalf("logical=%d err=%v", logical, err)
			}
			// The repeated 2 MiB payload requires only a footer/sparse directory
			// here; table or body scans would substantially exceed this bound.
			if reader.readBytes > 256 {
				t.Fatalf("metadata lookup read %d bytes", reader.readBytes)
			}
			if len(encoded) >= payloadSize/4 {
				t.Fatalf("test lacks high compression: %d bytes", len(encoded))
			}
			got, err := readHistoryCompactionLogicalBytes(context.Background(), dir, SegmentRef{Path: "test.seg", Size: uint64(len(encoded))})
			if err != nil || got != logical {
				t.Fatalf("file metadata=%d err=%v", got, err)
			}
		})
	}
	// An uncompressed body is measured physically, including its fixed header.
	dir := t.TempDir()
	raw := append(append([]byte{}, stateDomainChangeBinarySegmentMagic[:]...), payload...)
	if err := os.WriteFile(filepath.Join(dir, "raw.seg"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if got, err := readHistoryCompactionLogicalBytes(context.Background(), dir, SegmentRef{Path: "raw.seg"}); err != nil || got != uint64(len(raw)) {
		t.Fatalf("raw logical=%d err=%v", got, err)
	}
}

func TestHistoryCompactionLogicalMetadataRejectsBrokenBounds(t *testing.T) {
	header := make([]byte, compressedBlockHeaderSize)
	copy(header, compressedBlockMagic)
	binary.BigEndian.PutUint32(header[8:12], compressedBlockVersion)
	binary.BigEndian.PutUint32(header[12:16], 128<<10)
	binary.BigEndian.PutUint64(header[24:32], 1000)
	binary.BigEndian.PutUint64(header[32:40], 3<<30)
	binary.BigEndian.PutUint64(header[40:48], compressedBlockHeaderSize)
	if _, err := readHistoryCompactionContainerLogicalBytes(bytes.NewReader(header), uint64(len(header)), header); err == nil {
		t.Fatal("accepted table outside physical file")
	}
}

func TestBudgetedHistoryCompactionRejectsRealHighlyCompressedValues(t *testing.T) {
	dir := t.TempDir()
	var refs []SegmentRef
	for i := uint64(1); i <= 2; i++ {
		change := binaryStateDomainChange(i, i, 1, "delegation")
		change.PrevExists = true
		change.Prev = bytes.Repeat([]byte{7}, 1<<20)
		history, index, accessor, err := writeHistorySegmentFiles(dir, SegmentRef{
			Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: i, ToTxNum: i,
			Path: stateDomainChangeHistorySegmentPath(i, i),
		}, []*rawdb.StateDomainChange{change})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, history, accessor, index)
	}
	if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
		t.Fatal(err)
	}
	cfg := CompactionConfig{BusyLeafOnly: true, MaxSources: 2, MaxInputBytes: 512 << 20, MaxInputRecords: 8_000_000, MaxInputLogicalBytes: 2 << 20}
	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, cfg)
	if err != nil || result.Merged {
		t.Fatalf("ignored logical overhead/value budget: result=%+v err=%v", result, err)
	}
	cfg.MaxInputLogicalBytes = 3 << 20
	result, err = CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, cfg)
	if err != nil || !result.Merged || result.InputLogicalBytes <= 2<<20 || result.InputLogicalBytes >= 3<<20 || result.InputBytes >= 1<<20 || result.InputRecords != 2 {
		t.Fatalf("logical/physical accounting: result=%+v err=%v", result, err)
	}
}

func TestPendingBusyCompactionGetsOneIndependentPriority(t *testing.T) {
	dir := t.TempDir()
	refs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, binaryStateDomainChange(2, 2, 1, "b"))...)
	if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
		t.Fatal(err)
	}
	gate := maintenance.NewHeavyWorkGate()
	r := &Runner{chain: &coldBuilderChain{syncRemainingOK: true}, cfg: Config{Enabled: true, Dir: dir, HistoryDataset: SegmentDatasetStateDomainChange,
		HistoryCatchupMode: HistoryCatchupThroughput, CompactMaxSteps: 2, HeavyWorkGate: gate}}
	historyDeadline := time.Now().Add(time.Minute).UnixNano()
	r.historyNotBefore.Store(historyDeadline)
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("could not reserve gate")
	}
	result, err := r.compactHistory(context.Background(), true)
	if err != nil || result.DeferReason != "heavy-work-gate" || result.RetryDeadline.IsZero() || !r.compactionBudget.pending {
		t.Fatalf("pending=%v result=%+v err=%v", r.compactionBudget.pending, result, err)
	}
	if r.shouldPrioritizePendingCompaction(result.RetryDeadline.Add(-time.Nanosecond)) {
		t.Fatal("priority ignored independent retry deadline")
	}
	r.historyLoad.hard = true
	if r.shouldPrioritizePendingCompaction(result.RetryDeadline) || !r.compactionBudget.pending {
		t.Fatal("pressure consumed or admitted the pending priority")
	}
	r.historyLoad.hard = false
	if !r.shouldPrioritizePendingCompaction(result.RetryDeadline) || r.shouldPrioritizePendingCompaction(result.RetryDeadline) {
		t.Fatal("pending merge priority must be consumed exactly once")
	}
	if r.historyNotBefore.Load() != historyDeadline {
		t.Fatal("priority changed history admission deadline")
	}
	release()
	result, err = r.compactHistory(context.Background(), true)
	if err != nil || !result.Merged || r.compactionBudget.pending || result.RetryDeadline.IsZero() {
		t.Fatalf("priority result=%+v pending=%v err=%v", result, r.compactionBudget.pending, err)
	}
	if r.historyNotBefore.Load() != historyDeadline {
		t.Fatal("merge changed history admission deadline")
	}
}

func TestSmallLeafTailDoesNotArmEmptyMergePriority(t *testing.T) {
	dir := t.TempDir()
	refs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, binaryStateDomainChange(2, 2, 1, "b"))...)
	if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
		t.Fatal(err)
	}
	// Readiness may inspect the manifest but must not open inputs without a
	// lease. Removing this file makes any eager stat/header read fail the test.
	if err := os.Remove(filepath.Join(dir, refs[0].Path)); err != nil {
		t.Fatal(err)
	}
	gate := maintenance.NewHeavyWorkGate()
	release, _ := gate.TryAcquire()
	defer release()
	r := &Runner{chain: &coldBuilderChain{syncRemainingOK: true}, cfg: Config{Enabled: true, Dir: dir, HistoryDataset: SegmentDatasetStateDomainChange,
		HistoryCatchupMode: HistoryCatchupThroughput, CompactMaxSteps: 256, HeavyWorkGate: gate}}
	result, err := r.compactHistory(context.Background(), true)
	if err != nil || result.DeferReason != "leaf-target" || !result.RetryDeadline.IsZero() || r.compactionBudget.pending {
		t.Fatalf("small tail armed priority: result=%+v pending=%v err=%v", result, r.compactionBudget.pending, err)
	}
}

func TestEmptyBusyCompactionUsesIndependentRetry(t *testing.T) {
	dir := t.TempDir()
	refs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, binaryStateDomainChange(2, 2, 1, "b"))...)
	if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
		t.Fatal(err)
	}
	r := &Runner{chain: &coldBuilderChain{syncRemainingOK: true}, cfg: Config{Enabled: true, Dir: dir, HistoryDataset: SegmentDatasetStateDomainChange,
		HistoryCatchupMode: HistoryCatchupThroughput, CompactMaxSteps: 256}}
	r.compactionBudget.pending = true
	historyDeadline := time.Now().Add(5 * time.Second).UnixNano()
	r.historyNotBefore.Store(historyDeadline)
	result, err := r.compactHistory(context.Background(), true)
	if err != nil || result.Merged || !result.Deferred || result.Recovery != busyHistoryCompactionNoCandidateRetry || result.RetryDeadline.IsZero() || r.compactionBudget.pending {
		t.Fatalf("empty attempt result=%+v pending=%v err=%v", result, r.compactionBudget.pending, err)
	}
	result, err = r.compactHistory(context.Background(), true)
	if err != nil || result.DeferReason != "merge-recovery" || r.historyNotBefore.Load() != historyDeadline {
		t.Fatalf("empty retry result=%+v history deadline=%d err=%v", result, r.historyNotBefore.Load(), err)
	}
}
