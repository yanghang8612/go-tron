package snapshots

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestInspectHistorySpaceProfilesCompressedV6Trio(t *testing.T) {
	dir := t.TempDir()
	const records = uint64(4096)
	changes := make([]*rawdb.StateDomainChange, 0, records)
	owner := common.Address{0x41, 0x77}
	for txNum := uint64(1); txNum <= records; txNum++ {
		change := binaryStateDomainChange(txNum, txNum, 1, "key-"+string(rune('a'+txNum%32)))
		change.Owner = owner
		change.Generation = 1
		changes = append(changes, change)
	}
	refs := writeCompressedV6HistorySpaceTrio(t, dir, 1, records, changes)
	manifest := NewManifest(1, records, refs)
	manifest.Generation = 77
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}

	manifestBefore, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	filesBefore := make(map[string][]byte, len(refs))
	for _, ref := range refs {
		data, err := os.ReadFile(filepath.Join(dir, ref.Path))
		if err != nil {
			t.Fatal(err)
		}
		filesBefore[ref.Path] = data
	}

	var progress []HistorySpaceInspectProgress
	inspection, err := InspectHistorySpace(dir, HistorySpaceInspectOptions{
		SampleSegments:       1,
		SampleIndexEntries:   1024,
		SampleAccessorBlocks: 16,
		SampleHistoryBytes:   1 << 20,
		ProgressInterval:     time.Nanosecond,
		Progress: func(update HistorySpaceInspectProgress) {
			progress = append(progress, update)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if inspection.ManifestGeneration != 77 || inspection.ActiveHistoryTrios != 1 || inspection.SampledHistoryTrios != 1 {
		t.Fatalf("manifest summary = %+v", inspection)
	}
	var physical HistorySpacePhysicalBytes
	for _, ref := range refs {
		switch ref.Kind {
		case SegmentHistory:
			physical.History += ref.Size
		case SegmentAccessor:
			physical.Accessor += ref.Size
		case SegmentInverted:
			physical.Inverted += ref.Size
		}
		physical.Total += ref.Size
	}
	if inspection.ManifestPhysical != physical {
		t.Fatalf("physical = %+v, want %+v", inspection.ManifestPhysical, physical)
	}
	if inspection.Index.Entries != records || inspection.Index.SampledEntries == 0 {
		t.Fatalf("index = %+v", inspection.Index)
	}
	if inspection.Index.ProjectedBytes >= inspection.Index.CurrentBytes {
		t.Fatalf("delta index projection did not shrink: %+v", inspection.Index)
	}
	if inspection.Accessor.Records != records || inspection.Accessor.Keys == 0 || inspection.Accessor.SampledPostings == 0 {
		t.Fatalf("accessor = %+v", inspection.Accessor)
	}
	if inspection.Accessor.ProjectedBytes >= inspection.Accessor.CurrentBytes {
		t.Fatalf("delta posting projection did not shrink: %+v", inspection.Accessor)
	}
	if inspection.Values.SampledRecords == 0 || inspection.Values.PresentRecords == 0 || inspection.Values.SegmentDuplicateBytes == 0 {
		t.Fatalf("value reuse sample = %+v", inspection.Values)
	}
	if inspection.Values.ContentAddressedBytes >= inspection.Values.CurrentBytes || inspection.Values.SavingsBytes == 0 {
		t.Fatalf("content-addressed value model did not shrink repeated fixture: %+v", inspection.Values)
	}
	if inspection.Values.Domains[rawdb.StateFlatDomainKVLatest.String()].Records == 0 {
		t.Fatalf("value domain sample = %+v", inspection.Values.Domains)
	}
	if inspection.TrainedDictionary.SampledSegments != 1 || inspection.TrainedDictionary.TrainingFailures != 0 || inspection.TrainedDictionary.TrainingRawBytes == 0 || inspection.TrainedDictionary.EvaluationRawBytes == 0 {
		t.Fatalf("trained history dictionary sample = %+v", inspection.TrainedDictionary)
	}
	if inspection.TrainedDictionary.PlainStoredBytes == 0 || inspection.TrainedDictionary.DictionaryStoredBytes == 0 || inspection.TrainedDictionary.DictionaryBytes == 0 || inspection.TrainedDictionary.ProjectedHistoryBytes == 0 {
		t.Fatalf("trained history dictionary projection = %+v", inspection.TrainedDictionary)
	}
	if len(inspection.Candidates) != 1+len(historySpaceBlockSizes) {
		t.Fatalf("candidate count = %d", len(inspection.Candidates))
	}
	if inspection.Candidates[0].EstimatedPhysicalBytes != physical.Total {
		t.Fatalf("current candidate = %+v", inspection.Candidates[0])
	}
	for _, candidate := range inspection.Candidates[1:] {
		if candidate.EstimatedPhysicalBytes == 0 || candidate.ComparedPhysicalBytes != physical.Total {
			t.Fatalf("candidate = %+v", candidate)
		}
		if candidate.MaxTxIndexScanEntries != historySpaceTxIndexFrameEntries || candidate.MaxPostingScanEntries != historySpacePostingFrameEntries {
			t.Fatalf("candidate scan bounds = %+v", candidate)
		}
	}
	if len(inspection.Compression) != len(historySpaceBlockSizes) || inspection.Segments[0].SampledRawBytes == 0 {
		t.Fatalf("compression samples = %+v segment=%+v", inspection.Compression, inspection.Segments[0])
	}
	if len(progress) == 0 || progress[len(progress)-1].Phase != "complete" {
		t.Fatalf("progress = %+v", progress)
	}

	manifestAfter, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	if string(manifestAfter) != string(manifestBefore) {
		t.Fatal("history-space inspection modified manifest")
	}
	for path, before := range filesBefore {
		after, err := os.ReadFile(filepath.Join(dir, path))
		if err != nil {
			t.Fatal(err)
		}
		if string(after) != string(before) {
			t.Fatalf("history-space inspection modified %s", path)
		}
	}
}

func TestInspectHistorySpaceHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := InspectHistorySpaceFromManifest(t.TempDir(), NewManifest(0, 0, nil), HistorySpaceInspectOptions{Context: ctx})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestEvenlySpacedHistoryWindowStartsDoesNotOverlapAlignedFrames(t *testing.T) {
	starts := evenlySpacedHistoryWindowStarts(1025, 8, historySpaceTxIndexFrameEntries)
	seen := make(map[uint64]struct{}, len(starts))
	for _, start := range starts {
		if start%historySpaceTxIndexFrameEntries != 0 && start != 1025-historySpaceTxIndexFrameEntries {
			t.Fatalf("unaligned start %d", start)
		}
		if _, ok := seen[start]; ok {
			t.Fatalf("duplicate start %d in %v", start, starts)
		}
		seen[start] = struct{}{}
	}
}

func TestScaleHistorySpaceSampleUsesWideIntermediate(t *testing.T) {
	// The exact 10 GB projection fits in uint64, but the 100 GB * 100 GB
	// intermediate does not. This shape reproduces the mainnet history sample
	// projection that previously saturated to MaxUint64.
	const (
		value   = uint64(100_000_000_000)
		sampled = uint64(1_000_000_000_000)
		total   = uint64(100_000_000_000)
	)
	if got, want := scaleHistorySpaceSample(value, sampled, total), uint64(10_000_000_000); got != want {
		t.Fatalf("scaled value = %d, want %d", got, want)
	}
}

func TestSaturatingHistorySpaceSum(t *testing.T) {
	if got := saturatingHistorySpaceSum(^uint64(0)-10, 20); got != ^uint64(0) {
		t.Fatalf("overflowing sum = %d, want saturation", got)
	}
	if got := saturatingHistorySpaceSum(10, 20, 30); got != 60 {
		t.Fatalf("ordinary sum = %d, want 60", got)
	}
}

func writeCompressedV6HistorySpaceTrio(t testing.TB, dir string, fromTxNum, toTxNum uint64, changes []*rawdb.StateDomainChange) []SegmentRef {
	t.Helper()
	refs := writeV6StateDomainHistorySegmentForTest(t, dir, fromTxNum, toTxNum, changes)
	for i := range refs {
		if refs[i].Kind != SegmentHistory {
			continue
		}
		path := filepath.Join(dir, refs[i].Path)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		tmp := path + ".compressed"
		if err := compressBlobToFile(dir, tmp, raw, historyCompressChunkSize); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, path); err != nil {
			t.Fatal(err)
		}
		refs[i].Size, refs[i].Checksum, err = stateDomainChangeBinaryFileMetadata(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	return refs
}
