package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestHistoryReferenceCompactionBudgetBoundaries(t *testing.T) {
	const prefix = uint64(stateDomainChangeBinaryHeaderSize + stateDomainChangeBinaryV6DictionaryCommitmentSize + 8)
	for _, tc := range []struct {
		name   string
		input  historyReferenceCompactionInputBudget
		reason string
	}{
		{"small", historyReferenceCompactionInputBudget{HasReference: true, Records: 1, LogicalBytes: 100, ChunksUpper: 1, SpansUpper: 1, RawUpper: 100}, ""},
		{"exact-chunks", historyReferenceCompactionInputBudget{HasReference: true, ChunksUpper: historyReferenceMaxChunks - 1}, ""},
		{"chunks-over", historyReferenceCompactionInputBudget{HasReference: true, ChunksUpper: historyReferenceMaxChunks}, "reference-chunk-bound"},
		{"spans-over", historyReferenceCompactionInputBudget{HasReference: true, SpansUpper: historyReferenceMaxSpans}, "reference-span-bound"},
		{"exact-metadata", historyReferenceCompactionInputBudget{HasReference: true, ChunksUpper: 1, SpansUpper: (historyReferenceMaxMetadata-2*historyReferenceChunkEntrySize)/historyReferenceSpanEntrySize - 1}, ""},
		{"metadata-over", historyReferenceCompactionInputBudget{HasReference: true, ChunksUpper: 1, SpansUpper: (historyReferenceMaxMetadata - 2*historyReferenceChunkEntrySize) / historyReferenceSpanEntrySize}, "reference-metadata-bound"},
		{"raw-over", historyReferenceCompactionInputBudget{HasReference: true, RawUpper: historyReferenceMaxPhysical - historyReferenceMaxMetadata - historyReferenceHeaderSize}, "reference-raw-bound"},
		{"records-over", historyReferenceCompactionInputBudget{HasReference: true, Records: math.MaxUint32 + 1}, "reference-record-count"},
		{"converted-source", historyReferenceCompactionInputBudget{NeedsConversionBound: true, ConvertedChunksUpper: historyReferenceMaxChunks + 1}, "reference-source-conversion-bound"},
		{"overflow", historyReferenceCompactionInputBudget{HasReference: true, LogicalBytes: math.MaxUint64, Records: 1}, "reference-estimate-overflow"},
		{"source-overflow", historyReferenceCompactionInputBudget{HasReference: true, Overflow: true}, "reference-estimate-overflow"},
		// The sequential tx table can exceed the hot writer's random-patch
		// limit. Merge only patches the fixed header/count; it must still fit.
		{"large-sequential-table", historyReferenceCompactionInputBudget{HasReference: true, LogicalBytes: historyReferenceMaxPrefix + 1}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := estimateHistoryReferenceCompactionBudget([]historyReferenceCompactionInputBudget{tc.input})
			if got.Reason != tc.reason || got.Deferred != (tc.reason != "") || got.HasReference != tc.input.HasReference || got.PatchPrefixBytes != prefix {
				t.Fatalf("budget %+v, want reason %q prefix%d", got, tc.reason, prefix)
			}
			if tc.name == "exact-metadata" && got.MetadataBytesUpper != historyReferenceMaxMetadata {
				t.Fatal("boundary fixture did not reach limit", got)
			}
			if tc.name == "exact-chunks" && got.ChunksUpper != historyReferenceMaxChunks {
				t.Fatal("boundary fixture did not reach chunk limit", got)
			}
		})
	}
	if r := estimateHistoryReferenceCompactionBudget(nil); r.Reason != "reference-source-count" {
		t.Fatal("empty input", r)
	}
	inputs := make([]historyReferenceCompactionInputBudget, historyReferenceMergeMaxSources+1)
	if r := estimateHistoryReferenceCompactionBudget(inputs); r.Reason != "reference-source-count" {
		t.Fatal("too many sources", r)
	}
}

func TestHistoryReferenceCompactionBudgetGroupsAndOversizedBoundary(t *testing.T) {
	leaf := historyReferenceCompactionInputBudget{HasReference: true, Records: 12, LogicalBytes: 4096, ChunksUpper: 8, SpansUpper: 20, RawUpper: 32768}
	oversized := leaf
	oversized.ChunksUpper = historyReferenceMaxChunks
	if r := estimateHistoryReferenceCompactionBudget([]historyReferenceCompactionInputBudget{oversized}); !r.Deferred {
		t.Fatal("oversized leaf not a boundary")
	}
	if r := estimateHistoryReferenceCompactionBudget([]historyReferenceCompactionInputBudget{leaf, leaf}); r.Deferred || r.Sources != 2 || r.Records != 24 {
		t.Fatal("later independent leaves inherit an earlier failure", r)
	}
	// No dedup assumption is permitted across individually fitting sources.
	large := leaf
	large.ChunksUpper = historyReferenceMaxChunks/2 + 1
	if r := estimateHistoryReferenceCompactionBudget([]historyReferenceCompactionInputBudget{large}); r.Deferred {
		t.Fatal("single fixture must fit", r)
	}
	if r := estimateHistoryReferenceCompactionBudget([]historyReferenceCompactionInputBudget{large, large}); r.Reason != "reference-chunk-bound" {
		t.Fatal("combined capacity ignored", r)
	}
}

// These files deliberately have invalid payloads. The admission reader must
// get counts from physical metadata without decoding a block or pretending it
// has authenticated the input. Ordinary compaction remains responsible for it.
func referenceBudgetMetadataFixture(t *testing.T, format string, logical, records uint64) (string, historyCompactionCandidate) {
	t.Helper()
	dir := t.TempDir()
	h := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 1, ToTxNum: 2, Path: "history-1-2.seg"}
	a := h
	a.Kind, a.Path = SegmentAccessor, "history-1-2.hidx"
	var ah bytes.Buffer
	writeStateDomainChangeBinaryHeaderVersion(&ah, stateDomainChangeBinaryAccessorMagic, 1, 2, records, stateDomainChangeBinaryVersionV7)
	if err := os.WriteFile(filepath.Join(dir, a.Path), ah.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	a.Size, a.Checksum = uint64(ah.Len()), checksumBytes(ah.Bytes())
	var data []byte
	switch format {
	case "r1":
		head := historyReferenceHeader{logical: logical, physical: historyReferenceHeaderSize + historyReferenceChunkEntrySize + historyReferenceSpanEntrySize,
			chunkDir: historyReferenceHeaderSize, chunks: 1, spanDir: historyReferenceHeaderSize + historyReferenceChunkEntrySize, spans: 1}
		encoded := head.encode()
		data = make([]byte, head.physical)
		copy(data, encoded[:])
	case "v2":
		// V2's reserved logical-size header is zero; only its footer is valid.
		data = make([]byte, compressedBlockHeaderSize+1+compressedBlockTableEntry+compressedBlockTrailerSize)
		copy(data, compressedBlockMagic)
		binary.BigEndian.PutUint32(data[8:12], compressedBlockFooterVersion)
		binary.BigEndian.PutUint32(data[12:16], 1)
		data[compressedBlockHeaderSize] = 0xff // not a zstd frame
		trailer := data[len(data)-compressedBlockTrailerSize:]
		copy(trailer, compressedBlockFooterMagic)
		binary.BigEndian.PutUint64(trailer[8:16], records)
		binary.BigEndian.PutUint64(trailer[16:24], 1)
		binary.BigEndian.PutUint64(trailer[24:32], logical)
		binary.BigEndian.PutUint64(trailer[32:40], compressedBlockHeaderSize+1)
		binary.BigEndian.PutUint64(trailer[40:48], compressedBlockTableEntry)
	default:
		t.Fatal("bad fixture format")
	}
	if err := os.WriteFile(filepath.Join(dir, h.Path), data, 0600); err != nil {
		t.Fatal(err)
	}
	h.Size, h.Checksum = uint64(len(data)), checksumBytes(data)
	return dir, historyCompactionCandidate{history: h, companions: []SegmentRef{a}}
}

func TestHistoryReferenceCompactionBudgetDoesNotDecodePayload(t *testing.T) {
	for _, format := range []string{"r1", "v2"} {
		t.Run(format, func(t *testing.T) {
			dir, candidate := referenceBudgetMetadataFixture(t, format, 40<<20, 10)
			before := referenceMigrationFiles(t, dir)
			input, err := readHistoryReferenceCompactionInputBudget(context.Background(), dir, candidate)
			if err != nil || input.LogicalBytes != 40<<20 || input.Records != 10 || input.HasReference != (format == "r1") {
				t.Fatalf("metadata-only read: %+v %v", input, err)
			}
			if format == "v2" && input.ConvertedChunksUpper != 320 {
				t.Fatal("did not derive conversion pages from footer logical bytes", input)
			}
			if !reflect.DeepEqual(before, referenceMigrationFiles(t, dir)) {
				t.Fatal("admission changed source files")
			}
		})
	}
}

func TestHistoryReferenceCompactionBudgetRejectsMetadataFaults(t *testing.T) {
	for _, fault := range []string{"cancel", "missing", "history-size", "accessor-size", "accessor-range", "no-accessor", "footer", "magic"} {
		t.Run(fault, func(t *testing.T) {
			dir, candidate := referenceBudgetMetadataFixture(t, "v2", 1024, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch fault {
			case "cancel":
				cancel()
			case "missing":
				if err := os.Remove(filepath.Join(dir, candidate.history.Path)); err != nil {
					t.Fatal(err)
				}
			case "history-size":
				candidate.history.Size++
			case "accessor-size":
				candidate.companions[0].Size++
			case "accessor-range":
				candidate.companions[0].ToTxNum++
			case "no-accessor":
				candidate.companions = nil
			case "footer", "magic":
				data, err := os.ReadFile(filepath.Join(dir, candidate.history.Path))
				if err != nil {
					t.Fatal(err)
				}
				at := 0
				if fault == "footer" {
					at = len(data) - compressedBlockTrailerSize
				}
				data[at] ^= 1
				if err = os.WriteFile(filepath.Join(dir, candidate.history.Path), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := readHistoryReferenceCompactionInputBudget(ctx, dir, candidate); err == nil || fault == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal("input fault lost", err)
			}
		})
	}
}

func TestHistoryReferenceCompactionBudgetBoundsCompleteMerge(t *testing.T) {
	for _, mode := range []string{"r1", "raw", "codec1", "codec2", "codec3", "legacy-v5", "legacy-v2"} {
		t.Run(mode, func(t *testing.T) {
			dir, cfg, selection, sources := referenceMergeFixture(t, mode)
			budget, err := readHistoryReferenceCompactionBudget(context.Background(), dir, selection.candidates)
			if err != nil || budget.Deferred || !budget.HasReference {
				t.Fatalf("real small sources deferred: %+v %v", budget, err)
			}
			refs, err := compactStateDomainChangeReferenceHistoryRunContext(context.Background(), dir, cfg, selection, sources, nil)
			if err != nil {
				t.Fatal(err)
			}
			ref := referenceBuildHistoryRef(t, refs)
			r, err := openHistoryReferenceReader(context.Background(), filepath.Join(dir, ref.Path), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			var raw uint64
			for i := uint64(0); i < r.header.chunks; i++ {
				c, err := r.chunk(context.Background(), uint32(i))
				if err != nil {
					t.Fatal(err)
				}
				raw += uint64(c.raw)
			}
			metadata := r.header.chunks*historyReferenceChunkEntrySize + r.header.spans*historyReferenceSpanEntrySize
			if r.header.chunks > budget.ChunksUpper || r.header.spans > budget.SpansUpper || metadata > budget.MetadataBytesUpper || r.header.logical > budget.LogicalBytesUpper || raw > budget.RawBytesUpper {
				t.Fatalf("actual output exceeds conservative bounds: header%+v raw%d budget%+v", r.header, raw, budget)
			}
		})
	}
}
