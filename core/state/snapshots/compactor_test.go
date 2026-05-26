package snapshots

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestCompactHistoryDomainMergesContinuousBinarySegments(t *testing.T) {
	dir := t.TempDir()
	refs := append([]SegmentRef{},
		writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, binaryStateDomainChange(2, 2, 1, "b"))...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 3, 3, binaryStateDomainChange(3, 3, 1, "c"))...)
	if err := PublishManifest(dir, NewManifest(1, 3, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	oldPaths := segmentPaths(refs)

	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{
		MinSegments:    3,
		DeleteObsolete: true,
	})
	if err != nil {
		t.Fatalf("compact history domain: %v", err)
	}
	if !result.Merged || result.FromTxNum != 1 || result.ToTxNum != 3 || result.SegmentsMerged != 3 {
		t.Fatalf("result = %+v", result)
	}
	historyRef := compactionRefByKind(t, result, SegmentHistory)
	indexRef := compactionRefByKind(t, result, SegmentInverted)
	accessorRef := compactionRefByKind(t, result, SegmentAccessor)
	assertContentAddressedPath(t, historyRef.Path, "history/state-domain-change-1-3.seg", historyRef.Checksum)
	if indexRef.Path != stateDomainChangeBinaryIndexPath(historyRef.Path) {
		t.Fatalf("merged refs = %+v %+v", historyRef, indexRef)
	}
	if accessorRef.Path != stateDomainChangeBinaryAccessorPath(historyRef.Path) {
		t.Fatalf("merged refs = %+v %+v", historyRef, accessorRef)
	}

	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if len(manifest.Segments) != 3 {
		t.Fatalf("manifest segments = %d, want 3: %+v", len(manifest.Segments), manifest.Segments)
	}
	manifestHistoryRef := assertSegmentRef(t, manifest, SegmentDatasetStateDomainChange, 0, SegmentHistory)
	manifestAccessorRef := assertSegmentRef(t, manifest, SegmentDatasetStateDomainChange, 0, SegmentAccessor)
	manifestIndexRef := assertSegmentRef(t, manifest, SegmentDatasetStateDomainChange, 0, SegmentInverted)
	if manifestHistoryRef.FromTxNum != 1 || manifestHistoryRef.ToTxNum != 3 || manifestHistoryRef.Path != historyRef.Path {
		t.Fatalf("history ref = %+v", manifestHistoryRef)
	}
	if manifestAccessorRef.FromTxNum != 1 || manifestAccessorRef.ToTxNum != 3 || manifestAccessorRef.Path != accessorRef.Path {
		t.Fatalf("accessor ref = %+v", manifestAccessorRef)
	}
	if manifestIndexRef.FromTxNum != 1 || manifestIndexRef.ToTxNum != 3 || manifestIndexRef.Path != indexRef.Path {
		t.Fatalf("index ref = %+v", manifestIndexRef)
	}

	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	var got []*rawdb.StateDomainChange
	if err := mgr.IterateStateDomainChanges(1, 3, func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate state-domain-change history: %v", err)
	}
	assertBinaryChangeOrder(t, got, []binaryChangeOrder{
		{txNum: 1, seq: 1, key: "a"},
		{txNum: 2, seq: 1, key: "b"},
		{txNum: 3, seq: 1, key: "c"},
	})

	assertFileExists(t, filepath.Join(dir, historyRef.Path))
	assertFileExists(t, filepath.Join(dir, accessorRef.Path))
	assertFileExists(t, filepath.Join(dir, indexRef.Path))
	for _, path := range oldPaths {
		assertFileMissing(t, filepath.Join(dir, path))
	}
}

func TestCompactHistoryDomainReturnsGenericResult(t *testing.T) {
	dir := t.TempDir()
	refs := append([]SegmentRef{},
		writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, binaryStateDomainChange(2, 2, 1, "b"))...)
	if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{MinSegments: 2})
	if err != nil {
		t.Fatalf("compact generic history domain: %v", err)
	}
	if !result.Merged || result.Dataset != SegmentDatasetStateDomainChange || result.FromTxNum != 1 || result.ToTxNum != 2 || result.SegmentsMerged != 2 {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 3 {
		t.Fatalf("result segments = %d, want history/accessor/index: %+v", len(result.Segments), result.Segments)
	}
	kinds := make(map[SegmentKind]bool)
	for _, ref := range result.Segments {
		if ref.Dataset != SegmentDatasetStateDomainChange || ref.FromTxNum != 1 || ref.ToTxNum != 2 {
			t.Fatalf("generic result ref = %+v", ref)
		}
		kinds[ref.Kind] = true
	}
	if !kinds[SegmentHistory] || !kinds[SegmentAccessor] || !kinds[SegmentInverted] {
		t.Fatalf("generic result kinds = %+v", kinds)
	}
}

func TestCompactHistoryDomainPreservesRepeatedAccessorKeys(t *testing.T) {
	dir := t.TempDir()
	owner := binaryAddress(0xee)
	first := binaryStateDomainChange(1, 1, 1, "slot/shared")
	first.Owner = owner
	first.Generation = 7
	first.Domain = kvdomains.ContractStorage
	second := binaryStateDomainChange(2, 2, 1, "slot/shared")
	second.Owner = owner
	second.Generation = 7
	second.Domain = kvdomains.ContractStorage
	other := binaryStateDomainChange(3, 3, 1, "slot/other")
	other.Owner = owner
	other.Generation = 7
	other.Domain = kvdomains.ContractStorage
	refs := append([]SegmentRef{}, writeCompactionStateDomainChangeSegment(t, dir, 1, 1, first)...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, second)...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 3, 3, other)...)
	if err := PublishManifest(dir, NewManifest(1, 3, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{MinSegments: 3})
	if err != nil {
		t.Fatalf("compact history domain: %v", err)
	}
	if !result.Merged {
		t.Fatalf("result = %+v", result)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	var got []*rawdb.StateDomainChange
	if err := mgr.IterateStateDomainChangesByKey(1, 3, rawdb.StateFlatDomainKVLatest, owner, 7, kvdomains.ContractStorage, []byte("slot/shared"), func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate keyed compacted history: %v", err)
	}
	assertBinaryChangeOrder(t, got, []binaryChangeOrder{
		{txNum: 1, seq: 1, key: "slot/shared"},
		{txNum: 2, seq: 1, key: "slot/shared"},
	})
}

func TestCompactHistoryDomainValidatesAccessorAgainstSegment(t *testing.T) {
	dir := t.TempDir()
	refs := append([]SegmentRef{},
		writeCompactionStateDomainChangeSegment(t, dir, 1, 2, binaryStateDomainChange(1, 1, 1, "a"))...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 3, 3, binaryStateDomainChange(3, 3, 1, "b"))...)
	accessorRef := refs[1]
	data := mustReadFile(t, filepath.Join(dir, accessorRef.Path))
	entryOffset := binary.BigEndian.Uint64(data[stateDomainChangeBinaryHeaderSize : stateDomainChangeBinaryHeaderSize+8])
	keyLen := binary.BigEndian.Uint32(data[entryOffset : entryOffset+4])
	txNumOffset := entryOffset + 4 + uint64(keyLen)
	binary.BigEndian.PutUint64(data[txNumOffset:txNumOffset+8], 2)
	setStateDomainChangeBinaryRefMetadata(&accessorRef, data)
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, accessorRef.Path), data); err != nil {
		t.Fatalf("write corrupted accessor: %v", err)
	}
	refs[1] = accessorRef
	if err := PublishManifest(dir, NewManifest(1, 3, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	_, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{MinSegments: 2})
	if err == nil || !strings.Contains(err.Error(), "entry tx/seq") {
		t.Fatalf("compact err = %v, want accessor/segment mismatch", err)
	}
}

func TestCompactHistoryDomainNoOpWhenRunNotEligible(t *testing.T) {
	t.Run("not continuous", func(t *testing.T) {
		dir := t.TempDir()
		refs := append([]SegmentRef{},
			writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))...)
		refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 3, 3, binaryStateDomainChange(3, 3, 1, "c"))...)
		refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 5, 5, binaryStateDomainChange(5, 5, 1, "e"))...)
		if err := PublishManifest(dir, NewManifest(1, 5, refs)); err != nil {
			t.Fatalf("publish manifest: %v", err)
		}
		assertCompactionNoOp(t, dir, CompactionConfig{MinSegments: 2, DeleteObsolete: true}, refs)
	})

	t.Run("insufficient segments", func(t *testing.T) {
		dir := t.TempDir()
		refs := append([]SegmentRef{},
			writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))...)
		refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, binaryStateDomainChange(2, 2, 1, "b"))...)
		if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
			t.Fatalf("publish manifest: %v", err)
		}
		assertCompactionNoOp(t, dir, CompactionConfig{MinSegments: 3, DeleteObsolete: true}, refs)
	})
}

func TestCompactHistoryDomainSkipsMaxedFrontRun(t *testing.T) {
	dir := t.TempDir()
	refs := append([]SegmentRef{},
		writeCompactionStateDomainChangeSegment(t, dir, 1, 2,
			binaryStateDomainChange(1, 1, 1, "a"),
			binaryStateDomainChange(2, 2, 1, "b"))...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 3, 3, binaryStateDomainChange(3, 3, 1, "c"))...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 4, 4, binaryStateDomainChange(4, 4, 1, "d"))...)
	if err := PublishManifest(dir, NewManifest(1, 4, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{
		MinSegments:    2,
		MaxTxSpan:      2,
		DeleteObsolete: true,
	})
	if err != nil {
		t.Fatalf("compact history domain: %v", err)
	}
	if !result.Merged || result.FromTxNum != 3 || result.ToTxNum != 4 || result.SegmentsMerged != 2 {
		t.Fatalf("result = %+v, want merge [3,4]", result)
	}

	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	historyRefs := 0
	for _, ref := range manifest.Segments {
		if ref.Dataset == SegmentDatasetStateDomainChange && ref.Kind == SegmentHistory {
			historyRefs++
		}
	}
	if historyRefs != 2 {
		t.Fatalf("history refs = %d, want existing [1,2] and merged [3,4]: %+v", historyRefs, manifest.Segments)
	}
}

func writeCompactionStateDomainChangeSegment(t *testing.T, dir string, fromTxNum, toTxNum uint64, changes ...*rawdb.StateDomainChange) []SegmentRef {
	t.Helper()
	segRef, idxRef, accessorRef, err := writeStateDomainChangeBinaryFilesWithAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      stateDomainChangeHistorySegmentPath(fromTxNum, toTxNum),
	}, changes)
	if err != nil {
		t.Fatalf("write state-domain-change segment [%d,%d]: %v", fromTxNum, toTxNum, err)
	}
	return []SegmentRef{segRef, accessorRef, idxRef}
}

func assertCompactionNoOp(t *testing.T, dir string, cfg CompactionConfig, refs []SegmentRef) {
	t.Helper()
	before := segmentPaths(refs)
	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, cfg)
	if err != nil {
		t.Fatalf("compact history domain: %v", err)
	}
	if result.Merged {
		t.Fatalf("result merged unexpectedly: %+v", result)
	}
	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	after := segmentPaths(manifest.Segments)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("manifest paths = %v, want %v", after, before)
	}
	for _, path := range before {
		assertFileExists(t, filepath.Join(dir, path))
	}
}

func compactionRefByKind(t *testing.T, result HistoryCompactionResult, kind SegmentKind) SegmentRef {
	t.Helper()
	for _, ref := range result.Segments {
		if ref.Kind == kind {
			return ref
		}
	}
	t.Fatalf("compaction result missing %s ref: %+v", kind, result)
	return SegmentRef{}
}

func segmentPaths(refs []SegmentRef) []string {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		out = append(out, ref.Path)
	}
	sort.Strings(out)
	return out
}

func assertFileExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
}

func assertFileMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("stat %s err = %v, want not exist", path, err)
	}
}
