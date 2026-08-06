package snapshots

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestCompactHistoryDomainMergesContinuousBinarySegments(t *testing.T) {
	dir := t.TempDir()
	refs := append([]SegmentRef{},
		writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, binaryStateDomainChange(2, 2, 1, "b"))...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 3, 3, binaryStateDomainChange(3, 3, 1, "c"))...)
	setCompactionRefAggregationSteps(refs[:3], 2)
	if err := PublishManifest(dir, NewManifest(1, 3, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	oldPaths := segmentPaths(refs)

	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{
		DeleteObsolete: true,
	})
	if err != nil {
		t.Fatalf("compact history domain: %v", err)
	}
	if !result.Merged || result.FromTxNum != 1 || result.ToTxNum != 3 || result.AggregationSteps != 4 || result.SegmentsMerged != 3 {
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
	if manifestHistoryRef.AggregationSteps != 4 || manifestAccessorRef.AggregationSteps != 4 || manifestIndexRef.AggregationSteps != 4 {
		t.Fatalf("merged aggregation steps = history:%d accessor:%d index:%d, want 4", manifestHistoryRef.AggregationSteps, manifestAccessorRef.AggregationSteps, manifestIndexRef.AggregationSteps)
	}
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
		{txNum: 2, seq: 2, key: "b"},
		{txNum: 3, seq: 3, key: "c"},
	})

	assertFileExists(t, filepath.Join(dir, historyRef.Path))
	assertFileExists(t, filepath.Join(dir, accessorRef.Path))
	assertFileExists(t, filepath.Join(dir, indexRef.Path))
	for _, path := range oldPaths {
		assertFileMissing(t, filepath.Join(dir, path))
	}
}

func TestCompactedAccessorMatchesPostCompressionRebuild(t *testing.T) {
	dir := t.TempDir()
	refs := append([]SegmentRef{}, writeCompactionStateDomainChangeSegment(t, dir, 1, 2,
		binaryStateDomainChange(1, 1, 1, "alpha"),
		binaryStateDomainChange(2, 2, 1, "beta"))...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 3, 4,
		binaryStateDomainChange(3, 3, 1, "gamma"),
		binaryStateDomainChange(4, 4, 1, "delta"))...)
	if err := PublishManifest(dir, NewManifest(1, 4, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
	if err != nil {
		t.Fatalf("compact history domain: %v", err)
	}
	historyRef := compactionRefByKind(t, result, SegmentHistory)
	streamedRef := compactionRefByKind(t, result, SegmentAccessor)
	assertAccessorMatchesPostCompressionRebuild(t, dir, historyRef, streamedRef)
}

func assertAccessorMatchesPostCompressionRebuild(t *testing.T, dir string, historyRef, streamedRef SegmentRef) {
	t.Helper()
	rebuiltRef, _, err := buildStateDomainChangeBinaryAccessorV4FromHistorySegment(dir, historyRef, SegmentRef{
		Dataset:          SegmentDatasetStateDomainChange,
		Kind:             SegmentAccessor,
		FromTxNum:        historyRef.FromTxNum,
		ToTxNum:          historyRef.ToTxNum,
		AggregationSteps: historyRef.AggregationSteps,
		Path:             "rebuilt-accessor.kv",
	}, etl.Options{TempDir: filepath.Join(dir, "rebuilt-etl")})
	if err != nil {
		t.Fatalf("rebuild accessor from compacted segment: %v", err)
	}
	streamed, err := os.ReadFile(filepath.Join(dir, streamedRef.Path))
	if err != nil {
		t.Fatalf("read streamed accessor: %v", err)
	}
	rebuilt, err := os.ReadFile(filepath.Join(dir, rebuiltRef.Path))
	if err != nil {
		t.Fatalf("read rebuilt accessor: %v", err)
	}
	if !bytes.Equal(streamed, rebuilt) {
		t.Fatalf("streamed accessor differs from post-compression rebuild: streamed=%d rebuilt=%d", len(streamed), len(rebuilt))
	}
}

func TestCompactHistoryDomainPreservesPublishedInputLeases(t *testing.T) {
	dir := t.TempDir()
	refs := append([]SegmentRef{},
		writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, binaryStateDomainChange(2, 2, 1, "b"))...)
	identity := ChainIdentity{ChainID: 1, NetworkID: 2, GenesisHash: strings.Repeat("01", 32)}
	manifest := NewManifestForChain(1, 2, refs, identity)
	manifest.PublishedUnix = time.Now().Add(-48 * time.Hour).Unix()
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	oldCatalog, err := PublishSignedSnapshotCatalog(dir, privateKey)
	if err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog(old): %v", err)
	}
	oldPaths := segmentPaths(refs)

	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{
		DeleteObsolete: true,
	})
	if err != nil {
		t.Fatalf("compact history domain: %v", err)
	}
	if !result.Merged {
		t.Fatalf("result = %+v, want merged", result)
	}
	for _, path := range oldPaths {
		assertFileExists(t, filepath.Join(dir, path))
	}
	protected, err := PruneRetiredSegmentFiles(dir)
	if err != nil {
		t.Fatalf("PruneRetiredSegmentFiles(protected): %v", err)
	}
	if protected.FilesDeleted != 0 || protected.FilesSkippedPublished != len(oldPaths) {
		t.Fatalf("protected prune = %+v, want %d published skips", protected, len(oldPaths))
	}

	newCatalog, err := PublishSignedSnapshotCatalog(dir, privateKey)
	if err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog(new): %v", err)
	}
	if newCatalog.ManifestPath == oldCatalog.ManifestPath {
		t.Fatalf("catalog path did not advance: %q", newCatalog.ManifestPath)
	}
	if _, err := PrunePublishedSnapshotManifests(dir, 1, time.Hour); err != nil {
		t.Fatalf("PrunePublishedSnapshotManifests: %v", err)
	}
	reclaimed, err := PruneRetiredSegmentFiles(dir)
	if err != nil {
		t.Fatalf("PruneRetiredSegmentFiles(reclaimed): %v", err)
	}
	if reclaimed.FilesDeleted != len(oldPaths) {
		t.Fatalf("reclaimed prune = %+v, want %d deleted", reclaimed, len(oldPaths))
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

	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
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

func TestCompactHistoryDomainMergesDuplicateBlockTxRanges(t *testing.T) {
	dir := t.TempDir()
	first := binaryStateDomainChange(1, 10, 1, "first")
	second := binaryStateDomainChange(1, 11, 2, "second")
	sharedRange := &rawdb.StateTxRange{
		BlockNum:   1,
		BlockHash:  first.BlockHash,
		BeginTxNum: 10,
		EndTxNum:   11,
	}
	write := func(fromTxNum, toTxNum uint64, change *rawdb.StateDomainChange) []SegmentRef {
		t.Helper()
		segRef, idxRef, accessorRef, err := writeHistorySegmentFiles(dir, SegmentRef{
			Dataset:   SegmentDatasetStateDomainChange,
			Kind:      SegmentHistory,
			FromTxNum: fromTxNum,
			ToTxNum:   toTxNum,
			Path:      stateDomainChangeHistorySegmentPath(fromTxNum, toTxNum),
		}, []*rawdb.StateDomainChange{change}, []*rawdb.StateTxRange{sharedRange})
		if err != nil {
			t.Fatalf("write state-domain-change segment [%d,%d]: %v", fromTxNum, toTxNum, err)
		}
		return []SegmentRef{segRef, accessorRef, idxRef}
	}
	refs := append(write(10, 10, first), write(11, 11, second)...)
	if err := PublishManifest(dir, NewManifest(10, 11, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
	if err != nil {
		t.Fatalf("compact split block history: %v", err)
	}
	merged := compactionRefByKind(t, result, SegmentHistory)
	ranges, err := readStateDomainChangeBinaryTxRanges(dir, merged)
	if err != nil {
		t.Fatalf("read merged tx ranges: %v", err)
	}
	if len(ranges) != 1 {
		t.Fatalf("merged tx ranges = %+v, want one row", ranges)
	}
	if got := ranges[0]; got.BlockNum != 1 || got.BlockHash != first.BlockHash || got.BeginTxNum != 10 || got.EndTxNum != 11 {
		t.Fatalf("merged tx range = %+v, want block 1 [10,11]", got)
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
	setCompactionRefAggregationSteps(refs[:3], 2)
	if err := PublishManifest(dir, NewManifest(1, 3, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
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
		{txNum: 2, seq: 2, key: "slot/shared"},
	})
}

func TestCompactHistoryDomainUpgradesV2AccessorToV4(t *testing.T) {
	dir := t.TempDir()
	owner := binaryAddress(0xef)
	first := binaryStateDomainChange(1, 1, 1, "slot/a")
	first.Owner = owner
	first.Generation = 7
	first.Domain = kvdomains.ContractStorage
	second := binaryStateDomainChange(2, 2, 1, "slot/b")
	second.Owner = owner
	second.Generation = 7
	second.Domain = kvdomains.ContractStorage

	firstRefs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, first)
	rewriteStateDomainChangeAccessorAsV2(t, dir, &firstRefs[1])
	refs := append([]SegmentRef{}, firstRefs...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, second)...)
	if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
	if err != nil {
		t.Fatalf("compact mixed v2/v4 history: %v", err)
	}
	accessorRef := compactionRefByKind(t, result, SegmentAccessor)
	data := mustReadFile(t, filepath.Join(dir, accessorRef.Path))
	if got := binary.BigEndian.Uint32(data[8:12]); got != stateDomainChangeBinaryVersionV4 {
		t.Fatalf("compacted accessor version = %d, want %d", got, stateDomainChangeBinaryVersionV4)
	}

	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	var got []*rawdb.StateDomainChange
	if err := mgr.IterateStateDomainChangesByPrefix(1, 2, owner, 7, kvdomains.ContractStorage, []byte("slot/"), func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate compacted v4 prefix: %v", err)
	}
	assertBinaryChangeOrder(t, got, []binaryChangeOrder{{txNum: 1, seq: 1, key: "slot/a"}, {txNum: 2, seq: 2, key: "slot/b"}})
}

func TestCompactHistoryDomainTranscodesV2HistoryRecordsToV5(t *testing.T) {
	dir := t.TempDir()
	first := binaryStateDomainChange(1, 1, 1, "slot/a")
	second := binaryStateDomainChange(2, 2, 1, "slot/b")
	firstRefs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, first)
	rewriteStateDomainChangeHistoryAsV2(t, dir, firstRefs, first)
	refs := append([]SegmentRef{}, firstRefs...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, second)...)
	if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
		t.Fatal(err)
	}

	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
	if err != nil {
		t.Fatalf("compact v2/v5 history: %v", err)
	}
	historyRef := compactionRefByKind(t, result, SegmentHistory)
	reader, _, header, err := openHistorySegmentForRead(dir, historyRef)
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	if header.version != stateDomainChangeBinaryVersionV5 {
		t.Fatalf("compacted history version = %d, want v5", header.version)
	}
	changes, err := readStateDomainChangeBinarySegment(dir, historyRef)
	if err != nil {
		t.Fatal(err)
	}
	assertBinaryChangeOrder(t, changes, []binaryChangeOrder{{txNum: 1, seq: 1, key: "slot/a"}, {txNum: 2, seq: 2, key: "slot/b"}})
	if changes[0].NextExists || changes[1].NextExists {
		t.Fatal("compacted v5 history retained transient next images")
	}
}

func rewriteStateDomainChangeHistoryAsV2(t *testing.T, dir string, refs []SegmentRef, changes ...*rawdb.StateDomainChange) {
	t.Helper()
	normalized := normalizeStateDomainChangesForBinary(changes)
	var historyRef SegmentRef
	for _, ref := range refs {
		if ref.Kind == SegmentHistory {
			historyRef = ref
			break
		}
	}
	if historyRef.Path == "" {
		t.Fatal("history ref not found")
	}
	txRanges, err := normalizeStateTxRangesForBinary(historyRef.FromTxNum, historyRef.ToTxNum, normalized, nil)
	if err != nil {
		t.Fatal(err)
	}
	segmentData, index, accessor := encodeStateDomainChangeBinarySegmentV2IndexesForTest(t, historyRef.FromTxNum, historyRef.ToTxNum, normalized, txRanges)
	indexData, err := encodeStateDomainChangeBinaryIndex(historyRef.FromTxNum, historyRef.ToTxNum, index)
	if err != nil {
		t.Fatal(err)
	}
	accessorData, err := encodeStateDomainChangeBinaryAccessor(historyRef.FromTxNum, historyRef.ToTxNum, accessor)
	if err != nil {
		t.Fatal(err)
	}
	for i := range refs {
		var data []byte
		switch refs[i].Kind {
		case SegmentHistory:
			data = segmentData
		case SegmentInverted:
			data = indexData
		case SegmentAccessor:
			data = accessorData
		default:
			continue
		}
		if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, refs[i].Path), data); err != nil {
			t.Fatal(err)
		}
		setStateDomainChangeBinaryRefMetadata(&refs[i], data)
	}
}

func rewriteStateDomainChangeAccessorAsV2(t *testing.T, dir string, ref *SegmentRef) {
	t.Helper()
	entries, err := readStateDomainChangeBinaryAccessor(dir, *ref)
	if err != nil {
		t.Fatalf("read accessor: %v", err)
	}
	data, err := encodeStateDomainChangeBinaryAccessorV2ForTest(ref.FromTxNum, ref.ToTxNum, entries)
	if err != nil {
		t.Fatalf("encode v2 accessor: %v", err)
	}
	setStateDomainChangeBinaryRefMetadata(ref, data)
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, ref.Path), data); err != nil {
		t.Fatalf("write v2 accessor: %v", err)
	}
}

func TestCompactHistoryDomainValidatesAccessorAgainstSegment(t *testing.T) {
	dir := t.TempDir()
	refs := append([]SegmentRef{},
		writeCompactionStateDomainChangeSegment(t, dir, 1, 2, binaryStateDomainChange(1, 1, 1, "a"))...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 3, 3, binaryStateDomainChange(3, 3, 1, "b"))...)
	accessorRef := refs[1]
	data := mustReadFile(t, filepath.Join(dir, accessorRef.Path))
	if binary.BigEndian.Uint32(data[8:12]) >= stateDomainChangeBinaryVersionV3 {
		recordIndexOffset := stateDomainChangeBinaryHeaderSize + stateDomainChangeBinaryAccessorV3HeaderExtra + stateDomainChangeBinaryAccessorV3HashSize + 8
		binary.BigEndian.PutUint32(data[recordIndexOffset:recordIndexOffset+4], 1)
	} else {
		entryOffset := binary.BigEndian.Uint64(data[stateDomainChangeBinaryHeaderSize : stateDomainChangeBinaryHeaderSize+8])
		keyLen := binary.BigEndian.Uint32(data[entryOffset : entryOffset+4])
		txNumOffset := entryOffset + 4 + uint64(keyLen)
		binary.BigEndian.PutUint64(data[txNumOffset:txNumOffset+8], 2)
	}
	setStateDomainChangeBinaryRefMetadata(&accessorRef, data)
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, accessorRef.Path), data); err != nil {
		t.Fatalf("write corrupted accessor: %v", err)
	}
	refs[1] = accessorRef
	if err := PublishManifest(dir, NewManifest(1, 3, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	_, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
	if err == nil || (!strings.Contains(err.Error(), "entry tx/seq") && !strings.Contains(err.Error(), "exact entry") && !strings.Contains(err.Error(), "semantic coverage mismatch")) {
		t.Fatalf("compact err = %v, want accessor/segment mismatch", err)
	}
}

func TestCompactHistoryDomainRepairsStructurallyValidDerivedSidecarMismatch(t *testing.T) {
	dir := t.TempDir()
	refs := append([]SegmentRef{},
		writeCompactionStateDomainChangeSegment(t, dir, 1, 2,
			binaryStateDomainChange(1, 1, 1, "a"),
			binaryStateDomainChange(2, 2, 1, "b"))...)
	refs = append(refs,
		writeCompactionStateDomainChangeSegment(t, dir, 3, 3,
			binaryStateDomainChange(3, 3, 1, "c"))...)

	accessorRef := refs[1]
	data := mustReadFile(t, filepath.Join(dir, accessorRef.Path))
	if version := binary.BigEndian.Uint32(data[8:12]); version != stateDomainChangeBinaryVersionV4 {
		t.Fatalf("accessor version = %d, want v4", version)
	}
	// Keep the exact table structurally sorted and every record index in range,
	// but point its first entry at the other history record. Full installation/
	// pruning verification must reject it; compaction may repair it because the
	// history payload, not either derived sidecar, is the canonical merge input.
	firstRecordIndex := stateDomainChangeBinaryHeaderSize + stateDomainChangeBinaryAccessorV3HeaderExtra + stateDomainChangeBinaryAccessorV3HashSize + 8
	current := binary.BigEndian.Uint32(data[firstRecordIndex : firstRecordIndex+4])
	binary.BigEndian.PutUint32(data[firstRecordIndex:firstRecordIndex+4], 1-current)
	setStateDomainChangeBinaryRefMetadata(&accessorRef, data)
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, accessorRef.Path), data); err != nil {
		t.Fatalf("write semantically mismatched accessor: %v", err)
	}
	refs[1] = accessorRef
	if err := PublishManifest(dir, NewManifest(1, 3, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	if err := verifyStateDomainChangeBinaryCompanionsAgainstSegment(dir, refs[0], refs[2], refs[1]); err == nil {
		t.Fatal("full companion verification accepted semantically mismatched accessor")
	}

	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
	if err != nil {
		t.Fatalf("compact repairable sidecar mismatch: %v", err)
	}
	if !result.Merged {
		t.Fatalf("compaction did not merge repairable sources: %+v", result)
	}
	merged, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load merged manifest: %v", err)
	}
	historyRef := compactionRefByKind(t, result, SegmentHistory)
	indexRef := compactionRefByKind(t, result, SegmentInverted)
	mergedAccessorRef := compactionRefByKind(t, result, SegmentAccessor)
	if err := verifyStateDomainChangeBinaryCompanionsAgainstSegment(dir, historyRef, indexRef, mergedAccessorRef); err != nil {
		t.Fatalf("verify rebuilt companions: %v", err)
	}
	if merged.Generation != 2 {
		t.Fatalf("merged manifest = %+v", merged)
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
		assertCompactionNoOp(t, dir, CompactionConfig{DeleteObsolete: true}, refs)
	})

	t.Run("insufficient segments", func(t *testing.T) {
		dir := t.TempDir()
		refs := append([]SegmentRef{},
			writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))...)
		if err := PublishManifest(dir, NewManifest(1, 1, refs)); err != nil {
			t.Fatalf("publish manifest: %v", err)
		}
		assertCompactionNoOp(t, dir, CompactionConfig{DeleteObsolete: true}, refs)
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
	setCompactionRefAggregationSteps(refs[:3], 256)
	if err := PublishManifest(dir, NewManifest(1, 4, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{
		MaxSteps:       256,
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

func TestCompactHistoryDomainDefersBelowMinimumLogicalSpan(t *testing.T) {
	dir := t.TempDir()
	var refs []SegmentRef
	for txNum := uint64(1); txNum <= 4; txNum++ {
		refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, txNum, txNum,
			binaryStateDomainChange(txNum, txNum, 1, fmt.Sprintf("key-%d", txNum)))...)
	}
	if err := PublishManifest(dir, NewManifest(1, 4, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	deferred, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{
		MinSteps: 8,
		MaxSteps: 8,
	})
	if err != nil {
		t.Fatalf("deferred compaction: %v", err)
	}
	if deferred.Merged {
		t.Fatalf("four-step run merged below eight-step minimum: %+v", deferred)
	}

	merged, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{
		MinSteps: 4,
		MaxSteps: 8,
	})
	if err != nil {
		t.Fatalf("eligible compaction: %v", err)
	}
	if !merged.Merged || merged.MergePasses != 1 || merged.AggregationSteps != 4 || merged.SegmentsMerged != 4 {
		t.Fatalf("merged result = %+v, want one direct four-step merge", merged)
	}
}

func TestSelectAlignedHistoryCompactionRunMatchesErigonLevels(t *testing.T) {
	candidates := func(steps ...uint64) []historyCompactionCandidate {
		out := make([]historyCompactionCandidate, 0, len(steps))
		for i, aggregationSteps := range steps {
			out = append(out, historyCompactionCandidate{history: SegmentRef{
				FromTxNum:        uint64(i + 1),
				ToTxNum:          uint64(i + 1),
				AggregationSteps: aggregationSteps,
			}})
		}
		return out
	}

	tests := []struct {
		name       string
		steps      []uint64
		wantFrom   uint64
		wantTo     uint64
		wantSteps  uint64
		wantInputs int
		wantOK     bool
	}{
		{name: "large prefix plus leaf is stable", steps: []uint64{2, 1}, wantOK: false},
		{name: "outer range absorbs inner leaves", steps: []uint64{2, 1, 1}, wantFrom: 1, wantTo: 3, wantSteps: 4, wantInputs: 3, wantOK: true},
		{name: "four leaves merge directly", steps: []uint64{1, 1, 1, 1}, wantFrom: 1, wantTo: 4, wantSteps: 4, wantInputs: 4, wantOK: true},
		{name: "frozen prefix is not rewritten", steps: []uint64{256, 1, 1}, wantFrom: 2, wantTo: 3, wantSteps: 2, wantInputs: 2, wantOK: true},
		{name: "two half frozen files merge", steps: []uint64{128, 128}, wantFrom: 1, wantTo: 2, wantSteps: 256, wantInputs: 2, wantOK: true},
		{name: "two frozen files stay immutable", steps: []uint64{256, 256}, wantOK: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := selectAlignedHistoryCompactionRun(candidates(test.steps...), 256)
			if ok != test.wantOK {
				t.Fatalf("selection ok = %v, want %v: %+v", ok, test.wantOK, got)
			}
			if !ok {
				return
			}
			if got.fromTxNum != test.wantFrom || got.toTxNum != test.wantTo || got.aggregationSteps != test.wantSteps || len(got.candidates) != test.wantInputs {
				t.Fatalf("selection = %+v, want range [%d,%d] steps=%d inputs=%d", got, test.wantFrom, test.wantTo, test.wantSteps, test.wantInputs)
			}
		})
	}
}

func TestManifestRejectsHistoryCompanionAggregationStepMismatch(t *testing.T) {
	dir := t.TempDir()
	refs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))
	refs[1].AggregationSteps = 2
	err := PublishManifest(dir, NewManifest(1, 1, refs))
	if err == nil || !strings.Contains(err.Error(), "missing required accessor") {
		t.Fatalf("publish mismatched companion steps err = %v, want missing required accessor", err)
	}
}

func TestAlignedHistoryCompactionBoundsLogicalRewriteAmplification(t *testing.T) {
	const leaves = 1024
	var (
		shape          []historyCompactionCandidate
		rewrittenSteps uint64
	)
	for leaf := 1; leaf <= leaves; leaf++ {
		shape = append(shape, historyCompactionCandidate{history: SegmentRef{
			FromTxNum:        uint64(leaf),
			ToTxNum:          uint64(leaf),
			AggregationSteps: 1,
		}})
		for {
			selection, ok := selectAlignedHistoryCompactionRun(shape, 256)
			if !ok {
				break
			}
			var selectedSteps uint64
			for _, candidate := range selection.candidates {
				selectedSteps += candidate.history.effectiveAggregationSteps()
			}
			if selectedSteps != selection.aggregationSteps {
				t.Fatalf("selected steps = %d, want output steps %d", selectedSteps, selection.aggregationSteps)
			}
			rewrittenSteps += selectedSteps
			start, end := -1, -1
			for i := range shape {
				if shape[i].history.FromTxNum == selection.fromTxNum {
					start = i
				}
				if shape[i].history.ToTxNum == selection.toTxNum {
					end = i + 1
				}
			}
			if start < 0 || end <= start {
				t.Fatalf("selection [%d,%d] missing from shape %+v", selection.fromTxNum, selection.toTxNum, shape)
			}
			merged := historyCompactionCandidate{history: SegmentRef{
				FromTxNum:        selection.fromTxNum,
				ToTxNum:          selection.toTxNum,
				AggregationSteps: selection.aggregationSteps,
			}}
			shape = append(append(append([]historyCompactionCandidate(nil), shape[:start]...), merged), shape[end:]...)
		}
	}
	if len(shape) != leaves/256 {
		t.Fatalf("frozen shape files = %d, want %d: %+v", len(shape), leaves/256, shape)
	}
	for _, candidate := range shape {
		if candidate.history.AggregationSteps != 256 {
			t.Fatalf("frozen shape contains %d-step file, want 256: %+v", candidate.history.AggregationSteps, shape)
		}
	}
	if rewrittenSteps != 4_608 {
		t.Fatalf("eager shape rewrote %d logical steps, want 4608", rewrittenSteps)
	}
	// A leaf can participate only in the bounded 2,4,...,256 levels. Direct
	// outer-range absorption often does better, but it must never exceed that
	// logarithmic ceiling or approach an ever-growing whole-prefix rewrite.
	if max := uint64(leaves * 8); rewrittenSteps > max {
		t.Fatalf("rewritten logical steps = %d, exceed logarithmic ceiling %d", rewrittenSteps, max)
	}
}

func TestCatchupHistoryCompactionWritesEachLeafOnce(t *testing.T) {
	const (
		leaves   = 1024
		maxSteps = 256
	)
	var (
		shape          []historyCompactionCandidate
		rewrittenSteps uint64
	)
	for leaf := 1; leaf <= leaves; leaf++ {
		shape = append(shape, historyCompactionCandidate{history: SegmentRef{
			FromTxNum:        uint64(leaf),
			ToTxNum:          uint64(leaf),
			AggregationSteps: 1,
		}})
		selection, ok := selectAlignedHistoryCompactionRunAtLeast(shape, maxSteps, maxSteps)
		if !ok {
			continue
		}
		var selectedSteps uint64
		start, end := -1, -1
		for i, candidate := range shape {
			if candidate.history.FromTxNum == selection.fromTxNum {
				start = i
			}
			if candidate.history.ToTxNum == selection.toTxNum {
				end = i + 1
			}
		}
		for _, candidate := range selection.candidates {
			selectedSteps += candidate.history.effectiveAggregationSteps()
		}
		if start < 0 || end <= start || selectedSteps != maxSteps || selection.aggregationSteps != maxSteps {
			t.Fatalf("catch-up selection = %+v shape=%+v", selection, shape)
		}
		rewrittenSteps += selectedSteps
		merged := historyCompactionCandidate{history: SegmentRef{
			FromTxNum:        selection.fromTxNum,
			ToTxNum:          selection.toTxNum,
			AggregationSteps: selection.aggregationSteps,
		}}
		shape = append(append(append([]historyCompactionCandidate(nil), shape[:start]...), merged), shape[end:]...)
	}
	if rewrittenSteps != leaves {
		t.Fatalf("catch-up rewrote %d logical steps, want each of %d leaves exactly once", rewrittenSteps, leaves)
	}
	if len(shape) != leaves/maxSteps {
		t.Fatalf("catch-up shape files = %d, want %d: %+v", len(shape), leaves/maxSteps, shape)
	}
	for _, candidate := range shape {
		if candidate.history.AggregationSteps != maxSteps {
			t.Fatalf("catch-up shape contains non-frozen file: %+v", shape)
		}
	}
}

func writeCompactionStateDomainChangeSegment(t testing.TB, dir string, fromTxNum, toTxNum uint64, changes ...*rawdb.StateDomainChange) []SegmentRef {
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

func setCompactionRefAggregationSteps(refs []SegmentRef, steps uint64) {
	for i := range refs {
		refs[i].AggregationSteps = steps
	}
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
