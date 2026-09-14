package snapshots

// Frozen predecessor oracle. Only private helper identifiers are renamed.
import (
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"sort"
)

// Frozen from 833e4a0b02f3d490386b74a8114006658f035b80:core/state/snapshots/aggregator.go (writeEventLogBuildStage).
func frontier833WriteEventLogBuildStage(db any, manifest *Manifest) error {
	writer, ok := db.(ethdb.KeyValueWriter)
	if !ok {
		return nil
	}
	row, ok, err := frontier833EventLogBuildStageProgress(db, manifest)
	if err != nil || !ok {
		return err
	}
	return rawdb.WriteStageProgressRows(writer, []rawdb.StageProgress{row})
}

// Frozen from 833e4a0b02f3d490386b74a8114006658f035b80:core/state/snapshots/aggregator.go (eventLogBuildStageProgress).
func frontier833EventLogBuildStageProgress(db any, manifest *Manifest) (rawdb.StageProgress, bool, error) {
	if _, ok := db.(ethdb.KeyValueWriter); !ok {
		return rawdb.StageProgress{}, false, nil
	}
	block, ok := frontier833EventLogBuildBlockFromManifest(manifest)
	if !ok {
		return rawdb.StageProgress{}, false, nil
	}
	reader, ok := db.(ethdb.KeyValueReader)
	if !ok {
		return rawdb.StageProgress{}, false, fmt.Errorf("snapshots: %s stage block %d requires readable database", rawdb.StageSnapshotEventLogBuild, block)
	}
	hash, err := requireSnapshotStageBoundaryHash(reader, rawdb.StageSnapshotEventLogBuild, block)
	if err != nil {
		return rawdb.StageProgress{}, false, err
	}
	return rawdb.StageProgress{
		Stage:        rawdb.StageSnapshotEventLogBuild,
		BlockNum:     block,
		BlockHash:    hash,
		HasBlockHash: true,
	}, true, nil
}

// Frozen from 833e4a0b02f3d490386b74a8114006658f035b80:core/state/snapshots/aggregator.go (eventLogBuildBlockFromManifest).
func frontier833EventLogBuildBlockFromManifest(manifest *Manifest) (uint64, bool) {
	refs := frontier833EventLogRefs(manifest)
	if len(refs) == 0 {
		return 0, false
	}
	block, ok := frontier833EventLogCoverageBlockFromRefs(refs, 1)
	if !ok {
		return 0, false
	}
	indexedBlock, indexed := frontier833EventLogIndexedCoverageBlockFromRefs(frontier833EventLogIndexRefs(manifest), 1, block)
	return indexedBlock, indexed
}

// Frozen from 833e4a0b02f3d490386b74a8114006658f035b80:core/state/snapshots/aggregator.go (eventLogCoverageBlockFromRefs).
func frontier833EventLogCoverageBlockFromRefs(refs []SegmentRef, fromBlock uint64) (uint64, bool) {
	frontier833SortSegmentRefsAscending(refs)
	next := fromBlock
	for _, ref := range refs {
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			break
		}
		if ref.ToTxNum == ^uint64(0) {
			return ref.ToTxNum, true
		}
		next = ref.ToTxNum + 1
	}
	if next == fromBlock {
		return 0, false
	}
	return next - 1, true
}

// Frozen from 833e4a0b02f3d490386b74a8114006658f035b80:core/state/snapshots/aggregator.go (eventLogIndexedCoverageBlockFromRefs).
func frontier833EventLogIndexedCoverageBlockFromRefs(indexRefs []SegmentRef, fromBlock, maxBlock uint64) (uint64, bool) {
	if maxBlock < fromBlock {
		return 0, false
	}
	frontier833SortSegmentRefsAscending(indexRefs)
	next := fromBlock
	for _, ref := range indexRefs {
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			break
		}
		if ref.ToTxNum >= maxBlock {
			return maxBlock, true
		}
		if ref.ToTxNum == ^uint64(0) {
			return 0, false
		}
		next = ref.ToTxNum + 1
	}
	if next == fromBlock {
		return 0, false
	}
	return next - 1, true
}

// Frozen from 833e4a0b02f3d490386b74a8114006658f035b80:core/state/snapshots/event_log_segment.go (eventLogRefs).
func frontier833EventLogRefs(manifest *Manifest) []SegmentRef {
	if manifest == nil {
		return nil
	}
	var refs []SegmentRef
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentEventLog && ref.frontier833NormalizedDatasetPrivate() == SegmentDatasetEventLog {
			refs = append(refs, ref)
		}
	}
	frontier833SortSegments(refs)
	return refs
}

// Frozen from 833e4a0b02f3d490386b74a8114006658f035b80:core/state/snapshots/event_log_segment.go (eventLogIndexRefs).
func frontier833EventLogIndexRefs(manifest *Manifest) []SegmentRef {
	if manifest == nil {
		return nil
	}
	var refs []SegmentRef
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentEventLogIndex && ref.frontier833NormalizedDatasetPrivate() == SegmentDatasetEventLog {
			refs = append(refs, ref)
		}
	}
	frontier833SortSegments(refs)
	return refs
}

// Frozen from 833e4a0b02f3d490386b74a8114006658f035b80:core/state/snapshots/coverage.go (sortSegmentRefsAscending).
func frontier833SortSegmentRefsAscending(refs []SegmentRef) {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].FromTxNum != refs[j].FromTxNum {
			return refs[i].FromTxNum < refs[j].FromTxNum
		}
		if refs[i].ToTxNum != refs[j].ToTxNum {
			return refs[i].ToTxNum < refs[j].ToTxNum
		}
		return refs[i].Path < refs[j].Path
	})
}

// Frozen from 833e4a0b02f3d490386b74a8114006658f035b80:core/state/snapshots/manifest.go (sortSegments).
func frontier833SortSegments(segments []SegmentRef) {
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].frontier833NormalizedDatasetPrivate() != segments[j].frontier833NormalizedDatasetPrivate() {
			return segments[i].frontier833NormalizedDatasetPrivate() < segments[j].frontier833NormalizedDatasetPrivate()
		}
		if segments[i].Domain != segments[j].Domain {
			return segments[i].Domain < segments[j].Domain
		}
		if segments[i].Kind != segments[j].Kind {
			return segments[i].Kind < segments[j].Kind
		}
		if segments[i].FromTxNum != segments[j].FromTxNum {
			return segments[i].FromTxNum < segments[j].FromTxNum
		}
		if segments[i].ToTxNum != segments[j].ToTxNum {
			return segments[i].ToTxNum < segments[j].ToTxNum
		}
		return segments[i].Path < segments[j].Path
	})
}

// Frozen from 833e4a0b02f3d490386b74a8114006658f035b80:core/state/snapshots/manifest.go (normalizedDataset).
func (seg SegmentRef) frontier833NormalizedDatasetPrivate() SegmentDataset {
	return seg.frontier833NormalizedDataset()
}

// Frozen from 833e4a0b02f3d490386b74a8114006658f035b80:core/state/snapshots/manifest.go (NormalizedDataset).
func (seg SegmentRef) frontier833NormalizedDataset() SegmentDataset {
	if seg.Dataset == "" && seg.Kind == SegmentLatest {
		return SegmentDatasetKVLatest
	}
	return seg.Dataset
}

// Frozen from 833e4a0b02f3d490386b74a8114006658f035b80:core/state/snapshots/cold_builder.go (nextEventLogCatchupRange).
func frontier833NextEventLogCatchupRange(manifest *Manifest, toBlock, batchBlocks uint64) (coldSidecarBlockRange, bool) {
	if manifest == nil || toBlock < 1 {
		return coldSidecarBlockRange{}, false
	}
	next := uint64(1)
	if covered, ok := frontier833EventLogBuildBlockFromManifest(manifest); ok {
		if covered == ^uint64(0) || covered >= toBlock {
			return coldSidecarBlockRange{}, false
		}
		next = covered + 1
	}
	refs := append(frontier833EventLogRefs(manifest), frontier833EventLogIndexRefs(manifest)...)
	start, end := next, next
	foundOverlap := false
	for {
		changed := false
		for _, ref := range refs {
			if ref.FromTxNum > end || ref.ToTxNum < start {
				continue
			}
			foundOverlap = true
			if ref.FromTxNum < start {
				start = ref.FromTxNum
				changed = true
			}
			if ref.ToTxNum > end {
				end = ref.ToTxNum
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	if foundOverlap {
		return coldSidecarBlockRange{from: start, to: end}, true
	}
	end = toBlock
	for _, ref := range refs {
		if ref.FromTxNum > start && ref.FromTxNum-1 < end {
			end = ref.FromTxNum - 1
		}
	}
	return frontier833BoundedColdSidecarRange(start, end, batchBlocks)
}

// Frozen from 833e4a0b02f3d490386b74a8114006658f035b80:core/state/snapshots/cold_builder.go (boundedColdSidecarRange).
func frontier833BoundedColdSidecarRange(fromBlock, toBlock, batchBlocks uint64) (coldSidecarBlockRange, bool) {
	if toBlock < fromBlock {
		return coldSidecarBlockRange{}, false
	}
	if batchBlocks > 0 {
		batchEnd := fromBlock + batchBlocks - 1
		if batchEnd < fromBlock || batchEnd > toBlock {
			batchEnd = toBlock
		}
		toBlock = batchEnd
	}
	return coldSidecarBlockRange{from: fromBlock, to: toBlock}, true
}
