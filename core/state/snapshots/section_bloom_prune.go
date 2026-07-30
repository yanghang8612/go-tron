package snapshots

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type PruneHotSectionBloomResult struct {
	HasRange          bool
	FromSection       uint64
	ToSection         uint64
	RowsDeleted       uint64
	ColdBloomSegments uint64

	hasProcessedToBlock     bool
	processedToBlock        uint64
	hasProcessedToBlockHash bool
	processedToBlockHash    common.Hash
}

type sectionBloomPruneBounds struct {
	lastPrunedBlock     uint64
	hasLastPrunedBlock  bool
	requireProgressHash bool
}

// PruneHotSectionBlooms deletes hot section-bloom rows only after a registered
// cold section-bloom segment has been verified and every hot row in the
// segment's section range has an exact cold match.
func PruneHotSectionBlooms(db ethdb.KeyValueStore, dir string, manifest *Manifest) (*PruneHotSectionBloomResult, error) {
	return pruneHotSectionBlooms(db, dir, manifest, sectionBloomPruneBounds{})
}

// PruneHotSectionBloomsWithProgress prunes covered hot section-bloom rows after
// the persisted StageSnapshotSectionBloomPrune boundary and advances that
// boundary when a verified cold segment is processed.
func PruneHotSectionBloomsWithProgress(db ethdb.KeyValueStore, dir string, manifest *Manifest) (*PruneHotSectionBloomResult, error) {
	if db == nil {
		return nil, errors.New("snapshots: nil section bloom prune database")
	}
	resume, err := readHashBoundPruneResumeBoundary(db, rawdb.StageSnapshotSectionBloomPrune)
	if err != nil {
		return nil, err
	}
	if err := verifySectionBloomPruneResumeStage(db, resume); err != nil {
		return nil, err
	}
	result, err := pruneHotSectionBlooms(db, dir, manifest, sectionBloomPruneBounds{
		lastPrunedBlock:     resume.block,
		hasLastPrunedBlock:  resume.hasHash,
		requireProgressHash: true,
	})
	if err != nil {
		return nil, err
	}
	if result.hasProcessedToBlock {
		if err := writeSnapshotSectionBloomPruneStage(db, result.processedToBlock, result.processedToBlockHash, result.hasProcessedToBlockHash); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func pruneHotSectionBlooms(db ethdb.KeyValueStore, dir string, manifest *Manifest, bounds sectionBloomPruneBounds) (*PruneHotSectionBloomResult, error) {
	if db == nil {
		return nil, errors.New("snapshots: nil section bloom prune database")
	}
	if manifest == nil {
		return nil, errors.New("snapshots: nil manifest")
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	result := new(PruneHotSectionBloomResult)
	for _, ref := range sectionBloomRefsAscending(manifest) {
		if bounds.hasLastPrunedBlock && ref.ToTxNum <= bounds.lastPrunedBlock {
			continue
		}
		if err := CheckSectionBloomSegment(dir, ref); err != nil {
			return nil, err
		}
		var processedHash common.Hash
		var hasProcessedHash bool
		if bounds.requireProgressHash {
			var hashErr error
			processedHash, hashErr = sectionBloomPruneStageHash(db, ref.ToTxNum)
			if hashErr != nil {
				return nil, hashErr
			}
			hasProcessedHash = true
		}
		seg, err := OpenSectionBloomSegment(dir, ref)
		if err != nil {
			return nil, err
		}
		verified, err := verifyHotSectionBloomRowsCovered(db, seg, ref, bounds)
		if closeErr := seg.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
		if verified.hasRows {
			seg, err = OpenSectionBloomSegment(dir, ref)
			if err != nil {
				return nil, err
			}
			pruned, pruneErr := pruneHotSectionBloomRowsForSegment(db, seg, ref, result, bounds)
			closeErr := seg.Close()
			if pruneErr != nil {
				return nil, pruneErr
			}
			if closeErr != nil {
				return nil, closeErr
			}
			if pruned {
				result.ColdBloomSegments++
			}
		}
		markSectionBloomProcessedToBlock(result, ref.ToTxNum, processedHash, hasProcessedHash)
	}
	return result, nil
}

type sectionBloomCoverageCheck struct {
	hasRows bool
}

func verifyHotSectionBloomRowsCovered(db ethdb.Iteratee, seg *SectionBloomSegment, ref SegmentRef, bounds sectionBloomPruneBounds) (sectionBloomCoverageCheck, error) {
	var out sectionBloomCoverageCheck
	if err := rawdb.IterateSectionBloomRows(db, func(section, bitIndex uint64, raw []byte) (bool, error) {
		if !sectionBloomRefCoversSection(ref, section) || bounds.skipSection(section) {
			return true, nil
		}
		coldRaw, ok, err := seg.SectionBloom(section, bitIndex)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, fmt.Errorf("snapshots: cold section bloom segment %q missing hot row section=%d bit=%d", ref.Path, section, bitIndex)
		}
		if !bytes.Equal(raw, coldRaw) {
			return false, fmt.Errorf("snapshots: cold section bloom segment %q differs from hot row section=%d bit=%d", ref.Path, section, bitIndex)
		}
		out.hasRows = true
		return true, nil
	}); err != nil {
		return out, err
	}
	return out, nil
}

func pruneHotSectionBloomRowsForSegment(db ethdb.KeyValueStore, seg *SectionBloomSegment, ref SegmentRef, result *PruneHotSectionBloomResult, bounds sectionBloomPruneBounds) (bool, error) {
	var pruned bool
	if err := seg.IterateRows(func(section, bitIndex uint64, coldRaw []byte) error {
		if !sectionBloomRefCoversSection(ref, section) {
			return fmt.Errorf("snapshots: section bloom segment %q section %d outside block range [%d,%d]", ref.Path, section, ref.FromTxNum, ref.ToTxNum)
		}
		if bounds.skipSection(section) {
			return nil
		}
		hotRaw, ok, err := rawdb.ReadHotSectionBloomStrict(db, section, bitIndex)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if !bytes.Equal(hotRaw, coldRaw) {
			return fmt.Errorf("snapshots: hot section bloom row section=%d bit=%d changed after cold verification", section, bitIndex)
		}
		if err := rawdb.DeleteSectionBloom(db, section, bitIndex); err != nil {
			return err
		}
		markSectionBloomPrunedRange(result, section)
		result.RowsDeleted++
		pruned = true
		return nil
	}); err != nil {
		return false, err
	}
	return pruned, nil
}

func (b sectionBloomPruneBounds) skipSection(section uint64) bool {
	return b.hasLastPrunedBlock && sectionBloomSectionEndBlock(section) <= b.lastPrunedBlock
}

func markSectionBloomPrunedRange(result *PruneHotSectionBloomResult, section uint64) {
	if result == nil {
		return
	}
	if !result.HasRange {
		result.HasRange = true
		result.FromSection = section
		result.ToSection = section
		return
	}
	if section < result.FromSection {
		result.FromSection = section
	}
	if section > result.ToSection {
		result.ToSection = section
	}
}

func markSectionBloomProcessedToBlock(result *PruneHotSectionBloomResult, blockNum uint64, blockHash common.Hash, hasBlockHash bool) {
	if result == nil {
		return
	}
	if !result.hasProcessedToBlock || blockNum > result.processedToBlock {
		result.hasProcessedToBlock = true
		result.processedToBlock = blockNum
		result.hasProcessedToBlockHash = hasBlockHash
		result.processedToBlockHash = blockHash
		return
	}
	if blockNum == result.processedToBlock && hasBlockHash && !result.hasProcessedToBlockHash {
		result.hasProcessedToBlockHash = true
		result.processedToBlockHash = blockHash
	}
}

func writeSnapshotSectionBloomPruneStage(db ethdb.KeyValueStore, blockNum uint64, blockHash common.Hash, hasBlockHash bool) error {
	if !hasBlockHash {
		return fmt.Errorf("snapshots: missing hash for %s stage block %d", rawdb.StageSnapshotSectionBloomPrune, blockNum)
	}
	current, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotSectionBloomPrune)
	if err != nil {
		return err
	}
	if ok && current.BlockNum > blockNum {
		return nil
	}
	if ok && current.BlockNum == blockNum && current.HasBlockHash {
		if current.BlockHash != blockHash {
			return fmt.Errorf("snapshots: %s stage %d hash %x does not match pruned block hash %x", rawdb.StageSnapshotSectionBloomPrune, blockNum, current.BlockHash, blockHash)
		}
		return nil
	}
	return rawdb.WriteStageProgressWithHash(db, rawdb.StageSnapshotSectionBloomPrune, blockNum, blockHash)
}

func verifySectionBloomPruneResumeStage(db ethdb.KeyValueReader, resume hashBoundPruneResumeBoundary) error {
	if !resume.hasHash {
		return nil
	}
	hash, err := sectionBloomPruneStageHash(db, resume.block)
	if err != nil {
		return fmt.Errorf("snapshots: verify %s stage block %d: %w", rawdb.StageSnapshotSectionBloomPrune, resume.block, err)
	}
	if hash != resume.hash {
		return fmt.Errorf("snapshots: %s stage %d hash %x does not match canonical hash %x", rawdb.StageSnapshotSectionBloomPrune, resume.block, resume.hash, hash)
	}
	return nil
}

func sectionBloomPruneStageHash(db ethdb.KeyValueReader, blockNum uint64) (common.Hash, error) {
	return requireSnapshotStageBoundaryHash(db, rawdb.StageSnapshotSectionBloomPrune, blockNum)
}

func sectionBloomRefsAscending(manifest *Manifest) []SegmentRef {
	refs := sectionBloomRefs(manifest)
	for i, j := 0, len(refs)-1; i < j; i, j = i+1, j-1 {
		refs[i], refs[j] = refs[j], refs[i]
	}
	return refs
}

func sectionBloomSectionEndBlock(section uint64) uint64 {
	return (section+1)*rawdb.SectionBloomBlockPerSection - 1
}
