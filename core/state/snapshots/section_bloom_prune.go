package snapshots

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type PruneHotSectionBloomResult struct {
	HasRange          bool
	FromSection       uint64
	ToSection         uint64
	RowsDeleted       uint64
	ColdBloomSegments uint64
}

// PruneHotSectionBlooms deletes hot section-bloom rows only after a registered
// cold section-bloom segment has been verified and every hot row in the
// segment's section range has an exact cold match.
func PruneHotSectionBlooms(db ethdb.KeyValueStore, dir string, manifest *Manifest) (*PruneHotSectionBloomResult, error) {
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
		if err := CheckSectionBloomSegment(dir, ref); err != nil {
			return nil, err
		}
		seg, err := OpenSectionBloomSegment(dir, ref)
		if err != nil {
			return nil, err
		}
		verified, err := verifyHotSectionBloomRowsCovered(db, seg, ref)
		if closeErr := seg.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
		if !verified.hasRows {
			continue
		}
		seg, err = OpenSectionBloomSegment(dir, ref)
		if err != nil {
			return nil, err
		}
		pruned, pruneErr := pruneHotSectionBloomRowsForSegment(db, seg, ref, result)
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
	return result, nil
}

type sectionBloomCoverageCheck struct {
	hasRows bool
}

func verifyHotSectionBloomRowsCovered(db ethdb.Iteratee, seg *SectionBloomSegment, ref SegmentRef) (sectionBloomCoverageCheck, error) {
	var out sectionBloomCoverageCheck
	if err := rawdb.IterateSectionBloomRows(db, func(section, bitIndex uint64, raw []byte) (bool, error) {
		if !sectionBloomRefCoversSection(ref, section) {
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

func pruneHotSectionBloomRowsForSegment(db ethdb.KeyValueWriter, seg *SectionBloomSegment, ref SegmentRef, result *PruneHotSectionBloomResult) (bool, error) {
	var pruned bool
	if err := seg.IterateRows(func(section, bitIndex uint64, _ []byte) error {
		if !sectionBloomRefCoversSection(ref, section) {
			return fmt.Errorf("snapshots: section bloom segment %q section %d outside block range [%d,%d]", ref.Path, section, ref.FromTxNum, ref.ToTxNum)
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

func sectionBloomRefsAscending(manifest *Manifest) []SegmentRef {
	refs := sectionBloomRefs(manifest)
	for i, j := 0, len(refs)-1; i < j; i, j = i+1, j-1 {
		refs[i], refs[j] = refs[j], refs[i]
	}
	return refs
}
