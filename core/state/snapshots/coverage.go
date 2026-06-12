package snapshots

import (
	"fmt"
	"sort"
)

// ChainIndexRangeCovered reports whether every block in [fromBlock,toBlock] is
// covered by readable chain-freezer and matching chain-index sidecar pairs.
func (m *Manager) ChainIndexRangeCovered(fromBlock, toBlock uint64) (bool, error) {
	if m == nil {
		return false, nil
	}
	if toBlock < fromBlock {
		return false, fmt.Errorf("snapshots: chain-index coverage range [%d,%d] is inverted", fromBlock, toBlock)
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return false, err
	}
	refs := chainFreezerRefs(manifest)
	sortSegmentRefsAscending(refs)
	next := fromBlock
	for _, freezerRef := range refs {
		if freezerRef.ToTxNum < next {
			continue
		}
		if freezerRef.FromTxNum > next {
			return false, nil
		}
		indexRef, ok := chainIndexRefForFreezer(manifest, freezerRef)
		if !ok {
			return false, nil
		}
		if err := VerifyChainIndexSegmentAgainstChainFreezer(m.dir, indexRef, freezerRef); err != nil {
			return false, err
		}
		if freezerRef.ToTxNum >= toBlock {
			return true, nil
		}
		if freezerRef.ToTxNum == ^uint64(0) {
			return false, nil
		}
		next = freezerRef.ToTxNum + 1
	}
	return false, nil
}

// SectionBloomRangeCovered reports whether every block in [fromBlock,toBlock]
// is covered by readable section-bloom sidecars.
func (m *Manager) SectionBloomRangeCovered(fromBlock, toBlock uint64) (bool, error) {
	if m == nil {
		return false, nil
	}
	if toBlock < fromBlock {
		return false, fmt.Errorf("snapshots: section-bloom coverage range [%d,%d] is inverted", fromBlock, toBlock)
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return false, err
	}
	return segmentRangeCovered(sectionBloomRefs(manifest), fromBlock, toBlock, func(ref SegmentRef) error {
		return CheckSectionBloomSegment(m.dir, ref)
	})
}

// BalanceTraceRangeCovered reports whether every block in [fromBlock,toBlock]
// is covered by readable balance-trace sidecars.
func (m *Manager) BalanceTraceRangeCovered(fromBlock, toBlock uint64) (bool, error) {
	if m == nil {
		return false, nil
	}
	if toBlock < fromBlock {
		return false, fmt.Errorf("snapshots: balance-trace coverage range [%d,%d] is inverted", fromBlock, toBlock)
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return false, err
	}
	return segmentRangeCovered(balanceTraceRefs(manifest), fromBlock, toBlock, func(ref SegmentRef) error {
		return CheckBalanceTraceSegment(m.dir, ref)
	})
}

func segmentRangeCovered(refs []SegmentRef, fromBlock, toBlock uint64, check func(SegmentRef) error) (bool, error) {
	sortSegmentRefsAscending(refs)
	next := fromBlock
	for _, ref := range refs {
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			return false, nil
		}
		if check != nil {
			if err := check(ref); err != nil {
				return false, err
			}
		}
		if ref.ToTxNum >= toBlock {
			return true, nil
		}
		if ref.ToTxNum == ^uint64(0) {
			return false, nil
		}
		next = ref.ToTxNum + 1
	}
	return false, nil
}

func sortSegmentRefsAscending(refs []SegmentRef) {
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
