package snapshots

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// VerifyHistoryStagingTargetColdRange proves only the explicitly selected
// complete blocks in one canonical bucket. It uses the same ordered rows,
// per-Tx ordinals, exact Prev bytes and zero-change range proofs as full-bucket
// retirement. It does not certify any unselected block, publish a binding, or
// authorize payload deletion. A caller extending a binding must independently
// authenticate/rebind every old span and preserve their nonoverlapping union.
func VerifyHistoryStagingTargetColdRange(ctx context.Context, target rawdb.StateHistoryReadView, dir string, manifest *Manifest, blocks []rawdb.HistoryStagingBlockProof, needed []bool) ([]rawdb.HistoryStagingColdSpan, error) {
	if len(blocks) != int(rawdb.StateHistoryChunkBucketBlocks) || len(needed) != len(blocks) {
		return nil, errors.New("snapshots: partial TARGET proof needs a complete canonical bucket and mask")
	}
	selected := false
	for _, yes := range needed {
		selected = selected || yes
	}
	if !selected {
		return nil, errors.New("snapshots: partial TARGET proof mask is empty")
	}
	return verifyHistoryStagingSourceColdMask(ctx, target, dir, manifest, blocks, needed)
}

// VerifyOfflineHistoryRepairBoundaryCopies re-proves a resumed advisory
// journal's boundary copies from the retained old files. Physical checksums
// alone cannot establish that a candidate copied the same logical rows/Prev
// and canonical table. This reads existing files only; ordinary successful
// CopyStateHistoryReferenceTrioRangeContext already performed this comparison.
func VerifyOfflineHistoryRepairBoundaryCopies(ctx context.Context, dir string, plan *OfflineHistoryRepairPlan, newRefs []SegmentRef) error {
	if ctx == nil || plan == nil || len(newRefs) == 0 || len(newRefs)%3 != 0 {
		return errors.New("snapshots: invalid offline repair boundary verification")
	}
	for _, boundary := range []*OfflineHistoryRepairSlice{plan.Left, plan.Right} {
		if boundary == nil {
			continue
		}
		oldSources, oldCheck, err := offlineHistoryRepairReadSources(ctx, dir, boundary.SourceRefs)
		if err != nil {
			return err
		}
		if len(oldSources) != 1 || boundary.FromTxNum < oldSources[0].history.FromTxNum || boundary.ToTxNum > oldSources[0].history.ToTxNum || boundary.ToTxNum < boundary.FromTxNum {
			return errors.New("snapshots: invalid original repair boundary interval")
		}
		var selected []SegmentRef
		for i := 0; i < len(newRefs); i += 3 {
			h, _, _, _, err := historyReferenceTranscodeIdentity(newRefs[i : i+3])
			if err != nil {
				return err
			}
			if h.FromTxNum <= boundary.ToTxNum && h.ToTxNum >= boundary.FromTxNum {
				selected = append(selected, newRefs[i:i+3]...)
			}
		}
		newSources, newCheck, err := offlineHistoryRepairReadSources(ctx, dir, selected)
		if err != nil {
			return err
		}
		sort.Slice(newSources, func(i, j int) bool { return newSources[i].history.FromTxNum < newSources[j].history.FromTxNum })
		if len(newSources) == 0 || newSources[0].history.FromTxNum > boundary.FromTxNum || newSources[len(newSources)-1].history.ToTxNum < boundary.ToTxNum {
			return errors.New("snapshots: candidate does not cover repair boundary")
		}
		for i := 1; i < len(newSources); i++ {
			if newSources[i-1].history.ToTxNum == ^uint64(0) || newSources[i].history.FromTxNum != newSources[i-1].history.ToTxNum+1 {
				return errors.New("snapshots: candidate repair boundary has gap or overlap")
			}
		}
		if err := checkOfflineHistoryRepairBlockBoundary(ctx, dir, oldSources, boundary.FromTxNum, boundary.ToTxNum); err != nil {
			return err
		}
		before, err := offlineHistoryRepairRangeDigest(ctx, dir, oldSources, boundary.FromTxNum, boundary.ToTxNum)
		if err != nil {
			return err
		}
		after, err := offlineHistoryRepairRangeDigest(ctx, dir, newSources, boundary.FromTxNum, boundary.ToTxNum)
		if err != nil {
			return err
		}
		if before != after {
			return fmt.Errorf("snapshots: resumed repair boundary [%d,%d] differs from retained cold rows, Prev or block ranges", boundary.FromTxNum, boundary.ToTxNum)
		}
		if err := errors.Join(oldCheck(), newCheck()); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func offlineHistoryRepairReadSources(ctx context.Context, dir string, refs []SegmentRef) ([]stateDomainChangeBinaryCompactionSource, func() error, error) {
	if len(refs) == 0 || len(refs)%3 != 0 {
		return nil, nil, errors.New("snapshots: boundary verification requires complete trios")
	}
	type captured struct {
		refs   [3]SegmentRef
		states [3]historyStagingFileState
	}
	var sources []stateDomainChangeBinaryCompactionSource
	var files []captured
	for i := 0; i < len(refs); i += 3 {
		h, idx, acc, _, err := historyReferenceTranscodeIdentity(refs[i : i+3])
		if err != nil {
			return nil, nil, err
		}
		f := captured{refs: [3]SegmentRef{h, idx, acc}}
		for j, ref := range f.refs {
			f.states[j], err = historyStagingFileFingerprint(dir, ref)
			if err != nil {
				return nil, nil, err
			}
		}
		source, err := offlineHistoryRepairAuthenticatedSource(ctx, dir, f.refs)
		if err != nil {
			return nil, nil, err
		}
		sources = append(sources, source)
		files = append(files, f)
	}
	check := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, f := range files {
			if err := historyStagingCheckFileStates(dir, f.refs, f.states); err != nil {
				return err
			}
		}
		return nil
	}
	if err := check(); err != nil {
		return nil, nil, err
	}
	return sources, check, nil
}

// AuthenticateOfflineHistoryRepairTrios performs the complete ordinary trio
// audit and records every strong file fingerprint in the context's optional
// physical-fact collector. In particular, unchanged boundary copies with no
// staging binding still participate in the caller's final RecheckAll. This
// authenticates file/companion consistency, not TARGET semantic equivalence.
func AuthenticateOfflineHistoryRepairTrios(ctx context.Context, dir string, refs []SegmentRef) error {
	if ctx == nil || len(refs) == 0 || len(refs)%3 != 0 {
		return errors.New("snapshots: offline repair authentication needs complete trios")
	}
	for i := 0; i < len(refs); i += 3 {
		h, idx, acc, _, err := historyReferenceTranscodeIdentity(refs[i : i+3])
		if err != nil {
			return err
		}
		if _, err := offlineHistoryRepairAuthenticatedSource(ctx, dir, [3]SegmentRef{h, idx, acc}); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// OfflineRebindHistoryStagingRepair rebinds only reverse references to the
// exact old trios replaced by one offline repair. The stopped-node caller owns
// its start/manifest guard throughout. It neither registers online retention
// nor scans unrelated Retired trios, and it never unlinks a file. Each old
// durable subrange receipt is proved against the candidate before RebindCold
// publishes its bounded, synced metadata batch.
func OfflineRebindHistoryStagingRepair(ctx context.Context, dir string, manager *rawdb.HistoryStagingManager, candidate *Manifest, oldRefs []SegmentRef, verifyGuard func() error) error {
	if ctx == nil || manager == nil || candidate == nil || verifyGuard == nil || len(oldRefs) == 0 || len(oldRefs)%3 != 0 {
		return errors.New("snapshots: invalid offline repair rebind input")
	}
	if err := candidate.ValidateProduction(); err != nil {
		return err
	}
	retired := make(map[string]SegmentRef, len(candidate.Retired))
	active := make(map[string]bool, len(candidate.Segments))
	for _, ref := range candidate.Retired {
		retired[ref.Path] = ref
	}
	for _, ref := range candidate.Segments {
		active[ref.Path] = true
	}
	var ids [][32]byte
	seen := make(map[[32]byte]bool)
	for i := 0; i < len(oldRefs); i += 3 {
		h, idx, acc, _, err := historyReferenceTranscodeIdentity(oldRefs[i : i+3])
		if err != nil {
			return err
		}
		for _, ref := range []SegmentRef{h, idx, acc} {
			if retired[ref.Path] != ref || active[ref.Path] {
				return fmt.Errorf("snapshots: offline repair source %q is not exclusively retained", ref.Path)
			}
		}
		id, err := historyStagingTrioID([3]SegmentRef{h, idx, acc})
		if err != nil {
			return err
		}
		if seen[id] {
			return errors.New("snapshots: duplicate offline repair source trio")
		}
		seen[id] = true
		ids = append(ids, id)
	}
	proofCtx := ctx
	facts, shared := ctx.Value(historyStagingPhysicalFactKey{}).(*HistoryStagingPhysicalFactCollector)
	if !shared {
		var err error
		proofCtx, facts, err = WithHistoryStagingPhysicalFacts(ctx, dir)
		if err != nil {
			return err
		}
	}
	check := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return errors.Join(verifyGuard(), facts.RecheckAll(ctx))
	}
	if err := check(); err != nil {
		return err
	}
	var cachedBucket uint64
	var cachedBlocks []rawdb.HistoryStagingBlockProof
	for _, id := range ids {
		if err := check(); err != nil {
			return err
		}
		err := manager.RebindCold(proofCtx, id, candidate.Generation, func(old rawdb.HistoryStagingColdBinding) (rawdb.HistoryStagingColdBinding, error) {
			if err := check(); err != nil {
				return rawdb.HistoryStagingColdBinding{}, err
			}
			if cachedBlocks == nil || cachedBucket != old.Bucket {
				var err error
				cachedBlocks, err = manager.ReadBucketBlockProofs(proofCtx, old.Bucket)
				if err != nil {
					return rawdb.HistoryStagingColdBinding{}, err
				}
				cachedBucket = old.Bucket
			}
			return RebindHistoryStagingColdBinding(proofCtx, dir, candidate, old, cachedBlocks)
		}, func(old, new rawdb.HistoryStagingColdBinding) error {
			// The build callback just proved the replacement against the old
			// receipt. RebindCold independently checks exact old coverage and
			// versions; file fingerprints close the proof-to-batch boundary.
			return check()
		})
		if err != nil {
			return fmt.Errorf("snapshots: offline repair rebind %x: %w", id, err)
		}
	}
	if err := check(); err != nil {
		return err
	}
	if err := manager.SyncColdDependencyPublications(); err != nil {
		return err
	}
	if err := check(); err != nil {
		return err
	}
	if !shared {
		return facts.CommitPhysicalCertificates(ctx)
	}
	return nil
}
