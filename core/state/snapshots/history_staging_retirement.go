package snapshots

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// retiredHistoryStagingTrios derives immutable ContentIDs from complete
// retired state-history trios. An incomplete/legacy trio has no deletion
// authorization and remains on disk until it can be inspected explicitly.
func retiredHistoryStagingTrios(manifest *Manifest) (map[string][32]byte, [][32]byte, error) {
	byPath := make(map[string]SegmentRef, len(manifest.Retired))
	for _, ref := range manifest.Retired {
		if prior, exists := byPath[ref.Path]; exists && prior != ref {
			return nil, nil, fmt.Errorf("snapshots: retired path %q has conflicting metadata", ref.Path)
		}
		byPath[ref.Path] = ref
	}
	paths := make(map[string][32]byte)
	unique := make(map[[32]byte]struct{})
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		return nil, nil, errors.New("snapshots: missing state history domain registry")
	}
	for _, history := range manifest.Retired {
		if history.NormalizedDataset() != SegmentDatasetStateDomainChange || history.Kind != SegmentHistory {
			continue
		}
		idx, hasIdx := byPath[cfg.HistoryIndexPathFor(history.Path)]
		accessor, hasAccessor := byPath[cfg.HistoryAccessorPathFor(history.Path)]
		if !hasIdx || !hasAccessor || idx.Kind != SegmentInverted || accessor.Kind != SegmentAccessor || idx.FromTxNum != history.FromTxNum || idx.ToTxNum != history.ToTxNum || accessor.FromTxNum != history.FromTxNum || accessor.ToTxNum != history.ToTxNum {
			continue // incomplete retired trio remains physically protected
		}
		id, err := historyStagingTrioID([3]SegmentRef{history, idx, accessor})
		if err != nil {
			continue
		} // missing checksum is not a GC proof
		for _, ref := range []SegmentRef{history, idx, accessor} {
			if prior, present := paths[ref.Path]; present && prior != id {
				return nil, nil, fmt.Errorf("snapshots: retired path %q has conflicting content IDs", ref.Path)
			}
			paths[ref.Path] = id
		}
		unique[id] = struct{}{}
	}
	ids := make([][32]byte, 0, len(unique))
	for id := range unique {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return string(ids[i][:]) < string(ids[j][:]) })
	return paths, ids, nil
}

// IsolateHistoryStagingResetColdTail durably removes every pre-reset state
// history trio from the active cold manifest after complete genesis replay.
// Immutable files remain Retired for published catalogs and pinned readers;
// normal cold building may subsequently publish new-epoch trios. This is an
// idempotent startup phase, before ordinary queries or maintenance start.
func IsolateHistoryStagingResetColdTail(ctx context.Context, dir string, manager *rawdb.HistoryStagingManager, expectedChain ChainIdentity) error {
	if ctx == nil || dir == "" || manager == nil {
		return errors.New("snapshots: missing reset cold isolation input")
	}
	normalizeChainIdentity(&expectedChain)
	if err := validateChainIdentity(&expectedChain); err != nil {
		return fmt.Errorf("snapshots: invalid reset cold chain identity: %w", err)
	}
	binding, present := historyStagingRetentionFor(dir)
	if !present || binding.manager != manager {
		return errors.New("snapshots: reset cold directory is not bound to staging manager")
	}
	intent, present, err := manager.ReadResetIntent()
	if err != nil {
		return err
	}
	if !present {
		return nil // fresh staging with no reset has no old cold epoch
	}
	if !intent.Complete {
		return rawdb.ErrHistoryStagingResetting
	}
	epoch, err := manager.CurrentEpoch()
	if err != nil || epoch != intent.NewEpoch {
		return rawdb.ErrHistoryStagingConflict
	}
	releasePublication, err := binding.publication.acquireWrite(ctx)
	if err != nil {
		return err
	}
	defer releasePublication()
	binding.mu.Lock()
	defer binding.mu.Unlock()
	manifest, err := LoadProductionManifest(dir)
	if os.IsNotExist(err) {
		manifest = nil
	} else if err != nil {
		binding.ready = false
		return err
	}
	if manifest != nil {
		if err := manifest.ValidateChainIdentity(expectedChain); err != nil {
			binding.ready = false
			return err
		}
	}
	if manifest != nil && manifest.HistoryStagingResetEpoch > epoch {
		binding.ready = false
		return rawdb.ErrHistoryStagingConflict
	}
	if manifest != nil && manifest.HistoryStagingResetEpoch == epoch {
		binding.ready = true
		return nil
	}
	binding.ready = false
	if err := manager.VerifyReplayRouteCoverage(epoch, intent.TargetHeight); err != nil {
		return err
	}
	if manifest != nil && manifest.Generation == ^uint64(0) {
		return errors.New("snapshots: cold manifest generation overflow")
	}
	var next *Manifest
	if manifest == nil {
		next = NewManifestForChain(0, 0, nil, expectedChain)
	} else {
		next = cloneManifest(manifest)
		next.Generation++
	}
	next.PublishedUnix = time.Now().Unix()
	next.HistoryStagingResetEpoch = epoch
	if manifest != nil {
		next.Segments = next.Segments[:0]
		for _, ref := range manifest.Segments {
			if ref.NormalizedDataset() == SegmentDatasetStateDomainChange {
				next.Retired = append(next.Retired, ref)
			} else {
				next.Segments = append(next.Segments, ref)
			}
		}
	}
	if next.Progress != nil {
		next.Progress.HistoryBuildTxNum = 0
		next.Progress.AccessorBuildTxNum = 0
		next.Progress.HotPruneTxNum = 0
		next.Progress.HotPruneBlockNum = 0
		next.Progress.StateChangeIndexPruneBlockNum = 0
	}
	if err := PublishManifest(dir, next); err != nil {
		return err
	}
	binding.ready = true
	return nil
}

// VerifyHistoryStagingQuarantineColdTail authenticates the durable reset
// isolation marker. The publication phase above already proved complete new
// SOURCE coverage, so later handoffs need not still have SOURCE ownership.
func VerifyHistoryStagingQuarantineColdTail(ctx context.Context, manifest *Manifest, manager *rawdb.HistoryStagingManager) error {
	if ctx == nil || manifest == nil || manager == nil {
		return errors.New("snapshots: missing reset quarantine proof")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	intent, present, err := manager.ReadResetIntent()
	if err != nil || !present || !intent.Complete {
		return rawdb.ErrHistoryStagingIncomplete
	}
	epoch, err := manager.CurrentEpoch()
	if err != nil || epoch != intent.NewEpoch || manifest.HistoryStagingResetEpoch != epoch {
		return rawdb.ErrHistoryStagingIncomplete
	}
	return nil
}

// reconcileHistoryStagingColdDependencies runs after a manifest replacement
// and at process startup, before any reader is served or retired file is
// unlinked. RebindCold scans only the old ContentID's current-epoch reverse
// refs; each affected bucket undergoes full semantic/tx-range equivalence
// authentication against the new manifest, then each bounded page of at most
// 128 bindings receives one durable hot-DB Sync.
func reconcileHistoryStagingColdDependencies(ctx context.Context, dir string, manifest *Manifest, manager *rawdb.HistoryStagingManager) error {
	if ctx == nil || manifest == nil || manager == nil {
		return errors.New("snapshots: missing cold rebind input")
	}
	_, ids, err := retiredHistoryStagingTrios(manifest)
	if err != nil {
		return err
	}
	// Keep at most one bucket's tx-range proofs resident. A manifest may have
	// millions of historical blocks; caching every retired binding here would
	// grow without bound during startup reconciliation.
	var cachedBucket uint64
	var cachedBlocks []rawdb.HistoryStagingBlockProof
	var cached bool
	blocks := func(bucket uint64) ([]rawdb.HistoryStagingBlockProof, error) {
		if cached && cachedBucket == bucket {
			return cachedBlocks, nil
		}
		found, err := manager.ReadBucketBlockProofs(ctx, bucket)
		if err != nil {
			return nil, err
		}
		cachedBucket, cachedBlocks, cached = bucket, found, true
		return found, nil
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := manager.RebindCold(ctx, id, manifest.Generation,
			func(old rawdb.HistoryStagingColdBinding) (rawdb.HistoryStagingColdBinding, error) {
				proof, err := blocks(old.Bucket)
				if err != nil {
					return rawdb.HistoryStagingColdBinding{}, err
				}
				return RebindHistoryStagingColdBinding(ctx, dir, manifest, old, proof)
			},
			func(old, new rawdb.HistoryStagingColdBinding) error {
				proof, err := blocks(old.Bucket)
				if err != nil {
					return err
				}
				return VerifyHistoryStagingColdBinding(ctx, dir, manifest, new, proof)
			})
		if err != nil {
			return fmt.Errorf("snapshots: rebind retired cold ContentID %x: %w", id, err)
		}
	}
	return manager.SyncColdDependencyPublications()
}

// ReconcileHistoryStagingColdDependencies may be used by startup repair and
// tests. It shares the publication/GC gate registered by Bind.
func ReconcileHistoryStagingColdDependencies(ctx context.Context, dir string) error {
	binding, present := historyStagingRetentionFor(dir)
	if !present {
		return errors.New("snapshots: history staging manager not bound")
	}
	releasePublication, err := binding.publication.acquireWrite(ctx)
	if err != nil {
		return err
	}
	defer releasePublication()
	binding.mu.Lock()
	defer binding.mu.Unlock()
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return err
	}
	binding.ready = false
	if err := reconcileHistoryStagingColdDependencies(ctx, dir, manifest, binding.manager); err != nil {
		return err
	}
	binding.ready, err = binding.resetColdIsolated(manifest)
	if err != nil {
		return err
	}
	if !binding.ready {
		return errors.New("snapshots: reset cold tail isolation pending")
	}
	return nil
}
