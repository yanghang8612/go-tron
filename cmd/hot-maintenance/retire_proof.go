package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// A plan contains metadata only. Target payload is pinned for one bucket at
// a time and is never retained over the RO -> RW handoff.
type retireBucket struct {
	Bucket             uint64                           `json:"bucket"`
	Route              rawdb.HistoryStagingRoute        `json:"route"`
	Receipt            *rawdb.HistoryStagingReceipt     `json:"receipt,omitempty"`
	OldBinding         *rawdb.HistoryStagingColdBinding `json:"old_binding,omitempty"`
	Prepared           *rawdb.HistoryStagingColdBinding `json:"prepared_binding,omitempty"`
	NeedsCertification bool                             `json:"needs_certification"`
	CanonicalDigest    [32]byte                         `json:"canonical_digest"`
}

type retirePlan struct {
	Epoch     uint64                           `json:"epoch"`
	Barrier   rawdb.HistoryStagingRouteBarrier `json:"barrier"`
	Protected cleanupProtected                 `json:"protected"`
	Buckets   []retireBucket                   `json:"buckets"`
}

func validateRetireBounds(p cleanupProtected, barrier rawdb.HistoryStagingRouteBarrier, from, through, window uint64) error {
	if from == 0 || through < from || through-from >= 4096 || window == 0 {
		return errors.New("retire-target requires 1..4096 explicit nonzero buckets and positive history window")
	}
	_, last, err := rawdb.StateHistoryChunkBucketBounds(through)
	if err != nil {
		return err
	}
	if err := validateCleanupBounds(p, last); err != nil {
		return err
	}
	if through > barrier.EligibleThrough || through > barrier.ThroughBucket {
		return errors.New("retire-target exceeds immutable cutover barrier; no barrier expansion is supported")
	}
	if p.Boundary.SolidNumber <= window || last > p.Boundary.SolidNumber-window {
		return errors.New("retire-target overlaps configured retained history window")
	}
	return nil
}

func inspectRetirePlan(ctx context.Context, hot, stage ethdb.KeyValueStore, from, through, window uint64) (retirePlan, *rawdb.HistoryStagingManager, error) {
	var p retirePlan
	protected, err := captureCleanupProtected(hot)
	if err != nil {
		return p, nil, err
	}
	m, err := rawdb.NewHistoryStagingManager(hot, stage, protected.Guard.StagingIdentity.Value)
	if err != nil {
		return p, nil, err
	}
	if err := m.VerifyIdentity(); err != nil {
		return p, nil, err
	}
	epoch, err := m.CurrentEpoch()
	if err != nil {
		return p, nil, err
	}
	if _, found, err := m.ReadResetIntent(); err != nil || found {
		return p, nil, errors.Join(errors.New("retire-target refuses every reset intent"), err)
	}
	barrier, found, err := m.ReadHistoryStagingRouteBarrier()
	if err != nil || !found || barrier.Epoch != epoch {
		return p, nil, errors.Join(errors.New("retire-target requires current-epoch cutover barrier"), err)
	}
	if err := validateRetireBounds(protected, barrier, from, through, window); err != nil {
		return p, nil, err
	}
	p = retirePlan{Epoch: epoch, Barrier: barrier, Protected: protected}
	for bucket := from; bucket <= through; bucket++ {
		if err := ctx.Err(); err != nil {
			return p, nil, err
		}
		route, found, err := m.ReadRoute(bucket)
		if err != nil || !found || route.Epoch != epoch || !route.SourceCleared || (route.Owner != rawdb.HistoryStagingOwnerTarget && route.Owner != rawdb.HistoryStagingOwnerCold) || (route.Owner == rawdb.HistoryStagingOwnerTarget && route.TargetCleared) {
			return p, nil, fmt.Errorf("retire bucket %d is not source-cleared TARGET/COLD: %w", bucket, errors.Join(err, rawdb.ErrHistoryStagingConflict))
		}
		if claim, found, err := m.ReadClaim(bucket); err != nil || (found && claim.Epoch == epoch) {
			return p, nil, fmt.Errorf("retire bucket %d has current claim or invalid claim: %w", bucket, errors.Join(err, rawdb.ErrHistoryStagingConflict))
		}
		row := retireBucket{Bucket: bucket, Route: route}
		receipt, hasReceipt, err := m.ReadReceiptAt(epoch, bucket)
		if err != nil {
			return p, nil, err
		}
		if hasReceipt {
			row.Receipt = &receipt
		}
		if route.Owner == rawdb.HistoryStagingOwnerTarget {
			if !hasReceipt {
				return p, nil, fmt.Errorf("retire bucket %d target receipt missing: %w", bucket, errors.Join(err, rawdb.ErrHistoryStagingIncomplete))
			}
			digest, err := rawdb.HistoryStagingReceiptDigest(receipt)
			if err != nil || digest != route.ReceiptDigest {
				return p, nil, fmt.Errorf("retire bucket %d target receipt digest differs", bucket)
			}
			row.Receipt = &receipt
		}
		if route.TargetCleared && hasReceipt {
			return p, nil, fmt.Errorf("retire bucket %d cleared route retained target receipt", bucket)
		}
		binding, found, err := m.ReadColdBindingAt(epoch, bucket)
		if err != nil {
			return p, nil, err
		}
		if (route.ColdBindingEpoch == 0) != !found || (found && binding.BindingEpoch != route.ColdBindingEpoch) {
			return p, nil, fmt.Errorf("retire bucket %d route binding epoch differs", bucket)
		}
		if found {
			row.OldBinding = &binding
		}
		row.NeedsCertification = !found || !cleanupBindingCovers(bucket, binding)
		if route.Owner == rawdb.HistoryStagingOwnerCold && row.NeedsCertification {
			return p, nil, fmt.Errorf("retire bucket %d COLD has incomplete semantic binding", bucket)
		}
		p.Buckets = append(p.Buckets, row)
	}
	return p, m, nil
}

// ReadBucketBlockProofs reads retained tx ranges, not canonical bodies. The
// caller must compose hot with the explicitly opened read-only ancient store.
func retireCanonicalBlocks(ctx context.Context, m *rawdb.HistoryStagingManager, chain ethdb.KeyValueReader, bucket uint64) ([]rawdb.HistoryStagingBlockProof, error) {
	blocks, err := m.ReadBucketBlockProofs(ctx, bucket)
	if err != nil {
		return nil, err
	}
	for _, block := range blocks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		hash, found, err := rawdb.ReadBlockHashByNumberStrict(chain, block.Number)
		if err != nil || !found || hash != block.Hash {
			return nil, fmt.Errorf("retire bucket %d block %d tx-range is not strict canonical: %w", bucket, block.Number, errors.Join(err, rawdb.ErrHistoryStagingIncomplete))
		}
	}
	return blocks, nil
}

func authenticateRetirePlan(ctx context.Context, p *retirePlan, m *rawdb.HistoryStagingManager, chain ethdb.KeyValueReader, cold string, manifest *snapshots.Manifest, audit cleanupBindingAuditor, certify bool, progress func(uint64)) error {
	for i := range p.Buckets {
		row := &p.Buckets[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		if row.NeedsCertification {
			if !certify {
				return fmt.Errorf("retire bucket %d requires explicit --certify-missing", row.Bucket)
			}
			if row.Route.ColdBindingEpoch == ^uint64(0) {
				return errors.New("retire binding epoch overflow")
			}
			blocks, err := retireCanonicalBlocks(ctx, m, chain, row.Bucket)
			if err != nil {
				return err
			}
			row.CanonicalDigest, err = retireBlocksDigest(row.Bucket, blocks)
			if err != nil {
				return err
			}
			if manifest.VisibleTxEnd < blocks[len(blocks)-1].EndTxNum {
				return fmt.Errorf("retire bucket %d is not fully published cold", row.Bucket)
			}
			target, err := m.AcquireTargetBucketView(p.Epoch, row.Bucket)
			if err != nil {
				return err
			}
			var spans []rawdb.HistoryStagingColdSpan
			if row.OldBinding == nil {
				spans, err = snapshots.VerifyHistoryStagingTargetColdEquivalence(ctx, target, cold, manifest, blocks)
			} else {
				spans, err = snapshots.VerifyHistoryStagingMixedTargetColdEquivalence(ctx, target, cold, manifest, blocks, *row.OldBinding)
			}
			err = errors.Join(err, target.Close())
			if err != nil {
				return fmt.Errorf("retire bucket %d target/cold equivalence: %w", row.Bucket, err)
			}
			binding := rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion, Bucket: row.Bucket, Epoch: p.Epoch, BindingEpoch: row.Route.ColdBindingEpoch + 1, ManifestEpoch: manifest.Generation, Spans: spans}
			if !cleanupBindingCovers(row.Bucket, binding) {
				return fmt.Errorf("retire bucket %d semantic proof did not cover every height", row.Bucket)
			}
			row.Prepared = &binding
		} else {
			binding := *row.OldBinding
			row.Prepared = &binding
		}
		// New bindings already underwent full physical SHA and semantic proof.
		// The caller retains the shared physical-fact collector and rechecks its
		// fingerprints across handoff; a separate receipt audit would rehash the
		// same large trios. Only pre-existing full semantic receipts use audit.
		if !row.NeedsCertification {
			if err := audit.VerifyBinding(ctx, *row.Prepared); err != nil {
				return fmt.Errorf("retire bucket %d physical receipt audit: %w", row.Bucket, err)
			}
		}
		if progress != nil {
			progress(row.Bucket)
		}
	}
	return audit.RecheckAll(ctx)
}

func retireBlocksDigest(bucket uint64, blocks []rawdb.HistoryStagingBlockProof) ([32]byte, error) {
	h := sha256.New()
	if err := cleanupHashRow(h, bucket, blocks); err != nil {
		return [32]byte{}, err
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}

func recheckRetireCanonical(ctx context.Context, p retirePlan, m *rawdb.HistoryStagingManager, chain ethdb.KeyValueReader) error {
	for _, row := range p.Buckets {
		if !row.NeedsCertification {
			continue
		}
		blocks, err := retireCanonicalBlocks(ctx, m, chain, row.Bucket)
		if err != nil {
			return err
		}
		digest, err := retireBlocksDigest(row.Bucket, blocks)
		if err != nil {
			return err
		}
		if digest != row.CanonicalDigest {
			return fmt.Errorf("retire bucket %d canonical/tx-range proof changed", row.Bucket)
		}
	}
	return nil
}

func retirePlanDigest(p retirePlan) ([32]byte, error) {
	h := sha256.New()
	for _, row := range p.Buckets {
		if err := cleanupHashRow(h, row.Bucket, row); err != nil {
			return [32]byte{}, err
		}
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}

// Checks the exact allowed protocol progression, including interrupted
// certification, owner publication, and target deletion. All other route
// fields and the old/new binding bytes must remain unchanged.
func verifyRetireTransitions(before retirePlan, after retirePlan, complete bool) error {
	if before.Epoch != after.Epoch || before.Barrier != after.Barrier || !reflect.DeepEqual(before.Protected, after.Protected) || len(before.Buckets) != len(after.Buckets) {
		return errors.New("retire protected chain/identity/barrier changed")
	}
	for i, old := range before.Buckets {
		cur := after.Buckets[i]
		if cur.Bucket != old.Bucket {
			return errors.New("retire bucket list changed")
		}
		if cur.Receipt != nil && !reflect.DeepEqual(cur.Receipt, old.Receipt) {
			return fmt.Errorf("retire bucket %d immutable target receipt changed", old.Bucket)
		}
		allowed := old.Route
		if cur.Route == allowed && reflect.DeepEqual(cur.OldBinding, old.OldBinding) && !complete {
			continue
		}
		if old.Prepared == nil {
			return errors.New("retire changed route without prepared proof")
		}
		allowed.ColdBindingEpoch = old.Prepared.BindingEpoch
		if !reflect.DeepEqual(cur.OldBinding, old.Prepared) {
			return fmt.Errorf("retire bucket %d binding changed unexpectedly", old.Bucket)
		}
		if cur.Route == allowed && !complete {
			continue
		}
		if allowed.Owner == rawdb.HistoryStagingOwnerTarget {
			if allowed.WriteVersion == ^uint64(0) {
				return errors.New("retire route write version overflow")
			}
			allowed.Owner = rawdb.HistoryStagingOwnerCold
			allowed.WriteVersion++
		}
		if cur.Route == allowed && !complete {
			continue
		}
		allowed.TargetCleared = true
		if cur.Route != allowed {
			return fmt.Errorf("retire bucket %d route changed outside protocol", old.Bucket)
		}
		if complete && cur.Receipt != nil {
			return errors.New("retire completed target retained receipt")
		}
	}
	return nil
}
