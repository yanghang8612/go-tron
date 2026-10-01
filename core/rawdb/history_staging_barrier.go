package rawdb

import (
	"context"
	"crypto/sha256"
	"errors"
)

const historyStagingMaxBarrierBuckets = 1 << 22

func (m *HistoryStagingManager) ReadHistoryStagingRouteBarrier() (HistoryStagingRouteBarrier, bool, error) {
	var barrier HistoryStagingRouteBarrier
	if m == nil {
		return barrier, false, errors.New("rawdb: missing staging manager")
	}
	present, err := readHistoryStagingValue(m.hot, historyStagingRouteBarrierKey, &barrier)
	if err != nil || !present {
		return barrier, present, err
	}
	if barrier.Version != HistoryStagingFormatVersion || barrier.Epoch == 0 || barrier.ThroughBucket >= historyStagingMaxBarrierBuckets || barrier.EligibleThrough > barrier.ThroughBucket || barrier.PlanDigest == ([32]byte{}) || barrier.CandidateSHA == ([32]byte{}) || barrier.RouteDigest == ([32]byte{}) {
		return HistoryStagingRouteBarrier{}, false, ErrHistoryStagingConflict
	}
	return barrier, true, nil
}

func (m *HistoryStagingManager) scanOfflineRouteCoverage(ctx context.Context, epoch, eligibleThrough, throughBucket uint64) ([32]byte, error) {
	var digest [32]byte
	if epoch == 0 || eligibleThrough > throughBucket || throughBucket >= historyStagingMaxBarrierBuckets {
		return digest, ErrHistoryStagingConflict
	}
	h := sha256.New()
	_, _ = h.Write([]byte("go-tron-history-staging-route-barrier-v1"))
	for bucket := uint64(0); bucket <= throughBucket; bucket++ {
		if err := ctx.Err(); err != nil {
			return digest, err
		}
		route, present, err := m.ReadRoute(bucket)
		if err != nil || !present || route.Epoch != epoch {
			return digest, ErrHistoryStagingIncomplete
		}
		if bucket != 0 && bucket <= eligibleThrough && route.Owner == HistoryStagingOwnerSource {
			return digest, ErrHistoryStagingIncomplete
		}
		switch route.Owner {
		case HistoryStagingOwnerSource:
			if route.SourceCleared {
				return digest, ErrHistoryStagingConflict
			}
		case HistoryStagingOwnerTarget:
			if !route.SourceCleared {
				return digest, ErrHistoryStagingIncomplete
			}
			receipt, present, err := m.ReadReceiptAt(epoch, bucket)
			if err != nil || !present {
				return digest, ErrHistoryStagingIncomplete
			}
			receiptDigest, err := historyStagingReceiptDigest(receipt)
			if err != nil || receiptDigest != route.ReceiptDigest {
				return digest, ErrHistoryStagingConflict
			}
		case HistoryStagingOwnerCold:
			binding, present, err := m.ReadColdBindingAt(epoch, bucket)
			if err != nil || !present || binding.BindingEpoch != route.ColdBindingEpoch {
				return digest, ErrHistoryStagingIncomplete
			}
			first, last, err := StateHistoryChunkBucketBounds(bucket)
			if err != nil || !historyStagingBindingCovers(binding, first, last) {
				return digest, ErrHistoryStagingIncomplete
			}
		default:
			return digest, ErrHistoryStagingConflict
		}
		if claim, present, err := m.ReadClaim(bucket); err != nil {
			return digest, err
		} else if present && claim.Epoch == epoch {
			return digest, ErrHistoryStagingIncomplete
		}
		encoded, err := encodeHistoryStaging(route)
		if err != nil {
			return digest, err
		}
		historyStagingHashRow(h, historyStagingBucketKey(historyStagingRoutePrefix, bucket), encoded)
	}
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

// VerifyOfflineRouteCoverage requires a fully classified route for every
// fixed bucket through the frozen head. It is bounded by the chain inventory,
// but intentionally scans all routes before offline activation.
func (m *HistoryStagingManager) VerifyOfflineRouteCoverage(ctx context.Context, epoch, eligibleThrough, throughBucket uint64) ([32]byte, error) {
	if m == nil || ctx == nil {
		return [32]byte{}, ErrHistoryStagingConflict
	}
	return m.scanOfflineRouteCoverage(ctx, epoch, eligibleThrough, throughBucket)
}

// PublishHistoryStagingRouteBarrier is the last offline source mutation.
// verify must authenticate canonical/Finish/index/cold manifest/physical
// source and target against the frozen plan under the exclusive service latch.
func (m *HistoryStagingManager) PublishHistoryStagingRouteBarrier(ctx context.Context, barrier HistoryStagingRouteBarrier, verify func() error) error {
	if m == nil || ctx == nil || verify == nil || barrier.Version != HistoryStagingFormatVersion || barrier.PlanDigest == ([32]byte{}) || barrier.CandidateSHA == ([32]byte{}) {
		return ErrHistoryStagingConflict
	}
	m.jobMu.Lock()
	defer m.jobMu.Unlock()
	if err := verify(); err != nil {
		return err
	}
	if err := m.syncStage(); err != nil {
		return err
	}
	if err := m.syncHot(); err != nil {
		return err
	}
	epoch, err := m.CurrentEpoch()
	if err != nil || epoch != barrier.Epoch {
		return ErrHistoryStagingConflict
	}
	if intent, present, err := m.ReadResetIntent(); err != nil || (present && !intent.Complete) {
		return ErrHistoryStagingResetting
	}
	digest, err := m.scanOfflineRouteCoverage(ctx, barrier.Epoch, barrier.EligibleThrough, barrier.ThroughBucket)
	if err != nil {
		return err
	}
	barrier.RouteDigest = digest
	m.routeMu.Lock()
	if previous, present, err := m.ReadHistoryStagingRouteBarrier(); err != nil {
		m.routeMu.Unlock()
		return err
	} else if present && previous != barrier {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	err = writeHistoryStagingValue(m.hot, historyStagingRouteBarrierKey, barrier)
	if err == nil {
		m.generation++
	}
	m.routeMu.Unlock()
	if err != nil {
		return err
	}
	return m.syncHot()
}

// VerifyHistoryStagingStartup checks the immutable first-cutover marker and
// every route through the current head. Later owner changes and capable
// binary upgrades do not have to reproduce the original route digest or SHA.
// verify authenticates live target payload and pinned cold manifest content.
func (m *HistoryStagingManager) VerifyHistoryStagingStartup(ctx context.Context, headHeight uint64, verify func(uint64, HistoryStagingRoute) error) error {
	if m == nil || ctx == nil || verify == nil {
		return ErrHistoryStagingConflict
	}
	if err := m.VerifyIdentity(); err != nil {
		return err
	}
	barrier, present, err := m.ReadHistoryStagingRouteBarrier()
	if err != nil || !present {
		return ErrHistoryStagingUninitialized
	}
	epoch, err := m.CurrentEpoch()
	if err != nil {
		return err
	}
	intent, hasIntent, err := m.ReadResetIntent()
	if err != nil {
		return err
	}
	if hasIntent && !intent.Complete {
		return ErrHistoryStagingResetting
	}
	if epoch != barrier.Epoch && (!hasIntent || !intent.Complete || intent.NewEpoch != epoch || epoch < barrier.Epoch) {
		return ErrHistoryStagingConflict
	}
	headBucket := headHeight / StateHistoryChunkBucketBlocks
	if headBucket >= historyStagingMaxBarrierBuckets {
		return ErrHistoryStagingConflict
	}
	for bucket := uint64(0); bucket <= headBucket; bucket++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		route, present, err := m.ReadRoute(bucket)
		if err != nil || !present || route.Epoch != epoch {
			return ErrHistoryStagingIncomplete
		}
		switch route.Owner {
		case HistoryStagingOwnerSource:
			if route.SourceCleared || route.TargetCleared {
				return ErrHistoryStagingConflict
			}
		case HistoryStagingOwnerTarget:
			receipt, present, err := m.ReadReceiptAt(epoch, bucket)
			if err != nil || !present {
				return ErrHistoryStagingIncomplete
			}
			digest, err := historyStagingReceiptDigest(receipt)
			if err != nil || digest != route.ReceiptDigest {
				return ErrHistoryStagingIncomplete
			}
		case HistoryStagingOwnerCold:
			binding, present, err := m.ReadColdBindingAt(epoch, bucket)
			first, last, boundErr := StateHistoryChunkBucketBounds(bucket)
			if err != nil || boundErr != nil || !present || binding.BindingEpoch != route.ColdBindingEpoch || !historyStagingBindingCovers(binding, first, last) {
				return ErrHistoryStagingIncomplete
			}
		default:
			return ErrHistoryStagingConflict
		}
		if err := verify(bucket, route); err != nil {
			return err
		}
	}
	return nil
}
