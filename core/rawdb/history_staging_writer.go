package rawdb

import (
	"context"
	"errors"
)

// EnsureCanonicalSourceRoute initializes only the next, previously absent
// fixed bucket. The caller holds the canonical writer guard and calls this
// before its first block batch for that bucket. This method never changes an
// existing owner and never runs during reset replay.
func (m *HistoryStagingManager) EnsureCanonicalSourceRoute(ctx context.Context, epoch, bucket uint64) error {
	if m == nil || ctx == nil || epoch == 0 {
		return errors.New("rawdb: invalid canonical source route")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	m.routeMu.Lock()
	current, err := m.CurrentEpoch()
	if err != nil || current != epoch {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	intent, hasIntent, err := m.ReadResetIntent()
	if err != nil || (hasIntent && !intent.Complete) {
		m.routeMu.Unlock()
		return ErrHistoryStagingResetting
	}
	if route, present, err := m.ReadRoute(bucket); err != nil {
		m.routeMu.Unlock()
		return err
	} else if present {
		if route.Epoch == epoch && route.Owner == HistoryStagingOwnerSource {
			m.routeMu.Unlock()
			return nil
		}
		if !hasIntent || !intent.Complete || route.Epoch >= epoch || bucket <= intent.TargetHeight/StateHistoryChunkBucketBlocks {
			m.routeMu.Unlock()
			return ErrHistoryStagingConflict
		}
	}
	if bucket > 0 {
		if prior, present, err := m.ReadRoute(bucket - 1); err != nil || !present || prior.Epoch != epoch {
			m.routeMu.Unlock()
			return ErrHistoryStagingIncomplete
		}
	}
	route := HistoryStagingRoute{Version: HistoryStagingFormatVersion, Bucket: bucket, Epoch: epoch, WriteVersion: 1, Owner: HistoryStagingOwnerSource}
	batch := m.hot.NewBatch()
	defer batch.Reset()
	if err := writeHistoryStagingValue(batch, historyStagingBucketKey(historyStagingRoutePrefix, bucket), route); err != nil {
		m.routeMu.Unlock()
		return err
	}
	if old, present, err := m.ReadClaim(bucket); err != nil {
		m.routeMu.Unlock()
		return err
	} else if present && old.Epoch < epoch {
		if err := batch.Delete(historyStagingBucketKey(historyStagingClaimPrefix, bucket)); err != nil {
			m.routeMu.Unlock()
			return err
		}
	}
	if err := batch.Write(); err != nil {
		m.routeMu.Unlock()
		return err
	}
	m.generation++
	m.routeMu.Unlock()
	return m.syncHot()
}

// InitializeOfflineSourceRoutes seeds a bounded contiguous route page while
// the service is stopped under the persistent migration latch. It does not
// infer that missing payload is complete; barrier validation remains required.
func (m *HistoryStagingManager) InitializeOfflineSourceRoutes(ctx context.Context, epoch, fromBucket, toBucket uint64) error {
	if m == nil || ctx == nil || epoch == 0 || toBucket < fromBucket || toBucket-fromBucket >= 128 {
		return ErrHistoryStagingConflict
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	m.routeMu.Lock()
	current, err := m.CurrentEpoch()
	if err != nil || current != epoch {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	if intent, present, err := m.ReadResetIntent(); err != nil || (present && !intent.Complete) {
		m.routeMu.Unlock()
		return ErrHistoryStagingResetting
	}
	if fromBucket > 0 {
		if route, present, err := m.ReadRoute(fromBucket - 1); err != nil || !present || route.Epoch != epoch {
			m.routeMu.Unlock()
			return ErrHistoryStagingIncomplete
		}
	}
	batch := m.hot.NewBatch()
	defer batch.Reset()
	var added bool
	for bucket := fromBucket; bucket <= toBucket; bucket++ {
		if route, present, err := m.ReadRoute(bucket); err != nil {
			m.routeMu.Unlock()
			return err
		} else if present {
			if route.Epoch != epoch || route.Owner != HistoryStagingOwnerSource {
				m.routeMu.Unlock()
				return ErrHistoryStagingConflict
			}
			continue
		}
		route := HistoryStagingRoute{Version: HistoryStagingFormatVersion, Bucket: bucket, Epoch: epoch, WriteVersion: 1, Owner: HistoryStagingOwnerSource}
		if err := writeHistoryStagingValue(batch, historyStagingBucketKey(historyStagingRoutePrefix, bucket), route); err != nil {
			m.routeMu.Unlock()
			return err
		}
		added = true
	}
	if !added {
		m.routeMu.Unlock()
		return nil
	}
	if err := batch.Write(); err != nil {
		m.routeMu.Unlock()
		return err
	}
	m.generation++
	m.routeMu.Unlock()
	return m.syncHot()
}
