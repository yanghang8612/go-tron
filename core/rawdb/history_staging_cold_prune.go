package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
)

// ClearCertifiedSourceRange is the only source-owned cold-prune entrypoint
// when staging routing is active. A durable cold binding must already cover
// every height, and the caller must retain its canonical/prune writer guard.
// It deletes changesets, never tx-range/posting/directory. Bucket chunks are
// left for the existing whole-bucket shared GC after all references vanish.
func (m *HistoryStagingManager) ClearCertifiedSourceRange(ctx context.Context, bucket, from, to uint64, limits HistoryStagingLimits) error {
	if m == nil || ctx == nil || from > to {
		return errors.New("rawdb: invalid certified source clear")
	}
	if err := limits.validate(); err != nil {
		return err
	}
	first, last, err := StateHistoryChunkBucketBounds(bucket)
	if err != nil || from < first || to > last {
		return ErrHistoryStagingConflict
	}
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	if err := m.syncHot(); err != nil {
		return err
	}
	route, present, err := m.ReadRoute(bucket)
	if err != nil || !present || (route.Owner != HistoryStagingOwnerSource && route.Owner != HistoryStagingOwnerCold) {
		return ErrHistoryStagingConflict
	}
	if claim, claimed, err := m.ReadClaim(bucket); err != nil || (claimed && claim.Epoch == route.Epoch) {
		return ErrHistoryStagingConflict
	}
	binding, present, err := m.ReadColdBindingAt(route.Epoch, bucket)
	if err != nil || !present || binding.BindingEpoch != route.ColdBindingEpoch || !historyStagingBindingCovers(binding, from, to) {
		return ErrHistoryStagingIncomplete
	}
	if route.Owner == HistoryStagingOwnerCold {
		if from != first || to != last {
			return ErrHistoryStagingConflict
		}
		return m.retireCertifiedSourceChunks(ctx, bucket)
	}
	if intent, present, err := m.ReadResetIntent(); err != nil || (present && !intent.Complete) {
		return ErrHistoryStagingResetting
	}
	batch := m.hot.NewBatch()
	defer batch.Reset()
	var pending, work uint64
	flush := func() error {
		if pending == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if epoch, err := m.CurrentEpoch(); err != nil || epoch != route.Epoch {
			return ErrHistoryStagingConflict
		}
		if current, present, err := m.ReadRoute(bucket); err != nil || !present || current != route {
			return ErrHistoryStagingConflict
		}
		if free, err := limits.FreeBytes(); err != nil || free < limits.MinFreeBytes {
			return errors.New("rawdb: cold prune WAL space floor")
		}
		if err := batch.Write(); err != nil {
			return err
		}
		batch.Reset()
		pending = 0
		return nil
	}
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], from)
	it := m.hot.NewIterator(stateChangeSetPrefix, start[:])
	for it.Next() {
		key := bytes.Clone(it.Key())
		if !bytes.HasPrefix(key, stateChangeSetPrefix) || len(key) != len(stateChangeSetPrefix)+16 {
			it.Release()
			return ErrHistoryStagingConflict
		}
		block := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):])
		if block > to {
			break
		}
		cost := uint64(len(key)) + uint64(len(it.Value()))
		if cost > limits.MaxWorkBytes-work || uint64(len(key)) > limits.MaxBatchBytes {
			it.Release()
			return errors.New("rawdb: bounded cold prune work exhausted")
		}
		if pending != 0 && uint64(len(key)) > limits.MaxBatchBytes-pending {
			if err := flush(); err != nil {
				it.Release()
				return err
			}
		}
		if err := batch.Delete(key); err != nil {
			it.Release()
			return err
		}
		pending += uint64(len(key))
		work += cost
	}
	err = it.Error()
	it.Release()
	if err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	if err := m.syncHot(); err != nil {
		return err
	}
	// Fresh physical scan proves this entire range was removed before its
	// per-block completeness receipts are retired.
	it = m.hot.NewIterator(stateChangeSetPrefix, start[:])
	if it.Next() {
		key := bytes.Clone(it.Key())
		it.Release()
		if len(key) >= len(stateChangeSetPrefix)+8 && binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):]) <= to {
			return ErrHistoryStagingIncomplete
		}
	} else {
		err := it.Error()
		it.Release()
		if err != nil {
			return err
		}
	}
	m.routeMu.Lock()
	current, present, err := m.ReadRoute(bucket)
	if err != nil || !present || current != route {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	finish := m.hot.NewBatch()
	defer finish.Reset()
	for block := from; block <= to; block++ {
		if err := DeleteHistoryStagingBlockComplete(finish, route.Epoch, block); err != nil {
			m.routeMu.Unlock()
			return err
		}
	}
	if from == first && to == last && historyStagingBindingCovers(binding, first, last) {
		current.Owner = HistoryStagingOwnerCold
		current.WriteVersion++
		if err := writeHistoryStagingValue(finish, historyStagingBucketKey(historyStagingRoutePrefix, bucket), current); err != nil {
			m.routeMu.Unlock()
			return err
		}
	}
	if err := finish.Write(); err != nil {
		m.routeMu.Unlock()
		return err
	}
	m.generation++
	m.routeMu.Unlock()
	if err := m.syncHot(); err != nil {
		return err
	}
	if from == first && to == last {
		return m.retireCertifiedSourceChunks(ctx, bucket)
	}
	return nil
}

func (m *HistoryStagingManager) retireCertifiedSourceChunks(ctx context.Context, bucket uint64) error {
	meta, present, err := readPresentValue(m.hot, stateHistoryChunkBucketKey(bucket), "staging source chunk metadata")
	if err != nil {
		return err
	}
	if !present {
		it := m.hot.NewIterator(stateHistoryChunkBucketPrefix(bucket), nil)
		hasChunk := it.Next()
		err = it.Error()
		it.Release()
		if err != nil {
			return err
		}
		if hasChunk {
			return ErrHistoryStagingIncomplete
		}
		return nil
	}
	if len(meta) != 2 || meta[0] != 1 || meta[1] > 1 {
		return ErrHistoryStagingConflict
	}
	result, err := RetireStateHistoryChunkBucket(ctx, m.hot, bucket)
	if err != nil {
		return err
	}
	if result.NonEmpty {
		return ErrHistoryStagingIncomplete
	}
	return m.syncHot()
}
