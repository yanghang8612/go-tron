package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
)

// ReleaseTargetToCold publishes the verified cold owner before any target
// deletion. The caller keeps its canonical/cold manifest publication guard;
// verify must authenticate full bucket coverage and semantic identity.
func (m *HistoryStagingManager) ReleaseTargetToCold(ctx context.Context, bucket uint64, verify func(HistoryStagingColdBinding) error) error {
	if m == nil || ctx == nil || verify == nil {
		return ErrHistoryStagingConflict
	}
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	binding, present, err := m.ReadColdBindingAtMustCurrent(bucket)
	if err != nil || !present {
		return ErrHistoryStagingIncomplete
	}
	first, last, err := StateHistoryChunkBucketBounds(bucket)
	if err != nil || !historyStagingBindingCovers(binding, first, last) {
		return ErrHistoryStagingIncomplete
	}
	if err := verify(binding); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.routeMu.Lock()
	current, present, err := m.ReadRoute(bucket)
	if err != nil || !present || current.Epoch != binding.Epoch || current.ColdBindingEpoch != binding.BindingEpoch || !current.SourceCleared {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	if current.Owner == HistoryStagingOwnerCold {
		m.routeMu.Unlock()
		return m.syncHot()
	}
	if current.Owner != HistoryStagingOwnerTarget || current.TargetCleared || current.WriteVersion == ^uint64(0) {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	if intent, present, err := m.ReadResetIntent(); err != nil || (present && !intent.Complete) {
		m.routeMu.Unlock()
		return ErrHistoryStagingResetting
	}
	current.Owner = HistoryStagingOwnerCold
	current.WriteVersion++
	if err := writeHistoryStagingValue(m.hot, historyStagingBucketKey(historyStagingRoutePrefix, bucket), current); err != nil {
		m.routeMu.Unlock()
		return err
	}
	m.generation++
	m.routeMu.Unlock()
	return m.syncHot()
}

func (m *HistoryStagingManager) ReadColdBindingAtMustCurrent(bucket uint64) (HistoryStagingColdBinding, bool, error) {
	epoch, err := m.CurrentEpoch()
	if err != nil {
		return HistoryStagingColdBinding{}, false, err
	}
	return m.ReadColdBindingAt(epoch, bucket)
}

// ClearColdTarget reclaims only the epoch-private target keys for one
// cold-owned bucket. It waits for all previously captured route views and
// verifies every delete batch against the durable cold owner. Retries rescan
// remaining keys; target receipt is retired only after a fresh empty scan.
func (m *HistoryStagingManager) ClearColdTarget(ctx context.Context, bucket uint64, limits HistoryStagingLimits) error {
	if m == nil || ctx == nil {
		return ErrHistoryStagingConflict
	}
	if err := limits.validate(); err != nil {
		return err
	}
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	if err := m.syncHot(); err != nil {
		return err
	}
	m.routeMu.Lock()
	route, present, err := m.ReadRoute(bucket)
	if err != nil || !present || route.Owner != HistoryStagingOwnerCold || !route.SourceCleared {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	if route.TargetCleared {
		m.routeMu.Unlock()
		if err := m.stage.Delete(historyStagingReceiptKey(route.Epoch, bucket)); err != nil {
			return err
		}
		return m.syncStage()
	}
	binding, present, err := m.ReadColdBindingAt(route.Epoch, bucket)
	first, last, boundErr := StateHistoryChunkBucketBounds(bucket)
	if err != nil || boundErr != nil || !present || binding.BindingEpoch != route.ColdBindingEpoch || !historyStagingBindingCovers(binding, first, last) {
		m.routeMu.Unlock()
		return ErrHistoryStagingIncomplete
	}
	m.leaseMu.Lock()
	hasLease := len(m.leases) != 0
	m.leaseMu.Unlock()
	m.routeMu.Unlock()
	if hasLease {
		return ErrHistoryStagingConflict
	}
	batch := m.stage.NewBatch()
	defer batch.Reset()
	var pending, work uint64
	flush := func() error {
		if pending == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if current, present, err := m.ReadRoute(bucket); err != nil || !present || current != route {
			return ErrHistoryStagingConflict
		}
		free, err := limits.FreeBytes()
		if err != nil || free < limits.MinFreeBytes {
			return errors.New("rawdb: insufficient staging GC WAL space")
		}
		if err := batch.Write(); err != nil {
			return err
		}
		batch.Reset()
		pending = 0
		return nil
	}
	queue := func(key, value []byte) error {
		cost := uint64(len(key)) + uint64(len(value))
		if cost > limits.MaxRowBytes || cost > limits.MaxBatchBytes {
			return errors.New("rawdb: bounded staging GC work exhausted")
		}
		if cost > limits.MaxWorkBytes-work {
			if err := flush(); err != nil {
				return err
			}
			return errors.New("rawdb: bounded staging GC pass complete; resume")
		}
		if pending != 0 && uint64(len(key)) > limits.MaxBatchBytes-pending {
			if err := flush(); err != nil {
				return err
			}
		}
		if err := batch.Delete(key); err != nil {
			return err
		}
		pending += uint64(len(key))
		work += cost
		return nil
	}
	for block := first; block <= last; block++ {
		key := historyStagingPayloadKey(route.Epoch, stateTxRangeKey(block))
		value, present, err := readPresentValue(m.stage, key, "staged tx range")
		if err != nil {
			return err
		}
		if present {
			if err := queue(key, value); err != nil {
				return err
			}
		}
	}
	changePrefix := historyStagingPayloadKey(route.Epoch, stateChangeSetPrefix)
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], first)
	it := m.stage.NewIterator(changePrefix, start[:])
	for it.Next() {
		key := bytes.Clone(it.Key())
		value := bytes.Clone(it.Value())
		if len(key) != len(changePrefix)+16 || !bytes.HasPrefix(key, changePrefix) {
			it.Release()
			return ErrHistoryStagingConflict
		}
		block := binary.BigEndian.Uint64(key[len(changePrefix):])
		if block > last {
			break
		}
		if err := queue(key, value); err != nil {
			it.Release()
			return err
		}
	}
	err = it.Error()
	it.Release()
	if err != nil {
		return err
	}
	chunkPrefix := historyStagingPayloadKey(route.Epoch, stateHistoryChunkBucketPrefix(bucket))
	it = m.stage.NewIterator(chunkPrefix, nil)
	for it.Next() {
		key := bytes.Clone(it.Key())
		value := bytes.Clone(it.Value())
		if len(key) != len(chunkPrefix)+32 {
			it.Release()
			return ErrHistoryStagingConflict
		}
		if err := queue(key, value); err != nil {
			it.Release()
			return err
		}
	}
	err = it.Error()
	it.Release()
	if err != nil {
		return err
	}
	metaKey := historyStagingPayloadKey(route.Epoch, stateHistoryChunkBucketKey(bucket))
	if value, present, err := readPresentValue(m.stage, metaKey, "staged bucket meta"); err != nil {
		return err
	} else if present {
		if err := queue(metaKey, value); err != nil {
			return err
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if err := m.syncStage(); err != nil {
		return err
	}
	// Reinspect every family using exact row membership before retiring the receipt.
	for block := first; block <= last; block++ {
		if ok, err := m.stage.Has(historyStagingPayloadKey(route.Epoch, stateTxRangeKey(block))); err != nil || ok {
			return ErrHistoryStagingIncomplete
		}
	}
	it = m.stage.NewIterator(changePrefix, start[:])
	if it.Next() {
		key := it.Key()
		if len(key) >= len(changePrefix)+8 && binary.BigEndian.Uint64(key[len(changePrefix):]) <= last {
			it.Release()
			return ErrHistoryStagingIncomplete
		}
	}
	err = it.Error()
	it.Release()
	if err != nil {
		return err
	}
	it = m.stage.NewIterator(chunkPrefix, nil)
	if it.Next() {
		it.Release()
		return ErrHistoryStagingIncomplete
	}
	err = it.Error()
	it.Release()
	if err != nil {
		return err
	}
	if ok, err := m.stage.Has(metaKey); err != nil || ok {
		return ErrHistoryStagingIncomplete
	}
	if err := m.stage.Delete(historyStagingReceiptKey(route.Epoch, bucket)); err != nil {
		return err
	}
	if err := m.syncStage(); err != nil {
		return err
	}
	m.routeMu.Lock()
	current, present, err := m.ReadRoute(bucket)
	if err != nil || !present || current != route {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	current.TargetCleared = true
	finish := m.hot.NewBatch()
	defer finish.Reset()
	if err := writeHistoryStagingValue(finish, historyStagingBucketKey(historyStagingRoutePrefix, bucket), current); err != nil {
		m.routeMu.Unlock()
		return err
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
	return nil
}
