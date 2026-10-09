package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
)

// AbortClaim is for a sealed source whose canonical/cold proof can no longer
// be adopted. It durably cancels the claim before touching the target. A
// failed purge leaves source authoritative and can be retried with the same
// claim; no new claim can reuse this bucket until orphan cleanup completes.
func (m *HistoryStagingManager) AbortClaim(ctx context.Context, claim HistoryStagingClaim, limits HistoryStagingLimits) error {
	if m == nil || ctx == nil {
		return ErrHistoryStagingConflict
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := limits.validate(); err != nil {
		return err
	}
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	m.routeMu.Lock()
	current, present, err := m.ReadClaim(claim.Bucket)
	if err != nil || !present || current.Epoch != claim.Epoch || current.ClaimID != claim.ClaimID || current.ProofDigest != claim.ProofDigest {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	route, hasRoute, err := m.ReadRoute(claim.Bucket)
	if err != nil || (hasRoute && (route.Epoch != claim.Epoch || route.Owner != HistoryStagingOwnerSource)) {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	if intent, present, err := m.ReadResetIntent(); err != nil || (present && !intent.Complete) {
		m.routeMu.Unlock()
		return ErrHistoryStagingResetting
	}
	if !current.Cancelled {
		current.Cancelled = true
		if err := writeHistoryStagingValue(m.hot, historyStagingBucketKey(historyStagingClaimPrefix, claim.Bucket), current); err != nil {
			m.routeMu.Unlock()
			return err
		}
	}
	m.routeMu.Unlock()
	if err := m.syncHot(); err != nil {
		return err
	}
	return m.purgeCancelledClaim(ctx, current, limits)
}

func (m *HistoryStagingManager) purgeCancelledClaim(ctx context.Context, claim HistoryStagingClaim, limits HistoryStagingLimits) error {
	first, last, err := StateHistoryChunkBucketBounds(claim.Bucket)
	if err != nil {
		return err
	}
	// Count all orphan rows before queueing range tombstones. This bounds the
	// logical work and catches an oversized value even on a partially copied
	// target. No delete occurs if inventory is incomplete.
	var work uint64
	count := func(key, value []byte) error {
		cost := uint64(len(key)) + uint64(len(value))
		if cost > limits.MaxRowBytes || cost > limits.MaxWorkBytes-work {
			return errors.New("rawdb: cancelled claim target exceeds purge budget")
		}
		if err := limits.checkpoint(cost); err != nil {
			return err
		}
		work += cost
		return nil
	}
	for _, family := range [][]byte{stateTxRangePrefix, stateChangeSetPrefix, stateHistoryChunkBucketPrefix(claim.Bucket)} {
		prefix := historyStagingPayloadKey(claim.Epoch, family)
		var start []byte
		if bytes.Equal(family, stateTxRangePrefix) || bytes.Equal(family, stateChangeSetPrefix) {
			start = stateTxRangeKey(first)[len(stateTxRangePrefix):]
		}
		it := m.stage.NewIterator(prefix, start)
		for it.Next() {
			if err := ctx.Err(); err != nil {
				it.Release()
				return err
			}
			key := it.Key()
			if !bytes.HasPrefix(key, prefix) {
				it.Release()
				return ErrHistoryStagingConflict
			}
			if bytes.Equal(family, stateTxRangePrefix) || bytes.Equal(family, stateChangeSetPrefix) {
				if len(key) < len(prefix)+8 {
					it.Release()
					return ErrHistoryStagingConflict
				}
				block := binary.BigEndian.Uint64(key[len(prefix):])
				if block > last {
					break
				}
			}
			if err := count(key, it.Value()); err != nil {
				it.Release()
				return err
			}
		}
		err := it.Error()
		it.Release()
		if err != nil {
			return err
		}
	}
	metaKey := historyStagingPayloadKey(claim.Epoch, stateHistoryChunkBucketKey(claim.Bucket))
	if value, present, err := readPresentValue(m.stage, metaKey, "cancelled target meta"); err != nil {
		return err
	} else if present {
		if err := count(metaKey, value); err != nil {
			return err
		}
	}
	if err := limits.checkpoint(0); err != nil {
		return err
	}
	if free, err := limits.FreeBytes(); err != nil || free < limits.MinFreeBytes {
		return errors.New("rawdb: cancelled claim purge space floor")
	}
	batch := m.stage.NewBatch()
	defer batch.Reset()
	for _, family := range [][]byte{stateTxRangePrefix, stateChangeSetPrefix} {
		lower := historyStagingPayloadKey(claim.Epoch, append(bytes.Clone(family), stateTxRangeKey(first)[len(stateTxRangePrefix):]...))
		var upper []byte
		if last == ^uint64(0) {
			upper = prefixUpperBound(historyStagingPayloadKey(claim.Epoch, family))
		} else {
			upper = historyStagingPayloadKey(claim.Epoch, append(bytes.Clone(family), stateTxRangeKey(last + 1)[len(stateTxRangePrefix):]...))
		}
		if err := batch.DeleteRange(lower, upper); err != nil {
			return err
		}
	}
	chunkPrefix := historyStagingPayloadKey(claim.Epoch, stateHistoryChunkBucketPrefix(claim.Bucket))
	if err := batch.DeleteRange(chunkPrefix, prefixUpperBound(chunkPrefix)); err != nil {
		return err
	}
	if err := batch.Delete(metaKey); err != nil {
		return err
	}
	if err := batch.Delete(historyStagingReceiptKey(claim.Epoch, claim.Bucket)); err != nil {
		return err
	}
	if err := batch.Write(); err != nil {
		return err
	}
	if err := m.syncStage(); err != nil {
		return err
	}
	// A fresh target snapshot/iterator must prove no orphan row remains.
	for _, family := range [][]byte{stateTxRangePrefix, stateChangeSetPrefix, stateHistoryChunkBucketPrefix(claim.Bucket)} {
		prefix := historyStagingPayloadKey(claim.Epoch, family)
		var start []byte
		if bytes.Equal(family, stateTxRangePrefix) || bytes.Equal(family, stateChangeSetPrefix) {
			start = stateTxRangeKey(first)[len(stateTxRangePrefix):]
		}
		it := m.stage.NewIterator(prefix, start)
		if it.Next() {
			key := it.Key()
			if bytes.Equal(family, stateHistoryChunkBucketPrefix(claim.Bucket)) || (len(key) >= len(prefix)+8 && binary.BigEndian.Uint64(key[len(prefix):]) <= last) {
				it.Release()
				return ErrHistoryStagingIncomplete
			}
		}
		err := it.Error()
		it.Release()
		if err != nil {
			return err
		}
	}
	if present, err := m.stage.Has(metaKey); err != nil || present {
		return ErrHistoryStagingIncomplete
	}
	m.coldGCMu.Lock()
	defer m.coldGCMu.Unlock()
	m.routeMu.Lock()
	current, present, err := m.ReadClaim(claim.Bucket)
	if err != nil || !present || !current.Cancelled || current.ClaimID != claim.ClaimID {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	batchHot := m.hot.NewBatch()
	defer batchHot.Reset()
	if err := batchHot.Delete(historyStagingBucketKey(historyStagingClaimPrefix, claim.Bucket)); err != nil {
		m.routeMu.Unlock()
		return err
	}
	seen := make(map[[32]byte]struct{}, len(claim.Proof.ColdSpans))
	for _, span := range claim.Proof.ColdSpans {
		if _, ok := seen[span.ContentID]; ok {
			continue
		}
		seen[span.ContentID] = struct{}{}
		if err := batchHot.Delete(historyStagingClaimColdRefKey(span.ContentID, claim.Epoch, claim.Bucket)); err != nil {
			m.routeMu.Unlock()
			return err
		}
	}
	m.coldGCUncertain = true
	if err := batchHot.Write(); err != nil {
		m.routeMu.Unlock()
		return err
	}
	m.generation++
	m.routeMu.Unlock()
	if err := m.syncHot(); err != nil {
		return err
	}
	m.coldGCUncertain = false
	return nil
}
