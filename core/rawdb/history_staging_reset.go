package rawdb

import (
	"context"
	"encoding/binary"
	"errors"
)

// ReadResetIntent returns the durable reset journal. A completed journal is
// retained as an epoch fence; it is not an in-progress reset.
func (m *HistoryStagingManager) ReadResetIntent() (HistoryStagingResetIntent, bool, error) {
	var intent HistoryStagingResetIntent
	if m == nil {
		return intent, false, errors.New("rawdb: missing staging manager")
	}
	present, err := readHistoryStagingValue(m.hot, historyStagingResetIntentKey, &intent)
	if err != nil || !present {
		return intent, present, err
	}
	if intent.Version != HistoryStagingFormatVersion || intent.OldEpoch == 0 || intent.NewEpoch <= intent.OldEpoch || intent.TargetHeight == 0 || intent.TargetHash == ([32]byte{}) || intent.BlockDigest == ([32]byte{}) || intent.ReadyThrough > intent.TargetHeight {
		return HistoryStagingResetIntent{}, false, ErrHistoryStagingConflict
	}
	return intent, true, nil
}

// BeginResetIntent must be invoked only after core has verified every block
// needed for complete replay and has stopped writers/readers. The intent key
// intentionally survives ResetMutableState's mutable-prefix sweep.
func (m *HistoryStagingManager) BeginResetIntent(ctx context.Context, intent HistoryStagingResetIntent) error {
	if m == nil || ctx == nil || intent.Version != HistoryStagingFormatVersion || intent.OldEpoch == 0 || intent.NewEpoch <= intent.OldEpoch || intent.TargetHeight == 0 || intent.TargetHash == ([32]byte{}) || intent.BlockDigest == ([32]byte{}) || intent.Complete || intent.ReadyThrough != 0 {
		return errors.New("rawdb: invalid reset intent")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Drain any bounded transfer/delete phase before opening a new epoch.
	// jobMu is never acquired while holding routeMu or a Pebble iterator.
	m.jobMu.Lock()
	defer m.jobMu.Unlock()
	m.routeMu.Lock()
	current, present, err := m.ReadResetIntent()
	if err != nil {
		m.routeMu.Unlock()
		return err
	}
	if present {
		if current == intent {
			m.routeMu.Unlock()
			return nil
		}
		if !current.Complete || current.NewEpoch != intent.OldEpoch {
			m.routeMu.Unlock()
			return ErrHistoryStagingConflict
		}
	}
	epoch, err := m.CurrentEpoch()
	if err != nil || epoch != intent.OldEpoch {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	if err := writeHistoryStagingValue(m.hot, historyStagingResetIntentKey, intent); err != nil {
		m.routeMu.Unlock()
		return err
	}
	m.generation++
	m.routeMu.Unlock()
	return m.syncHot()
}

// CompleteResetIntent requires an external complete stage/root/route proof.
// verify runs before routeMu is taken, so it may acquire core's chain/index
// guards without reversing lock order. The caller must have already synced
// replay writes and kept ordinary readers blocked by RESETTING.
func (m *HistoryStagingManager) CompleteResetIntent(ctx context.Context, intent HistoryStagingResetIntent, verify func() error) error {
	if m == nil || ctx == nil || verify == nil || intent.Version != HistoryStagingFormatVersion || !intent.Complete || intent.ReadyThrough != intent.TargetHeight {
		return errors.New("rawdb: invalid reset completion")
	}
	if err := verify(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.routeMu.Lock()
	current, present, err := m.ReadResetIntent()
	if err != nil || !present || current.Complete || current.OldEpoch != intent.OldEpoch || current.NewEpoch != intent.NewEpoch || current.TargetHeight != intent.TargetHeight || current.TargetHash != intent.TargetHash || current.BlockDigest != intent.BlockDigest {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	if err := m.verifyReplayRouteCoverageLocked(intent.NewEpoch, intent.TargetHeight); err != nil {
		m.routeMu.Unlock()
		return err
	}
	batch := m.hot.NewBatch()
	defer batch.Reset()
	if err := writeHistoryStagingValue(batch, historyStagingResetIntentKey, intent); err != nil {
		m.routeMu.Unlock()
		return err
	}
	var epoch [8]byte
	binary.BigEndian.PutUint64(epoch[:], intent.NewEpoch)
	if err := batch.Put(historyStagingEpochKey, epoch[:]); err != nil {
		m.routeMu.Unlock()
		return err
	}
	if err := batch.Write(); err != nil {
		m.routeMu.Unlock()
		return err
	}
	m.generation++
	m.routeMu.Unlock()
	return m.syncHot()
}

// PrepareResetReplay checks the durable intent before the caller destroys
// mutable state. A retry after interruption uses the same intent and redoes
// genesis plus replay; no old route is made visible while RESETTING remains.
func (m *HistoryStagingManager) PrepareResetReplay(ctx context.Context, intent HistoryStagingResetIntent) error {
	if m == nil || ctx == nil {
		return errors.New("rawdb: invalid reset replay")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	current, present, err := m.ReadResetIntent()
	if err != nil || !present || current.Complete || current.OldEpoch != intent.OldEpoch || current.NewEpoch != intent.NewEpoch || current.BlockDigest != intent.BlockDigest || current.TargetHash != intent.TargetHash {
		return ErrHistoryStagingConflict
	}
	return nil
}

// CheckReplayBucketWritable is for the reset-owned writer only. The ordinary
// canonical writer always uses CheckHistoryStagingBucketWritable instead.
func (m *HistoryStagingManager) CheckReplayBucketWritable(epoch, bucket uint64) error {
	if m == nil || epoch == 0 {
		return ErrHistoryStagingConflict
	}
	m.routeMu.RLock()
	defer m.routeMu.RUnlock()
	intent, present, err := m.ReadResetIntent()
	if err != nil || !present || intent.Complete || intent.NewEpoch != epoch || bucket > intent.TargetHeight/StateHistoryChunkBucketBlocks {
		return ErrHistoryStagingResetting
	}
	return nil
}

// WriteReplaySourceRoutes publishes at most 128 route rows after the replay
// writer has synced every block through readyThrough. It advances the durable
// progress in the same hot DB batch; ordinary views remain blocked.
func (m *HistoryStagingManager) WriteReplaySourceRoutes(ctx context.Context, epoch, fromBucket, toBucket, readyThrough uint64) error {
	if m == nil || ctx == nil || epoch == 0 || toBucket < fromBucket || toBucket-fromBucket >= 128 {
		return ErrHistoryStagingConflict
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	m.routeMu.Lock()
	intent, present, err := m.ReadResetIntent()
	if err != nil || !present || intent.Complete || intent.NewEpoch != epoch || readyThrough < intent.ReadyThrough || readyThrough > intent.TargetHeight || toBucket > readyThrough/StateHistoryChunkBucketBlocks {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	batch := m.hot.NewBatch()
	defer batch.Reset()
	for bucket := fromBucket; bucket <= toBucket; bucket++ {
		if route, present, err := m.ReadRoute(bucket); err != nil {
			m.routeMu.Unlock()
			return err
		} else if present && route.Epoch == epoch && route.Owner != HistoryStagingOwnerSource {
			m.routeMu.Unlock()
			return ErrHistoryStagingConflict
		}
		route := HistoryStagingRoute{Version: HistoryStagingFormatVersion, Bucket: bucket, Epoch: epoch, WriteVersion: 1, Owner: HistoryStagingOwnerSource}
		if err := writeHistoryStagingValue(batch, historyStagingBucketKey(historyStagingRoutePrefix, bucket), route); err != nil {
			m.routeMu.Unlock()
			return err
		}
	}
	intent.ReadyThrough = readyThrough
	if err := writeHistoryStagingValue(batch, historyStagingResetIntentKey, intent); err != nil {
		m.routeMu.Unlock()
		return err
	}
	if err := batch.Write(); err != nil {
		m.routeMu.Unlock()
		return err
	}
	m.generation++
	m.routeMu.Unlock()
	return m.syncHot()
}

func (m *HistoryStagingManager) verifyReplayRouteCoverageLocked(epoch, through uint64) error {
	for bucket := uint64(0); bucket <= through/StateHistoryChunkBucketBlocks; bucket++ {
		route, present, err := m.ReadRoute(bucket)
		if err != nil || !present || route.Epoch != epoch || route.Owner != HistoryStagingOwnerSource {
			return ErrHistoryStagingIncomplete
		}
	}
	return nil
}

func (m *HistoryStagingManager) VerifyReplayRouteCoverage(epoch, through uint64) error {
	if m == nil {
		return ErrHistoryStagingConflict
	}
	m.routeMu.RLock()
	defer m.routeMu.RUnlock()
	return m.verifyReplayRouteCoverageLocked(epoch, through)
}
