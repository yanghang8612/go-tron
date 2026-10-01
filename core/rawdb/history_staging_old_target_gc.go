package rawdb

import (
	"bytes"
	"context"
)

type historyStagingOldTargetGCCursor struct {
	Version uint8
	Epoch   uint64
	Phase   uint8 // payload, receipt, complete
	After   []byte
}

// ClearQuarantinedTargetEpoch incrementally removes every epoch-private
// target payload and receipt left by a completed full reset. The stage-local
// cursor is committed atomically with each delete page and synced. It never
// selects current-epoch keys. verify authenticates the post-reset canonical
// and cold-tail reconciliation under the caller's maintenance guard.
func (m *HistoryStagingManager) ClearQuarantinedTargetEpoch(ctx context.Context, oldEpoch, maxRows, maxWorkBytes uint64, verify func() error) (uint64, bool, error) {
	if m == nil || ctx == nil || verify == nil || oldEpoch == 0 || maxRows == 0 || maxRows > 4096 || maxWorkBytes == 0 {
		return 0, false, ErrHistoryStagingConflict
	}
	if err := verify(); err != nil {
		return 0, false, err
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	m.routeMu.RLock()
	intent, present, err := m.ReadResetIntent()
	epoch, epochErr := m.CurrentEpoch()
	if err != nil || epochErr != nil || !present || !intent.Complete || oldEpoch >= epoch {
		m.routeMu.RUnlock()
		return 0, false, ErrHistoryStagingResetting
	}
	m.leaseMu.Lock()
	hasLease := len(m.leases) != 0
	m.leaseMu.Unlock()
	m.routeMu.RUnlock()
	if hasLease {
		return 0, false, ErrHistoryStagingConflict
	}
	var cursor historyStagingOldTargetGCCursor
	key := historyStagingOldTargetGCKey(oldEpoch)
	hasCursor, err := readHistoryStagingValue(m.stage, key, &cursor)
	if err != nil {
		return 0, false, err
	}
	if !hasCursor {
		cursor = historyStagingOldTargetGCCursor{Version: HistoryStagingFormatVersion, Epoch: oldEpoch}
	}
	if cursor.Version != HistoryStagingFormatVersion || cursor.Epoch != oldEpoch || cursor.Phase > 2 {
		return 0, false, ErrHistoryStagingConflict
	}
	if cursor.Phase == 2 {
		return 0, true, nil
	}
	prefix := historyStagingPayloadEpochPrefix(oldEpoch)
	if cursor.Phase == 1 {
		prefix = historyStagingReceiptEpochPrefix(oldEpoch)
	}
	it := m.stage.NewIterator(prefix, cursor.After)
	defer it.Release()
	batch := m.stage.NewBatch()
	defer batch.Reset()
	var rows, work uint64
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		rowKey := bytes.Clone(it.Key())
		if !bytes.HasPrefix(rowKey, prefix) {
			return 0, false, ErrHistoryStagingConflict
		}
		cost := uint64(len(rowKey)) + uint64(len(it.Value()))
		if cost > maxWorkBytes && rows == 0 {
			return 0, false, ErrHistoryStagingIncomplete
		}
		if cost > maxWorkBytes || (rows > 0 && cost > maxWorkBytes-work) {
			break
		}
		if err := batch.Delete(rowKey); err != nil {
			return 0, false, err
		}
		cursor.After = append(bytes.Clone(rowKey[len(prefix):]), 0)
		rows++
		work += cost
		if rows == maxRows {
			break
		}
	}
	if err := it.Error(); err != nil {
		return 0, false, err
	}
	if rows == 0 {
		cursor.Phase++
		cursor.After = nil
	}
	if err := writeHistoryStagingValue(batch, key, cursor); err != nil {
		return 0, false, err
	}
	if err := batch.Write(); err != nil {
		return 0, false, err
	}
	if err := m.syncStage(); err != nil {
		return rows, false, err
	}
	return rows, cursor.Phase == 2, nil
}
