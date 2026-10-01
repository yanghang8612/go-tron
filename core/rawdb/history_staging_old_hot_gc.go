package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
)

type historyStagingOldHotGCCursor struct {
	Version uint8
	Epoch   uint64 // current epoch
	Phase   uint8  // block-complete, binding, claim, route, complete
	After   []byte
}

// RetireQuarantinedHotMetadata removes replay-obsolete hot metadata after the
// old cold references are durably retired. It preserves every current-epoch
// SOURCE completeness receipt, route, claim and binding. Both scanned rows and
// bytes are bounded per call; its cursor is synced with each deletion page.
func (m *HistoryStagingManager) RetireQuarantinedHotMetadata(ctx context.Context, maxRows, maxWorkBytes uint64, verify func() error) (uint64, bool, error) {
	if m == nil || ctx == nil || verify == nil || maxRows == 0 || maxRows > 128 || maxWorkBytes == 0 {
		return 0, false, errors.New("rawdb: invalid quarantined hot metadata retirement")
	}
	if err := verify(); err != nil {
		return 0, false, err
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	m.coldGCMu.Lock()
	defer m.coldGCMu.Unlock()
	m.routeMu.Lock()
	intent, present, err := m.ReadResetIntent()
	if err != nil || !present || !intent.Complete {
		m.routeMu.Unlock()
		return 0, false, ErrHistoryStagingResetting
	}
	epoch, err := m.CurrentEpoch()
	if err != nil || epoch != intent.NewEpoch {
		m.routeMu.Unlock()
		return 0, false, ErrHistoryStagingConflict
	}
	m.leaseMu.Lock()
	hasLease := len(m.leases) != 0
	m.leaseMu.Unlock()
	if hasLease {
		m.routeMu.Unlock()
		return 0, false, ErrHistoryStagingConflict
	}
	doneValue, present, err := readPresentValue(m.hot, historyStagingOldColdRefsDoneKey(epoch), "old cold refs done")
	if err != nil || !present || len(doneValue) != 1 || doneValue[0] != 1 {
		m.routeMu.Unlock()
		return 0, false, ErrHistoryStagingIncomplete
	}
	key := historyStagingOldHotGCKey(epoch)
	var cursor historyStagingOldHotGCCursor
	hasCursor, err := readHistoryStagingValue(m.hot, key, &cursor)
	if err != nil {
		m.routeMu.Unlock()
		return 0, false, err
	}
	if !hasCursor {
		cursor = historyStagingOldHotGCCursor{Version: HistoryStagingFormatVersion, Epoch: epoch}
	}
	if cursor.Version != HistoryStagingFormatVersion || cursor.Epoch != epoch || cursor.Phase > 4 {
		m.routeMu.Unlock()
		return 0, false, ErrHistoryStagingConflict
	}
	if cursor.Phase == 4 {
		m.routeMu.Unlock()
		return 0, true, nil
	}
	prefixes := [][]byte{historyStagingBlockCompletePrefix, historyStagingColdBindingPrefix, historyStagingClaimPrefix, historyStagingRoutePrefix}
	batch := m.hot.NewBatch()
	defer batch.Reset()
	var scanned, work, deleted uint64
	for cursor.Phase < 4 && scanned < maxRows {
		prefix := prefixes[cursor.Phase]
		it := m.hot.NewIterator(prefix, cursor.After)
		exhausted := true
		for scanned < maxRows && it.Next() {
			if err := ctx.Err(); err != nil {
				it.Release()
				m.routeMu.Unlock()
				return 0, false, err
			}
			rowKey := bytes.Clone(it.Key())
			rowValue := it.Value()
			cost := uint64(len(rowKey)) + uint64(len(rowValue))
			if cost > maxWorkBytes || cost > maxWorkBytes-work {
				if scanned == 0 {
					it.Release()
					m.routeMu.Unlock()
					return 0, false, ErrHistoryStagingIncomplete
				}
				exhausted = false
				break
			}
			if !bytes.HasPrefix(rowKey, prefix) {
				it.Release()
				m.routeMu.Unlock()
				return 0, false, ErrHistoryStagingConflict
			}
			var rowEpoch uint64
			switch cursor.Phase {
			case 0, 1:
				if len(rowKey) != len(prefix)+16 {
					it.Release()
					m.routeMu.Unlock()
					return 0, false, ErrHistoryStagingConflict
				}
				rowEpoch = binary.BigEndian.Uint64(rowKey[len(prefix):])
			case 2:
				if len(rowKey) != len(prefix)+8 {
					it.Release()
					m.routeMu.Unlock()
					return 0, false, ErrHistoryStagingConflict
				}
				var claim HistoryStagingClaim
				if err := decodeHistoryStaging(rowValue, &claim); err != nil || claim.Version != HistoryStagingFormatVersion || claim.Bucket != binary.BigEndian.Uint64(rowKey[len(prefix):]) {
					it.Release()
					m.routeMu.Unlock()
					return 0, false, ErrHistoryStagingConflict
				}
				rowEpoch = claim.Epoch
			case 3:
				if len(rowKey) != len(prefix)+8 {
					it.Release()
					m.routeMu.Unlock()
					return 0, false, ErrHistoryStagingConflict
				}
				var route HistoryStagingRoute
				if err := decodeHistoryStaging(rowValue, &route); err != nil || route.Version != HistoryStagingFormatVersion || route.Bucket != binary.BigEndian.Uint64(rowKey[len(prefix):]) {
					it.Release()
					m.routeMu.Unlock()
					return 0, false, ErrHistoryStagingConflict
				}
				rowEpoch = route.Epoch
			}
			if rowEpoch == 0 || rowEpoch > epoch {
				it.Release()
				m.routeMu.Unlock()
				return 0, false, ErrHistoryStagingConflict
			}
			if rowEpoch < epoch {
				if err := batch.Delete(rowKey); err != nil {
					it.Release()
					m.routeMu.Unlock()
					return 0, false, err
				}
				deleted++
			}
			cursor.After = append(rowKey[len(prefix):], 0)
			scanned++
			work += cost
		}
		if scanned == maxRows {
			exhausted = false
		}
		err := it.Error()
		it.Release()
		if err != nil {
			m.routeMu.Unlock()
			return 0, false, err
		}
		if !exhausted {
			break
		}
		cursor.Phase++
		cursor.After = nil
	}
	if err := writeHistoryStagingValue(batch, key, cursor); err != nil {
		m.routeMu.Unlock()
		return 0, false, err
	}
	m.coldGCUncertain = true
	if err := batch.Write(); err != nil {
		m.routeMu.Unlock()
		return 0, false, err
	}
	m.generation++
	m.routeMu.Unlock()
	if err := m.syncHot(); err != nil {
		return deleted, false, err
	}
	m.coldGCUncertain = false
	return deleted, cursor.Phase == 4, nil
}
