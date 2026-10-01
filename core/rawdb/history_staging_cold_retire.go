package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
)

type historyStagingOldColdRefGCCursor struct {
	Version uint8
	Epoch   uint64 // current epoch whose older references were audited
	Phase   uint8  // binding refs, claim refs, complete
	After   []byte
}

// RetireQuarantinedColdRefs removes old-epoch durable dependencies after
// reset replay and external cold-tail isolation. Each call scans at most
// maxRows keys, including current-epoch rows, and persists a resume cursor.
// The final synced page records a durable done marker for hot metadata GC.
func (m *HistoryStagingManager) RetireQuarantinedColdRefs(ctx context.Context, maxRows uint64, verify func() error) (uint64, bool, error) {
	if m == nil || ctx == nil || verify == nil || maxRows == 0 || maxRows > 128 {
		return 0, false, errors.New("rawdb: invalid quarantined cold ref retirement")
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
	doneValue, donePresent, err := readPresentValue(m.hot, historyStagingOldColdRefsDoneKey(epoch), "quarantined cold refs done")
	if err != nil {
		m.routeMu.Unlock()
		return 0, false, err
	}
	if donePresent {
		m.routeMu.Unlock()
		if len(doneValue) != 1 || doneValue[0] != 1 {
			return 0, false, ErrHistoryStagingConflict
		}
		return 0, true, nil
	}
	var cursor historyStagingOldColdRefGCCursor
	cursorKey := historyStagingOldColdRefGCKey(epoch)
	hasCursor, err := readHistoryStagingValue(m.hot, cursorKey, &cursor)
	if err != nil {
		m.routeMu.Unlock()
		return 0, false, err
	}
	if !hasCursor {
		cursor = historyStagingOldColdRefGCCursor{Version: HistoryStagingFormatVersion, Epoch: epoch}
	}
	if cursor.Version != HistoryStagingFormatVersion || cursor.Epoch != epoch || cursor.Phase > 2 {
		m.routeMu.Unlock()
		return 0, false, ErrHistoryStagingConflict
	}
	batch := m.hot.NewBatch()
	defer batch.Reset()
	var deleted, scanned uint64
	prefixes := [][]byte{historyStagingColdRefPrefix, historyStagingClaimColdRefPrefix}
	for cursor.Phase < 2 && scanned < maxRows {
		prefix := prefixes[cursor.Phase]
		it := m.hot.NewIterator(prefix, cursor.After)
		exhausted := true
		for scanned < maxRows && it.Next() {
			if err := ctx.Err(); err != nil {
				it.Release()
				m.routeMu.Unlock()
				return 0, false, err
			}
			key := bytes.Clone(it.Key())
			if len(key) != len(prefix)+48 || !bytes.HasPrefix(key, prefix) || len(it.Value()) != 1 || it.Value()[0] != 1 {
				it.Release()
				m.routeMu.Unlock()
				return 0, false, ErrHistoryStagingConflict
			}
			refEpoch := binary.BigEndian.Uint64(key[len(prefix)+32:])
			if refEpoch < epoch {
				if err := batch.Delete(key); err != nil {
					it.Release()
					m.routeMu.Unlock()
					return 0, false, err
				}
				deleted++
			}
			cursor.After = append(key[len(prefix):], 0)
			scanned++
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
	if err := writeHistoryStagingValue(batch, cursorKey, cursor); err != nil {
		m.routeMu.Unlock()
		return 0, false, err
	}
	if cursor.Phase == 2 {
		if err := batch.Put(historyStagingOldColdRefsDoneKey(epoch), []byte{1}); err != nil {
			m.routeMu.Unlock()
			return 0, false, err
		}
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
	return deleted, cursor.Phase == 2, nil
}
