package snapshots

import (
	"errors"
	"os"
	"sync"
)

// Physical retirement and history readers share this gate. A read lease pins
// the complete immutable manifest view until the query/build releases it.
// Retirement never waits for a potentially long query and can retry later.
var historyReadLeaseGate sync.RWMutex

var ErrHistoryReadLeaseBusy = errors.New("snapshots: history file retirement in progress")

// PinHistoryReadView fixes both the cold manifest used by the reader and its
// file-retention lease. It deliberately does not fall back to a changing
// Manager: a missing manifest means no cold history is yet published.
func (m *Manager) PinHistoryReadView() (*Manager, func(), error) {
	if m == nil {
		return nil, nil, errors.New("snapshots: nil history manager")
	}
	if !historyReadLeaseGate.TryRLock() {
		return nil, nil, ErrHistoryReadLeaseBusy
	}
	release := sync.OnceFunc(historyReadLeaseGate.RUnlock)
	manifest, err := m.currentManifest()
	if os.IsNotExist(err) {
		return nil, release, nil
	}
	if err != nil {
		release()
		return nil, nil, err
	}
	if manifest == nil {
		return nil, release, nil
	}
	pinned, err := openPinnedManager(m.dir, manifest, m.chainVerificationCache)
	if err != nil {
		release()
		return nil, nil, err
	}
	return pinned, release, nil
}

// tryHistoryRetirement serializes every physical deletion of a published
// history segment with in-process read leases. A busy result is a harmless
// deferral: the retired metadata and files remain durable for a later pass.
func tryHistoryRetirement() (func(), bool) {
	if !historyReadLeaseGate.TryLock() {
		return nil, false
	}
	return historyReadLeaseGate.Unlock, true
}
