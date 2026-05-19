package sync

import (
	"sync"
	"time"
)

// PauseGate is the sticky-pause flag. Set on any InsertBlock failure
// during sync; once set, all sync entry points (StartSync, watchdog
// checkIsolation, tryFindSyncPeer, drainBufferedBlocks, onPeerFetchReady)
// short-circuit until process restart. Peers are NOT disconnected —
// gtron is more likely the culprit than a peer (re-impl racing toward
// java-tron parity). Keep the connection so the operator can diagnose
// without losing peer state.
//
// PauseGate owns its own mutex. SyncService must acquire pause.mu only
// while holding ss.mu, or not at all — never the reverse. In practice,
// Enter is called outside ss.mu's critical section so the two locks
// are never co-held, and Paused/Status are short read-only calls that
// keep the gate's lock duration minimal.
type PauseGate struct {
	mu     sync.Mutex
	paused bool
	atNum  uint64
	atTime time.Time
	err    error
}

// NewPauseGate returns an unpaused gate.
func NewPauseGate() *PauseGate {
	return &PauseGate{}
}

// Paused reports whether the gate is currently engaged.
func (p *PauseGate) Paused() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused
}

// Enter latches the pause. Idempotent — subsequent calls keep the
// originally-recorded block num / time / error. The first reason wins;
// later failures don't overwrite the captured cause.
func (p *PauseGate) Enter(blockNum uint64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused {
		return
	}
	p.paused = true
	p.atNum = blockNum
	p.atTime = time.Now()
	p.err = err
}

// Status returns the gate state and (if paused) the block num, time,
// and error captured at Enter.
func (p *PauseGate) Status() (paused bool, atNum uint64, at time.Time, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused, p.atNum, p.atTime, p.err
}
