package sync

import (
	"time"

	"github.com/tronprotocol/go-tron/p2p"
)

// DefaultWatchdogInterval is the cadence at which the watchdog polls the
// chain for stall. Matches the literal 30s ticker the pre-refactor
// SyncService.watchdog goroutine used.
const DefaultWatchdogInterval = 30 * time.Second

// DefaultStallThreshold is how long the chain head must have sat unchanged
// before the watchdog kicks off a poll. Mirrors the pre-refactor literal
// `time.Since(ss.chain.LastInsertTime()) < 30*time.Second` check.
const DefaultStallThreshold = 30 * time.Second

// PeerSource is the subset of TronHandler the watchdog uses to find a peer
// to poll when the chain is stalled.
type PeerSource interface {
	// BestSyncCandidate returns the highest-head handshaked peer above our
	// own head, excluding `exclude` when non-nil.
	BestSyncCandidate(exclude *p2p.Peer) *p2p.Peer
	// HandshakedPeers returns every handshaked peer; used as a fallback when
	// no peer advertises a strictly-higher head (java-tron's AdvService
	// gates INVENTORY behind a ready-check so cached headNums may lag).
	HandshakedPeers() []*p2p.Peer
}

// ChainStatus reports head and last-insert time. *core.BlockChain does not
// satisfy this directly because its head accessor returns *types.Block
// (CurrentBlock().Number()); net/sync.go adapts it with a thin shim so this
// package stays free of core/types imports.
type ChainStatus interface {
	LastInsertTime() time.Time
	CurrentBlockNum() uint64
}

// PauseStatus reports whether sync is currently paused. *PauseGate satisfies
// this via its Paused() method.
type PauseStatus interface {
	Paused() bool
}

// SyncStarter is what the watchdog calls when it finds a candidate during a
// stall. *SyncService satisfies this via StartSync + IsSyncing.
type SyncStarter interface {
	StartSync(peer *p2p.Peer)
	IsSyncing() bool
}

// StallLogger formats the "Polling peer (chain stalled)" log line. Injectable
// so the watchdog can stay package-internal yet still emit through the
// hosting package's logger. nil is allowed (silent watchdog, for tests).
type StallLogger func(peer *p2p.Peer, head uint64, stalledFor time.Duration)

// Watchdog periodically checks whether the chain is stalled and, if so,
// kicks off a sync against the best candidate peer. Owns its own goroutine
// and ticker; Start launches them, Stop joins.
//
// The Interval and StallThreshold fields are exposed so tests can shrink
// them without sleeping for 30s. Production callers leave them at the
// defaults set by NewWatchdog.
type Watchdog struct {
	chain   ChainStatus
	peers   PeerSource
	pause   PauseStatus
	starter SyncStarter
	logf    StallLogger

	// Interval is the ticker cadence. Defaults to DefaultWatchdogInterval.
	Interval time.Duration
	// StallThreshold is how long the chain must have been idle before a
	// poll fires. Defaults to DefaultStallThreshold.
	StallThreshold time.Duration

	quit chan struct{}
	done chan struct{}
}

// NewWatchdog constructs a Watchdog wired against the production
// dependencies. logf may be nil (silent watchdog, useful in tests).
func NewWatchdog(chain ChainStatus, peers PeerSource, pause PauseStatus, starter SyncStarter, logf StallLogger) *Watchdog {
	return &Watchdog{
		chain:          chain,
		peers:          peers,
		pause:          pause,
		starter:        starter,
		logf:           logf,
		Interval:       DefaultWatchdogInterval,
		StallThreshold: DefaultStallThreshold,
	}
}

// Start launches the watchdog goroutine. Idempotent — calling Start twice
// without an intervening Stop is a no-op on the second call.
func (w *Watchdog) Start() {
	if w.quit != nil {
		return
	}
	w.quit = make(chan struct{})
	w.done = make(chan struct{})
	go w.loop()
}

// Stop signals the goroutine to exit and waits for it to acknowledge.
// Safe to call multiple times; Stop on an unstarted watchdog is a no-op.
func (w *Watchdog) Stop() {
	if w.quit == nil {
		return
	}
	select {
	case <-w.quit:
		// already stopped
	default:
		close(w.quit)
	}
	<-w.done
	w.quit = nil
	w.done = nil
}

func (w *Watchdog) loop() {
	defer close(w.done)
	ticker := time.NewTicker(w.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.checkIsolation()
		case <-w.quit:
			return
		}
	}
}

// checkIsolation starts a sync if we are not already syncing and the chain
// head has not advanced past StallThreshold. Tries BestSyncCandidate first
// (peer with strictly-higher advertised head) and falls back to any
// handshaked peer — java-tron's AdvService does not advertise new blocks
// via INVENTORY until it considers our peer "ready", so the peer's cached
// headNum can lag arbitrarily behind reality. Polling BuildChainSummary
// against any peer lets java-tron re-evaluate.
func (w *Watchdog) checkIsolation() {
	if w.starter == nil || w.starter.IsSyncing() {
		return
	}
	if w.pause != nil && w.pause.Paused() {
		return
	}
	if w.chain == nil || w.peers == nil {
		return
	}
	stalledFor := time.Since(w.chain.LastInsertTime())
	if stalledFor < w.StallThreshold {
		return
	}
	candidate := w.peers.BestSyncCandidate(nil)
	if candidate == nil {
		// Fall back: any handshaked peer. java-tron will respond with an
		// empty CHAIN_INVENTORY if we're already at head, so this is cheap.
		if peers := w.peers.HandshakedPeers(); len(peers) > 0 {
			candidate = peers[0]
		}
	}
	if candidate == nil {
		return
	}
	if w.logf != nil {
		w.logf(candidate, w.chain.CurrentBlockNum(), stalledFor)
	}
	w.starter.StartSync(candidate)
}
