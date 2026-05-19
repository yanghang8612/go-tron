package sync

import (
	"sync"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/p2p"
)

// fakeChain implements ChainStatus.
type fakeChain struct {
	mu          sync.Mutex
	lastInsert  time.Time
	currentHead uint64
}

func (f *fakeChain) LastInsertTime() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastInsert
}

func (f *fakeChain) CurrentBlockNum() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.currentHead
}

func (f *fakeChain) setLastInsert(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastInsert = t
}

// fakePeerSource implements PeerSource.
type fakePeerSource struct {
	mu       sync.Mutex
	best     *p2p.Peer
	fallback []*p2p.Peer
}

func (f *fakePeerSource) BestSyncCandidate(exclude *p2p.Peer) *p2p.Peer {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.best != nil && f.best == exclude {
		return nil
	}
	return f.best
}

func (f *fakePeerSource) HandshakedPeers() []*p2p.Peer {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*p2p.Peer, len(f.fallback))
	copy(out, f.fallback)
	return out
}

// fakePauseStatus implements PauseStatus.
type fakePauseStatus struct {
	mu     sync.Mutex
	paused bool
}

func (f *fakePauseStatus) Paused() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.paused
}

func (f *fakePauseStatus) setPaused(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paused = v
}

// fakeStarter implements SyncStarter and records every StartSync call.
type fakeStarter struct {
	mu      sync.Mutex
	calls   []*p2p.Peer
	syncing bool
}

func (f *fakeStarter) StartSync(peer *p2p.Peer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, peer)
}

func (f *fakeStarter) IsSyncing() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.syncing
}

func (f *fakeStarter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeStarter) lastPeer() *p2p.Peer {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return nil
	}
	return f.calls[len(f.calls)-1]
}

// newTestWatchdog builds a watchdog with sub-millisecond timers so tests can
// observe checkIsolation firing without waiting 30 s. The caller is
// responsible for invoking Stop.
func newTestWatchdog(t *testing.T, chain ChainStatus, peers PeerSource, pause PauseStatus, starter SyncStarter) *Watchdog {
	t.Helper()
	w := NewWatchdog(chain, peers, pause, starter, nil)
	w.Interval = 2 * time.Millisecond
	w.StallThreshold = 5 * time.Millisecond
	return w
}

// TestWatchdog_DirectCheckIsolation_NoTickerNeeded drives checkIsolation
// directly so the goroutine + ticker plumbing isn't on the critical test
// path. Each subtest sets up a different combination of inputs and asserts
// whether StartSync fires.
func TestWatchdog_DirectCheckIsolation_NoTickerNeeded(t *testing.T) {
	dummyPeer := &p2p.Peer{}
	fallbackPeer := &p2p.Peer{}

	t.Run("stall+candidate→start", func(t *testing.T) {
		chain := &fakeChain{lastInsert: time.Now().Add(-time.Hour)}
		peers := &fakePeerSource{best: dummyPeer}
		pause := &fakePauseStatus{}
		starter := &fakeStarter{}
		w := newTestWatchdog(t, chain, peers, pause, starter)

		w.checkIsolation()
		if starter.callCount() != 1 {
			t.Fatalf("StartSync calls = %d, want 1", starter.callCount())
		}
		if starter.lastPeer() != dummyPeer {
			t.Fatal("StartSync called with wrong peer")
		}
	})

	t.Run("recent-insert→no-start", func(t *testing.T) {
		chain := &fakeChain{lastInsert: time.Now()}
		peers := &fakePeerSource{best: dummyPeer}
		pause := &fakePauseStatus{}
		starter := &fakeStarter{}
		w := newTestWatchdog(t, chain, peers, pause, starter)

		w.checkIsolation()
		if starter.callCount() != 0 {
			t.Fatalf("StartSync calls = %d, want 0", starter.callCount())
		}
	})

	t.Run("paused→no-start", func(t *testing.T) {
		chain := &fakeChain{lastInsert: time.Now().Add(-time.Hour)}
		peers := &fakePeerSource{best: dummyPeer}
		pause := &fakePauseStatus{paused: true}
		starter := &fakeStarter{}
		w := newTestWatchdog(t, chain, peers, pause, starter)

		w.checkIsolation()
		if starter.callCount() != 0 {
			t.Fatalf("paused: StartSync calls = %d, want 0", starter.callCount())
		}
	})

	t.Run("syncing→no-start", func(t *testing.T) {
		chain := &fakeChain{lastInsert: time.Now().Add(-time.Hour)}
		peers := &fakePeerSource{best: dummyPeer}
		pause := &fakePauseStatus{}
		starter := &fakeStarter{syncing: true}
		w := newTestWatchdog(t, chain, peers, pause, starter)

		w.checkIsolation()
		if starter.callCount() != 0 {
			t.Fatalf("syncing: StartSync calls = %d, want 0", starter.callCount())
		}
	})

	t.Run("no-candidate→no-start", func(t *testing.T) {
		chain := &fakeChain{lastInsert: time.Now().Add(-time.Hour)}
		peers := &fakePeerSource{}
		pause := &fakePauseStatus{}
		starter := &fakeStarter{}
		w := newTestWatchdog(t, chain, peers, pause, starter)

		w.checkIsolation()
		if starter.callCount() != 0 {
			t.Fatalf("no-candidate: StartSync calls = %d, want 0", starter.callCount())
		}
	})

	t.Run("falls-back-to-handshaked-peer", func(t *testing.T) {
		chain := &fakeChain{lastInsert: time.Now().Add(-time.Hour)}
		peers := &fakePeerSource{fallback: []*p2p.Peer{fallbackPeer}}
		pause := &fakePauseStatus{}
		starter := &fakeStarter{}
		w := newTestWatchdog(t, chain, peers, pause, starter)

		w.checkIsolation()
		if starter.callCount() != 1 {
			t.Fatalf("fallback: StartSync calls = %d, want 1", starter.callCount())
		}
		if starter.lastPeer() != fallbackPeer {
			t.Fatal("fallback: StartSync called with wrong peer")
		}
	})
}

// TestWatchdog_StartStopLifecycle confirms the goroutine path actually
// triggers checkIsolation when started. The ticker fires every 2ms; we wait
// up to 200ms for the first StartSync call and then assert Stop joins.
func TestWatchdog_StartStopLifecycle(t *testing.T) {
	dummyPeer := &p2p.Peer{}
	chain := &fakeChain{lastInsert: time.Now().Add(-time.Hour)}
	peers := &fakePeerSource{best: dummyPeer}
	pause := &fakePauseStatus{}
	starter := &fakeStarter{}

	w := newTestWatchdog(t, chain, peers, pause, starter)
	w.Start()

	deadline := time.Now().Add(500 * time.Millisecond)
	for starter.callCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	w.Stop()

	if starter.callCount() < 1 {
		t.Fatalf("after Start+200ms, StartSync was not called")
	}

	// After Stop the goroutine has joined. A subsequent Stop must be a
	// no-op (no panic on double-close).
	w.Stop()
}

// TestWatchdog_StopWithoutStart is a no-op and must not panic.
func TestWatchdog_StopWithoutStart(t *testing.T) {
	chain := &fakeChain{}
	peers := &fakePeerSource{}
	pause := &fakePauseStatus{}
	starter := &fakeStarter{}
	w := newTestWatchdog(t, chain, peers, pause, starter)
	w.Stop() // must not panic / block
}

// TestWatchdog_StallLoggerInvoked confirms the optional StallLogger callback
// fires once per stall poll with the expected arguments.
func TestWatchdog_StallLoggerInvoked(t *testing.T) {
	dummyPeer := &p2p.Peer{}
	chain := &fakeChain{lastInsert: time.Now().Add(-time.Hour), currentHead: 12345}
	peers := &fakePeerSource{best: dummyPeer}
	pause := &fakePauseStatus{}
	starter := &fakeStarter{}

	var (
		mu          sync.Mutex
		lastPeer    *p2p.Peer
		lastHead    uint64
		lastStalled time.Duration
		hits        int
	)
	logf := func(peer *p2p.Peer, head uint64, stalledFor time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		hits++
		lastPeer = peer
		lastHead = head
		lastStalled = stalledFor
	}
	w := NewWatchdog(chain, peers, pause, starter, logf)
	w.Interval = 2 * time.Millisecond
	w.StallThreshold = 5 * time.Millisecond

	w.checkIsolation()

	mu.Lock()
	defer mu.Unlock()
	if hits != 1 {
		t.Fatalf("logger hits = %d, want 1", hits)
	}
	if lastPeer != dummyPeer {
		t.Fatal("logger received wrong peer")
	}
	if lastHead != 12345 {
		t.Fatalf("logger head = %d, want 12345", lastHead)
	}
	if lastStalled <= 0 {
		t.Fatalf("logger stalledFor = %v, want > 0", lastStalled)
	}
}
