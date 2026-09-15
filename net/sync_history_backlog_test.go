package net

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
)

func backlogTestSample(lag uint64) HistoryBacklogSample {
	return HistoryBacklogSample{ObservedAt: time.Now(), HeadBlock: math.MaxUint64, EligibleBlock: lag}
}

func backlogTestAdmission(t *testing.T, probe func(context.Context) (HistoryBacklogSample, error)) *historyBacklogAdmission {
	t.Helper()
	ss := &SyncService{}
	if err := ss.ConfigureHistoryBacklogAdmission(HistoryBacklogAdmissionConfig{Enabled: true, HighBlocks: 20, LowBlocks: 10, Probe: probe}); err != nil {
		t.Fatal(err)
	}
	a := ss.historyBacklog.Load()
	a.ctx, a.cancel = context.WithCancel(context.Background())
	t.Cleanup(a.cancel)
	return a
}

// Advance to another explicit observation without sleeping between unit cases.
func backlogNextCheck(a *historyBacklogAdmission) bool {
	a.mu.Lock()
	a.nextCheck = time.Time{}
	a.mu.Unlock()
	return a.check()
}

func TestHistoryBacklogAdmissionConfiguration(t *testing.T) {
	probe := func(context.Context) (HistoryBacklogSample, error) { panic("disabled probe") }
	for _, cfg := range []HistoryBacklogAdmissionConfig{
		{Enabled: true, HighBlocks: 20, LowBlocks: 10},
		{Enabled: true, HighBlocks: 20, Probe: probe},
		{Enabled: true, HighBlocks: 10, LowBlocks: 10, Probe: probe},
		{Enabled: true, HighBlocks: 10, LowBlocks: 20, Probe: probe},
	} {
		if err := (&SyncService{}).ConfigureHistoryBacklogAdmission(cfg); err == nil {
			t.Fatalf("invalid config accepted: %+v", cfg)
		}
	}
	ss := NewSyncService(makeTestChain(t), nil)
	if err := ss.ConfigureHistoryBacklogAdmission(HistoryBacklogAdmissionConfig{Probe: probe}); err != nil {
		t.Fatal(err)
	}
	ss.Start()
	t.Cleanup(ss.Stop)
	if ss.historyBacklog.Load() != nil || !ss.historyBacklogAdmissionReady() || ss.Status().HistoryBacklog.Enabled {
		t.Fatal("disabled mode acquired admission state")
	}
	if err := ss.ConfigureHistoryBacklogAdmission(HistoryBacklogAdmissionConfig{}); err == nil {
		t.Fatal("configuration after Start accepted")
	}
}

func TestHistoryBacklogAdmissionHysteresisAndProbeErrors(t *testing.T) {
	var lag uint64
	var probeErr error
	a := backlogTestAdmission(t, func(context.Context) (HistoryBacklogSample, error) {
		return backlogTestSample(lag), probeErr
	})
	for _, tc := range []struct {
		lag   uint64
		ready bool
	}{
		{19, true}, {20, false}, {19, false}, {11, false}, {10, true},
		{math.MaxUint64, false}, {0, true},
	} {
		lag = tc.lag
		if got := backlogNextCheck(a); got != tc.ready {
			t.Fatalf("lag %d: ready=%v want %v", lag, got, tc.ready)
		}
	}
	lag, probeErr = 15, errors.New("canonical lock busy")
	if backlogNextCheck(a) || !a.status.Holding || a.status.Reason != "probe_error" {
		t.Fatal("unknown observation admitted a session")
	}
	probeErr = nil
	if !backlogNextCheck(a) {
		t.Fatal("transient error invented a high-water crossing")
	}
	lag = 20
	backlogNextCheck(a)
	lag, probeErr = 15, errors.New("read failed")
	backlogNextCheck(a)
	probeErr = nil
	if backlogNextCheck(a) {
		t.Fatal("read error discarded a pre-existing high-water latch")
	}
}

func TestHistoryBacklogAdmissionRejectsInvalidSamples(t *testing.T) {
	for name, transform := range map[string]func(*HistoryBacklogSample){
		"unknown":         func(s *HistoryBacklogSample) { s.ObservedAt = time.Time{} },
		"stale":           func(s *HistoryBacklogSample) { s.ObservedAt = time.Now().Add(-time.Second) },
		"future":          func(s *HistoryBacklogSample) { s.ObservedAt = time.Now().Add(2 * time.Second) },
		"eligible ahead":  func(s *HistoryBacklogSample) { s.HeadBlock, s.EligibleBlock = 1, 2 },
		"published ahead": func(s *HistoryBacklogSample) { s.HeadBlock, s.PublishedBlock = 1, 2 },
	} {
		t.Run(name, func(t *testing.T) {
			a := backlogTestAdmission(t, func(context.Context) (HistoryBacklogSample, error) {
				s := backlogTestSample(0)
				transform(&s)
				return s, nil
			})
			if a.check() || a.status.ProbeErrors != 1 || a.status.LastError == "" {
				t.Fatal("invalid sample was accepted")
			}
		})
	}
}

func TestHistoryBacklogAdmissionResetPreservesDebtHysteresis(t *testing.T) {
	lag := uint64(20)
	a := backlogTestAdmission(t, func(context.Context) (HistoryBacklogSample, error) {
		return backlogTestSample(lag), nil
	})
	if a.check() {
		t.Fatal("high debt admitted")
	}
	a.reset()
	if !a.status.ObservedAt.IsZero() || a.status.Reason != "refresh" {
		t.Fatal("reset retained the previous observation")
	}
	lag = 15
	if backlogNextCheck(a) {
		t.Fatal("session reset bypassed the existing low-water requirement")
	}
	lag = 10
	if !backlogNextCheck(a) {
		t.Fatal("fresh low-water observation did not release the reset session")
	}
	a.reset()
	lag = 15
	if !backlogNextCheck(a) {
		t.Fatal("reset invented a new high-water latch after recovery")
	}
}

func TestHistoryBacklogAdmissionResetRejectsInflightObservation(t *testing.T) {
	entered, returned := make(chan struct{}), make(chan bool, 1)
	var calls atomic.Int32
	a := backlogTestAdmission(t, func(ctx context.Context) (HistoryBacklogSample, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-ctx.Done()
			// Even a provider returning success after cancellation cannot open
			// a new peer/session using this old low-backlog observation.
			return backlogTestSample(0), nil
		}
		return backlogTestSample(25), nil
	})
	go func() { returned <- a.check() }()
	<-entered
	a.reset()
	if <-returned {
		t.Fatal("reset accepted an old probe")
	}
	if backlogNextCheck(a) || a.status.LagBlocks != 25 || calls.Load() != 2 {
		t.Fatal("new generation did not obtain its own sample")
	}
}

func TestHistoryBacklogAdmissionRegressedCanonicalSample(t *testing.T) {
	current := HistoryBacklogSample{HeadBlock: 100, EligibleBlock: 90, PublishedBlock: 60}
	var fail bool
	a := backlogTestAdmission(t, func(context.Context) (HistoryBacklogSample, error) {
		if fail {
			return HistoryBacklogSample{}, errors.New("canonical stage mismatch")
		}
		current.ObservedAt = time.Now()
		return current, nil
	})
	if a.check() {
		t.Fatal("high debt admitted")
	}
	fail = true
	if backlogNextCheck(a) || a.status.PublishedBlock != 60 || a.status.ObservedAt.IsZero() {
		t.Fatal("failed reorg proof fabricated new coverage")
	}
	fail = false
	current = HistoryBacklogSample{HeadBlock: 60, EligibleBlock: 40, PublishedBlock: 50}
	if !backlogNextCheck(a) || a.status.LagBlocks != 0 || a.status.HeadGapBlocks != 10 {
		t.Fatal("fresh lower canonical observation underflowed or retained old heights")
	}
}

func TestHistoryBacklogAdmissionRealServiceHoldResume(t *testing.T) {
	for _, depth := range []int{0, 4} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", "4")
			bc := makeTestChain(t)
			if depth > 0 {
				bc.SetAsyncCommit(true)
			}
			ss := NewSyncService(bc, nil)
			var lag atomic.Uint64
			lag.Store(20)
			var calls atomic.Int32
			if err := ss.ConfigureHistoryBacklogAdmission(HistoryBacklogAdmissionConfig{Enabled: true, HighBlocks: 20, LowBlocks: 10,
				Probe: func(ctx context.Context) (HistoryBacklogSample, error) {
					calls.Add(1)
					_ = ss.Status() // Must not be called while ss.mu/admission.mu is held.
					// The real sampler refuses a live session lifetime gate. This
					// minimal chain may instead report history-disabled.
					if _, err := bc.SampleHistoryBacklog(ctx, 1); errors.Is(err, core.ErrHistoryBacklogBusy) {
						return HistoryBacklogSample{}, err
					}
					return backlogTestSample(lag.Load()), nil
				}}); err != nil {
				t.Fatal(err)
			}
			first := stubBlock(1, bc.CurrentBlock().Hash())
			last := stubBlock(2, first.Hash())
			for _, block := range []*types.Block{first, last} {
				if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
					t.Fatal(err)
				}
			}
			ss.mu.Lock()
			ss.initSessionLocked(time.Now())
			ss.mu.Unlock()
			ss.Start()
			t.Cleanup(ss.Stop)
			ss.drainBufferedBlocks()
			if !ss.StallRecoveryBlocked() || ss.IsPaused() || !ss.IsSyncing() || bc.CurrentBlock().Number() != 0 {
				t.Fatal("capacity hold changed canonical or sync state")
			}
			var wg sync.WaitGroup
			for i := 0; i < 30; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); ss.drainBufferedBlocks() }()
			}
			wg.Wait()
			ss.RecoverStalledFetch()
			if calls.Load() != 1 || bc.CurrentBlock().Number() != 0 || ss.Status().BufferedBlocks != 2 {
				t.Fatal("reentrant drain/watchdog escaped hold or multiplied probes")
			}
			if _, exists, err := rawdb.ReadSyncStagedBlock(bc.DB(), 2); err != nil || !exists {
				t.Fatalf("held staged body disappeared: %v %v", exists, err)
			}
			lag.Store(10)
			if !waitUntil(4*time.Second, func() bool { return bc.CurrentBlock().Number() == 2 }) {
				t.Fatalf("poller did not resume canonical import: %+v", ss.Status().HistoryBacklog)
			}
			ss.waitForDrain()
			assertSyncPipelineProgress(t, bc.DB(), last)
			if ss.StallRecoveryBlocked() || ss.IsPaused() {
				t.Fatal("completed import retained watchdog block or sticky pause")
			}
		})
	}
}

func TestHistoryBacklogAdmissionStopCancelsProbeAndRestart(t *testing.T) {
	ss := NewSyncService(makeTestChain(t), nil)
	entered := make(chan struct{})
	var once sync.Once
	if err := ss.ConfigureHistoryBacklogAdmission(HistoryBacklogAdmissionConfig{Enabled: true, HighBlocks: 20, LowBlocks: 10,
		Probe: func(ctx context.Context) (HistoryBacklogSample, error) {
			once.Do(func() { close(entered) })
			<-ctx.Done()
			return HistoryBacklogSample{}, ctx.Err()
		}}); err != nil {
		t.Fatal(err)
	}
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()
	ss.Start()
	drainDone := make(chan struct{})
	go func() { ss.drainBufferedBlocks(); close(drainDone) }()
	<-entered
	stopped := make(chan struct{})
	go func() { ss.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not cancel and join the probe/drain")
	}
	<-drainDone
	ss.Stop()
	ss.Start()
	ss.Stop()
	if a := ss.historyBacklog.Load(); a.cancel != nil || a.done != nil || a.checking {
		t.Fatal("Stop/restart leaked an admission worker")
	}
}

func TestHistoryBacklogAdmissionStopQuiescesPollerOwnedDrain(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	if err := ss.SetImportBatchSize(2); err != nil {
		t.Fatal(err)
	}
	var lag atomic.Uint64
	lag.Store(20)
	if err := ss.ConfigureHistoryBacklogAdmission(HistoryBacklogAdmissionConfig{Enabled: true, HighBlocks: 20, LowBlocks: 10,
		Probe: func(context.Context) (HistoryBacklogSample, error) { return backlogTestSample(lag.Load()), nil }}); err != nil {
		t.Fatal(err)
	}
	parent := bc.CurrentBlock().Hash()
	for n := int64(1); n <= 6; n++ {
		block := stubBlock(n, parent)
		if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
			t.Fatal(err)
		}
		parent = block.Hash()
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	bc.AddBlockHook(func(block *types.Block) {
		if block.Number() == 1 {
			close(entered)
			<-release
		}
	})
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()
	ss.Start()
	t.Cleanup(func() { unblock(); ss.Stop() })
	ss.drainBufferedBlocks() // Establish hold; only the real poller resumes it.
	lag.Store(10)
	select {
	case <-entered:
	case <-time.After(4 * time.Second):
		t.Fatal("poller did not enter the actual import batch")
	}
	stopped := make(chan struct{})
	go func() { ss.Stop(); close(stopped) }()
	if !waitUntil(2*time.Second, func() bool { return !ss.IsSyncing() }) {
		t.Error("Stop joined the poller before quiescing its active session")
	}
	if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), 6); err != nil || !ok {
		t.Errorf("Stop deleted staged bodies before the import barrier: %v %v", ok, err)
	}
	unblock()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not join the poller after the selected batch finished")
	}
	if got := bc.CurrentBlock().Number(); got != 2 {
		t.Fatalf("Stop imported beyond its already selected two-block batch: head=%d", got)
	}
	assertSyncPipelineProgress(t, bc.DB(), bc.CurrentBlock())
}

func TestHistoryBacklogAdmissionResetWaitsForActiveSessionBarrier(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	var calls atomic.Int32
	if err := ss.ConfigureHistoryBacklogAdmission(HistoryBacklogAdmissionConfig{Enabled: true, HighBlocks: 20, LowBlocks: 10,
		Probe: func(context.Context) (HistoryBacklogSample, error) {
			calls.Add(1)
			return backlogTestSample(20), nil
		}}); err != nil {
		t.Fatal(err)
	}
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()
	// A reset can occur while the old drain still owns a real executor. Its
	// marker remains set until Finish releases the lifetime gate.
	session := bc.BeginSyncInsertSession()
	ss.drainMu.Lock()
	ss.draining = true
	ss.drainMu.Unlock()
	ss.Start()
	ss.mu.Lock()
	ss.doReset()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()
	time.Sleep(1100 * time.Millisecond)
	if calls.Load() != 0 {
		t.Error("recovery poll probed while the previous session remained active")
	}
	if err := session.Finish(); err != nil {
		t.Error(err)
	}
	ss.drainMu.Lock()
	ss.draining = false
	ss.drainCond.Broadcast()
	ss.drainMu.Unlock()
	t.Cleanup(ss.Stop)
	if !waitUntil(2*time.Second, func() bool { return calls.Load() > 0 }) {
		t.Fatal("poller did not observe again after the full session barrier")
	}
}

func TestHistoryBacklogAdmissionStopHeightTakesPrecedence(t *testing.T) {
	ss := NewSyncService(makeTestChain(t), nil)
	var calls atomic.Int32
	if err := ss.ConfigureHistoryBacklogAdmission(HistoryBacklogAdmissionConfig{Enabled: true, HighBlocks: 20, LowBlocks: 10,
		Probe: func(context.Context) (HistoryBacklogSample, error) {
			calls.Add(1)
			return backlogTestSample(20), nil
		}}); err != nil {
		t.Fatal(err)
	}
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()
	ss.SetStopAtHeight(0)
	ss.Start()
	t.Cleanup(ss.Stop)
	ss.drainBufferedBlocks()
	ss.RecoverStalledFetch()
	if calls.Load() != 0 || !ss.IsPaused() {
		t.Fatal("admission overrode the existing sticky stop-height pause")
	}
	_, _, _, err := ss.PausedStatus()
	if !errors.Is(err, ErrSyncStopHeightReached) {
		t.Fatalf("pause error replaced: %v", err)
	}
}
