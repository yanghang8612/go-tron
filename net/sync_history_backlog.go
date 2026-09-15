package net

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

const historyBacklogPollInterval = time.Second

// Disabled mode exposes zero gauges, rather than omitting the schema. These
// observations are updated from admission state; reading them never probes.
var historyBacklogGauges = func() map[string]*metrics.Gauge {
	out := make(map[string]*metrics.Gauge)
	for _, name := range []string{"enabled", "holding", "lag", "high", "low", "head_gap", "probe_errors", "checks"} {
		out[name] = metrics.GetOrRegisterGauge("sync/history_backlog/"+name, nil)
	}
	return out
}()

// HistoryBacklogSample is a fresh canonical scheduling observation. Published
// is the durable cold-build watermark, not permission to delete hot history.
type HistoryBacklogSample struct {
	ObservedAt     time.Time
	HeadBlock      uint64
	EligibleBlock  uint64
	PublishedBlock uint64
}

// HistoryBacklogAdmissionConfig is opt-in and immutable after service Start.
// Probe must support cooperative cancellation; it must not wait for chainmu.
// The context deadline cannot interrupt an already running storage Get.
type HistoryBacklogAdmissionConfig struct {
	Enabled    bool
	HighBlocks uint64
	LowBlocks  uint64
	Probe      func(context.Context) (HistoryBacklogSample, error)
}

// HistoryBacklogAdmissionStatus keeps intentional capacity holds separate from
// sticky import errors, missing peers, and the head-minus-cold coverage gap.
type HistoryBacklogAdmissionStatus struct {
	Enabled         bool
	Holding         bool
	Reason          string
	HighBlocks      uint64
	LowBlocks       uint64
	HeadBlock       uint64
	EligibleBlock   uint64
	PublishedBlock  uint64
	LagBlocks       uint64
	HeadGapBlocks   uint64
	ObservedAt      time.Time
	CheckedAt       time.Time
	Since           time.Time
	LastError       string
	Checks          uint64
	ProbeErrors     uint64
	HoldTransitions uint64
}

type historyBacklogAdmission struct {
	mu          sync.Mutex
	cfg         HistoryBacklogAdmissionConfig
	status      HistoryBacklogAdmissionStatus
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	probeCancel context.CancelFunc
	generation  uint64
	checking    bool
	latched     bool
	nextCheck   time.Time
}

// ConfigureHistoryBacklogAdmission must run before Start. Disabled mode has no
// probe, timer or allocation on the import path. Production watermarks must be
// explicitly provided; no observed historical rate silently enables the gate.
func (ss *SyncService) ConfigureHistoryBacklogAdmission(cfg HistoryBacklogAdmissionConfig) error {
	if ss == nil {
		return errors.New("history backlog admission: nil sync service")
	}
	if ss.historyBacklogStarted.Load() {
		return errors.New("history backlog admission: configure before Start")
	}
	if !cfg.Enabled {
		ss.historyBacklog.Store(nil)
		updateHistoryBacklogMetrics(HistoryBacklogAdmissionStatus{})
		return nil
	}
	if cfg.Probe == nil || cfg.LowBlocks == 0 || cfg.LowBlocks >= cfg.HighBlocks {
		return errors.New("history backlog admission: enabled mode requires a probe and 0 < low < high")
	}
	a := &historyBacklogAdmission{cfg: cfg}
	a.status = HistoryBacklogAdmissionStatus{Enabled: true, HighBlocks: cfg.HighBlocks, LowBlocks: cfg.LowBlocks}
	updateHistoryBacklogMetrics(a.status)
	ss.historyBacklog.Store(a)
	return nil
}

func (ss *SyncService) historyBacklogStatus() HistoryBacklogAdmissionStatus {
	if a := ss.historyBacklog.Load(); a != nil {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.status
	}
	return HistoryBacklogAdmissionStatus{}
}

func (ss *SyncService) historyBacklogHolding() bool {
	if a := ss.historyBacklog.Load(); a != nil {
		// Never take ss.mu under the admission mutex; reset/status have the
		// opposite order. A disconnected session still needs its watchdog.
		return ss.IsSyncing() && ss.historyBacklogStatus().Holding
	}
	return false
}

func (ss *SyncService) historyBacklogAdmissionReady() bool {
	a := ss.historyBacklog.Load()
	if a == nil {
		return true
	}
	if ss.stopping.Load() {
		return false
	}
	if !ss.IsSyncing() || ss.pause.Paused() {
		return true // Existing idle/pause handling still owns these paths.
	}
	return a.check()
}

func (a *historyBacklogAdmission) start(ss *SyncService) {
	a.mu.Lock()
	if a.cancel != nil {
		a.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	a.ctx, a.cancel, a.done = ctx, cancel, done
	a.resetLocked()
	a.mu.Unlock()
	go func() {
		defer close(done)
		ticker := time.NewTicker(historyBacklogPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ss.stopping.Load() || !ss.IsSyncing() || ss.pause.Paused() {
					continue
				}
				ss.drainMu.Lock()
				draining := ss.draining
				ss.drainMu.Unlock()
				if draining {
					// Reset may invalidate admission while an older session is
					// still finishing. Its owner must reach the barrier first.
					continue
				}
				a.mu.Lock()
				holding := a.status.Holding
				a.mu.Unlock()
				if holding && a.check() && !ss.stopping.Load() {
					// The existing drain gate coalesces this with all peer wakes.
					// A new session performs another fresh admission check.
					ss.drainBufferedBlocks()
				}
			}
		}
	}()
}

func (a *historyBacklogAdmission) stop() {
	a.mu.Lock()
	cancel, done := a.cancel, a.done
	if cancel == nil {
		a.mu.Unlock()
		return
	}
	cancel()
	a.resetLocked()
	a.status.Holding, a.status.Reason = false, "stopped"
	updateHistoryBacklogMetrics(a.status)
	a.mu.Unlock()
	<-done
	a.mu.Lock()
	if a.done == done {
		a.ctx, a.cancel, a.done = nil, nil, nil
	}
	a.mu.Unlock()
}

func (a *historyBacklogAdmission) reset() {
	a.mu.Lock()
	a.resetLocked()
	a.mu.Unlock()
}

func (a *historyBacklogAdmission) resetLocked() {
	a.generation++
	if a.probeCancel != nil {
		a.probeCancel()
	}
	// A peer/session reset invalidates observations, not already observed
	// debt. Only a fresh sample at/below low releases a high-water latch.
	a.nextCheck = time.Time{}
	// A canceled check remains physically in flight until it returns. Keeping
	// checking set prevents a reset from creating another concurrent callback.
	a.status.Holding, a.status.Reason = a.ctx != nil && a.ctx.Err() == nil, "refresh"
	if !a.status.Holding {
		a.status.Reason = "stopped"
	}
	a.status.HeadBlock, a.status.EligibleBlock, a.status.PublishedBlock = 0, 0, 0
	a.status.LagBlocks, a.status.HeadGapBlocks = 0, 0
	a.status.ObservedAt, a.status.CheckedAt = time.Time{}, time.Time{}
	a.status.Since, a.status.LastError = time.Now(), ""
	updateHistoryBacklogMetrics(a.status)
}

// check is called only before a session or by the sole hold poller. It owns no
// caller locks while probing. Reentrant drain wakes cannot multiply checks.
func (a *historyBacklogAdmission) check() bool {
	a.mu.Lock()
	started := time.Now()
	if a.ctx == nil || a.ctx.Err() != nil || a.checking || a.status.Holding && started.Before(a.nextCheck) {
		a.mu.Unlock()
		return false
	}
	ctx, cancel := context.WithTimeout(a.ctx, historyBacklogPollInterval)
	generation := a.generation
	a.checking, a.probeCancel = true, cancel
	a.status.Checks++
	a.mu.Unlock()

	sample, err := a.cfg.Probe(ctx)
	finished := time.Now()
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = validateHistoryBacklogSample(sample, started, finished)
	}
	cancel()
	a.mu.Lock()
	a.checking, a.probeCancel = false, nil
	if generation != a.generation || a.ctx == nil || a.ctx.Err() != nil {
		a.mu.Unlock()
		return false
	}
	previousHolding, previousReason := a.status.Holding, a.status.Reason
	a.status.CheckedAt = finished
	a.nextCheck = finished.Add(historyBacklogPollInterval)
	if err != nil {
		// Unknown capacity cannot admit work, but a transient failed probe must
		// not invent a high-water crossing. Preserve an existing debt latch.
		a.status.Holding = true
		a.status.Reason, a.status.LastError = "probe_error", err.Error()
		a.status.ProbeErrors++
	} else {
		lag := uint64(0)
		if sample.EligibleBlock > sample.PublishedBlock {
			lag = sample.EligibleBlock - sample.PublishedBlock
		}
		a.latched = lag >= a.cfg.HighBlocks || a.latched && lag > a.cfg.LowBlocks
		a.status.Holding, a.status.Reason = a.latched, "ready"
		if a.latched {
			a.status.Reason = "backlog"
		}
		a.status.LastError = ""
		a.status.HeadBlock, a.status.EligibleBlock, a.status.PublishedBlock = sample.HeadBlock, sample.EligibleBlock, sample.PublishedBlock
		a.status.LagBlocks, a.status.HeadGapBlocks = lag, sample.HeadBlock-sample.PublishedBlock
		a.status.ObservedAt = sample.ObservedAt
	}
	if !previousHolding && a.status.Holding {
		a.status.Since = finished
		a.status.HoldTransitions++
	} else if !a.status.Holding {
		a.status.Since = time.Time{}
	}
	status := a.status
	updateHistoryBacklogMetrics(status)
	changed := previousHolding != status.Holding || previousReason != status.Reason
	a.mu.Unlock()
	if changed {
		syncLog.Info("History backlog admission changed", "holding", status.Holding,
			"reason", status.Reason, "eligible", status.EligibleBlock, "published", status.PublishedBlock,
			"lag", status.LagBlocks, "high", status.HighBlocks, "low", status.LowBlocks, "err", status.LastError)
	}
	return !status.Holding
}

func validateHistoryBacklogSample(sample HistoryBacklogSample, started, finished time.Time) error {
	if sample.ObservedAt.IsZero() || sample.ObservedAt.Before(started) || sample.ObservedAt.After(finished.Add(time.Second)) {
		return errors.New("history backlog admission: sample must be fresh within the probe")
	}
	if sample.EligibleBlock > sample.HeadBlock || sample.PublishedBlock > sample.HeadBlock {
		return fmt.Errorf("history backlog admission: inconsistent heights head=%d eligible=%d published=%d", sample.HeadBlock, sample.EligibleBlock, sample.PublishedBlock)
	}
	return nil
}

func updateHistoryBacklogMetrics(s HistoryBacklogAdmissionStatus) {
	boolValue := func(v bool) int64 {
		if v {
			return 1
		}
		return 0
	}
	uintValue := func(v uint64) int64 {
		if v > math.MaxInt64 {
			return math.MaxInt64
		}
		return int64(v)
	}
	for key, value := range map[string]int64{
		"enabled": boolValue(s.Enabled), "holding": boolValue(s.Holding),
		"lag": uintValue(s.LagBlocks), "high": uintValue(s.HighBlocks), "low": uintValue(s.LowBlocks),
		"head_gap": uintValue(s.HeadGapBlocks), "probe_errors": uintValue(s.ProbeErrors), "checks": uintValue(s.Checks),
	} {
		historyBacklogGauges[key].Update(value)
	}
}
