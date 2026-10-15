package freezer

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/maintenance"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
)

const directV2LeaseQuantum = 10 * time.Second
const directV2HistoryFresh = 10 * time.Second
const directV2HistoryOpportunity = 3 * time.Second

// Recovery diagnostics reuse the fixed txindex health reasons 0..15, plus 16.
const directV2HealthRepeatedEvidence transactionIndexHealthReason = 16

// The Runner's workMu owns this object for the entire direct job. A rotating
// release callback is never captured by defer: Close always releases the latest
// lease, including after a canceled fused build switches to an unfused retry.
type directV2Lease struct {
	r          *Runner
	release    func()
	since      time.Time
	jobStart   time.Time
	phaseName  string
	phaseSince time.Time
	pressure   transactionIndexPressureState // independent from txIndexBudget
	recovery   *directV2Recovery
}

func (l *directV2Lease) metric(name string, value int64) {
	metrics.GetOrRegisterGauge(normalizeMetricNamespace(l.r.cfg.MetricsNamespace)+"v2/cooperative/"+name, nil).Update(value)
}

// Each acquisition owns a fresh release decision. The gate invokes this pure
// policy with its actual hold time while returning that same token.
type directV2Recovery struct {
	begin                      transactionIndexHealthReason
	engineSample, deviceSample time.Time
	cooperative                bool
}

func (r *directV2Recovery) cooldown(held, defaultCooldown time.Duration) time.Duration {
	if r.cooperative {
		return min(defaultCooldown, max(time.Second, held/4))
	}
	return defaultCooldown
}
func (l *directV2Lease) probePressure() (transactionIndexHealthReason, maintenance.StoragePressure) {
	p := maintenance.StoragePressure{}
	if l.r.cfg.TransactionIndexLoadProbe != nil {
		p = l.r.cfg.TransactionIndexLoadProbe()
	}
	// Assess after the bounded probe returns, including evidence age then.
	return l.pressure.assess(p, time.Now()), p
}
func (l *directV2Lease) tryAcquire() (ok bool) {
	decision := &directV2Recovery{}
	release, ok := l.r.cfg.HeavyWorkGate.TryAcquireWithReleaseCooldown(decision.cooldown, maintenance.HeavyWorkDirectV2)
	if !ok {
		return false
	}
	l.release, l.recovery = release, decision
	l.since = time.Now()
	l.phaseSince = l.since
	// A failing probe cannot leak a token or install optimistic recovery.
	completed := false
	defer func() {
		if !completed {
			l.Close()
		}
	}()
	reason, sample := l.probePressure()
	decision.begin = reason
	decision.engineSample, decision.deviceSample = sample.SampledAt, sample.DeviceSampledAt
	l.metric("recovery/begin_reason", int64(decision.begin))
	l.metric("recovery/end_reason", int64(txIndexHealthUnobserved))
	l.metric("recovery/requested", 0)
	l.metric("recovery/mode", 0)
	l.metric("held", 1)
	completed = true
	return true
}
func (l *directV2Lease) releaseCurrent() {
	if l.recovery != nil && l.recovery.cooperative {
		l.addMetric("recovery/bounded_policy_count", 1)
	} else {
		l.addMetric("recovery/default_policy_count", 1)
	}
	l.chargePhase()
	l.release()
	l.release, l.recovery = nil, nil
	l.metric("held", 0)
}
func (l *directV2Lease) Close() {
	if l != nil && l.release != nil {
		if l.recovery != nil {
			l.recovery.cooperative = false
		}
		l.metric("recovery/requested", 0)
		l.metric("recovery/mode", 0)
		l.releaseCurrent()
	}
}
func (l *directV2Lease) releaseForYield(ctx context.Context) time.Duration {
	// Any exceptional probe completion releases with the conservative default.
	defer l.Close()
	l.metric("recovery/requested", 1)
	end := txIndexHealthWorkError
	if ctx.Err() == nil {
		var sample maintenance.StoragePressure
		end, sample = l.probePressure()
		if end == txIndexHealthHealthy && l.recovery != nil &&
			(!sample.SampledAt.After(l.recovery.engineSample) || !sample.DeviceSampledAt.After(l.recovery.deviceSample)) {
			end = directV2HealthRepeatedEvidence
		}
	}
	if ctx.Err() != nil {
		end = txIndexHealthWorkError
	}
	l.metric("recovery/end_reason", int64(end))
	if l.recovery != nil {
		l.recovery.cooperative = ctx.Err() == nil && l.recovery.begin == txIndexHealthHealthy && end == txIndexHealthHealthy
		if l.recovery.cooperative {
			l.metric("recovery/mode", 1)
		} else {
			l.metric("recovery/mode", 0)
		}
	}
	held := time.Since(l.since)
	l.releaseCurrent()
	return held
}
func (l *directV2Lease) ensure(ctx context.Context) error {
	if l == nil {
		return ctx.Err()
	}
	if l.release != nil {
		return ctx.Err()
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if l.r.cfg.V2PromotionAllowed == nil || l.r.cfg.V2PromotionAllowed() {
			if l.tryAcquire() {
				if err := ctx.Err(); err != nil {
					l.Close()
					return err
				}
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func (l *directV2Lease) hooks() *rawdbfreezer.V2CooperativeYield {
	if l == nil || l.r.cfg.HeavyWorkGate == nil {
		return nil
	}
	return &rawdbfreezer.V2CooperativeYield{Due: func() bool {
		return l.release != nil && time.Since(l.since) >= directV2LeaseQuantum && l.r.cfg.HeavyWorkGate.HistoryContended(directV2HistoryFresh)
	}, Yield: l.yield, Phase: l.phase}
}
func (l *directV2Lease) yield(ctx context.Context) error {
	g := l.r.cfg.HeavyWorkGate
	before := g.Observation().HistoryAcquisitions
	held := l.releaseForYield(ctx)
	started := time.Now()
	l.metric("yield_count", metrics.GetOrRegisterGauge(normalizeMetricNamespace(l.r.cfg.MetricsNamespace)+"v2/cooperative/yield_count", nil).Snapshot().Value()+1)
	// Give history one observed acquisition, or a bounded opportunity after the
	// latest recovery deadline. A third owner may renew recovery while we wait;
	// reset the opportunity then instead of racing it at the old deadline.
	var opportunity time.Time
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if g.Observation().HistoryAcquisitions > before {
			l.metric("history_handoffs", metrics.GetOrRegisterGauge(normalizeMetricNamespace(l.r.cfg.MetricsNamespace)+"v2/cooperative/history_handoffs", nil).Snapshot().Value()+1)
			break
		}
		if !g.CanTryAcquire() {
			opportunity = time.Time{}
		} else if opportunity.IsZero() {
			opportunity = time.Now().Add(directV2HistoryOpportunity)
		} else if !time.Now().Before(opportunity) {
			l.metric("history_timeouts", metrics.GetOrRegisterGauge(normalizeMetricNamespace(l.r.cfg.MetricsNamespace)+"v2/cooperative/history_timeouts", nil).Snapshot().Value()+1)
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	if err := l.ensure(ctx); err != nil {
		return err
	}
	waited := time.Since(started)
	l.metric("last_hold_ns", int64(held))
	l.metric("last_wait_ns", int64(waited))
	o := g.Observation()
	l.metric("gate_owner", int64(o.Owner))
	l.metric("gate_hold_total_ns", int64(o.TotalHold))
	l.metric("gate_hold_max_ns", int64(o.MaxHold))
	l.metric("gate_reject_held", int64(o.RejectedHeld))
	l.metric("gate_reject_cooldown", int64(o.RejectedCooldown))
	l.metric("gate_reject_admission", int64(o.RejectedAdmission))
	log.Info("Freezer: direct V2 shared lease resumed", "held", held, "waited", waited, "historyAcquired", o.HistoryAcquisitions > before)
	return nil
}

func (l *directV2Lease) addMetric(name string, value int64) {
	g := metrics.GetOrRegisterGauge(normalizeMetricNamespace(l.r.cfg.MetricsNamespace)+"v2/cooperative/"+name, nil)
	g.Update(g.Snapshot().Value() + value)
}
func (l *directV2Lease) chargePhase() {
	if l.release != nil && !l.phaseSince.IsZero() && l.phaseName != "" {
		l.addMetric("phase_hold_ns/"+l.phaseName, int64(time.Since(l.phaseSince)))
		l.phaseSince = time.Now()
	}
}
func (l *directV2Lease) phase(name string) {
	if l == nil {
		return
	}
	l.chargePhase()
	var id int64
	switch name {
	case "writing":
		id = 1
	case "verification":
		id = 2
	case "index":
		id = 3
	case "commit":
		id = 4
	case "prune":
		id = 5
	case "compact":
		id = 6
	default:
		return
	}
	l.phaseName = name
	l.phaseSince = time.Now()
	l.metric("phase", id)
}
func (l *directV2Lease) finish(err error) {
	if l == nil {
		return
	}
	l.Close()
	l.metric("job_active", 0)
	l.metric("job_finish_unix_ns", time.Now().UnixNano())
	l.metric("last_job_duration_ns", int64(time.Since(l.jobStart)))
	log.Info("Freezer: direct V2 shared job finished", "duration", time.Since(l.jobStart), "phase", l.phaseName, "err", err)
}
