// Package maintenance coordinates optional background work which competes
// with canonical block import for CPU and storage bandwidth.
package maintenance

import (
	"sync"
	"sync/atomic"
	"time"
)

// HeavyWorkGate admits at most one optional heavy task at a time. Admission is
// deliberately non-blocking: a lifecycle which loses the race records a
// deferral and retries on its normal cadence instead of adding an invisible
// queue behind another long-running maintenance task.
type HeavyWorkGate struct {
	token         chan struct{}
	cooldown      time.Duration
	cooldownAfter time.Duration
	nextAllowed   atomic.Int64
	now           func() time.Time
}

// NewHeavyWorkGate constructs an idle process-wide maintenance gate.
func NewHeavyWorkGate() *HeavyWorkGate {
	return NewHeavyWorkGateWithCooldown(0)
}

// NewHeavyWorkGateWithCooldown constructs a gate which leaves an importer-only
// recovery window after every admitted task. The cooldown starts when work
// releases the gate, preventing individually bounded snapshot/freezer jobs from
// becoming one long back-to-back maintenance burst.
func NewHeavyWorkGateWithCooldown(cooldown time.Duration) *HeavyWorkGate {
	return NewHeavyWorkGateWithCooldownAfter(cooldown, 0)
}

// NewHeavyWorkGateWithCooldownAfter applies the recovery window only when an
// admitted lease lasts at least cooldownAfter. Production uses this to keep
// cheap readiness/no-op checks from continuously renewing the cooldown while
// still spacing out jobs which materially consume CPU or storage bandwidth.
func NewHeavyWorkGateWithCooldownAfter(cooldown, cooldownAfter time.Duration) *HeavyWorkGate {
	if cooldown < 0 {
		cooldown = 0
	}
	if cooldownAfter < 0 {
		cooldownAfter = 0
	}
	return &HeavyWorkGate{
		token:         make(chan struct{}, 1),
		cooldown:      cooldown,
		cooldownAfter: cooldownAfter,
		now:           time.Now,
	}
}

// TryAcquire returns an idempotent release callback when the gate is idle.
// A nil gate is treated as unlimited so package-level tests and deployments
// which do not wire the coordinator preserve their previous behavior.
func (g *HeavyWorkGate) TryAcquire() (release func(), ok bool) {
	if g == nil {
		return func() {}, true
	}
	now := g.currentTime()
	if g.coolingDown(now) {
		return nil, false
	}
	select {
	case g.token <- struct{}{}:
		// Recheck after owning the token. A release may have installed a
		// cooldown between the optimistic timestamp check and admission.
		if g.coolingDown(g.currentTime()) {
			<-g.token
			return nil, false
		}
		acquiredAt := g.currentTime()
		var once sync.Once
		return func() {
			once.Do(func() {
				now := g.currentTime()
				if g.cooldown > 0 && now.Sub(acquiredAt) >= g.cooldownAfter {
					g.nextAllowed.Store(now.Add(g.cooldown).UnixNano())
				}
				<-g.token
			})
		}, true
	default:
		return nil, false
	}
}

func (g *HeavyWorkGate) currentTime() time.Time {
	if g != nil && g.now != nil {
		return g.now()
	}
	return time.Now()
}

func (g *HeavyWorkGate) coolingDown(now time.Time) bool {
	if g == nil || g.cooldown <= 0 {
		return false
	}
	return now.UnixNano() < g.nextAllowed.Load()
}
